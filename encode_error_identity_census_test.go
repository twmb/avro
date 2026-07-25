package avro_test

import (
	"encoding/json"
	"errors"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// Error-identity contract (doc.go "# Errors"): encode-side USER-VALUE
// failures are errors.As-able to *SemanticError on BOTH wire formats;
// decode-side WIRE-CONTENT failures are plain errors on both. Three
// encode-side families deliberately agree as PLAIN on both wires instead:
// a TYPED nil pointer (both wires surface the shared indirection
// sentinel), a CustomType.Encode callback error (the user's own error
// value is returned verbatim), and an invalid UUID string against
// fixed(16)+uuid (parseUUID's error is bare on both).
//
// Identity must be asserted at TOP LEVEL: at record positions
// recordFieldError wraps EVERY field error in a *SemanticError, so a
// record-position probe cannot distinguish a wrapped arm from an
// unwrapped one. The record-position subtest below documents exactly
// that masking.

// encodeIdentityBothWires runs v through Encode and EncodeJSON and
// asserts both reject it with the same *SemanticError verdict.
func encodeIdentityBothWires(t *testing.T, schema string, v any, wantSemantic bool) {
	t.Helper()
	s := mustParse(t, schema)
	_, errB := s.Encode(v)
	_, errJ := s.EncodeJSON(v)
	if errB == nil || errJ == nil {
		t.Fatalf("want both wires to reject; binary=%v json=%v", errB, errJ)
	}
	var seB, seJ *avro.SemanticError
	asB := errors.As(errB, &seB)
	asJ := errors.As(errJ, &seJ)
	if asB != asJ {
		t.Errorf("SemanticError identity diverges across wires: binary=%v json=%v (binary err %q, json err %q)",
			asB, asJ, errB, errJ)
	}
	if asB != wantSemantic {
		t.Errorf("binary SemanticError=%v, want %v (err %q)", asB, wantSemantic, errB)
	}
	if asJ != wantSemantic {
		t.Errorf("json SemanticError=%v, want %v (err %q)", asJ, wantSemantic, errJ)
	}
}

// TestRegression_UntypedNilEncodeSemanticErrorBothWires pins that an
// UNTYPED nil at top level against a non-nullable schema is an
// encode-side user-value failure carrying *SemanticError identity on
// both wire formats — Encode wraps it at the entry guard (and via the
// union serializer), and EncodeJSON must match. A TYPED nil pointer is
// a different family: both wires surface the plain indirection
// sentinel, and nil against a null schema or a union with a null
// branch succeeds on both wires.
func TestRegression_UntypedNilEncodeSemanticErrorBothWires(t *testing.T) {
	t.Run("non-nullable primitive", func(t *testing.T) {
		encodeIdentityBothWires(t, `"string"`, nil, true)
	})
	t.Run("union without a null branch", func(t *testing.T) {
		encodeIdentityBothWires(t, `["int","string"]`, nil, true)
	})
	t.Run("control: typed nil pointer is plain on both wires", func(t *testing.T) {
		encodeIdentityBothWires(t, `"string"`, (*string)(nil), false)
	})
	t.Run("control: nil against null schema succeeds on both wires", func(t *testing.T) {
		s := mustParse(t, `"null"`)
		if _, err := s.Encode(nil); err != nil {
			t.Errorf("binary: %v", err)
		}
		out, err := s.EncodeJSON(nil)
		if err != nil || string(out) != "null" {
			t.Errorf("json: out %q err %v, want null", out, err)
		}
	})
	t.Run("control: nil against nullable union succeeds on both wires", func(t *testing.T) {
		s := mustParse(t, `["null","int"]`)
		if _, err := s.Encode(nil); err != nil {
			t.Errorf("binary: %v", err)
		}
		out, err := s.EncodeJSON(nil)
		if err != nil || string(out) != "null" {
			t.Errorf("json: out %q err %v, want null", out, err)
		}
	})
}

// TestMatrix_EncodeErrorIdentityCensus drives one triggering input per
// error-return family at TOP LEVEL through both encoders and asserts
// the family's identity contract. The census exists so a new or edited
// error return in either encoder that drops (or spuriously adds)
// *SemanticError identity for a whole family fails here rather than
// surfacing as a per-wire errors.As difference in user code.
func TestMatrix_EncodeErrorIdentityCensus(t *testing.T) {
	semantic := []struct {
		name   string
		schema string
		v      any
	}{
		{"untyped nil, non-nullable", `"string"`, nil},
		{"untyped nil, no-null union", `["int","string"]`, nil},
		{"type mismatch, string", `"string"`, 42},
		{"type mismatch, int", `"int"`, "hello"},
		{"type mismatch, bytes", `"bytes"`, 42},
		{"type mismatch, boolean", `"boolean"`, "x"},
		{"json.Number content, fractional into int", `"int"`, json.Number("1.5")},
		{"enum unknown symbol", `{"type":"enum","name":"E","symbols":["A","B"]}`, "C"},
		{"enum ordinal out of range", `{"type":"enum","name":"E","symbols":["A","B"]}`, 99},
		{"fixed size mismatch", `{"type":"fixed","name":"F","size":4}`, []byte{1, 2, 3}},
		{"missing defaultless field", `{"type":"record","name":"R","fields":[{"name":"f","type":"int"}]}`, map[string]any{}},
		{"union no branch matched", `["int","string"]`, struct{ X int }{1}},
		// The no-match wrap must be UNCONDITIONAL (as serUnion's is), not
		// inherited from the last branch error's chain: a typed nil's
		// per-branch failure is the PLAIN indirection sentinel, so this
		// row fails if the union dispatcher forwards it bare.
		{"union no branch matched, typed nil", `["int","string"]`, (*string)(nil)},
		{"decimal precision exceeded", `{"type":"bytes","logicalType":"decimal","precision":2,"scale":0}`, big.NewRat(12345, 1)},
		{"decimal non-numeric string", `{"type":"bytes","logicalType":"decimal","precision":4,"scale":0}`, "12x"},
		{"map key not a JSON number", `{"type":"map","values":"int"}`, map[json.Number]int32{json.Number("abc"): 1}},
		{"timestamp-millis out of range", `{"type":"long","logicalType":"timestamp-millis"}`, time.Date(300000000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"date out of range", `{"type":"int","logicalType":"date"}`, time.Date(6000000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"time-millis duration out of range", `{"type":"int","logicalType":"time-millis"}`, 700 * time.Hour},
	}
	for _, row := range semantic {
		t.Run("semantic/"+row.name, func(t *testing.T) {
			encodeIdentityBothWires(t, row.schema, row.v, true)
		})
	}

	plain := []struct {
		name   string
		schema string
		v      any
	}{
		{"typed nil pointer", `"string"`, (*string)(nil)},
		{"invalid UUID string into fixed-uuid", `{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`, "zz"},
	}
	for _, row := range plain {
		t.Run("plain/"+row.name, func(t *testing.T) {
			encodeIdentityBothWires(t, row.schema, row.v, false)
		})
	}

	t.Run("plain/CustomType.Encode error returned verbatim", func(t *testing.T) {
		type myStr string
		boom := errors.New("boom")
		ct := avro.CustomType{
			AvroType: "string",
			GoType:   reflect.TypeOf(myStr("")),
			Encode:   func(v any, _ *avro.SchemaNode) (any, error) { return nil, boom },
		}
		s, err := avro.Parse(`"string"`, avro.WithCustomType(ct))
		if err != nil {
			t.Fatal(err)
		}
		_, errB := s.Encode(myStr("x"))
		_, errJ := s.EncodeJSON(myStr("x"))
		if !errors.Is(errB, boom) || !errors.Is(errJ, boom) {
			t.Fatalf("want the callback's own error on both wires; binary=%v json=%v", errB, errJ)
		}
		var se *avro.SemanticError
		if errors.As(errB, &se) || errors.As(errJ, &se) {
			t.Errorf("callback errors are returned verbatim, not wrapped: binary As=%v json As=%v",
				errors.As(errB, &se), errors.As(errJ, &se))
		}
	})

	// Decode-side WIRE-CONTENT failures stay plain on both wire formats:
	// the wire named a symbol/index that the schema does not have.
	t.Run("decode/binary enum ordinal out of range is plain", func(t *testing.T) {
		s := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B"]}`)
		var out any
		_, err := s.Decode([]byte{0xC6, 0x01}, &out) // zigzag varint 99
		var se *avro.SemanticError
		if err == nil || errors.As(err, &se) {
			t.Errorf("want plain wire-content error, got %v (SemanticError=%v)", err, errors.As(err, &se))
		}
	})
	t.Run("decode/json enum unknown symbol is plain", func(t *testing.T) {
		s := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B"]}`)
		var out any
		err := s.DecodeJSON([]byte(`"C"`), &out)
		var se *avro.SemanticError
		if err == nil || errors.As(err, &se) {
			t.Errorf("want plain wire-content error, got %v (SemanticError=%v)", err, errors.As(err, &se))
		}
	})
	t.Run("decode/binary union index out of range is plain", func(t *testing.T) {
		s := mustParse(t, `["int","string"]`)
		var out any
		_, err := s.Decode([]byte{0xC6, 0x01}, &out) // union index 99
		var se *avro.SemanticError
		if err == nil || errors.As(err, &se) {
			t.Errorf("want plain wire-content error, got %v (SemanticError=%v)", err, errors.As(err, &se))
		}
	})

	// Record positions mask family identity: recordFieldError wraps every
	// field error into a *SemanticError with the field path, so even the
	// families that are plain at top level carry SemanticError identity
	// here. This is why every row above asserts at top level.
	t.Run("record position wraps every family", func(t *testing.T) {
		s := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"f","type":"string"}]}`)
		in := map[string]any{"f": nil} // untyped nil field value
		_, errB := s.Encode(in)
		_, errJ := s.EncodeJSON(in)
		var seB, seJ *avro.SemanticError
		if !errors.As(errB, &seB) || seB.Field != "f" {
			t.Errorf("binary: want SemanticError with Field=f, got %v", errB)
		}
		if !errors.As(errJ, &seJ) || seJ.Field != "f" {
			t.Errorf("json: want SemanticError with Field=f, got %v", errJ)
		}
	})
}
