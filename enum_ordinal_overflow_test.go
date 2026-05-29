package avro

import (
	"strings"
	"testing"
)

// An integer-kind enum carrier is validated as an ordinal in [0, len(symbols))
// in the carrier's own width BEFORE narrowing to int. Narrowing first
// (int(v.Uint())) truncates a value ≥ 2^32 to its low bits on a 32-bit build,
// so an out-of-range ordinal like uint64(1<<32+5) would wrap to 5 and encode
// the wrong symbol there while erroring on 64-bit — a platform-dependent
// silent-wrong-output divergence. The wide comparison rejects it on every
// platform; this also pins that the error reports the TRUE value, not a
// truncated/sign-wrapped one (the observable proxy on a 64-bit host).
func TestRegression_EnumOrdinalOverflowRejected(t *testing.T) {
	const schema = `{"type":"enum","name":"e","symbols":["a","b","c"]}` // len 3

	// A uint64 ordinal whose low bits (mod 2^32) land inside [0,3): on a 32-bit
	// build int(v.Uint()) would truncate to 1 and wrongly accept. Must reject,
	// and the error must name the real value, not the truncated 1.
	reject := func(t *testing.T, enc func(*Schema) ([]byte, error), wantInMsg string) {
		t.Helper()
		s := MustParse(schema)
		_, err := enc(s)
		if err == nil {
			t.Fatal("expected out-of-range error, got nil (ordinal silently truncated and accepted)")
		}
		if !strings.Contains(err.Error(), wantInMsg) {
			t.Errorf("error %q does not mention the true ordinal %q", err, wantInMsg)
		}
	}

	t.Run("binary uint64 ordinal past 2^32", func(t *testing.T) {
		v := uint64(1<<32 + 1) // 4294967297; low 32 bits = 1, which is a valid index
		reject(t, func(s *Schema) ([]byte, error) { return s.AppendEncode(nil, &v) }, "4294967297")
	})
	t.Run("json uint64 ordinal past 2^32", func(t *testing.T) {
		v := uint64(1<<32 + 1)
		reject(t, func(s *Schema) ([]byte, error) { return s.AppendEncodeJSON(nil, &v) }, "4294967297")
	})
	t.Run("binary int64 ordinal past 2^32", func(t *testing.T) {
		v := int64(1<<32 + 2) // low 32 bits = 2, a valid index on 32-bit
		reject(t, func(s *Schema) ([]byte, error) { return s.AppendEncode(nil, &v) }, "4294967298")
	})

	// Boundaries that MUST still encode: valid ordinals across int/uint carriers.
	for _, tc := range []struct {
		name string
		v    any
	}{
		{"uint 0", ptrAny(uint64(0))},
		{"uint last", ptrAny(uint64(2))},
		{"int 0", ptrAny(int64(0))},
		{"int last", ptrAny(int64(2))},
	} {
		t.Run("accept "+tc.name, func(t *testing.T) {
			s := MustParse(schema)
			if _, err := s.AppendEncode(nil, tc.v); err != nil {
				t.Errorf("binary encode of valid ordinal %v: %v", tc.v, err)
			}
			if _, err := s.AppendEncodeJSON(nil, tc.v); err != nil {
				t.Errorf("json encode of valid ordinal %v: %v", tc.v, err)
			}
		})
	}

	// A negative int ordinal still rejects (unchanged behavior).
	t.Run("negative int rejects", func(t *testing.T) {
		v := int64(-1)
		s := MustParse(schema)
		if _, err := s.AppendEncode(nil, &v); err == nil {
			t.Error("expected error for negative ordinal")
		}
	})
}

func ptrAny[T any](v T) *T { return &v }
