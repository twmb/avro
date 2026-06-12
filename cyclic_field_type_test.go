package avro_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// A cyclic non-struct pointer type (type P *P, whose reflect type graph has
// P.Elem() == P) used as a struct field must NOT crash the process. The unsafe
// struct-field encode compiler walks the field's pointer type graph; without a
// bound it recurses forever at compile time and overflows the goroutine stack
// (a fatal, unrecoverable crash). The bound declines to the reflect slow path,
// which errors cleanly on the nil cyclic value — matching every other indirect
// walk in the package.
type cyclicPtrFieldType *cyclicPtrFieldType

type recordWithCyclicPtrField struct {
	F cyclicPtrFieldType `avro:"F"`
}

func TestRegression_EncodeStructCyclicPointerFieldTerminates(t *testing.T) {
	s := avro.MustParse(`{"type":"record","name":"S","fields":[{"name":"F","type":"int"}]}`)
	// Run in a goroutine with a generous deadline so a regression (the bound
	// removed) surfaces as a timeout rather than hanging the whole suite —
	// though a true stack overflow is fatal and would abort the binary, which
	// is itself an unmistakable failure signal.
	done := make(chan error, 1)
	go func() {
		_, err := s.Encode(&recordWithCyclicPtrField{})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("encode of a cyclic-pointer-typed field must error, got nil")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("encode of a cyclic-pointer-typed field did not terminate")
	}
}

// The unsafe struct-field fast encode path must accept EXACTLY the pointer-chain
// depths the reflect encoder accepts, and every accepted chain must round-trip
// through Decode. Before the bound, the fast path accepted arbitrarily deep
// chains the reflect encoder (and the package's own Decode) rejected — encoding
// wire that could not be read back. This drives a struct field of int-pointer
// depth 1..8 and asserts: struct-field encode succeeds IFF the reflect scalar
// encode of the same value succeeds (fast ≡ reflect), and when it succeeds the
// wire decodes back to the original int.
func TestRegression_StructFieldPointerChainMatchesReflect(t *testing.T) {
	rec := avro.MustParse(`{"type":"record","name":"S","fields":[{"name":"F","type":"int"}]}`)
	scalar := avro.MustParse(`"int"`)
	intType := reflect.TypeOf(int(0))

	anyAccepted := false
	for depth := 1; depth <= 8; depth++ {
		ptrType := intType
		for i := 0; i < depth; i++ {
			ptrType = reflect.PointerTo(ptrType)
		}
		structType := reflect.StructOf([]reflect.StructField{
			{Name: "F", Type: ptrType, Tag: `avro:"F"`},
		})

		// Build a fully-allocated chain ending in int(7).
		leaf := reflect.New(intType)
		leaf.Elem().SetInt(7)
		cur := leaf
		for i := 1; i < depth; i++ {
			p := reflect.New(cur.Type())
			p.Elem().Set(cur)
			cur = p
		}
		sv := reflect.New(structType)
		sv.Elem().Field(0).Set(cur)

		_, structErr := rec.Encode(sv.Interface())
		_, scalarErr := scalar.Encode(cur.Interface())

		if (structErr == nil) != (scalarErr == nil) {
			t.Fatalf("depth=%d: struct-field encode (err=%v) and reflect scalar encode (err=%v) disagree — fast path accepts a depth the reflect path rejects",
				depth, structErr, scalarErr)
		}
		if structErr == nil {
			anyAccepted = true
			// Accepted: the wire must round-trip back to the int.
			wire, err := rec.Encode(sv.Interface())
			if err != nil {
				t.Fatalf("depth=%d: re-encode failed: %v", depth, err)
			}
			out := reflect.New(structType)
			if _, err := rec.Decode(wire, out.Interface()); err != nil {
				t.Fatalf("depth=%d: struct-field encode accepted but Decode rejected the wire: %v", depth, err)
			}
		}
	}
	if !anyAccepted {
		t.Fatal("expected at least the shallow pointer depths to be accepted")
	}
}

// The same bound must cover the array-element route into the pointer arm: a
// cyclic pointer as a SLICE element must not crash the field-type compile, and
// a shallow pointer element must still round-trip.
type cyclicSliceElem *cyclicSliceElem

func TestRegression_SliceElementPointerChainBounded(t *testing.T) {
	t.Run("cyclic_element_terminates", func(t *testing.T) {
		type R struct {
			F []cyclicSliceElem `avro:"f"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"array","items":"int"}}]}`)
		done := make(chan struct{}, 1)
		go func() {
			s.Encode(&R{}) // nil slice; the compile of the cyclic element type must terminate
			done <- struct{}{}
		}()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Fatal("slice-of-cyclic-pointer field compile did not terminate")
		}
	})
	t.Run("shallow_element_roundtrips", func(t *testing.T) {
		type R struct {
			F []*int32 `avro:"f"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"array","items":"int"}}]}`)
		v := int32(7)
		wire, err := s.Encode(&R{F: []*int32{&v}})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var out R
		if _, err := s.Decode(wire, &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(out.F) != 1 || out.F[0] == nil || *out.F[0] != 7 {
			t.Fatalf("roundtrip wrong: %#v", out)
		}
	})
}
