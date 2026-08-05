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

// Encode and decode must agree on the maximum pointer-chain depth they accept.
// The encode-side indirect formerly accepted only maxIndirectDepth-1 levels
// (it confirmed a non-pointer base on a FOLLOWING loop iteration the cap had
// already spent), while the decode-side indirectAlloc accepted maxIndirectDepth
// — so a value exactly maxIndirectDepth pointers deep DECODED but failed to
// ENCODE, a round-trip break for a hand-written schema independent of SchemaFor.
// Both now accept a chain bottoming at a non-pointer base within
// maxIndirectDepth levels and reject one deeper, symmetrically, on binary AND
// JSON.
func TestRegression_PointerChainEncodeDecodeDepthParity(t *testing.T) {
	s := avro.MustParse(`["null","int"]`)

	// At maxIndirectDepth (5) levels: a non-nil chain must round-trip on both
	// wires. Build *****int = &&&&&(7).
	n := int64(7)
	p1 := &n
	p2 := &p1
	p3 := &p2
	p4 := &p3
	p5 := &p4 // 5 pointers, non-nil all the way down

	bin, err := s.Encode(p5)
	if err != nil {
		t.Fatalf("binary Encode of a %d-deep non-nil pointer must succeed (encode↔decode parity): %v", maxDepthLevels, err)
	}
	var bg *****int64
	if _, err := s.Decode(bin, &bg); err != nil {
		t.Fatalf("binary Decode into a %d-deep pointer must succeed: %v", maxDepthLevels, err)
	}
	if bg == nil || *****bg != 7 {
		t.Fatalf("binary round-trip mismatch at depth %d", maxDepthLevels)
	}

	js, err := s.EncodeJSON(p5)
	if err != nil {
		t.Fatalf("JSON Encode of a %d-deep non-nil pointer must succeed: %v", maxDepthLevels, err)
	}
	var jg *****int64
	if err := s.DecodeJSON(js, &jg); err != nil {
		t.Fatalf("JSON Decode into a %d-deep pointer must succeed: %v", maxDepthLevels, err)
	}
	if jg == nil || *****jg != 7 {
		t.Fatalf("JSON round-trip mismatch at depth %d", maxDepthLevels)
	}

	// One level deeper (6) must be rejected SYMMETRICALLY across both wires AND
	// both directions: encode (indirect) and decode (indirectAlloc) refuse a
	// chain past the cap (the cyclic-interface DoS guard is preserved). JSON
	// matches binary on decode too — a union pointer target is indirected
	// exactly once (unionTarget returns a concrete target un-peeled so the
	// branch decode's single indirectAlloc caps it), not twice.
	q6 := &p5 // 6 pointers
	if _, err := s.Encode(q6); err == nil {
		t.Fatalf("binary Encode of a %d-deep pointer must be rejected", maxDepthLevels+1)
	}
	var b6 ******int64
	if _, err := s.Decode(bin, &b6); err == nil {
		t.Fatalf("binary Decode into a %d-deep pointer must be rejected", maxDepthLevels+1)
	}
	if _, err := s.EncodeJSON(q6); err == nil {
		t.Fatalf("JSON Encode of a %d-deep pointer must be rejected", maxDepthLevels+1)
	}
	var j6 ******int64
	if err := s.DecodeJSON(js, &j6); err == nil {
		t.Fatalf("JSON Decode into a %d-deep pointer must be rejected (parity with binary)", maxDepthLevels+1)
	}
}

// Binary and JSON decode must accept the SAME set of pointer-target depths.
// JSON decode of a UNION target formerly indirected at two stages (unionTarget
// then the branch's decodeKind), so it peeled up to 2*maxIndirectDepth levels
// and accepted targets binary rejected. Sweep every depth and assert the two
// wires agree.
func TestRegression_UnionPointerTargetDepthBinaryJSONParity(t *testing.T) {
	s := avro.MustParse(`["null","int"]`)
	bin, _ := s.Encode(int64(7))
	js, _ := s.EncodeJSON(int64(7))
	intType := reflect.TypeOf(int64(0))
	for depth := 1; depth <= 9; depth++ {
		pt := intType
		for i := 0; i < depth; i++ {
			pt = reflect.PointerTo(pt)
		}
		_, binErr := s.Decode(bin, reflect.New(pt).Interface())
		jsonErr := s.DecodeJSON(js, reflect.New(pt).Interface())
		if (binErr != nil) != (jsonErr != nil) {
			t.Errorf("depth=%d: binary and JSON decode disagree on accept/reject (binErr=%v, jsonErr=%v)", depth, binErr, jsonErr)
		}
	}
}

// maxDepthLevels mirrors the package-internal maxIndirectDepth (5) for the
// external test above; if the internal cap changes, the depth-parity test's
// literal pointer types (*****T at the cap, ******T past it) must change with it.
const maxDepthLevels = 5

// An array<record> element's pointer chain must be capped at the same
// maxIndirectDepth every other context enforces. The unsafe struct-field array
// fast path peels one pointer level inline (usArrayRecord → usArrayPtrRecord on
// encode, the case "record" arm → udArrayPtrRecord on decode) and then hands
// the element to the record (de)serializer's own indirect/indirectAlloc budget.
// Without declining a multi-level-pointer element it accepted a chain
// 1+maxIndirectDepth deep — one past the cap the top-level reflect path, the
// JSON encoder, and the nullunion array arms all enforce — so a []******Record
// struct field encoded a binary wire that the struct's own JSON encoder, a
// top-level encode of the same slice, and a top-level decode into the same type
// all reject. Every context must agree on the accept/reject boundary; an
// accepted chain must round-trip. The fast path stays single-pointer
// ([]*Record); deeper-but-in-cap chains route to the reflect path.
func TestRegression_ArrayRecordElementPointerChainDepthParity(t *testing.T) {
	const recJSON = `{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}`
	sArr := avro.MustParse(`{"type":"array","items":` + recJSON + `}`)
	sOuter := avro.MustParse(`{"type":"record","name":"Outer","fields":[{"name":"arr","type":{"type":"array","items":` + recJSON + `}}]}`)

	innerStruct := reflect.StructOf([]reflect.StructField{
		{Name: "X", Type: reflect.TypeOf(int32(0)), Tag: `avro:"x"`},
	})
	// chain returns the slice element type at the given pointer depth and a
	// non-nil value of that type bottoming at Inner{X:7}.
	chain := func(depth int) (reflect.Type, reflect.Value) {
		elemType := innerStruct
		for range depth {
			elemType = reflect.PointerTo(elemType)
		}
		v := reflect.New(innerStruct).Elem()
		v.Field(0).SetInt(7)
		for range depth {
			p := reflect.New(v.Type())
			p.Elem().Set(v)
			v = p
		}
		return elemType, v
	}

	for depth := 0; depth <= maxDepthLevels+1; depth++ {
		elemType, elemVal := chain(depth)
		sliceType := reflect.SliceOf(elemType)

		// Oracle: the same []*…*Inner value at top level goes through the reflect
		// serArray path, which peels with a single indirect budget (caps at the
		// cap). Its accept/reject is the boundary every other context must match.
		topSlice := reflect.MakeSlice(sliceType, 1, 1)
		topSlice.Index(0).Set(elemVal)
		_, topErr := sArr.AppendEncode(nil, topSlice.Interface())

		// Struct field exercises the unsafe usArrayRecord fast path (the value is
		// addressable through reflect.New, so the unsafe compile is selected).
		outerType := reflect.StructOf([]reflect.StructField{
			{Name: "Arr", Type: sliceType, Tag: `avro:"arr"`},
		})
		outer := reflect.New(outerType)
		sl := reflect.MakeSlice(sliceType, 1, 1)
		sl.Index(0).Set(elemVal)
		outer.Elem().Field(0).Set(sl)

		binWire, binErr := sOuter.AppendEncode(nil, outer.Interface())
		_, jsonErr := sOuter.AppendEncodeJSON(nil, outer.Interface())

		if (binErr == nil) != (topErr == nil) {
			t.Errorf("depth %d: struct-field unsafe encode and top-level reflect encode disagree (structErr=%v topErr=%v)", depth, binErr, topErr)
		}
		if (binErr == nil) != (jsonErr == nil) {
			t.Errorf("depth %d: binary and JSON struct-field encode disagree (binErr=%v jsonErr=%v)", depth, binErr, jsonErr)
		}

		if depth <= maxDepthLevels {
			// Within the cap: must encode and round-trip back to the value.
			if binErr != nil {
				t.Errorf("depth %d (<= cap %d) must encode: %v", depth, maxDepthLevels, binErr)
				continue
			}
			got := reflect.New(outerType)
			if _, err := sOuter.Decode(binWire, got.Interface()); err != nil {
				t.Errorf("depth %d (<= cap) must decode: %v", depth, err)
				continue
			}
			gv := got.Elem().Field(0).Index(0)
			for gv.Kind() == reflect.Pointer {
				if gv.IsNil() {
					t.Errorf("depth %d round-trip produced a nil element", depth)
					break
				}
				gv = gv.Elem()
			}
			if gv.Kind() == reflect.Struct && gv.Field(0).Int() != 7 {
				t.Errorf("depth %d round-trip value mismatch: got %v", depth, gv.Field(0).Int())
			}
		} else if binErr == nil {
			// depth == cap+1: every encode context must reject.
			t.Errorf("depth %d (> cap %d) struct-field binary encode must reject; wire=%x", depth, maxDepthLevels, binWire)
		}
	}

	// Decode-side neuter-proof: a valid wire (one record element) decoded into a
	// struct whose array element is one level past the cap must reject — the fast
	// udArrayPtrRecord must not peel 1+maxIndirectDepth levels.
	depth0Slice := reflect.SliceOf(innerStruct)
	d0 := reflect.New(reflect.StructOf([]reflect.StructField{
		{Name: "Arr", Type: depth0Slice, Tag: `avro:"arr"`},
	}))
	one := reflect.New(innerStruct).Elem()
	one.Field(0).SetInt(7)
	s0 := reflect.MakeSlice(depth0Slice, 1, 1)
	s0.Index(0).Set(one)
	d0.Elem().Field(0).Set(s0)
	validWire, err := sOuter.AppendEncode(nil, d0.Interface())
	if err != nil {
		t.Fatalf("setup: encode of []Inner must succeed: %v", err)
	}
	deepElem := innerStruct
	for range maxDepthLevels + 1 {
		deepElem = reflect.PointerTo(deepElem)
	}
	deepOuter := reflect.New(reflect.StructOf([]reflect.StructField{
		{Name: "Arr", Type: reflect.SliceOf(deepElem), Tag: `avro:"arr"`},
	}))
	if _, err := sOuter.Decode(validWire, deepOuter.Interface()); err == nil {
		t.Errorf("decode into an array field whose element is %d pointers deep must reject (cap %d)", maxDepthLevels+1, maxDepthLevels)
	}
}

// A ["null", record] nullunion record FIELD's pointer chain must be capped at
// maxIndirectDepth on decode, matching encode and every other context. The
// unsafe udNullUnionRecord consumes the nullunion's outer pointer (goType.Elem)
// and then indirectAllocs the remainder, so without declining a multi-level
// pointer target it accepted a chain 1+maxIndirectDepth deep — one past the cap
// the encode side (which already declines **…T to reflect), the reflect deser,
// and the JSON paths all enforce. A valid wire decoded into a ******record
// nullunion field then succeeded where encode of the same type rejected: an
// encode/decode boundary asymmetry. Decode must reject at the same depth encode
// does; the fast path stays single-pointer (*record), deeper-but-in-cap chains
// route to the reflect deser.
func TestRegression_NullUnionRecordFieldPointerChainDepthParity(t *testing.T) {
	const recJSON = `{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}`
	s := avro.MustParse(`{"type":"record","name":"Outer","fields":[{"name":"f","type":["null",` + recJSON + `]}]}`)

	innerStruct := reflect.StructOf([]reflect.StructField{
		{Name: "X", Type: reflect.TypeOf(int32(0)), Tag: `avro:"x"`},
	})
	ptrType := func(depth int) reflect.Type {
		ft := innerStruct
		for range depth {
			ft = reflect.PointerTo(ft)
		}
		return ft
	}
	nonNilVal := func(depth int) reflect.Value {
		v := reflect.New(innerStruct).Elem()
		v.Field(0).SetInt(7)
		for range depth {
			p := reflect.New(v.Type())
			p.Elem().Set(v)
			v = p
		}
		return v
	}
	outerOf := func(fieldType reflect.Type) reflect.Type {
		return reflect.StructOf([]reflect.StructField{{Name: "F", Type: fieldType, Tag: `avro:"f"`}})
	}

	// A valid wire: a depth-1 (*record) non-nil value encodes the value branch.
	d1Type := outerOf(ptrType(1))
	d1 := reflect.New(d1Type)
	d1.Elem().Field(0).Set(nonNilVal(1))
	validWire, err := s.AppendEncode(nil, d1.Interface())
	if err != nil {
		t.Fatalf("setup: encode of a *record nullunion value must succeed: %v", err)
	}

	for depth := 1; depth <= maxDepthLevels+1; depth++ {
		outerType := outerOf(ptrType(depth))
		outer := reflect.New(outerType)
		outer.Elem().Field(0).Set(nonNilVal(depth))

		binWire, binErr := s.AppendEncode(nil, outer.Interface())
		_, jsonErr := s.AppendEncodeJSON(nil, outer.Interface())
		// Decode the valid depth-1 wire into this depth's target type — the
		// decode-side boundary, independent of whether encode produced a wire.
		_, decErr := s.Decode(validWire, reflect.New(outerType).Interface())

		if (binErr == nil) != (jsonErr == nil) {
			t.Errorf("depth %d: binary vs JSON encode disagree (bin=%v json=%v)", depth, binErr, jsonErr)
		}
		if (binErr == nil) != (decErr == nil) {
			t.Errorf("depth %d: encode and decode disagree on the cap boundary (encErr=%v decErr=%v)", depth, binErr, decErr)
		}

		if depth <= maxDepthLevels {
			if binErr != nil {
				t.Errorf("depth %d (<= cap %d) must encode: %v", depth, maxDepthLevels, binErr)
				continue
			}
			got := reflect.New(outerType)
			if _, err := s.Decode(binWire, got.Interface()); err != nil {
				t.Errorf("depth %d (<= cap) must decode: %v", depth, err)
				continue
			}
			gv := got.Elem().Field(0)
			for gv.Kind() == reflect.Pointer {
				if gv.IsNil() {
					t.Errorf("depth %d round-trip produced a nil value", depth)
					break
				}
				gv = gv.Elem()
			}
			if gv.Kind() == reflect.Struct && gv.Field(0).Int() != 7 {
				t.Errorf("depth %d round-trip value mismatch: got %v", depth, gv.Field(0).Int())
			}
		} else if decErr == nil {
			// depth == cap+1: decode into the too-deep target must reject (the
			// decode-side neuter-proof), matching encode.
			t.Errorf("depth %d (> cap %d) decode into a too-deep nullunion field must reject", depth, maxDepthLevels)
		}
	}
}
