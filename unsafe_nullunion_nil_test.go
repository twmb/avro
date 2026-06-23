package avro_test

import (
	"bytes"
	"testing"

	"github.com/twmb/avro"
)

// The 2-branch ["null",T] / [T,"null"] optimization treats a value as the null
// branch exactly when avro's isNilValue reports it nil — which peels
// pointer/interface layers and then nil-checks the bottom kind, so a non-nil
// pointer to a nil slice/map (&(nilSlice)) is null. The reflect-binary and JSON
// encoders both honor isNilValue. The unsafe struct fast path must agree: the
// SAME value must encode to the SAME union branch whether the struct is passed
// addressable (&v, which reaches the unsafe path) or by value (v, reflect), and
// JSON must match too. A divergence is addressability-dependent wire corruption.
//
// These pin the slice-backed inner shapes (array, bytes) of the null-union field
// optimization (usNullUnionPtr) and its array-element sibling (usArrayNullUnionPtr),
// plus map/non-nil controls.

// nullUnionParity encodes v addressable (unsafe) and by value (reflect) and as
// JSON, and asserts all three pick the same union branch: the two binary wires
// are byte-identical, the JSON wires are byte-identical, and the binary wire
// decodes to wantNull (true => the field comes back nil/absent).
func nullUnionParity(t *testing.T, schema string, v any, vptr any, wantNull bool) {
	t.Helper()
	s := avro.MustParse(schema)

	wPtr, err := s.AppendEncode(nil, vptr) // addressable struct -> unsafe fast path
	if err != nil {
		t.Fatalf("Encode(&v): %v", err)
	}
	wVal, err := s.AppendEncode(nil, v) // non-addressable -> reflect path
	if err != nil {
		t.Fatalf("Encode(v): %v", err)
	}
	if !bytes.Equal(wPtr, wVal) {
		t.Errorf("binary branch divergence (addressable vs value): Encode(&v)=% x  Encode(v)=% x", wPtr, wVal)
	}

	jPtr, err := s.AppendEncodeJSON(nil, vptr)
	if err != nil {
		t.Fatalf("EncodeJSON(&v): %v", err)
	}
	jVal, err := s.AppendEncodeJSON(nil, v)
	if err != nil {
		t.Fatalf("EncodeJSON(v): %v", err)
	}
	if !bytes.Equal(jPtr, jVal) {
		t.Errorf("JSON branch divergence (addressable vs value): EncodeJSON(&v)=%s  EncodeJSON(v)=%s", jPtr, jVal)
	}

	// The unsafe binary wire must agree with JSON on the branch too. Decode the
	// unsafe binary wire into a map and confirm the field is/isn't null.
	var got map[string]any
	if _, err := s.Decode(wPtr, &got); err != nil {
		t.Fatalf("Decode(Encode(&v)): %v", err)
	}
	isNull := got["f"] == nil
	if isNull != wantNull {
		t.Errorf("decode(Encode(&v)).f null=%v, want null=%v (value=%#v; wire=% x)", isNull, wantNull, got["f"], wPtr)
	}
}

func TestRegression_NullUnionPtrToNilSliceEncodeParity(t *testing.T) {
	t.Run("ptr-to-nil-slice/array-null-first", func(t *testing.T) {
		type Rec struct {
			F *[]string `avro:"f"`
		}
		var nilSlice []string
		nullUnionParity(t,
			`{"type":"record","name":"R","fields":[{"name":"f","type":["null",{"type":"array","items":"string"}]}]}`,
			Rec{F: &nilSlice}, &Rec{F: &nilSlice}, true)
	})

	t.Run("ptr-to-nil-slice/array-null-second", func(t *testing.T) {
		type Rec struct {
			F *[]string `avro:"f"`
		}
		var nilSlice []string
		nullUnionParity(t,
			`{"type":"record","name":"R","fields":[{"name":"f","type":[{"type":"array","items":"string"},"null"]}]}`,
			Rec{F: &nilSlice}, &Rec{F: &nilSlice}, true)
	})

	t.Run("ptr-to-nil-bytes", func(t *testing.T) {
		type Rec struct {
			F *[]byte `avro:"f"`
		}
		var nilBytes []byte
		nullUnionParity(t,
			`{"type":"record","name":"R","fields":[{"name":"f","type":["null","bytes"]}]}`,
			Rec{F: &nilBytes}, &Rec{F: &nilBytes}, true)
	})

	t.Run("array-element-ptr-to-nil-slice", func(t *testing.T) {
		type Rec struct {
			A []*[]string `avro:"a"`
		}
		var nilSlice []string
		// One element: a non-nil pointer to a nil slice -> that element is the
		// null branch; reflect/JSON agree, unsafe must too.
		s := `{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":["null",{"type":"array","items":"string"}]}}]}`
		sc := avro.MustParse(s)
		wPtr, err := sc.AppendEncode(nil, &Rec{A: []*[]string{&nilSlice}})
		if err != nil {
			t.Fatalf("Encode(&v): %v", err)
		}
		wVal, err := sc.AppendEncode(nil, Rec{A: []*[]string{&nilSlice}})
		if err != nil {
			t.Fatalf("Encode(v): %v", err)
		}
		if !bytes.Equal(wPtr, wVal) {
			t.Errorf("array-element branch divergence: Encode(&v)=% x  Encode(v)=% x", wPtr, wVal)
		}
		jPtr, _ := sc.AppendEncodeJSON(nil, &Rec{A: []*[]string{&nilSlice}})
		jVal, _ := sc.AppendEncodeJSON(nil, Rec{A: []*[]string{&nilSlice}})
		if !bytes.Equal(jPtr, jVal) {
			t.Errorf("array-element JSON divergence: %s vs %s", jPtr, jVal)
		}
	})
}

func TestRegression_NullUnionPtrToNilMapEncodeParity(t *testing.T) {
	type Rec struct {
		F *map[string]string `avro:"f"`
	}
	var nilMap map[string]string
	nullUnionParity(t,
		`{"type":"record","name":"R","fields":[{"name":"f","type":["null",{"type":"map","values":"string"}]}]}`,
		Rec{F: &nilMap}, &Rec{F: &nilMap}, true)
}

func TestRegression_NullUnionPtrToNonNilSliceControl(t *testing.T) {
	// Control: a non-nil slice behind the pointer is the VALUE branch on every
	// path; the fix must not regress this (it currently agrees).
	type Rec struct {
		F *[]string `avro:"f"`
	}
	good := []string{"x"}
	nullUnionParity(t,
		`{"type":"record","name":"R","fields":[{"name":"f","type":["null",{"type":"array","items":"string"}]}]}`,
		Rec{F: &good}, &Rec{F: &good}, false)
}
