package avro_test

import (
	"bytes"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// This file is the doc-contract net for doc.go's "# Struct tags" section: one
// oracle test per documented claim, so the documented behavior and the
// implementation cannot drift (the class that produced the omitzero and embed
// bugs). The oracle for the encode/decode claims is map[string]any — the
// separately-tested, documented record encoding — plus a reflect/unsafe parity
// check (&R), since the unsafe path has its own field handling.
//
// Coverage map of the section:
//   - avro:"name" / empty-name      -> TestTagContract_FieldNameMapping
//   - avro:"-" (exclude)            -> TestTagContract_ExcludeField
//   - avro:",inline"                -> TestTagContract_Inline
//   - avro:",omitzero"              -> TestRegression_OmitzeroFillsSchemaDefault
//   - embedded inlining/precedence  -> embed_selection_test.go
//   - IsZero()                      -> ser_test.go / deser_test.go
//   - SchemaFor inference options   -> TestTagContract_SchemaForOptions

// encodeBoth returns the reflect-path wire (R value) and asserts the unsafe
// path (&R, addressable) produces the identical wire — catching a path that
// handles a tag differently (as the unsafe omitzero emit did).
func encodeBoth[T any](t *testing.T, s *avro.Schema, v T) []byte {
	t.Helper()
	valWire, err := s.AppendEncode(nil, v)
	if err != nil {
		t.Fatalf("encode value: %v", err)
	}
	ptrWire, err := s.AppendEncode(nil, &v)
	if err != nil {
		t.Fatalf("encode &value (unsafe path): %v", err)
	}
	if !bytes.Equal(valWire, ptrWire) {
		t.Fatalf("reflect vs unsafe path diverge: value=%x ptr=%x", valWire, ptrWire)
	}
	return valWire
}

// TestTagContract_FieldNameMapping: avro:"name" maps the Go field to that Avro
// field name; an empty tag uses the Go field name. Oracle: a map[string]any
// keyed by the Avro names encodes to the same wire, and decode round-trips.
func TestTagContract_FieldNameMapping(t *testing.T) {
	type R struct {
		Renamed int32 `avro:"actualName"` // explicit name
		Plain   int32 // no tag -> Go field name "Plain"
	}
	s := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"actualName","type":"int"},{"name":"Plain","type":"int"}]}`)

	structWire := encodeBoth(t, s, R{Renamed: 7, Plain: 9})
	mapWire, err := s.AppendEncode(nil, map[string]any{"actualName": int32(7), "Plain": int32(9)})
	if err != nil {
		t.Fatalf("map encode: %v", err)
	}
	if !bytes.Equal(structWire, mapWire) {
		t.Errorf("name mapping != map oracle: struct=%x map=%x", structWire, mapWire)
	}

	var got R
	if _, err := s.Decode(structWire, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Renamed != 7 || got.Plain != 9 {
		t.Errorf("decode round-trip: got %+v, want {7 9}", got)
	}
}

// TestTagContract_ExcludeField: avro:"-" excludes the field from the record on
// encode, decode, and SchemaFor. Oracle: a map with only the kept field.
func TestTagContract_ExcludeField(t *testing.T) {
	type R struct {
		Secret int32 `avro:"-"`    // excluded
		Kept   int32 `avro:"kept"` // present
	}
	s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"kept","type":"int"}]}`)

	structWire := encodeBoth(t, s, R{Secret: 99, Kept: 7})
	mapWire, err := s.AppendEncode(nil, map[string]any{"kept": int32(7)})
	if err != nil {
		t.Fatalf("map encode: %v", err)
	}
	if !bytes.Equal(structWire, mapWire) {
		t.Errorf("excluded field leaked into the wire: struct=%x map=%x", structWire, mapWire)
	}

	var got R
	if _, err := s.Decode(structWire, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Kept != 7 {
		t.Errorf("decode kept: got %d want 7", got.Kept)
	}

	// SchemaFor must omit the excluded field.
	inferred, err := avro.SchemaFor[R]()
	if err != nil {
		t.Fatalf("SchemaFor: %v", err)
	}
	if got := inferred.String(); bytes.Contains([]byte(got), []byte("Secret")) {
		t.Errorf("SchemaFor included the excluded field: %s", got)
	}
}

// TestTagContract_Inline: avro:",inline" on a NAMED nested-struct field
// flattens its fields into the parent record (like anonymous embedding).
// Oracle: a map with the flattened keys, and SchemaFor produces a flat record.
func TestTagContract_Inline(t *testing.T) {
	type Inner struct {
		A int32 `avro:"a"`
		B int32 `avro:"b"`
	}
	type Outer struct {
		I Inner `avro:",inline"`
		C int32 `avro:"c"`
	}
	s := avro.MustParse(`{"type":"record","name":"Outer","fields":[
		{"name":"a","type":"int"},{"name":"b","type":"int"},{"name":"c","type":"int"}]}`)

	structWire := encodeBoth(t, s, Outer{I: Inner{A: 1, B: 2}, C: 3})
	mapWire, err := s.AppendEncode(nil, map[string]any{"a": int32(1), "b": int32(2), "c": int32(3)})
	if err != nil {
		t.Fatalf("map encode: %v", err)
	}
	if !bytes.Equal(structWire, mapWire) {
		t.Errorf("inline != flattened map oracle: struct=%x map=%x", structWire, mapWire)
	}

	var got Outer
	if _, err := s.Decode(structWire, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.I.A != 1 || got.I.B != 2 || got.C != 3 {
		t.Errorf("inline decode round-trip: got %+v, want {{1 2} 3}", got)
	}

	// SchemaFor must produce a flat record (a, b, c), not a nested one.
	inferred, err := avro.SchemaFor[Outer]()
	if err != nil {
		t.Fatalf("SchemaFor: %v", err)
	}
	if bytes.Contains([]byte(inferred.String()), []byte(`"type":"record","name":"Inner"`)) {
		t.Errorf("inline did not flatten in SchemaFor: %s", inferred.String())
	}
}

// TestTagContract_SchemaForOptions pins each documented SchemaFor inference
// option: the tag produces the expected attribute in the inferred schema.
func TestTagContract_SchemaForOptions(t *testing.T) {
	has := func(t *testing.T, s *avro.Schema, want string) {
		t.Helper()
		if got := s.String(); !strings.Contains(got, want) {
			t.Errorf("inferred schema missing %q:\n%s", want, got)
		}
	}
	t.Run("default", func(t *testing.T) {
		type R struct {
			X int32 `avro:"X,default=5"`
		}
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		has(t, s, `"default":5`)
	})
	t.Run("alias", func(t *testing.T) {
		type R struct {
			X int32 `avro:"X,alias=oldX"`
		}
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		has(t, s, `"aliases":["oldX"]`)
	})
	t.Run("type-alias", func(t *testing.T) {
		type Named struct {
			V int32 `avro:"v"`
		}
		type R struct {
			N Named `avro:"n,type-alias=oldName"`
		}
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		has(t, s, `"aliases":["oldName"]`)
	})
	t.Run("logical-override", func(t *testing.T) {
		type R struct {
			T time.Time `avro:"t,timestamp-micros"`
		}
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		has(t, s, `"logicalType":"timestamp-micros"`)
	})
	t.Run("decimal", func(t *testing.T) {
		type R struct {
			D *big.Rat `avro:"d,decimal(10,2)"`
		}
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		has(t, s, `"logicalType":"decimal"`)
		has(t, s, `"precision":10`)
		has(t, s, `"scale":2`)
	})
	t.Run("uuid", func(t *testing.T) {
		type R struct {
			U [16]byte `avro:"u,uuid"`
		}
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		has(t, s, `"logicalType":"uuid"`)
	})
}
