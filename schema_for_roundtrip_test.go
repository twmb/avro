package avro_test

import (
	"math"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// Tier-2 SchemaFor behavioral round-trip (CORRECTNESS_PLAN.md SchemaFor gap).
// The existing schema_for tests assert the GENERATED SCHEMA STRING -- they
// check the schema looks right, never that it behaves right. Two missing
// oracles: (1) a schema SchemaFor builds for a Go type must losslessly carry a
// value of that type (Encode -> Decode -> DeepEqual); (2) the emitted schema
// must itself be a valid, stable schema (String -> Parse -> identical
// canonical form). A SchemaFor change that emits a structurally-plausible but
// behaviorally-wrong schema -- wrong integer width, a dangling named-type
// reference, a malformed default -- is invisible to a string assertion but
// caught here. (The dangling-reference case is exactly the uuid/plain dedup
// class: the same Go array type used once uuid-tagged and once plain.)

func schemaForRoundTrip[T any](t *testing.T, name string, vals []T) {
	t.Helper()
	s, err := avro.SchemaFor[T]()
	if err != nil {
		t.Fatalf("%s: SchemaFor: %v", name, err)
	}
	// Schema stability: the emitted schema must re-parse to the same canonical
	// form. SchemaFor parses internally, so a failure here points at String().
	reparsed, err := avro.Parse(s.String())
	if err != nil {
		t.Fatalf("%s: re-Parse(SchemaFor.String()): %v\nschema: %s", name, err, s.String())
	}
	if got, want := string(reparsed.Canonical()), string(s.Canonical()); got != want {
		t.Errorf("%s: canonical drift after String->Parse:\n got %s\nwant %s", name, got, want)
	}
	for i, v := range vals {
		wire, err := s.Encode(v)
		if err != nil {
			t.Fatalf("%s[%d]: Encode(%+v): %v", name, i, v, err)
		}
		var got T
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatalf("%s[%d]: Decode: %v", name, i, err)
		}
		if !equalRoundTrip(got, v) {
			t.Errorf("%s[%d]: round-trip mismatch\n got %+v\nwant %+v", name, i, got, v)
		}
	}
}

// equalRoundTrip compares two Go values for round-trip equality, treating a
// nil and an empty slice/map as equal (Avro has no null-vs-empty distinction
// for arrays/maps, so decode legitimately materializes one for the other) and
// recursing through pointers and structs. It avoids reflect.DeepEqual's
// nil/empty strictness without masking real value differences.
func equalRoundTrip(a, b any) bool { return eqValue(reflect.ValueOf(a), reflect.ValueOf(b)) }

func eqValue(a, b reflect.Value) bool {
	if a.IsValid() != b.IsValid() {
		return false
	}
	if !a.IsValid() {
		return true
	}
	if a.Kind() != b.Kind() {
		return false
	}
	switch a.Kind() {
	case reflect.Slice, reflect.Array:
		if a.Len() != b.Len() { // both-empty (nil or len 0) compares equal
			return false
		}
		for i := 0; i < a.Len(); i++ {
			if !eqValue(a.Index(i), b.Index(i)) {
				return false
			}
		}
		return true
	case reflect.Map:
		if a.Len() != b.Len() {
			return false
		}
		for _, k := range a.MapKeys() {
			bv := b.MapIndex(k)
			if !bv.IsValid() || !eqValue(a.MapIndex(k), bv) {
				return false
			}
		}
		return true
	case reflect.Pointer, reflect.Interface:
		if a.IsNil() || b.IsNil() {
			return a.IsNil() == b.IsNil()
		}
		return eqValue(a.Elem(), b.Elem())
	case reflect.Struct:
		for i := 0; i < a.NumField(); i++ {
			if !eqValue(a.Field(i), b.Field(i)) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(a.Interface(), b.Interface())
	}
}

type sfPrims struct {
	B   bool    `avro:"b"`
	I8  int8    `avro:"i8"`
	I16 int16   `avro:"i16"`
	I32 int32   `avro:"i32"`
	I64 int64   `avro:"i64"`
	U8  uint8   `avro:"u8"`
	U16 uint16  `avro:"u16"`
	U32 uint32  `avro:"u32"`
	F32 float32 `avro:"f32"`
	F64 float64 `avro:"f64"`
	S   string  `avro:"s"`
	By  []byte  `avro:"by"`
}

type sfInner struct {
	X int32  `avro:"x"`
	Y string `avro:"y"`
}

type sfComposite struct {
	Inner  sfInner          `avro:"inner"`
	List   []int32          `avro:"list"`
	Dict   map[string]int64 `avro:"dict"`
	Ptr    *int32           `avro:"ptr"`
	Nested []sfInner        `avro:"nested"`
}

// sfUUIDPlain uses the SAME Go array type once uuid-tagged and once plain.
// These are distinct Avro types (the uuid form carries a logicalType), so
// SchemaFor must emit a full definition for each rather than a name reference
// to the other -- the latter is a dangling reference Parse rejects. SchemaFor
// returning an error (or the round-trip mismatching) on this shape catches the
// dedup regression.
type sfUUIDPlain struct {
	A [16]byte `avro:"a,uuid"`
	B [16]byte `avro:"b"`
}

func TestSchemaForValueRoundTrip(t *testing.T) {
	t.Run("primitives", func(t *testing.T) {
		schemaForRoundTrip(t, "sfPrims", []sfPrims{
			{},
			{B: true, I8: math.MaxInt8, I16: math.MaxInt16, I32: math.MaxInt32, I64: math.MaxInt64,
				U8: math.MaxUint8, U16: math.MaxUint16, U32: math.MaxUint32,
				F32: math.MaxFloat32, F64: math.MaxFloat64, S: "héllo", By: []byte{0, 1, 2, 0xff}},
			{I8: math.MinInt8, I16: math.MinInt16, I32: math.MinInt32, I64: math.MinInt64,
				F32: -1.5, F64: -2.5, S: "", By: []byte{}},
		})
	})

	t.Run("composite", func(t *testing.T) {
		p := int32(7)
		schemaForRoundTrip(t, "sfComposite", []sfComposite{
			{
				Inner:  sfInner{X: 1, Y: "a"},
				List:   []int32{1, 2, 3},
				Dict:   map[string]int64{"k": 5, "j": -9},
				Ptr:    &p,
				Nested: []sfInner{{X: 10, Y: "x"}, {X: 20, Y: "y"}},
			},
			{
				Inner:  sfInner{},
				List:   []int32{},
				Dict:   map[string]int64{},
				Ptr:    nil, // nullable -> null branch
				Nested: []sfInner{},
			},
		})
	})

	t.Run("uuid-and-plain-same-array-type", func(t *testing.T) {
		schemaForRoundTrip(t, "sfUUIDPlain", []sfUUIDPlain{
			{A: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, B: [16]byte{15: 0xff}},
			{},
		})
	})
}

// TestSchemaForLogicalRoundTrip round-trips the logical-typed fields whose Go
// representations (time.Time, *big.Rat) do not compare correctly under
// DeepEqual, so they need type-aware equality. SchemaFor's time/decimal
// inference is what binds these Go types to their Avro logical types; this
// pins that the generated schema actually carries the value back.
func TestSchemaForLogicalRoundTrip(t *testing.T) {
	type sfLogical struct {
		TS  time.Time `avro:"ts,timestamp-micros"`
		Dur time.Time `avro:"d,timestamp-millis"`
	}
	s, err := avro.SchemaFor[sfLogical]()
	if err != nil {
		t.Fatalf("SchemaFor: %v", err)
	}
	in := sfLogical{
		TS:  time.UnixMicro(1_600_000_000_123_456).UTC(),
		Dur: time.UnixMilli(1_600_000_000_123).UTC(),
	}
	wire, err := s.Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var got sfLogical
	if _, err := s.Decode(wire, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !got.TS.Equal(in.TS) {
		t.Errorf("timestamp-micros round-trip: got %v, want %v", got.TS, in.TS)
	}
	if !got.Dur.Equal(in.Dur) {
		t.Errorf("timestamp-millis round-trip: got %v, want %v", got.Dur, in.Dur)
	}

	// Decimal (bytes + decimal logical) over *big.Rat, compared via Cmp.
	type sfDecimal struct {
		D *big.Rat `avro:"d,decimal(10,4)"`
	}
	ds, err := avro.SchemaFor[sfDecimal]()
	if err != nil {
		t.Fatalf("SchemaFor decimal: %v", err)
	}
	din := sfDecimal{D: big.NewRat(12345, 100)} // 123.45, fits scale 4
	dwire, err := ds.Encode(din)
	if err != nil {
		t.Fatalf("Encode decimal: %v", err)
	}
	var dgot sfDecimal
	if _, err := ds.Decode(dwire, &dgot); err != nil {
		t.Fatalf("Decode decimal: %v", err)
	}
	if dgot.D == nil || dgot.D.Cmp(din.D) != 0 {
		t.Errorf("decimal round-trip: got %v, want %v", dgot.D, din.D)
	}
}
