package avro_test

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// Tier-2 cross-path byte identity (CORRECTNESS_PLAN.md §T2c). The library
// encodes an addressable struct through the unsafe fast path and a
// map[string]any through the reflect path; for the same logical record they
// MUST produce byte-identical wire. The recurring class this guards is
// multi-representation float values — a signaling-NaN payload survives a raw
// bit read but is quieted by reflect's float32->float64->float32 round-trip,
// so the two paths can silently diverge on the exact bits.

type cpRec struct {
	B  bool    `avro:"b"`
	I  int32   `avro:"i"`
	L  int64   `avro:"l"`
	F  float32 `avro:"f"`
	D  float64 `avro:"d"`
	S  string  `avro:"s"`
	By []byte  `avro:"by"`
}

const cpSchema = `{"type":"record","name":"CP","fields":[
	{"name":"b","type":"boolean"},
	{"name":"i","type":"int"},
	{"name":"l","type":"long"},
	{"name":"f","type":"float"},
	{"name":"d","type":"double"},
	{"name":"s","type":"string"},
	{"name":"by","type":"bytes"}
]}`

// structToMap builds the map[string]any equivalent of a struct using its avro
// tags, so the struct and map encode the identical logical record.
func structToMap(v any) map[string]any {
	rv := reflect.ValueOf(v)
	rt := rv.Type()
	m := make(map[string]any, rt.NumField())
	for i := range rt.NumField() {
		tag := strings.Split(rt.Field(i).Tag.Get("avro"), ",")[0]
		m[tag] = rv.Field(i).Interface()
	}
	return m
}

func TestCrossPath_StructVsMapBytes(t *testing.T) {
	s := avro.MustParse(cpSchema)

	f32 := func(bits uint32) float32 { return math.Float32frombits(bits) }
	f64 := func(bits uint64) float64 { return math.Float64frombits(bits) }
	recs := []cpRec{
		{true, 42, 1 << 40, 1.5, 2.5, "hi", []byte{1, 2, 3}},
		{false, -1, -1, f32(0x7f800001), f64(0x7ff0000000000001), "", nil},              // signaling NaNs
		{true, 0, 0, f32(0x7fc00000), f64(0x7ff8000000000000), "x", []byte{}},           // quiet NaNs
		{false, 1, 2, f32(0x80000000), f64(0x8000000000000000), "neg0", []byte{0}},      // -0.0
		{true, -7, 7, f32(0x00000001), f64(0x0000000000000001), "denorm", []byte{0xff}}, // denormals
		{false, 1 << 30, -(1 << 50), math.MaxFloat32, math.MaxFloat64, "max", []byte{0x80}},
	}

	for i, r := range recs {
		structBytes, err := s.Encode(&r) // &r ⇒ addressable ⇒ unsafe struct fast path
		if err != nil {
			t.Fatalf("rec %d: Encode(struct): %v", i, err)
		}
		mapBytes, err := s.Encode(structToMap(r)) // map ⇒ reflect path
		if err != nil {
			t.Fatalf("rec %d: Encode(map): %v", i, err)
		}
		if string(structBytes) != string(mapBytes) {
			t.Errorf("rec %d: unsafe struct path vs reflect map path differ\n struct %x\n map    %x\n (rec %+v)",
				i, structBytes, mapBytes, r)
		}
	}
}

// TestCrossPath_FloatSignalingNaN isolates the float32/float64 signaling-NaN
// case across struct (unsafe) and map (reflect) paths and a top-level encode,
// pinning that the raw bits survive on every path.
func TestCrossPath_FloatSignalingNaN(t *testing.T) {
	floatS := avro.MustParse(`"float"`)
	for _, bits := range []uint32{0x7f800001, 0x7fa00000, 0x7fbfffff, 0xff800001} {
		want := bits
		type fr struct {
			F float32 `avro:"f"`
		}
		recS := avro.MustParse(`{"type":"record","name":"FR","fields":[{"name":"f","type":"float"}]}`)
		v := math.Float32frombits(bits)

		sb, _ := recS.Encode(&fr{F: v})              // unsafe
		mb, _ := recS.Encode(map[string]any{"f": v}) // reflect (record-as-map)
		tb, _ := floatS.Encode(v)                    // top-level reflect
		if string(sb) != string(mb) {
			t.Errorf("bits %#08x: struct vs map differ: %x vs %x", bits, sb, mb)
		}
		// The 4 trailing wire bytes of the top-level float are the raw LE bits.
		if got := fmt.Sprintf("%08x", leUint32(tb)); got != fmt.Sprintf("%08x", want) {
			t.Errorf("bits %#08x: top-level encode quieted to %s", bits, got)
		}
	}
}

func leUint32(b []byte) uint32 {
	if len(b) < 4 {
		return 0
	}
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}
