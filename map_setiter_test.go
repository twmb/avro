package avro

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"testing"
)

type ptrTextString string

func (p *ptrTextString) MarshalText() ([]byte, error) { return []byte("MT:" + string(*p)), nil }

// long→int JSON native decode must never silently truncate: a wire value
// outside int32 range into []int / map[string]int is preserved on 64-bit
// (native) and rejected on 32-bit (reflect fallback) — but never garbage. The
// int32 value type always rejects (parseJSONInt32 range-checks). Locks the
// 32-bit narrowing fix.
func TestRegression_JSONNativeLongIntNoTruncate(t *testing.T) {
	const big = `5000000000` // > math.MaxInt32

	// int32 value type: must error regardless of platform.
	if err := MustParse(`{"type":"array","items":"int"}`).DecodeJSON([]byte("["+big+"]"), &[]int32{}); err == nil {
		t.Fatal("[]int32: expected overflow error for 5000000000")
	}

	// []int "long":
	var sl []int
	errSl := MustParse(`{"type":"array","items":"long"}`).DecodeJSON([]byte("["+big+"]"), &sl)
	// map[string]int "long":
	var mp map[string]int
	errMp := MustParse(`{"type":"map","values":"long"}`).DecodeJSON([]byte(`{"k":`+big+`}`), &mp)

	if strconv.IntSize == 64 {
		if errSl != nil || len(sl) != 1 || int64(sl[0]) != 5000000000 {
			t.Fatalf("64-bit []int: err=%v val=%v (want [5000000000])", errSl, sl)
		}
		if errMp != nil || int64(mp["k"]) != 5000000000 {
			t.Fatalf("64-bit map[string]int: err=%v val=%v", errMp, mp)
		}
	} else {
		if errSl == nil {
			t.Fatalf("32-bit []int: expected overflow error, got %v", sl)
		}
		if errMp == nil {
			t.Fatalf("32-bit map[string]int: expected overflow error, got %v", mp)
		}
	}
}

// []fpString (named string, no text method) decodes via the array fast loop
// (deserArrayStringLoop / JSON reflect) — the native loop's exact-string
// assertion misses it. map[string]fpString covers the map fast block; this
// covers the array path, binary and JSON.
func TestRegression_ArrayNamedStringFastLoopDecode(t *testing.T) {
	s := MustParse(`{"type":"array","items":"string"}`)
	want := []fpString{"a", "", "b\x00c", "héllo"}
	wire, err := s.Encode(want)
	if err != nil {
		t.Fatal(err)
	}
	var out []fpString
	if _, err := s.Decode(wire, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("binary: %v != %v", out, want)
	}
	j, err := s.EncodeJSON(want)
	if err != nil {
		t.Fatal(err)
	}
	var jout []fpString
	if err := s.DecodeJSON(j, &jout); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(jout, want) {
		t.Fatalf("json: %v != %v", jout, want)
	}
}

// A pointer-receiver MarshalText does not fire on a non-addressable by-value
// scalar (the value's method set lacks the pointer method), so it encodes as
// the raw string — matching encoding/json. By pointer (addressable) it fires.
func TestRegression_PointerMarshalTextNonAddressableScalar(t *testing.T) {
	s := MustParse(`"string"`)
	v := ptrTextString("hi")

	byVal, err := s.Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	rawHi, _ := s.Encode("hi")
	if !bytes.Equal(byVal, rawHi) {
		t.Fatalf("by-value pointer-MarshalText fired: got % x, want raw % x", byVal, rawHi)
	}
	jv, err := json.Marshal(v) // parity baseline: encoding/json also doesn't fire
	if err != nil {
		t.Fatal(err)
	}
	if string(jv) != `"hi"` {
		t.Fatalf("encoding/json parity broken: got %s want \"hi\"", jv)
	}

	byPtr, err := s.Encode(&v)
	if err != nil {
		t.Fatal(err)
	}
	rawMT, _ := s.Encode("MT:hi")
	if !bytes.Equal(byPtr, rawMT) {
		t.Fatalf("by-pointer MarshalText did not fire: got % x, want % x", byPtr, rawMT)
	}
}

// appendMapPrimitive, serMap.ser, and the JSON map encoder reuse two
// addressable Values via SetIterKey/SetIterValue instead of allocating a
// fresh Value per entry. Because the reused value Value is addressable
// (iter.Value() is not), a struct-valued map now reaches serRecord's
// unsafe fast path. These tests pin that the change is behavior-neutral:
// every map shape round-trips (binary AND JSON) to a deep-equal value,
// and the struct-valued map's record bytes match a standalone encode.
//
// Maps iterate in randomized order, so multi-entry wire is not
// byte-stable across encodes — we compare decoded values, not bytes
// (except the single-entry struct case, which is deterministic).

type setIterRec struct {
	A int32  `avro:"a"`
	B string `avro:"b"`
}

const setIterRecSchema = `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`

func TestRegression_MapSetIterRoundTrip(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		val    any
	}{
		{"builtinString", `{"type":"map","values":"string"}`,
			map[string]string{"a": "1", "b": "two", "c": "", "d": "x\x00y"}},
		{"namedString", `{"type":"map","values":"string"}`,
			map[string]fpString{"a": "1", "b": "two", "c": ""}},
		{"int", `{"type":"map","values":"int"}`,
			map[string]int32{"a": 0, "b": -1, "c": math.MaxInt32, "d": math.MinInt32}},
		{"long", `{"type":"map","values":"long"}`,
			map[string]int64{"a": 0, "b": -1, "c": math.MaxInt64, "d": math.MinInt64}},
		{"double", `{"type":"map","values":"double"}`,
			map[string]float64{"a": 0, "b": -1.5, "c": math.MaxFloat64}},
		{"float", `{"type":"map","values":"float"}`,
			map[string]float32{"a": 0, "b": -1.5, "c": math.MaxFloat32}},
		{"bool", `{"type":"map","values":"boolean"}`,
			map[string]bool{"a": true, "b": false}},
		{"structVal", `{"type":"map","values":` + setIterRecSchema + `}`,
			map[string]setIterRec{"x": {1, "one"}, "y": {2, "two"}, "z": {-3, ""}}},
		{"ptrStructVal", `{"type":"map","values":` + setIterRecSchema + `}`,
			map[string]*setIterRec{"x": {1, "one"}, "y": {2, "two"}}},
		{"nestedMap", `{"type":"map","values":{"type":"map","values":"int"}}`,
			map[string]map[string]int32{"o": {"i": 1, "j": 2}, "p": {"k": 3}}},
		{"jsonNumberKey", `{"type":"map","values":"string"}`,
			map[json.Number]string{"1": "a", "22": "b", "-3": "c"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := MustParse(c.schema)

			// Binary round-trip.
			b, err := s.Encode(c.val)
			if err != nil {
				t.Fatalf("binary encode: %v", err)
			}
			bOut := reflect.New(reflect.TypeOf(c.val)).Interface()
			if _, err := s.Decode(b, bOut); err != nil {
				t.Fatalf("binary decode: %v", err)
			}
			if got := reflect.ValueOf(bOut).Elem().Interface(); !reflect.DeepEqual(got, c.val) {
				t.Fatalf("binary round-trip mismatch:\n got=%#v\n want=%#v", got, c.val)
			}

			// JSON round-trip.
			j, err := s.EncodeJSON(c.val)
			if err != nil {
				t.Fatalf("json encode: %v", err)
			}
			jOut := reflect.New(reflect.TypeOf(c.val)).Interface()
			if err := s.DecodeJSON(j, jOut); err != nil {
				t.Fatalf("json decode: %v (json=%s)", err, j)
			}
			if got := reflect.ValueOf(jOut).Elem().Interface(); !reflect.DeepEqual(got, c.val) {
				t.Fatalf("json round-trip mismatch:\n got=%#v\n want=%#v", got, c.val)
			}
		})
	}
}

// The numeric/bool value switch in appendMapPrimitive must be byte-identical
// to the general (named-type) path it replaced. Single-entry maps have
// deterministic wire order, so bytes are directly comparable: a builtin
// value type takes the switch, a same-underlying named type takes the
// general appendFn path.
func TestRegression_MapValueSwitchMatchesGeneral(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		fast   any
		gen    any
	}{
		{"int", `{"type":"map","values":"int"}`,
			map[string]int32{"k": -123456}, map[string]fpInt32{"k": -123456}},
		{"long", `{"type":"map","values":"long"}`,
			map[string]int64{"k": math.MinInt64}, map[string]fpInt64{"k": math.MinInt64}},
		{"longFromInt", `{"type":"map","values":"long"}`,
			map[string]int{"k": -1}, map[string]fpInt{"k": -1}},
		{"float", `{"type":"map","values":"float"}`,
			map[string]float32{"k": 3.14}, map[string]fpFloat32{"k": 3.14}},
		{"floatSignalingNaN", `{"type":"map","values":"float"}`,
			map[string]float32{"k": math.Float32frombits(0x7f800001)},
			map[string]fpFloat32{"k": fpFloat32(math.Float32frombits(0x7f800001))}},
		{"double", `{"type":"map","values":"double"}`,
			map[string]float64{"k": 2.718281828}, map[string]fpFloat64{"k": 2.718281828}},
		{"bool", `{"type":"map","values":"boolean"}`,
			map[string]bool{"k": true}, map[string]fpBool{"k": true}},
		{"string", `{"type":"map","values":"string"}`,
			map[string]string{"k": "v"}, map[string]fpString{"k": "v"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := MustParse(c.schema)
			fast, err := s.Encode(c.fast)
			if err != nil {
				t.Fatalf("fast: %v", err)
			}
			gen, err := s.Encode(c.gen)
			if err != nil {
				t.Fatalf("gen: %v", err)
			}
			if !bytes.Equal(fast, gen) {
				t.Fatalf("map value switch diverges from general path:\n fast=% x\n gen =% x", fast, gen)
			}
		})
	}
}

type f32Field struct {
	F float32 `avro:"f"`
}

// float32 must preserve exact bits (signaling-NaN payload included) on every
// path — matching Java (floatToRawIntBits/intBitsToFloat), fastavro, and IEEE
// "float is 4 opaque bytes." reflect.Value.Float()/SetFloat would quiet sNaN
// via a float64 round-trip; the encode/decode paths avoid that. Pins that the
// unsafe (addressable) and reflect (by-value) paths agree, and that maps and
// arrays agree with both.
func TestRegression_Float32SignalingNaNPreserved(t *testing.T) {
	const bits = uint32(0x7f800001) // signaling NaN (quiet bit clear)
	f := math.Float32frombits(bits)
	wire := []byte{0x01, 0x00, 0x80, 0x7f} // little-endian 0x7f800001

	// ENCODE: record field, both by-value (reflect) and by-pointer (unsafe).
	rec := MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":"float"}]}`)
	for _, enc := range []any{f32Field{f}, &f32Field{f}} {
		b, err := rec.Encode(enc)
		if err != nil {
			t.Fatalf("encode %T: %v", enc, err)
		}
		if !bytes.Equal(b, wire) {
			t.Fatalf("encode %T quieted sNaN: got % x want % x", enc, b, wire)
		}
	}
	mapS := MustParse(`{"type":"map","values":"float"}`)
	arrS := MustParse(`{"type":"array","items":"float"}`)
	mb, _ := mapS.Encode(map[string]float32{"k": f})
	if !bytes.Contains(mb, wire) {
		t.Fatalf("map encode quieted sNaN: % x", mb)
	}
	ab, _ := arrS.Encode([]float32{f})
	if !bytes.Contains(ab, wire) {
		t.Fatalf("array encode quieted sNaN: % x", ab)
	}

	// DECODE: record field, map value, array element, interface — all preserve.
	var sf f32Field
	if _, err := rec.Decode(wire, &sf); err != nil {
		t.Fatal(err)
	}
	if got := math.Float32bits(sf.F); got != bits {
		t.Fatalf("record decode quieted: got %08x want %08x", got, bits)
	}
	var m map[string]float32
	if _, err := mapS.Decode(mb, &m); err != nil {
		t.Fatal(err)
	}
	if got := math.Float32bits(m["k"]); got != bits {
		t.Fatalf("map decode quieted: got %08x want %08x", got, bits)
	}
	var a []float32
	if _, err := arrS.Decode(ab, &a); err != nil {
		t.Fatal(err)
	}
	if got := math.Float32bits(a[0]); got != bits {
		t.Fatalf("array decode quieted: got %08x want %08x", got, bits)
	}
	var anyV any
	if _, err := MustParse(`"float"`).Decode(wire, &anyV); err != nil {
		t.Fatal(err)
	}
	if got := math.Float32bits(anyV.(float32)); got != bits {
		t.Fatalf("interface decode quieted: got %08x want %08x", got, bits)
	}
}

// JSON array encode native must be byte-identical to the reflect path.
// Arrays are ordered, so the whole encoding is byte-comparable.
func TestRegression_ArrayJSONNativeMatchesGeneral(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		fast   any
		gen    any
	}{
		{"string", `{"type":"array","items":"string"}`,
			[]string{"a", "b\"c\n", ""}, []fpString{"a", "b\"c\n", ""}},
		{"int", `{"type":"array","items":"int"}`,
			[]int32{0, -1, math.MaxInt32, math.MinInt32}, []fpInt32{0, -1, math.MaxInt32, math.MinInt32}},
		{"long", `{"type":"array","items":"long"}`,
			[]int64{0, math.MinInt64, math.MaxInt64}, []fpInt64{0, math.MinInt64, math.MaxInt64}},
		{"float", `{"type":"array","items":"float"}`,
			[]float32{3.5, -1, 0}, []fpFloat32{3.5, -1, 0}},
		{"double", `{"type":"array","items":"double"}`,
			[]float64{2.5, -1}, []fpFloat64{2.5, -1}},
		{"bool", `{"type":"array","items":"boolean"}`,
			[]bool{true, false, true}, []fpBool{true, false, true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := MustParse(c.schema)
			fast, err := s.EncodeJSON(c.fast)
			if err != nil {
				t.Fatalf("fast: %v", err)
			}
			gen, err := s.EncodeJSON(c.gen)
			if err != nil {
				t.Fatalf("gen: %v", err)
			}
			if !bytes.Equal(fast, gen) {
				t.Fatalf("JSON array native diverges from reflect:\n fast=%s\n gen =%s", fast, gen)
			}
		})
	}
}

// JSON map encode native must be byte-identical to the reflect path. Single
// entry → deterministic order. A builtin value type takes the native path; a
// same-underlying named type takes the reflect (appendAvroJSON) path.
func TestRegression_MapJSONNativeMatchesGeneral(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		fast   any
		gen    any
	}{
		{"string", `{"type":"map","values":"string"}`,
			map[string]string{"k": "a\"b\n"}, map[string]fpString{"k": "a\"b\n"}},
		{"int", `{"type":"map","values":"int"}`,
			map[string]int32{"k": -123456}, map[string]fpInt32{"k": -123456}},
		{"long", `{"type":"map","values":"long"}`,
			map[string]int64{"k": math.MinInt64}, map[string]fpInt64{"k": math.MinInt64}},
		{"float", `{"type":"map","values":"float"}`,
			map[string]float32{"k": 3.5}, map[string]fpFloat32{"k": 3.5}},
		{"double", `{"type":"map","values":"double"}`,
			map[string]float64{"k": 2.5}, map[string]fpFloat64{"k": 2.5}},
		{"bool", `{"type":"map","values":"boolean"}`,
			map[string]bool{"k": true}, map[string]fpBool{"k": true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := MustParse(c.schema)
			fast, err := s.EncodeJSON(c.fast)
			if err != nil {
				t.Fatalf("fast: %v", err)
			}
			gen, err := s.EncodeJSON(c.gen)
			if err != nil {
				t.Fatalf("gen: %v", err)
			}
			if !bytes.Equal(fast, gen) {
				t.Fatalf("JSON native diverges from reflect path:\n fast=%s\n gen =%s", fast, gen)
			}
		})
	}
}

// JSON decode native (map[string]V via parse leaves, []V via append) must
// round-trip, and named slice/elem/map types must fall back to reflect.
func TestRegression_JSONDecodeNative(t *testing.T) {
	type nsInt []int32
	type nElem int32
	arrS := MustParse(`{"type":"array","items":"int"}`)
	in := []int32{0, 1, -1, math.MaxInt32, math.MinInt32}
	aj, err := arrS.EncodeJSON(in)
	if err != nil {
		t.Fatal(err)
	}
	var aOut []int32 // native slice
	if err := arrS.DecodeJSON(aj, &aOut); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(aOut, in) {
		t.Fatalf("json array native: %v != %v", aOut, in)
	}
	var nsOut nsInt // named slice → fallback
	if err := arrS.DecodeJSON(aj, &nsOut); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual([]int32(nsOut), in) {
		t.Fatalf("json named-slice fallback: %v", nsOut)
	}
	var neOut []nElem // named elem → fallback
	if err := arrS.DecodeJSON(aj, &neOut); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(neOut, []nElem{0, 1, -1, math.MaxInt32, math.MinInt32}) {
		t.Fatalf("json named-elem fallback: %v", neOut)
	}

	mapS := MustParse(`{"type":"map","values":"long"}`)
	m := map[string]int64{"a": 1, "b": math.MinInt64, "c": math.MaxInt64}
	mj, err := mapS.EncodeJSON(m)
	if err != nil {
		t.Fatal(err)
	}
	var mOut map[string]int64 // native map
	if err := mapS.DecodeJSON(mj, &mOut); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(mOut, m) {
		t.Fatalf("json map native: %v != %v", mOut, m)
	}
}

// A named map type (type M map[string]int32) has Key()==string and
// Elem()==int32, so it enters appendMapPrimitive's native switch — but
// v.Interface() yields the named type, so the comma-ok assertion to the
// unnamed map[string]int32 fails and it must fall through to the reflect
// path (not panic, not mis-encode). Single-entry wire must match the
// unnamed map.
type namedIntMap map[string]int32

func TestRegression_MapNamedTypeFallsThroughToReflect(t *testing.T) {
	s := MustParse(`{"type":"map","values":"int"}`)
	m := namedIntMap{"a": 1, "b": -2, "c": math.MaxInt32}
	b, err := s.Encode(m)
	if err != nil {
		t.Fatalf("encode named map: %v", err)
	}
	var out namedIntMap
	if _, err := s.Decode(b, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(out, m) {
		t.Fatalf("round-trip mismatch:\n got=%#v\n want=%#v", out, m)
	}
	got, _ := s.Encode(namedIntMap{"k": 7})
	want, _ := s.Encode(map[string]int32{"k": 7})
	if !bytes.Equal(got, want) {
		t.Fatalf("named vs unnamed map wire differ:\n named  =% x\n unnamed=% x", got, want)
	}
}

// The struct-valued-map flip: a single-entry map[string]Struct encodes its
// value through serRecord's unsafe fast path (now that valV is
// addressable). Its record bytes must be byte-identical to encoding that
// struct standalone. Single entry → deterministic wire layout:
// [count=1: 0x02][key "k": 0x02 'k'][record bytes...][terminator 0x00].
func TestRegression_MapStructValueMatchesStandaloneRecord(t *testing.T) {
	mapS := MustParse(`{"type":"map","values":` + setIterRecSchema + `}`)
	recS := MustParse(setIterRecSchema)

	val := setIterRec{A: 7, B: "hi"}
	mapWire, err := mapS.Encode(map[string]setIterRec{"k": val})
	if err != nil {
		t.Fatal(err)
	}
	recWire, err := recS.Encode(val)
	if err != nil {
		t.Fatal(err)
	}
	// Strip 3-byte prefix (count 0x02, keylen 0x02, 'k') and 1-byte
	// terminator.
	if len(mapWire) < 4 {
		t.Fatalf("map wire too short: % x", mapWire)
	}
	inner := mapWire[3 : len(mapWire)-1]
	if !bytes.Equal(inner, recWire) {
		t.Fatalf("struct-valued map record bytes differ from standalone:\n map-inner=% x\n standalone=% x", inner, recWire)
	}
	if term := mapWire[len(mapWire)-1]; term != 0 {
		t.Fatalf("expected 0x00 block terminator, got 0x%02x", term)
	}
}
