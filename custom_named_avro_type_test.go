package avro_test

import (
	"reflect"
	"testing"

	"github.com/twmb/avro"
)

// A CustomType's Avro-native type A (the second NewCustomType parameter) may be
// a NAMED Go type over a canonical kind — e.g. type UnixMillis int64 as the
// long representation. inferAvroType classifies A by reflect.Kind, so such an A
// registers as an ordinary primitive custom. On decode, the base deserializer
// produces the CANONICAL Go value for that kind (int64, []byte, ...); the
// generated decode wrapper must CONVERT that value to A before invoking the
// user's decode, because the canonical value's dynamic type is the base kind,
// not the named type, so a bare type assertion panics. The encode side already
// type-guards (it fires only for an exact-A value), so only decode is exposed.
// This pins every canonical kind on both wire formats.

type namedBool bool
type namedInt32 int32
type namedInt64 int64
type namedFloat32 float32
type namedFloat64 float64
type namedString string
type namedBytes []byte

func TestRegression_CustomNamedAvroNativeTypeDecodes(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		ct     avro.CustomType
		in     any // a G value to encode
		want   any // the expected decoded G value
	}{
		{"boolean", `{"type":"boolean"}`,
			avro.NewCustomType[bool, namedBool]("",
				func(g bool, _ *avro.SchemaNode) (namedBool, error) { return namedBool(g), nil },
				func(a namedBool, _ *avro.SchemaNode) (bool, error) { return bool(a), nil }),
			true, true},
		{"int", `{"type":"int"}`,
			avro.NewCustomType[int32, namedInt32]("",
				func(g int32, _ *avro.SchemaNode) (namedInt32, error) { return namedInt32(g), nil },
				func(a namedInt32, _ *avro.SchemaNode) (int32, error) { return int32(a), nil }),
			int32(5), int32(5)},
		{"long", `{"type":"long"}`,
			avro.NewCustomType[int64, namedInt64]("",
				func(g int64, _ *avro.SchemaNode) (namedInt64, error) { return namedInt64(g), nil },
				func(a namedInt64, _ *avro.SchemaNode) (int64, error) { return int64(a), nil }),
			int64(1700000000000), int64(1700000000000)},
		{"float", `{"type":"float"}`,
			avro.NewCustomType[float32, namedFloat32]("",
				func(g float32, _ *avro.SchemaNode) (namedFloat32, error) { return namedFloat32(g), nil },
				func(a namedFloat32, _ *avro.SchemaNode) (float32, error) { return float32(a), nil }),
			float32(2.5), float32(2.5)},
		{"double", `{"type":"double"}`,
			avro.NewCustomType[float64, namedFloat64]("",
				func(g float64, _ *avro.SchemaNode) (namedFloat64, error) { return namedFloat64(g), nil },
				func(a namedFloat64, _ *avro.SchemaNode) (float64, error) { return float64(a), nil }),
			float64(2.5), float64(2.5)},
		{"string", `{"type":"string"}`,
			avro.NewCustomType[string, namedString]("",
				func(g string, _ *avro.SchemaNode) (namedString, error) { return namedString(g), nil },
				func(a namedString, _ *avro.SchemaNode) (string, error) { return string(a), nil }),
			"hello", "hello"},
		{"bytes", `{"type":"bytes"}`,
			avro.NewCustomType[[]byte, namedBytes]("",
				func(g []byte, _ *avro.SchemaNode) (namedBytes, error) { return namedBytes(g), nil },
				func(a namedBytes, _ *avro.SchemaNode) ([]byte, error) { return []byte(a), nil }),
			[]byte("hi"), []byte("hi")},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := avro.Parse(c.schema, avro.WithCustomType(c.ct))
			if err != nil {
				t.Fatalf("registration/parse: %v", err)
			}
			// binary
			decodeNoPanic(t, "binary", c.want, func() (any, error) {
				wire, err := s.Encode(c.in)
				if err != nil {
					return nil, err
				}
				var got any
				_, err = s.Decode(wire, &got)
				return got, err
			})
			// JSON
			decodeNoPanic(t, "json", c.want, func() (any, error) {
				js, err := s.AppendEncodeJSON(nil, c.in)
				if err != nil {
					return nil, err
				}
				var got any
				err = s.DecodeJSON(js, &got)
				return got, err
			})
		})
	}
}

func decodeNoPanic(t *testing.T, label string, want any, run func() (any, error)) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("%s: decode PANIC on a registration-accepted custom: %v", label, r)
		}
	}()
	got, err := run()
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: round-trip = %#v, want %#v", label, got, want)
	}
}
