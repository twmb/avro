package avro

import (
	"math"
	"reflect"
)

// promotionDeser returns a deserfn that reads the writer's wire type and
// sets the reader's Go type, or nil if the promotion is not supported.
func promotionDeser(writerKind, readerKind string) deserfn {
	key := writerKind + ">" + readerKind
	return promotions[key]
}

var promotions = map[string]deserfn{
	"int>long":   promoteIntToLong,
	"int>float":  promoteIntToFloat,
	"int>double": promoteIntToDouble,

	"long>float":  promoteLongToFloat,
	"long>double": promoteLongToDouble,

	"float>double": promoteFloatToDouble,

	"string>bytes": promoteStringToBytes,
	"bytes>string": promoteBytesToString,
}

func promoteIntToLong(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
	val, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(int64(val)))
		return src, nil
	}
	return src, setLongValue(v, int64(val))
}

func promoteIntToFloat(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
	val, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(float32(val)))
		return src, nil
	}
	if !v.CanFloat() {
		return nil, &SemanticError{GoType: v.Type(), AvroType: "float"}
	}
	v.SetFloat(float64(val))
	return src, nil
}

func promoteIntToDouble(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
	val, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(float64(val)))
		return src, nil
	}
	if !v.CanFloat() {
		return nil, &SemanticError{GoType: v.Type(), AvroType: "double"}
	}
	v.SetFloat(float64(val))
	return src, nil
}

func promoteLongToFloat(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(float32(val)))
		return src, nil
	}
	if !v.CanFloat() {
		return nil, &SemanticError{GoType: v.Type(), AvroType: "float"}
	}
	v.SetFloat(float64(val))
	return src, nil
}

func promoteLongToDouble(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(float64(val)))
		return src, nil
	}
	if !v.CanFloat() {
		return nil, &SemanticError{GoType: v.Type(), AvroType: "double"}
	}
	v.SetFloat(float64(val))
	return src, nil
}

func promoteFloatToDouble(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
	u, src, err := readUint32(src)
	if err != nil {
		return nil, err
	}
	f := float64(math.Float32frombits(u))
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(f))
		return src, nil
	}
	if !v.CanFloat() {
		return nil, &SemanticError{GoType: v.Type(), AvroType: "double"}
	}
	v.SetFloat(f)
	return src, nil
}

func promoteStringToBytes(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
	length, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, &SemanticError{AvroType: "bytes"}
	}
	n := int(length)
	if len(src) < n {
		return nil, &ShortBufferError{Type: "string", Need: n, Have: len(src)}
	}
	b := make([]byte, n)
	copy(b, src[:n])
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(b))
		return src[n:], nil
	}
	if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		v.SetBytes(b)
		return src[n:], nil
	}
	return nil, &SemanticError{GoType: v.Type(), AvroType: "bytes"}
}

func promoteBytesToString(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
	length, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, &SemanticError{AvroType: "string"}
	}
	n := int(length)
	if len(src) < n {
		return nil, &ShortBufferError{Type: "bytes", Need: n, Have: len(src)}
	}
	s := string(src[:n])
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(s))
		return src[n:], nil
	}
	if v.Kind() == reflect.String {
		v.SetString(s)
		return src[n:], nil
	}
	return nil, &SemanticError{GoType: v.Type(), AvroType: "string"}
}
