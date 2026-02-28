package avro

import (
	"encoding"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
)

type serfn func([]byte, reflect.Value) ([]byte, error)

func (s *Schema) AppendEncode(dst []byte, v interface{}) ([]byte, error) {
	return s.ser(dst, reflect.ValueOf(v))
}

///////////
// UNION //
///////////

type serUnion struct {
	fns []serfn
}

func (s *serUnion) ser(dst []byte, v reflect.Value) ([]byte, error) {
	start := len(dst)
	var err error
	for i, fn := range s.fns {
		dst = appendVarint(dst[:start], int32(i))
		if dst, err = fn(dst, v); err == nil {
			return dst, nil
		}
	}
	return nil, errors.New("unable to encode into any union option")
}

func serNullUnion(u *serUnion) serfn {
	return func(dst []byte, v reflect.Value) ([]byte, error) {
		switch v.Kind() {
		case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice:
			if v.IsNil() {
				return append(dst, 0), nil
			}
		}
		return u.fns[1](append(dst, 2), v)
	}
}

////////////////
// PRIMITIVES //
////////////////

var serPrimitive = map[string]serfn{
	"null":    serNull,
	"boolean": serBoolean,
	"int":     serInt,
	"long":    serLong,
	"float":   serFloat,
	"double":  serDouble,
	"bytes":   serBytes,
	"string":  serString,
}

// For unions, we try encoding across all values until one works, and often we
// hit "null" at the start with an error. This error is saved to avoid allocs.
var errNonNil = errors.New("cannot encode non-nil value as null")

func serNull(dst []byte, v reflect.Value) ([]byte, error) {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
		if v.IsNil() {
			return dst, nil
		}
	}
	return dst, errNonNil
}

func serBoolean(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.Kind() != reflect.Bool {
		return nil, fmt.Errorf("cannot encode %s as boolean", v.Type())
	}
	if v.Bool() {
		return append(dst, 1), nil
	}
	return append(dst, 0), nil
}

func serInt(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.CanInt() {
		return appendVarint(dst, int32(v.Int())), nil
	} else if v.CanUint() {
		return appendVarint(dst, int32(v.Uint())), nil
	}
	return nil, fmt.Errorf("cannot encode %s as int", v.Type())
}

func serLong(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.CanInt() {
		return appendVarlong(dst, int64(v.Int())), nil
	} else if v.CanUint() {
		return appendVarlong(dst, int64(v.Uint())), nil
	}
	return nil, fmt.Errorf("cannot encode %s as long", v.Type())
}

func serFloat(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.CanFloat() {
		return appendUint32(dst, math.Float32bits(float32(v.Float()))), nil
	}
	return nil, fmt.Errorf("cannot encode %s as float", v.Type())
}

func serDouble(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.CanFloat() {
		return appendUint64(dst, math.Float64bits(v.Float())), nil
	}
	return nil, fmt.Errorf("cannot encode %s as double", v.Type())
}

func serBytes(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if (v.Kind() != reflect.Array && v.Kind() != reflect.Slice) || v.Type().Elem().Kind() != reflect.Uint8 {
		return nil, fmt.Errorf("cannot encode %s as bytes", v.Type())
	}
	return doSerBytes(dst, v), nil
}

func serString(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.Kind() == reflect.String {
		return doSerString(dst, v.String()), nil
	}

	if v.CanInterface() {
		i := v.Interface()
		if s, ok := i.(stringer); ok {
			return doSerString(dst, s.String()), nil
		}
		if m, ok := i.(encoding.TextMarshaler); ok {
			text, err := m.MarshalText()
			if err != nil {
				return nil, err
			}
			return doSerString(dst, string(text)), nil
		}
	}
	return nil, fmt.Errorf("cannot encode %s as string", v.Type())
}

////////////////////
// STRING & BYTES //
////////////////////

func doSerBytes(dst []byte, v reflect.Value) []byte {
	l := v.Len()
	dst = appendVarlong(dst, int64(l))
	if l == 0 {
		return dst
	}
	if v.CanAddr() {
		return append(dst, v.Slice(0, l).Bytes()...)
	}
	for i := 0; i < l; i++ {
		dst = append(dst, v.Index(i).Interface().(byte))
	}
	return dst
}

func doSerString(dst []byte, s string) []byte {
	dst = appendVarlong(dst, int64(len(s)))
	return append(dst, s...)
}

/////////////
// COMPLEX //
/////////////

type serRecordField struct {
	name     string
	fn       serfn
	avroType string
	meta     *fieldMeta
}

type serRecord struct {
	fields []serRecordField
	names  []string
	cache  sync.Map                       // map[reflect.Type][][]int
	fast   atomic.Pointer[fastRecordSer]  // precompiled unsafe fast path
}

func (s *serRecord) ser(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	k := v.Kind()
	t := v.Type()
	if k != reflect.Struct && (k != reflect.Map || t.Key().Kind() != reflect.String) {
		return nil, fmt.Errorf("cannot encode %s as record", t)
	}
	if k == reflect.Map {
		for _, f := range s.fields {
			value := v.MapIndex(reflect.ValueOf(f.name))
			if !value.IsValid() {
				return nil, fmt.Errorf("missing key %s", f.name)
			}
			if dst, err = f.fn(dst, value); err != nil {
				return nil, err
			}
		}
		return dst, nil
	}
	// Struct: try precompiled unsafe fast path.
	if v.CanAddr() {
		if fast := s.fast.Load(); fast != nil && fast.typ == t {
			return serRecordFast(dst, fast, v)
		}
		if fast := compileFastSer(s.fields, s.names, &s.cache, t); fast != nil {
			s.fast.Store(fast)
			return serRecordFast(dst, fast, v)
		}
	}
	// Slow path: reflect-based field access.
	ats, err := typeFieldMapping(s.names, &s.cache, t)
	if err != nil {
		return nil, err
	}
	for i, f := range s.fields {
		if dst, err = f.fn(dst, v.FieldByIndex(ats[i])); err != nil {
			return nil, fmt.Errorf("record type %s field %s error: %v", t, f.name, err)
		}
	}
	return dst, nil
}

type serEnum struct {
	symbols []string
}

func (s *serEnum) ser(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	switch {
	case v.Kind() == reflect.String:
		needle := v.String()
		for i, symbol := range s.symbols {
			if symbol == needle {
				return appendVarint(dst, int32(i)), nil
			}
		}
		return nil, fmt.Errorf("unknown enum symbol %q", needle)

	case v.CanInt() || v.CanUint():
		var n int
		if v.CanInt() {
			n = int(v.Int())
		} else {
			n = int(v.Uint())
		}
		if n < 0 || n >= len(s.symbols) {
			return nil, fmt.Errorf("invalid enum index %d/%d", n, len(s.symbols))
		}
		return appendVarint(dst, int32(n)), nil

	default:
		return nil, fmt.Errorf("cannot encode %s as enum", v.Type())
	}
}

type serArray struct {
	serItem serfn
}

func (s *serArray) ser(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.Kind() != reflect.Array && v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("cannot encode %s as array", v.Type())
	}
	l := v.Len()
	dst = appendVarlong(dst, int64(l))
	if l == 0 {
		return dst, nil
	}
	for i := 0; i < l; i++ {
		if dst, err = s.serItem(dst, v.Index(i)); err != nil {
			return nil, err
		}
	}
	return append(dst, 0), nil
}

type serMap struct {
	serItem serfn
}

func (s *serMap) ser(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	t := v.Type()
	if t.Kind() != reflect.Map || t.Key().Kind() != reflect.String {
		return nil, fmt.Errorf("cannot encode %s as map", t)
	}
	l := v.Len()
	dst = appendVarlong(dst, int64(l))
	if l == 0 {
		return dst, nil
	}
	iter := v.MapRange()
	for iter.Next() {
		dst = doSerString(dst, iter.Key().String())
		if dst, err = s.serItem(dst, iter.Value()); err != nil {
			return nil, err
		}
	}
	return append(dst, 0), nil
}

type serSize struct {
	n int
}

func (s *serSize) ser(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	t := v.Type()
	if t.Kind() != reflect.Array || t.Elem().Kind() != reflect.Uint8 {
		return nil, fmt.Errorf("cannot encode %s as fixed", t)
	}
	if t.Len() != s.n {
		return nil, fmt.Errorf("cannot encode %s as fixed of size %d", t, s.n)
	}
	// Fixed is written as raw bytes with no length prefix.
	if v.CanAddr() {
		return append(dst, v.Slice(0, s.n).Bytes()...), nil
	}
	for i := 0; i < s.n; i++ {
		dst = append(dst, v.Index(i).Interface().(byte))
	}
	return dst, nil
}

