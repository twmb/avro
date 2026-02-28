package avro

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
)

type deserfn func(src []byte, v reflect.Value) ([]byte, error)

var anyType = reflect.TypeOf((*any)(nil)).Elem()

func (s *Schema) Decode(src []byte, v interface{}) ([]byte, error) {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return nil, errors.New("decode requires a non-nil pointer")
	}
	return s.deser(src, rv.Elem())
}

///////////
// UNION //
///////////

type deserUnion struct {
	fns []deserfn
}

func (s *deserUnion) deser(src []byte, v reflect.Value) ([]byte, error) {
	idx, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	if idx < 0 || int(idx) >= len(s.fns) {
		return nil, fmt.Errorf("union index %d out of range [0, %d)", idx, len(s.fns))
	}
	return s.fns[idx](src, v)
}

func deserNullUnion(u *deserUnion) deserfn {
	return func(src []byte, v reflect.Value) ([]byte, error) {
		if len(src) < 1 {
			return nil, errors.New("short buffer for union index")
		}
		switch src[0] {
		case 0:
			switch v.Kind() {
			case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice:
				v.Set(reflect.Zero(v.Type()))
			}
			return src[1:], nil
		case 2:
			if v.Kind() == reflect.Ptr {
				if v.IsNil() {
					v.Set(reflect.New(v.Type().Elem()))
				}
				return u.fns[1](src[1:], v.Elem())
			}
			return u.fns[1](src[1:], v)
		default:
			return nil, fmt.Errorf("invalid null-union index byte 0x%02x", src[0])
		}
	}
}

////////////////
// PRIMITIVES //
////////////////

var deserPrimitive = map[string]deserfn{
	"null":    deserNull,
	"boolean": deserBoolean,
	"int":     deserInt,
	"long":    deserLong,
	"float":   deserFloat,
	"double":  deserDouble,
	"bytes":   deserBytes,
	"string":  deserString,
}

func deserNull(src []byte, v reflect.Value) ([]byte, error) {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice:
		v.Set(reflect.Zero(v.Type()))
	}
	return src, nil
}

func deserBoolean(src []byte, v reflect.Value) ([]byte, error) {
	if len(src) < 1 {
		return nil, errors.New("short buffer for boolean")
	}
	b := src[0] != 0
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(b))
		return src[1:], nil
	}
	if v.Kind() != reflect.Bool {
		return nil, fmt.Errorf("cannot decode boolean into %s", v.Type())
	}
	v.SetBool(b)
	return src[1:], nil
}

func deserInt(src []byte, v reflect.Value) ([]byte, error) {
	val, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(val))
		return src, nil
	}
	if v.CanInt() {
		v.SetInt(int64(val))
	} else if v.CanUint() {
		v.SetUint(uint64(val))
	} else {
		return nil, fmt.Errorf("cannot decode int into %s", v.Type())
	}
	return src, nil
}

func deserLong(src []byte, v reflect.Value) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(val))
		return src, nil
	}
	if v.CanInt() {
		v.SetInt(val)
	} else if v.CanUint() {
		v.SetUint(uint64(val))
	} else {
		return nil, fmt.Errorf("cannot decode long into %s", v.Type())
	}
	return src, nil
}

func deserFloat(src []byte, v reflect.Value) ([]byte, error) {
	u, src, err := readUint32(src)
	if err != nil {
		return nil, err
	}
	f := math.Float32frombits(u)
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(f))
		return src, nil
	}
	if !v.CanFloat() {
		return nil, fmt.Errorf("cannot decode float into %s", v.Type())
	}
	v.SetFloat(float64(f))
	return src, nil
}

func deserDouble(src []byte, v reflect.Value) ([]byte, error) {
	u, src, err := readUint64(src)
	if err != nil {
		return nil, err
	}
	f := math.Float64frombits(u)
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(f))
		return src, nil
	}
	if !v.CanFloat() {
		return nil, fmt.Errorf("cannot decode double into %s", v.Type())
	}
	v.SetFloat(f)
	return src, nil
}

func deserBytes(src []byte, v reflect.Value) ([]byte, error) {
	length, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, fmt.Errorf("invalid negative bytes length %d", length)
	}
	n := int(length)
	if len(src) < n {
		return nil, fmt.Errorf("short buffer for bytes: need %d, have %d", n, len(src))
	}
	b := make([]byte, n)
	copy(b, src[:n])
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(b))
		return src[n:], nil
	}
	switch v.Kind() {
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.Uint8 {
			return nil, fmt.Errorf("cannot decode bytes into %s", v.Type())
		}
		v.SetBytes(b)
	case reflect.Array:
		if v.Type().Elem().Kind() != reflect.Uint8 {
			return nil, fmt.Errorf("cannot decode bytes into %s", v.Type())
		}
		if v.Len() != n {
			return nil, fmt.Errorf("cannot decode %d bytes into array of length %d", n, v.Len())
		}
		reflect.Copy(v, reflect.ValueOf(src[:n]))
	default:
		return nil, fmt.Errorf("cannot decode bytes into %s", v.Type())
	}
	return src[n:], nil
}

func deserString(src []byte, v reflect.Value) ([]byte, error) {
	length, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, fmt.Errorf("invalid negative string length %d", length)
	}
	n := int(length)
	if len(src) < n {
		return nil, fmt.Errorf("short buffer for string: need %d, have %d", n, len(src))
	}
	s := string(src[:n])
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(s))
		return src[n:], nil
	}
	if v.Kind() != reflect.String {
		return nil, fmt.Errorf("cannot decode string into %s", v.Type())
	}
	v.SetString(s)
	return src[n:], nil
}

/////////////
// COMPLEX //
/////////////

type deserRecordField struct {
	name     string
	fn       deserfn
	avroType string
	meta     *fieldMeta
}

type deserRecord struct {
	fields []deserRecordField
	names  []string
	cache  sync.Map                         // map[reflect.Type][][]int
	fast   atomic.Pointer[fastRecordDeser]  // precompiled unsafe fast path
}

func (s *deserRecord) deser(src []byte, v reflect.Value) ([]byte, error) {
	v = indirectAlloc(v)
	k := v.Kind()
	if k == reflect.Interface {
		// Generic decode: create map[string]any.
		m := make(map[string]any, len(s.fields))
		var err error
		for _, f := range s.fields {
			elem := reflect.New(anyType).Elem()
			if src, err = f.fn(src, elem); err != nil {
				return nil, fmt.Errorf("record field %s error: %v", f.name, err)
			}
			m[f.name] = elem.Interface()
		}
		v.Set(reflect.ValueOf(m))
		return src, nil
	}
	t := v.Type()
	if k != reflect.Struct && (k != reflect.Map || t.Key().Kind() != reflect.String) {
		return nil, fmt.Errorf("cannot decode record into %s", t)
	}
	var err error
	if k == reflect.Map {
		if v.IsNil() {
			v.Set(reflect.MakeMap(t))
		}
		for _, f := range s.fields {
			elem := reflect.New(t.Elem()).Elem()
			if src, err = f.fn(src, elem); err != nil {
				return nil, fmt.Errorf("record field %s error: %v", f.name, err)
			}
			v.SetMapIndex(reflect.ValueOf(f.name), elem)
		}
		return src, nil
	}
	// Struct: try precompiled unsafe fast path.
	if v.CanAddr() {
		if fast := s.fast.Load(); fast != nil && fast.typ == t {
			return deserRecordFast(src, fast, v)
		}
		if fast := compileFastDeser(s.fields, s.names, &s.cache, t); fast != nil {
			s.fast.Store(fast)
			return deserRecordFast(src, fast, v)
		}
	}
	// compileFastDeser returned nil because typeFieldMapping failed;
	// re-call to surface the error.
	_, err = typeFieldMapping(s.names, &s.cache, t)
	return nil, err
}

type deserEnum struct {
	symbols []string
}

func (s *deserEnum) deser(src []byte, v reflect.Value) ([]byte, error) {
	idx, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	if idx < 0 || int(idx) >= len(s.symbols) {
		return nil, fmt.Errorf("enum index %d out of range [0, %d)", idx, len(s.symbols))
	}
	v = indirectAlloc(v)
	switch {
	case v.Kind() == reflect.Interface:
		v.Set(reflect.ValueOf(s.symbols[idx]))
	case v.Kind() == reflect.String:
		v.SetString(s.symbols[idx])
	case v.CanInt():
		v.SetInt(int64(idx))
	case v.CanUint():
		v.SetUint(uint64(idx))
	default:
		return nil, fmt.Errorf("cannot decode enum into %s", v.Type())
	}
	return src, nil
}

type deserArray struct {
	deserItem deserfn
}

func (s *deserArray) deser(src []byte, v reflect.Value) ([]byte, error) {
	v = indirectAlloc(v)
	iface := v.Kind() == reflect.Interface
	if !iface && v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("cannot decode array into %s", v.Type())
	}
	// For interface targets, build a []any.
	var sliceVal reflect.Value
	if iface {
		sliceVal = reflect.MakeSlice(reflect.SliceOf(anyType), 0, 0)
	} else {
		v.SetLen(0)
		sliceVal = v
	}
	var err error
	for {
		var count int64
		count, src, err = readVarlong(src)
		if err != nil {
			return nil, err
		}
		if count == 0 {
			if iface {
				v.Set(sliceVal)
			}
			return src, nil
		}
		if count < 0 {
			count = -count
			if count < 0 {
				return nil, errors.New("invalid array block count")
			}
			_, src, err = readVarlong(src) // skip block size
			if err != nil {
				return nil, err
			}
		}
		n := int(count)
		start := sliceVal.Len()
		newLen := start + n
		if sliceVal.Cap() < newLen {
			ns := reflect.MakeSlice(sliceVal.Type(), newLen, newLen)
			reflect.Copy(ns, sliceVal)
			sliceVal = ns
			if !iface {
				v.Set(sliceVal)
			}
		} else {
			sliceVal.SetLen(newLen)
		}
		elemType := sliceVal.Type().Elem()
		if elemType.Kind() == reflect.Ptr {
			innerType := elemType.Elem()
			backing := reflect.MakeSlice(reflect.SliceOf(innerType), n, n)
			for i := 0; i < n; i++ {
				sliceVal.Index(start + i).Set(backing.Index(i).Addr())
			}
		}
		for i := start; i < newLen; i++ {
			src, err = s.deserItem(src, sliceVal.Index(i))
			if err != nil {
				return nil, err
			}
		}
	}
}

type deserMap struct {
	deserItem deserfn
}

func (s *deserMap) deser(src []byte, v reflect.Value) ([]byte, error) {
	v = indirectAlloc(v)
	iface := v.Kind() == reflect.Interface
	var (
		mapVal  reflect.Value
		elemTyp reflect.Type
	)
	if iface {
		mapVal = reflect.MakeMap(reflect.MapOf(reflect.TypeOf(""), anyType))
		elemTyp = anyType
	} else {
		t := v.Type()
		if t.Kind() != reflect.Map || t.Key().Kind() != reflect.String {
			return nil, fmt.Errorf("cannot decode map into %s", t)
		}
		if v.IsNil() {
			v.Set(reflect.MakeMap(t))
		}
		mapVal = v
		elemTyp = t.Elem()
	}
	var err error
	for {
		var count int64
		count, src, err = readVarlong(src)
		if err != nil {
			return nil, err
		}
		if count == 0 {
			if iface {
				v.Set(mapVal)
			}
			return src, nil
		}
		if count < 0 {
			count = -count
			if count < 0 {
				return nil, errors.New("invalid map block count")
			}
			_, src, err = readVarlong(src) // skip block size
			if err != nil {
				return nil, err
			}
		}
		for range int(count) {
			var keyLen int64
			keyLen, src, err = readVarlong(src)
			if err != nil {
				return nil, err
			}
			if keyLen < 0 || int(keyLen) > len(src) {
				return nil, fmt.Errorf("invalid map key length %d", keyLen)
			}
			key := string(src[:int(keyLen)])
			src = src[int(keyLen):]

			elem := reflect.New(elemTyp).Elem()
			src, err = s.deserItem(src, elem)
			if err != nil {
				return nil, err
			}
			mapVal.SetMapIndex(reflect.ValueOf(key), elem)
		}
	}
}

type deserFixed struct {
	n int
}

func (s *deserFixed) deser(src []byte, v reflect.Value) ([]byte, error) {
	if len(src) < s.n {
		return nil, fmt.Errorf("short buffer for fixed: need %d, have %d", s.n, len(src))
	}
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		b := make([]byte, s.n)
		copy(b, src[:s.n])
		v.Set(reflect.ValueOf(b))
		return src[s.n:], nil
	}
	t := v.Type()
	if t.Kind() != reflect.Array || t.Elem().Kind() != reflect.Uint8 {
		return nil, fmt.Errorf("cannot decode fixed into %s", t)
	}
	if t.Len() != s.n {
		return nil, fmt.Errorf("cannot decode fixed of size %d into array of length %d", s.n, t.Len())
	}
	reflect.Copy(v, reflect.ValueOf(src[:s.n]))
	return src[s.n:], nil
}
