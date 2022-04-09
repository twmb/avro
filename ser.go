package main

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"
)

func (s *Schema) AppendEncode(dst []byte, v interface{}) ([]byte, error) {
	return s.s.ser(dst, reflect.ValueOf(v))
}

func (s *aschema) ser(dst []byte, v reflect.Value) ([]byte, error) {
	switch {
	case len(s.primitive) > 0:
		return serPrimitive(dst, s.primitive, v)
	case s.object != nil:
		return serComplex(dst, s.object, v)
	case len(s.union) > 0:
		start := len(dst)
		var err error
		for i := range s.union {
			dst = appendVarint(dst[:start], int32(i))
			dst, err = s.union[i].ser(dst, v)
			if err == nil {
				return dst, nil
			}
		}
		return nil, errors.New("unable to encode into any union option")
	}
	panic("unreachable")
}

///////////////
// PRIMITIVE //
///////////////

// For unions, we try encoding across all values until one works, and often we
// hit "null" at the start with an error. This error is saved to avoid allocs.
var errNonNil = errors.New("cannot encode non-nil value as null")

func serPrimitive(dst []byte, primitive string, v reflect.Value) ([]byte, error) {
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			if primitive == "null" {
				return dst, nil
			}
			return nil, fmt.Errorf("cannot encode nil as %s", primitive)
		}
		v = reflect.Indirect(v)
	}
	if v == (reflect.Value{}) {
		return nil, errors.New("cannot encode nil in non-union type")
	}

	switch primitive {
	case "null":
		return dst, errNonNil

	case "boolean":
		if v.Kind() != reflect.Bool {
			return nil, fmt.Errorf("cannot encode %s as boolean", v.Type())
		}
		if v.Bool() {
			return append(dst, 1), nil
		}
		return append(dst, 0), nil

	case "int":
		switch {
		case v.CanInt():
			return appendVarint(dst, int32(v.Int())), nil
		case v.CanUint():
			return appendVarint(dst, int32(v.Uint())), nil
		default:
			return nil, fmt.Errorf("cannot encode %s as int", v.Type())
		}

	case "long":
		switch {
		case v.CanInt():
			return appendVarlong(dst, v.Int()), nil
		case v.CanUint():
			return appendVarlong(dst, int64(v.Uint())), nil
		default:
			return nil, fmt.Errorf("cannot encode %s as long", v.Type())
		}

	case "float":
		if !v.CanFloat() {
			return nil, fmt.Errorf("cannot encode %s as float", v.Type())
		}
		return appendUint32(dst, math.Float32bits(float32(v.Float()))), nil

	case "double":
		if !v.CanFloat() {
			return nil, fmt.Errorf("cannot encode %s as double", v.Type())
		}
		return appendUint64(dst, math.Float64bits(v.Float())), nil

	case "bytes":
		if (v.Kind() != reflect.Array && v.Kind() != reflect.Slice) || v.Elem().Kind() != reflect.Uint8 {
			return nil, fmt.Errorf("cannot encode %s as bytes", v.Type())
		}
		return serBytes(dst, v), nil

	case "string":
		if v.Kind() == reflect.String {
			return serString(dst, v.String()), nil
		}
		if stringer, ok := v.Interface().(interface {
			String() string
		}); ok {
			return serString(dst, stringer.String()), nil
		}
		return nil, fmt.Errorf("cannot encode %s as string", v.Type())

	default:
		panic("unreachable")
	}
}

////////////////////
// STRING & BYTES //
////////////////////

func serBytes(dst []byte, v reflect.Value) []byte {
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

func serString(dst []byte, s string) []byte {
	dst = appendVarlong(dst, int64(len(s)))
	return append(dst, s...)
}

/////////////
// COMPLEX //
/////////////

func serComplex(dst []byte, o *aobject, v reflect.Value) ([]byte, error) {
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, errors.New("cannot encode nil in non-union type")
		}
		v = reflect.Indirect(v)
	}
	if v == (reflect.Value{}) {
		return nil, errors.New("cannot encode nil in non-union type")
	}

	var err error
	switch {
	case len(o.Fields) > 0:
		t := v.Type()
		k := t.Kind()
		if k != reflect.Struct && (k != reflect.Map || k == reflect.Map && t.Key().Kind() != reflect.String) {
			return nil, fmt.Errorf("cannot encode %s as record", t)
		}
		if k == reflect.Map {
			for _, f := range o.Fields {
				value := v.MapIndex(reflect.ValueOf(f.Name))
				if value.IsZero() {
					return nil, fmt.Errorf("missing key %s", f.Name)
				}
				if dst, err = f.Type.ser(dst, value); err != nil {
					return nil, err
				}
			}
			return dst, nil
		}
		ats, err := o.typeFieldMapping(t)
		if err != nil {
			return nil, err
		}
		for i := range o.Fields {
			if dst, err = o.Fields[i].Type.ser(dst, v.Field(ats[i])); err != nil {
				return nil, fmt.Errorf("record type %s field %s error: %v", t, o.Fields[i].Name, err)
			}
		}
		return dst, nil

	case len(o.Symbols) > 0:
		switch {
		case v.Kind() == reflect.String:
			needle := v.String()
			for i, symbol := range o.Symbols {
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
			if n < 0 || n > len(o.Symbols) {
				return nil, fmt.Errorf("invalid enum index %d/%d", n, len(o.Symbols))
			}
			return appendVarint(dst, int32(n)), nil

		default:
			return nil, fmt.Errorf("cannot encode %s as enum", v.Type())
		}

	case o.Items != nil:
		if v.Kind() != reflect.Array && v.Kind() != reflect.Slice {
			return nil, fmt.Errorf("cannot encode %s as array", v.Type())
		}
		l := v.Len()
		dst = appendVarlong(dst, int64(l))
		if l == 0 {
			return dst, nil
		}
		for i := 0; i < l; i++ {
			if dst, err = o.Items.ser(dst, v.Index(i)); err != nil {
				return nil, err
			}
		}
		return append(dst, 0), nil

	case o.Values != nil:
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
			dst = serString(dst, iter.Key().String())
			if dst, err = o.Values.ser(dst, iter.Value()); err != nil {
				return dst, err
			}
		}
		return append(dst, 0), nil

	case o.Size != nil:
		t := v.Type()
		if t.Kind() != reflect.Array || t.Elem().Kind() != reflect.Uint8 {
			return nil, fmt.Errorf("cannot encode %s as fixed", t)
		}
		if t.Len() != *o.Size {
			return nil, fmt.Errorf("cannot encode %s as fixed of size %d", t, *o.Size)
		}
		return serBytes(dst, v), nil
	}

	// We create encoders on the input schema, which is not canonicalized
	// (because we support logical types). If this is a primitive with an
	// unsupported logical type, we use our serPrimitive.
	switch o.Logical {
	case "date",
		"timestamp-millis",
		"timestamp-micros",
		"local-timestamp-millis",
		"local-timestamp-micros":
		if v.Type() != reflect.TypeOf(time.Time{}) {
			return nil, fmt.Errorf("cannot encode %v as %s", v, o.Logical)
		}
		switch o.Logical {
		case "date":
			return serDate(dst, v), nil
		case "timestamp-millis", "local-timestamp-millis":
			return serTimestampMillis(dst, v), nil
		case "timestamp-micros", "local-timestamp-micros":
			return serTimestampMicros(dst, v), nil
		}
	case "duration":
		if v.Type() != reflect.TypeOf(time.Duration(0)) {
			return nil, fmt.Errorf("cannot encode %s as %s", v, o.Logical)
		}
		return serDuration(dst, v), nil
	}
	return serPrimitive(dst, o.Type, v)
}

/////////////
// LOGICAL //
/////////////

func yearDays(year int) int {
	return time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC).Add(-1).YearDay()
}

func serDate(dst []byte, v reflect.Value) []byte {
	t := v.Interface().(time.Time)

	var days int
	if t.Before(unixEpoch) {
		for y := t.Year() + 1; y != 1970; y++ {
			days -= yearDays(y)
		}
		days -= (yearDays(t.Year()) - t.YearDay() - 1)
	} else {
		for y := 1970; y < t.Year(); y++ {
			days += yearDays(y)
		}
		days += t.YearDay()
	}
	return appendVarint(dst, int32(days))
}

func serTimestampMillis(dst []byte, v reflect.Value) []byte {
	t := v.Interface().(time.Time)
	millis := t.Sub(unixEpoch).Milliseconds()
	return appendVarlong(dst, int64(millis))
}

func serTimestampMicros(dst []byte, v reflect.Value) []byte {
	t := v.Interface().(time.Time)
	micros := t.Sub(unixEpoch).Microseconds()
	return appendVarlong(dst, int64(micros))
}

func serDuration(dst []byte, v reflect.Value) []byte {
	d := v.Interface().(time.Duration)
	days := uint16(d.Hours()) / 24

	remns := d - time.Duration(days)*24*time.Hour
	ms := uint16(remns.Milliseconds())

	return append(dst,
		0, 0, // no months; all durations can be represented in days and millis
		uint8(days), uint8(days>>8), // little endian
		uint8(ms), uint8(ms>>8),
	)
}
