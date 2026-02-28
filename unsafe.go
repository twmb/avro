package avro

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"
	"unsafe"
)

// userfn serializes the value at p into dst. p points directly to the Go
// field's memory; for reading only.
type userfn func(dst []byte, p unsafe.Pointer) ([]byte, error)

// udeserfn deserializes from src into the value at p.
type udeserfn func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error)

type fastRecordSer struct {
	typ     reflect.Type
	allFast bool
	fields  []fastFieldSer
}

// fastFieldSer uses a hybrid approach: primitive fields get the unsafe fast
// path (ser != nil), complex/union fields use reflect-based FieldByIndex
// (slowFn != nil). This avoids the overhead of reflect.NewAt that the
// previous wrapSlowSer approach incurred.
type fastFieldSer struct {
	offset   uintptr
	name     string
	ser      userfn // non-nil for unsafe-optimized fields (primitives)
	slowFn   serfn  // non-nil for reflect-based fields (complex types)
	slowIdx  []int  // field index path for FieldByIndex (used with slowFn)
	omitzero bool   // if true and field is nullunion, check IsZero before ser
}

type fastRecordDeser struct {
	typ     reflect.Type
	allFast bool
	fields  []fastFieldDeser
}

type fastFieldDeser struct {
	offset  uintptr
	name    string
	deser   udeserfn // non-nil for unsafe-optimized fields (primitives)
	slowFn  deserfn  // non-nil for reflect-based fields (complex types)
	slowIdx []int    // field index path for fieldByIndex (used with slowFn)
}

func compileFastSer(fields []serRecordField, names []string, cache *sync.Map, t reflect.Type) *fastRecordSer {
	mapping, err := typeFieldMapping(names, cache, t)
	if err != nil {
		return nil
	}
	fast := &fastRecordSer{typ: t, fields: make([]fastFieldSer, len(fields))}
	allFast := true
	for i := range fields {
		f := &fields[i]
		offset, goType, ok := computeFieldOffset(t, mapping.indices[i])
		oz := mapping.omitzero[i] && f.avroType == "nullunion"
		var fn userfn
		if ok && !oz {
			fn = tryCompileFieldSer(f, goType)
		}
		if fn != nil {
			fast.fields[i] = fastFieldSer{
				offset: offset,
				name:   f.name,
				ser:    fn,
			}
		} else {
			allFast = false
			fast.fields[i] = fastFieldSer{
				name:     f.name,
				slowFn:   f.fn,
				slowIdx:  mapping.indices[i],
				omitzero: oz,
			}
		}
	}
	fast.allFast = allFast
	return fast
}

func compileFastDeser(fields []deserRecordField, names []string, cache *sync.Map, t reflect.Type) *fastRecordDeser {
	mapping, err := typeFieldMapping(names, cache, t)
	if err != nil {
		return nil
	}
	fast := &fastRecordDeser{typ: t, fields: make([]fastFieldDeser, len(fields))}
	allFast := true
	for i := range fields {
		f := &fields[i]
		offset, goType, ok := computeFieldOffset(t, mapping.indices[i])
		var fn udeserfn
		if ok {
			fn = tryCompileFieldDeser(f, goType)
		}
		if fn != nil {
			fast.fields[i] = fastFieldDeser{
				offset: offset,
				name:   f.name,
				deser:  fn,
			}
		} else {
			allFast = false
			fast.fields[i] = fastFieldDeser{
				name:    f.name,
				slowFn:  f.fn,
				slowIdx: mapping.indices[i],
			}
		}
	}
	fast.allFast = allFast
	return fast
}

// computeFieldOffset computes a flat byte offset for a struct field index
// path. Returns false if the path goes through a pointer (which requires
// runtime dereferencing and cannot be precomputed).
func computeFieldOffset(t reflect.Type, index []int) (uintptr, reflect.Type, bool) {
	var offset uintptr
	for _, i := range index {
		if t.Kind() == reflect.Ptr {
			return 0, nil, false
		}
		f := t.Field(i)
		offset += f.Offset
		t = f.Type
	}
	return offset, t, true
}

func serRecordFast(dst []byte, fast *fastRecordSer, v reflect.Value) ([]byte, error) {
	base := v.Addr().UnsafePointer()
	var err error
	for i := range fast.fields {
		f := &fast.fields[i]
		if f.ser != nil {
			dst, err = f.ser(dst, unsafe.Add(base, f.offset))
		} else {
			fv := v.FieldByIndex(f.slowIdx)
			if f.omitzero && valueIsZero(fv) {
				dst = append(dst, 0)
				continue
			}
			dst, err = f.slowFn(dst, fv)
		}
		if err != nil {
			return nil, &SemanticError{GoType: fast.typ, AvroType: "record", Field: f.name, Err: err}
		}
	}
	return dst, nil
}

func deserRecordFast(src []byte, fast *fastRecordDeser, v reflect.Value, sl *slab) ([]byte, error) {
	base := v.Addr().UnsafePointer()
	var err error
	for i := range fast.fields {
		f := &fast.fields[i]
		if f.deser != nil {
			src, err = f.deser(src, unsafe.Add(base, f.offset), sl)
		} else {
			src, err = f.slowFn(src, fieldByIndex(v, f.slowIdx), sl)
		}
		if err != nil {
			return nil, &SemanticError{GoType: fast.typ, AvroType: "record", Field: f.name, Err: err}
		}
	}
	return src, nil
}

// serRecordFastPtr serializes a record when all fields have unsafe ser fns.
// Only requires a raw pointer to the struct base — no reflect.Value needed.
func serRecordFastPtr(dst []byte, fast *fastRecordSer, base unsafe.Pointer) ([]byte, error) {
	var err error
	for i := range fast.fields {
		f := &fast.fields[i]
		dst, err = f.ser(dst, unsafe.Add(base, f.offset))
		if err != nil {
			return nil, &SemanticError{GoType: fast.typ, AvroType: "record", Field: f.name, Err: err}
		}
	}
	return dst, nil
}

// deserRecordFastPtr deserializes a record when all fields have unsafe deser fns.
func deserRecordFastPtr(src []byte, fast *fastRecordDeser, base unsafe.Pointer, sl *slab) ([]byte, error) {
	var err error
	for i := range fast.fields {
		f := &fast.fields[i]
		src, err = f.deser(src, unsafe.Add(base, f.offset), sl)
		if err != nil {
			return nil, &SemanticError{GoType: fast.typ, AvroType: "record", Field: f.name, Err: err}
		}
	}
	return src, nil
}

var errUnsafeNilPtr = fmt.Errorf("invalid nil in non-union, non-null")

// tryCompileFieldSer returns a userfn for fields that can be fully handled
// via unsafe pointer access. Returns nil for complex types that must use
// the reflect-based slow path.
func tryCompileFieldSer(f *serRecordField, goType reflect.Type) userfn {
	k := goType.Kind()

	// Regular unions need the reflect slow path.
	if f.avroType == "union" {
		return nil
	}

	// Null-union: *T mapped to ["null", T].
	if f.avroType == "nullunion" {
		if k != reflect.Ptr {
			return nil
		}
		if f.meta == nil || f.meta.inner == nil {
			return nil
		}
		inner := f.meta.inner
		innerGoType := goType.Elem()
		if inner.serRecord != nil {
			return usNullUnionRecord(inner.serRecord, innerGoType)
		}
		innerFn := tryCompileFieldSer(&serRecordField{avroType: inner.avroType, meta: inner}, innerGoType)
		if innerFn != nil {
			return usNullUnionPtr(innerFn)
		}
		return nil
	}

	// Array: []T or []*T.
	if f.avroType == "array" {
		if k != reflect.Slice {
			return nil
		}
		if f.meta == nil || f.meta.inner == nil {
			return nil
		}
		inner := f.meta.inner
		elemGoType := goType.Elem()
		switch inner.avroType {
		case "nullunion":
			if elemGoType.Kind() != reflect.Ptr {
				return nil
			}
			if inner.inner != nil && inner.inner.serRecord != nil {
				return usArrayNullUnionRecord(inner.inner.serRecord, elemGoType.Elem())
			}
			innerFn := tryCompileFieldSer(&serRecordField{avroType: inner.inner.avroType, meta: inner.inner}, elemGoType.Elem())
			if innerFn != nil {
				return usArrayNullUnionPtr(innerFn)
			}
		case "record":
			if inner.serRecord != nil {
				return usArrayRecord(inner.serRecord, elemGoType)
			}
		default:
			innerFn := tryCompileFieldSer(&serRecordField{avroType: inner.avroType, meta: inner}, elemGoType)
			if innerFn != nil {
				return usArrayDirect(innerFn, elemGoType.Size())
			}
		}
		return nil
	}

	// Record: struct T.
	if f.avroType == "record" {
		if k != reflect.Struct {
			return nil
		}
		if f.meta == nil || f.meta.serRecord == nil {
			return nil
		}
		rec := f.meta.serRecord
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			if fast := rec.fast.Load(); fast != nil && fast.typ == goType && fast.allFast {
				return serRecordFastPtr(dst, fast, p)
			}
			return rec.ser(dst, reflect.NewAt(goType, p).Elem())
		}
	}

	if k == reflect.Ptr {
		inner := tryCompileFieldSer(f, goType.Elem())
		if inner == nil {
			return nil
		}
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			pp := *(*unsafe.Pointer)(p)
			if pp == nil {
				return nil, errUnsafeNilPtr
			}
			return inner(dst, pp)
		}
	}

	// Logical type fast paths for time.Time and time.Duration.
	if f.meta != nil && f.meta.logical != "" {
		return tryCompileLogicalSer(f.meta.logical, goType)
	}

	switch f.avroType {
	case "boolean":
		if k == reflect.Bool {
			return usBool
		}
	case "int":
		return usInt(k)
	case "long":
		return usLong(k)
	case "float":
		return usFloat(k)
	case "double":
		return usDouble(k)
	case "string":
		if k == reflect.String {
			return usString
		}
	case "bytes":
		if k == reflect.Slice && goType.Elem().Kind() == reflect.Uint8 {
			return usBytes
		}
	}

	return nil
}

// tryCompileFieldDeser returns a udeserfn for fields that can be written
// directly via unsafe. Returns nil for complex types that must use the
// reflect-based slow path.
func tryCompileFieldDeser(f *deserRecordField, goType reflect.Type) udeserfn {
	k := goType.Kind()

	if f.avroType == "union" {
		return nil
	}

	// Null-union: *T mapped to ["null", T].
	if f.avroType == "nullunion" {
		if k != reflect.Ptr {
			return nil
		}
		if f.meta == nil || f.meta.inner == nil {
			return nil
		}
		inner := f.meta.inner
		innerGoType := goType.Elem()
		if inner.deserRecord != nil {
			return udNullUnionRecord(inner.deserRecord, innerGoType)
		}
		innerFn := tryCompileFieldDeser(&deserRecordField{avroType: inner.avroType, meta: inner}, innerGoType)
		if innerFn != nil {
			return udNullUnionPtr(innerFn, innerGoType)
		}
		return nil
	}

	// Array: []T or []*T.
	if f.avroType == "array" {
		if k != reflect.Slice {
			return nil
		}
		if f.meta == nil || f.meta.inner == nil {
			return nil
		}
		inner := f.meta.inner
		elemGoType := goType.Elem()
		switch inner.avroType {
		case "record":
			if inner.deserRecord != nil && elemGoType.Kind() == reflect.Ptr {
				return udArrayPtrRecord(inner.deserRecord, elemGoType.Elem(), goType)
			}
		default:
			innerFn := tryCompileFieldDeser(&deserRecordField{avroType: inner.avroType, meta: inner}, elemGoType)
			if innerFn != nil {
				return udArrayDirect(innerFn, elemGoType.Size(), goType)
			}
		}
		return nil
	}

	// Record: struct T.
	if f.avroType == "record" {
		if k != reflect.Struct {
			return nil
		}
		if f.meta == nil || f.meta.deserRecord == nil {
			return nil
		}
		rec := f.meta.deserRecord
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			if fast := rec.fast.Load(); fast != nil && fast.typ == goType && fast.allFast {
				return deserRecordFastPtr(src, fast, p, sl)
			}
			return rec.deser(src, reflect.NewAt(goType, p).Elem(), sl)
		}
	}

	// Pointer fields need GC write barriers for allocation; use slow path.
	if k == reflect.Ptr {
		return nil
	}

	// Logical type fast paths for time.Time and time.Duration.
	if f.meta != nil && f.meta.logical != "" {
		return tryCompileLogicalDeser(f.meta.logical, goType)
	}

	switch f.avroType {
	case "boolean":
		if k == reflect.Bool {
			return udBool
		}
	case "int":
		return udInt(k)
	case "long":
		return udLong(k)
	case "float":
		return udFloat(k)
	case "double":
		return udDouble(k)
	case "string":
		if k == reflect.String {
			return udStringDeser
		}
	case "bytes":
		if k == reflect.Slice && goType.Elem().Kind() == reflect.Uint8 {
			return udBytesDeser
		}
	}

	return nil
}

// ---- Logical type unsafe serializers ----

func tryCompileLogicalSer(logical string, goType reflect.Type) userfn {
	switch logical {
	case "timestamp-millis", "local-timestamp-millis":
		if goType == timeType {
			return usTimestampMillis
		}
		return usLong(goType.Kind())
	case "timestamp-micros", "local-timestamp-micros":
		if goType == timeType {
			return usTimestampMicros
		}
		return usLong(goType.Kind())
	case "date":
		if goType == timeType {
			return usDate
		}
		return usInt(goType.Kind())
	case "time-millis":
		if goType == durationType {
			return usTimeMillis
		}
		return usInt(goType.Kind())
	case "time-micros":
		if goType == durationType {
			return usTimeMicros
		}
		return usLong(goType.Kind())
	case "duration":
		if goType == avroDurationType {
			return usDuration
		}
		return nil
	default:
		return nil
	}
}

func tryCompileLogicalDeser(logical string, goType reflect.Type) udeserfn {
	switch logical {
	case "timestamp-millis", "local-timestamp-millis":
		if goType == timeType {
			return udTimestampMillis
		}
		return udLong(goType.Kind())
	case "timestamp-micros", "local-timestamp-micros":
		if goType == timeType {
			return udTimestampMicros
		}
		return udLong(goType.Kind())
	case "date":
		if goType == timeType {
			return udDate
		}
		return udInt(goType.Kind())
	case "time-millis":
		if goType == durationType {
			return udTimeMillis
		}
		return udInt(goType.Kind())
	case "time-micros":
		if goType == durationType {
			return udTimeMicros
		}
		return udLong(goType.Kind())
	case "duration":
		if goType == avroDurationType {
			return udDuration
		}
		return nil
	default:
		return nil
	}
}

func usTimestampMillis(dst []byte, p unsafe.Pointer) ([]byte, error) {
	t := *(*time.Time)(p)
	return appendVarlong(dst, t.UnixMilli()), nil
}

func usTimestampMicros(dst []byte, p unsafe.Pointer) ([]byte, error) {
	t := *(*time.Time)(p)
	return appendVarlong(dst, t.UnixMicro()), nil
}

func usDate(dst []byte, p unsafe.Pointer) ([]byte, error) {
	t := *(*time.Time)(p)
	days := int32(t.Unix() / 86400)
	return appendVarint(dst, days), nil
}

func usTimeMillis(dst []byte, p unsafe.Pointer) ([]byte, error) {
	d := *(*time.Duration)(p)
	return appendVarint(dst, int32(d.Milliseconds())), nil
}

func usTimeMicros(dst []byte, p unsafe.Pointer) ([]byte, error) {
	d := *(*time.Duration)(p)
	return appendVarlong(dst, d.Microseconds()), nil
}

func udTimestampMillis(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	*(*time.Time)(p) = time.UnixMilli(val)
	return src, nil
}

func udTimestampMicros(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	*(*time.Time)(p) = time.UnixMicro(val)
	return src, nil
}

func udDate(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	val, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	*(*time.Time)(p) = time.Unix(int64(val)*86400, 0).UTC()
	return src, nil
}

func udTimeMillis(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	val, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	*(*time.Duration)(p) = time.Duration(val) * time.Millisecond
	return src, nil
}

func udTimeMicros(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	*(*time.Duration)(p) = time.Duration(val) * time.Microsecond
	return src, nil
}

func usDuration(dst []byte, p unsafe.Pointer) ([]byte, error) {
	d := *(*Duration)(p)
	dst = appendUint32(dst, d.Months)
	dst = appendUint32(dst, d.Days)
	dst = appendUint32(dst, d.Milliseconds)
	return dst, nil
}

func udDuration(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	if len(src) < 12 {
		return nil, &ShortBufferError{Type: "duration", Need: 12, Have: len(src)}
	}
	d := (*Duration)(p)
	d.Months = uint32(src[0]) | uint32(src[1])<<8 | uint32(src[2])<<16 | uint32(src[3])<<24
	d.Days = uint32(src[4]) | uint32(src[5])<<8 | uint32(src[6])<<16 | uint32(src[7])<<24
	d.Milliseconds = uint32(src[8]) | uint32(src[9])<<8 | uint32(src[10])<<16 | uint32(src[11])<<24
	return src[12:], nil
}

// ---- Unsafe serializers ----
// These read values directly via unsafe.Pointer. No string→[]byte
// conversions; all reads go through typed pointer dereferences.

func usBool(dst []byte, p unsafe.Pointer) ([]byte, error) {
	if *(*bool)(p) {
		return append(dst, 1), nil
	}
	return append(dst, 0), nil
}

func usInt(k reflect.Kind) userfn {
	switch k {
	case reflect.Int:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarint(dst, int32(*(*int)(p))), nil
		}
	case reflect.Int8:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarint(dst, int32(*(*int8)(p))), nil
		}
	case reflect.Int16:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarint(dst, int32(*(*int16)(p))), nil
		}
	case reflect.Int32:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarint(dst, *(*int32)(p)), nil
		}
	case reflect.Int64:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarint(dst, int32(*(*int64)(p))), nil
		}
	case reflect.Uint:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarint(dst, int32(*(*uint)(p))), nil
		}
	case reflect.Uint8:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarint(dst, int32(*(*uint8)(p))), nil
		}
	case reflect.Uint16:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarint(dst, int32(*(*uint16)(p))), nil
		}
	case reflect.Uint32:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarint(dst, int32(*(*uint32)(p))), nil
		}
	case reflect.Uint64:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarint(dst, int32(*(*uint64)(p))), nil
		}
	default:
		return nil
	}
}

func usLong(k reflect.Kind) userfn {
	switch k {
	case reflect.Int:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarlong(dst, int64(*(*int)(p))), nil
		}
	case reflect.Int8:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarlong(dst, int64(*(*int8)(p))), nil
		}
	case reflect.Int16:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarlong(dst, int64(*(*int16)(p))), nil
		}
	case reflect.Int32:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarlong(dst, int64(*(*int32)(p))), nil
		}
	case reflect.Int64:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarlong(dst, *(*int64)(p)), nil
		}
	case reflect.Uint:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarlong(dst, int64(*(*uint)(p))), nil
		}
	case reflect.Uint8:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarlong(dst, int64(*(*uint8)(p))), nil
		}
	case reflect.Uint16:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarlong(dst, int64(*(*uint16)(p))), nil
		}
	case reflect.Uint32:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarlong(dst, int64(*(*uint32)(p))), nil
		}
	case reflect.Uint64:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendVarlong(dst, int64(*(*uint64)(p))), nil
		}
	default:
		return nil
	}
}

func usFloat(k reflect.Kind) userfn {
	switch k {
	case reflect.Float32:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendUint32(dst, math.Float32bits(*(*float32)(p))), nil
		}
	case reflect.Float64:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendUint32(dst, math.Float32bits(float32(*(*float64)(p)))), nil
		}
	default:
		return nil
	}
}

func usDouble(k reflect.Kind) userfn {
	switch k {
	case reflect.Float32:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendUint64(dst, math.Float64bits(float64(*(*float32)(p)))), nil
		}
	case reflect.Float64:
		return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return appendUint64(dst, math.Float64bits(*(*float64)(p))), nil
		}
	default:
		return nil
	}
}

// usString reads the string header directly from memory.
func usString(dst []byte, p unsafe.Pointer) ([]byte, error) {
	return doSerString(dst, *(*string)(p)), nil
}

// usBytes reads the slice header directly from memory.
func usBytes(dst []byte, p unsafe.Pointer) ([]byte, error) {
	b := *(*[]byte)(p)
	dst = appendVarlong(dst, int64(len(b)))
	return append(dst, b...), nil
}

// ---- Unsafe deserializers ----
// For types without GC pointers (bool, ints, floats), write directly via
// unsafe. For types containing GC pointers (string, []byte), typed pointer
// stores trigger GC write barriers automatically. All decoded values are
// freshly allocated copies; no aliasing of src.

func udBool(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	if len(src) < 1 {
		return nil, &ShortBufferError{Type: "boolean"}
	}
	*(*bool)(p) = src[0] != 0
	return src[1:], nil
}

func udInt(k reflect.Kind) udeserfn {
	switch k {
	case reflect.Int:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarint(src)
			if err != nil {
				return nil, err
			}
			*(*int)(p) = int(v)
			return src, nil
		}
	case reflect.Int8:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarint(src)
			if err != nil {
				return nil, err
			}
			*(*int8)(p) = int8(v)
			return src, nil
		}
	case reflect.Int16:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarint(src)
			if err != nil {
				return nil, err
			}
			*(*int16)(p) = int16(v)
			return src, nil
		}
	case reflect.Int32:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarint(src)
			if err != nil {
				return nil, err
			}
			*(*int32)(p) = v
			return src, nil
		}
	case reflect.Int64:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarint(src)
			if err != nil {
				return nil, err
			}
			*(*int64)(p) = int64(v)
			return src, nil
		}
	case reflect.Uint:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarint(src)
			if err != nil {
				return nil, err
			}
			*(*uint)(p) = uint(v)
			return src, nil
		}
	case reflect.Uint8:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarint(src)
			if err != nil {
				return nil, err
			}
			*(*uint8)(p) = uint8(v)
			return src, nil
		}
	case reflect.Uint16:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarint(src)
			if err != nil {
				return nil, err
			}
			*(*uint16)(p) = uint16(v)
			return src, nil
		}
	case reflect.Uint32:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarint(src)
			if err != nil {
				return nil, err
			}
			*(*uint32)(p) = uint32(v)
			return src, nil
		}
	case reflect.Uint64:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarint(src)
			if err != nil {
				return nil, err
			}
			*(*uint64)(p) = uint64(v)
			return src, nil
		}
	default:
		return nil
	}
}

func udLong(k reflect.Kind) udeserfn {
	switch k {
	case reflect.Int:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarlong(src)
			if err != nil {
				return nil, err
			}
			*(*int)(p) = int(v)
			return src, nil
		}
	case reflect.Int8:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarlong(src)
			if err != nil {
				return nil, err
			}
			*(*int8)(p) = int8(v)
			return src, nil
		}
	case reflect.Int16:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarlong(src)
			if err != nil {
				return nil, err
			}
			*(*int16)(p) = int16(v)
			return src, nil
		}
	case reflect.Int32:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarlong(src)
			if err != nil {
				return nil, err
			}
			*(*int32)(p) = int32(v)
			return src, nil
		}
	case reflect.Int64:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarlong(src)
			if err != nil {
				return nil, err
			}
			*(*int64)(p) = v
			return src, nil
		}
	case reflect.Uint:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarlong(src)
			if err != nil {
				return nil, err
			}
			*(*uint)(p) = uint(v)
			return src, nil
		}
	case reflect.Uint8:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarlong(src)
			if err != nil {
				return nil, err
			}
			*(*uint8)(p) = uint8(v)
			return src, nil
		}
	case reflect.Uint16:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarlong(src)
			if err != nil {
				return nil, err
			}
			*(*uint16)(p) = uint16(v)
			return src, nil
		}
	case reflect.Uint32:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarlong(src)
			if err != nil {
				return nil, err
			}
			*(*uint32)(p) = uint32(v)
			return src, nil
		}
	case reflect.Uint64:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			v, src, err := readVarlong(src)
			if err != nil {
				return nil, err
			}
			*(*uint64)(p) = uint64(v)
			return src, nil
		}
	default:
		return nil
	}
}

func udFloat(k reflect.Kind) udeserfn {
	switch k {
	case reflect.Float32:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			u, src, err := readUint32(src)
			if err != nil {
				return nil, err
			}
			*(*float32)(p) = math.Float32frombits(u)
			return src, nil
		}
	case reflect.Float64:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			u, src, err := readUint32(src)
			if err != nil {
				return nil, err
			}
			*(*float64)(p) = float64(math.Float32frombits(u))
			return src, nil
		}
	default:
		return nil
	}
}

func udDouble(k reflect.Kind) udeserfn {
	switch k {
	case reflect.Float32:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			u, src, err := readUint64(src)
			if err != nil {
				return nil, err
			}
			*(*float32)(p) = float32(math.Float64frombits(u))
			return src, nil
		}
	case reflect.Float64:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			u, src, err := readUint64(src)
			if err != nil {
				return nil, err
			}
			*(*float64)(p) = math.Float64frombits(u)
			return src, nil
		}
	default:
		return nil
	}
}

// udStringDeser writes the string directly via typed pointer store.
// *(*string)(p) = s triggers GC write barriers automatically.
// string(src[:n]) always copies, so the decoded string owns its memory.
func udStringDeser(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	length, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, fmt.Errorf("invalid negative string length %d", length)
	}
	n := int(length)
	if len(src) < n {
		return nil, &ShortBufferError{Type: "string", Need: n, Have: len(src)}
	}
	*(*string)(p) = sl.string(src, n)
	return src[n:], nil
}

// udBytesDeser writes the byte slice directly via typed pointer store.
// *(*[]byte)(p) = b triggers GC write barriers automatically.
// make+copy ensures the decoded slice owns its memory.
func udBytesDeser(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	length, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, fmt.Errorf("invalid negative bytes length %d", length)
	}
	n := int(length)
	if len(src) < n {
		return nil, &ShortBufferError{Type: "bytes", Need: n, Have: len(src)}
	}
	b := make([]byte, n)
	copy(b, src[:n])
	*(*[]byte)(p) = b
	return src[n:], nil
}

// ---- Null-union unsafe ser/deser ----

// usNullUnionPtr handles null-union ser for *T where T has a primitive unsafe serializer.
func usNullUnionPtr(inner userfn) userfn {
	return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
		pp := *(*unsafe.Pointer)(p)
		if pp == nil {
			return append(dst, 0), nil
		}
		return inner(append(dst, 2), pp)
	}
}

// usNullUnionRecord handles null-union ser for *T where T is a record.
func usNullUnionRecord(rec *serRecord, innerType reflect.Type) userfn {
	return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
		pp := *(*unsafe.Pointer)(p)
		if pp == nil {
			return append(dst, 0), nil
		}
		dst = append(dst, 2)
		if fast := rec.fast.Load(); fast != nil && fast.typ == innerType && fast.allFast {
			return serRecordFastPtr(dst, fast, pp)
		}
		return rec.ser(dst, reflect.NewAt(innerType, pp).Elem())
	}
}

// udNullUnionPtr handles null-union deser for *T where T has a primitive unsafe deserializer.
func udNullUnionPtr(inner udeserfn, innerType reflect.Type) udeserfn {
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		if len(src) < 1 {
			return nil, &ShortBufferError{Type: "union index"}
		}
		switch src[0] {
		case 0:
			*(*unsafe.Pointer)(p) = nil
			return src[1:], nil
		case 2:
			pp := *(*unsafe.Pointer)(p)
			if pp == nil {
				v := reflect.New(innerType)
				pp = v.UnsafePointer()
				*(*unsafe.Pointer)(p) = pp
			}
			return inner(src[1:], pp, sl)
		default:
			return nil, fmt.Errorf("invalid null-union index byte 0x%02x", src[0])
		}
	}
}

// udNullUnionRecord handles null-union deser for *T where T is a record.
func udNullUnionRecord(rec *deserRecord, innerType reflect.Type) udeserfn {
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		if len(src) < 1 {
			return nil, &ShortBufferError{Type: "union index"}
		}
		switch src[0] {
		case 0:
			*(*unsafe.Pointer)(p) = nil
			return src[1:], nil
		case 2:
			pp := *(*unsafe.Pointer)(p)
			if pp == nil {
				v := reflect.New(innerType)
				pp = v.UnsafePointer()
				*(*unsafe.Pointer)(p) = pp
			}
			if fast := rec.fast.Load(); fast != nil && fast.typ == innerType && fast.allFast {
				return deserRecordFastPtr(src[1:], fast, pp, sl)
			}
			return rec.deser(src[1:], reflect.NewAt(innerType, pp).Elem(), sl)
		default:
			return nil, fmt.Errorf("invalid null-union index byte 0x%02x", src[0])
		}
	}
}

// ---- Array unsafe ser/deser ----

// usArrayRecord handles array ser for []T or []*T where items are records.
func usArrayRecord(rec *serRecord, elemGoType reflect.Type) userfn {
	if elemGoType.Kind() == reflect.Ptr {
		return usArrayPtrRecord(rec, elemGoType.Elem())
	}
	elemSize := elemGoType.Size()
	return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
		bs := *(*[]byte)(p)
		n := len(bs)
		dst = appendVarlong(dst, int64(n))
		if n == 0 {
			return dst, nil
		}
		data := unsafe.Pointer(unsafe.SliceData(bs))
		var err error
		for i := 0; i < n; i++ {
			elemP := unsafe.Add(data, uintptr(i)*elemSize)
			if fast := rec.fast.Load(); fast != nil && fast.typ == elemGoType && fast.allFast {
				dst, err = serRecordFastPtr(dst, fast, elemP)
			} else {
				dst, err = rec.ser(dst, reflect.NewAt(elemGoType, elemP).Elem())
			}
			if err != nil {
				return nil, err
			}
		}
		return append(dst, 0), nil
	}
}

// usArrayPtrRecord handles array ser for []*T where items are records.
func usArrayPtrRecord(rec *serRecord, innerType reflect.Type) userfn {
	return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
		s := *(*[]unsafe.Pointer)(p)
		n := len(s)
		dst = appendVarlong(dst, int64(n))
		if n == 0 {
			return dst, nil
		}
		var err error
		for _, pp := range s {
			if pp == nil {
				return nil, errUnsafeNilPtr
			}
			if fast := rec.fast.Load(); fast != nil && fast.typ == innerType && fast.allFast {
				dst, err = serRecordFastPtr(dst, fast, pp)
			} else {
				dst, err = rec.ser(dst, reflect.NewAt(innerType, pp).Elem())
			}
			if err != nil {
				return nil, err
			}
		}
		return append(dst, 0), nil
	}
}

// usArrayNullUnionRecord handles array ser for []*T where items are ["null", Record].
func usArrayNullUnionRecord(rec *serRecord, innerType reflect.Type) userfn {
	return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
		s := *(*[]unsafe.Pointer)(p)
		n := len(s)
		dst = appendVarlong(dst, int64(n))
		if n == 0 {
			return dst, nil
		}
		var err error
		for _, pp := range s {
			if pp == nil {
				dst = append(dst, 0)
			} else {
				dst = append(dst, 2)
				if fast := rec.fast.Load(); fast != nil && fast.typ == innerType && fast.allFast {
					dst, err = serRecordFastPtr(dst, fast, pp)
				} else {
					dst, err = rec.ser(dst, reflect.NewAt(innerType, pp).Elem())
				}
				if err != nil {
					return nil, err
				}
			}
		}
		return append(dst, 0), nil
	}
}

// usArrayNullUnionPtr handles array ser for []*T where items are ["null", primitive].
func usArrayNullUnionPtr(inner userfn) userfn {
	return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
		s := *(*[]unsafe.Pointer)(p)
		n := len(s)
		dst = appendVarlong(dst, int64(n))
		if n == 0 {
			return dst, nil
		}
		var err error
		for _, pp := range s {
			if pp == nil {
				dst = append(dst, 0)
			} else {
				dst, err = inner(append(dst, 2), pp)
				if err != nil {
					return nil, err
				}
			}
		}
		return append(dst, 0), nil
	}
}

// usArrayDirect handles array ser for []T where items are primitives.
func usArrayDirect(inner userfn, elemSize uintptr) userfn {
	return func(dst []byte, p unsafe.Pointer) ([]byte, error) {
		bs := *(*[]byte)(p)
		n := len(bs)
		dst = appendVarlong(dst, int64(n))
		if n == 0 {
			return dst, nil
		}
		data := unsafe.Pointer(unsafe.SliceData(bs))
		var err error
		for i := 0; i < n; i++ {
			dst, err = inner(dst, unsafe.Add(data, uintptr(i)*elemSize))
			if err != nil {
				return nil, err
			}
		}
		return append(dst, 0), nil
	}
}

// udArrayPtrRecord handles array deser for []*T where items are records.
// Uses reflect for slice management, unsafe for per-element record deser.
func udArrayPtrRecord(rec *deserRecord, innerType, sliceType reflect.Type) udeserfn {
	innerSize := innerType.Size()
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		v := reflect.NewAt(sliceType, p).Elem()
		v.SetLen(0)
		var err error
		for {
			var count int64
			count, src, err = readVarlong(src)
			if err != nil {
				return nil, err
			}
			if count == 0 {
				return src, nil
			}
			if count < 0 {
				count = -count
				if count < 0 {
					return nil, errors.New("invalid array block count")
				}
				_, src, err = readVarlong(src)
				if err != nil {
					return nil, err
				}
			}
			n := int(count)
			start := v.Len()
			newLen := start + n
			if v.Cap() < newLen {
				ns := reflect.MakeSlice(sliceType, newLen, newLen)
				reflect.Copy(ns, v)
				v.Set(ns)
			} else {
				v.SetLen(newLen)
			}
			s := *(*[]unsafe.Pointer)(p)
			// Count how many elements need fresh allocation.
			var need int
			for i := 0; i < n; i++ {
				if s[start+i] == nil {
					need++
				}
			}
			// Batch allocate only for nil slots.
			if need > 0 {
				backing := reflect.MakeSlice(reflect.SliceOf(innerType), need, need)
				backingBase := backing.Index(0).Addr().UnsafePointer()
				j := 0
				for i := 0; i < n; i++ {
					if s[start+i] == nil {
						s[start+i] = unsafe.Add(backingBase, uintptr(j)*innerSize)
						j++
					}
				}
			}
			// Deserialize each element.
			for i := 0; i < n; i++ {
				elemP := s[start+i]
				if fast := rec.fast.Load(); fast != nil && fast.typ == innerType && fast.allFast {
					src, err = deserRecordFastPtr(src, fast, elemP, sl)
				} else {
					src, err = rec.deser(src, reflect.NewAt(innerType, elemP).Elem(), sl)
				}
				if err != nil {
					return nil, err
				}
			}
		}
	}
}

// udArrayDirect handles array deser for []T where items are primitives.
// Uses reflect for slice management, unsafe for per-element deser.
func udArrayDirect(inner udeserfn, elemSize uintptr, sliceType reflect.Type) udeserfn {
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		v := reflect.NewAt(sliceType, p).Elem()
		v.SetLen(0)
		var err error
		for {
			var count int64
			count, src, err = readVarlong(src)
			if err != nil {
				return nil, err
			}
			if count == 0 {
				return src, nil
			}
			if count < 0 {
				count = -count
				if count < 0 {
					return nil, errors.New("invalid array block count")
				}
				_, src, err = readVarlong(src)
				if err != nil {
					return nil, err
				}
			}
			n := int(count)
			start := v.Len()
			newLen := start + n
			if v.Cap() < newLen {
				ns := reflect.MakeSlice(sliceType, newLen, newLen)
				reflect.Copy(ns, v)
				v.Set(ns)
			} else {
				v.SetLen(newLen)
			}
			// Access data pointer after potential reallocation.
			bs := *(*[]byte)(p)
			data := unsafe.Pointer(unsafe.SliceData(bs))
			for i := start; i < newLen; i++ {
				src, err = inner(src, unsafe.Add(data, uintptr(i)*elemSize), sl)
				if err != nil {
					return nil, err
				}
			}
		}
	}
}
