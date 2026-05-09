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
type userfn func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error)

// udeserfn deserializes from src into the value at p.
type udeserfn func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error)

type fastRecordSer struct {
	typ     reflect.Type
	allFast bool
	fields  []fastFieldSer
}

// fastFieldSer uses a hybrid approach: primitive fields get the unsafe
// fast path (ser != nil), complex/union fields use reflect-based
// FieldByIndex (slowFn != nil). Mixing avoids paying reflect.NewAt
// overhead for the primitive subset.
type fastFieldSer struct {
	offset   uintptr
	name     string
	ser      userfn // non-nil for unsafe-optimized fields (primitives)
	slowFn   serfn  // non-nil for reflect-based fields (complex types)
	slowIdx  []int  // field index path for FieldByIndex (used with slowFn)
	omitzero bool   // if true and field is nullunion, check IsZero before ser
	nullByte byte   // when omitzero fires, this is the null-branch index byte
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
		// omitzero + nullunion fields need a runtime zero check before
		// encoding, which the unsafe fast path can't do — fall back to
		// the reflect-based slow path for these fields.
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
			ffs := fastFieldSer{
				name:     f.name,
				slowFn:   f.fn,
				slowIdx:  mapping.indices[i],
				omitzero: oz,
			}
			if oz {
				ffs.nullByte, _ = nullUnionBytes(f.meta != nil && f.meta.nullSecond)
			}
			fast.fields[i] = ffs
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
		if t.Kind() == reflect.Pointer {
			return 0, nil, false
		}
		f := t.Field(i)
		offset += f.Offset
		t = f.Type
	}
	return offset, t, true
}

func serRecordFast(dst []byte, fast *fastRecordSer, v reflect.Value, depth int) ([]byte, error) {
	if depth >= maxDepth {
		return nil, errTooDeep
	}
	base := v.Addr().UnsafePointer()
	var err error
	for i := range fast.fields {
		f := &fast.fields[i]
		if f.ser != nil {
			dst, err = f.ser(dst, unsafe.Add(base, f.offset), depth+1)
		} else {
			fv := v.FieldByIndex(f.slowIdx)
			if f.omitzero && valueIsZero(fv) {
				// f.nullByte is populated at compile (compileFastSer)
				// from fieldMeta.nullSecond — 0x00 for ["null",T],
				// 0x02 for ["T","null"].
				dst = append(dst, f.nullByte)
				continue
			}
			dst, err = f.slowFn(dst, fv, depth+1)
		}
		if err != nil {
			return nil, recordFieldError(fast.typ, f.name, err)
		}
	}
	return dst, nil
}

func deserRecordFast(src []byte, fast *fastRecordDeser, v reflect.Value, sl *slab) ([]byte, error) {
	if sl.depth >= maxDepth {
		return nil, errTooDeep
	}
	sl.depth++
	defer func() { sl.depth-- }()
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
			return nil, recordFieldError(fast.typ, f.name, err)
		}
	}
	return src, nil
}

// serRecordFastPtr serializes a record when all fields have unsafe ser fns.
// Only requires a raw pointer to the struct base — no reflect.Value needed.
func serRecordFastPtr(dst []byte, fast *fastRecordSer, base unsafe.Pointer, depth int) ([]byte, error) {
	if depth >= maxDepth {
		return nil, errTooDeep
	}
	var err error
	for i := range fast.fields {
		f := &fast.fields[i]
		dst, err = f.ser(dst, unsafe.Add(base, f.offset), depth+1)
		if err != nil {
			return nil, recordFieldError(fast.typ, f.name, err)
		}
	}
	return dst, nil
}

// deserRecordFastPtr deserializes a record when all fields have unsafe deser fns.
func deserRecordFastPtr(src []byte, fast *fastRecordDeser, base unsafe.Pointer, sl *slab) ([]byte, error) {
	if sl.depth >= maxDepth {
		return nil, errTooDeep
	}
	sl.depth++
	defer func() { sl.depth-- }()
	var err error
	for i := range fast.fields {
		f := &fast.fields[i]
		src, err = f.deser(src, unsafe.Add(base, f.offset), sl)
		if err != nil {
			return nil, recordFieldError(fast.typ, f.name, err)
		}
	}
	return src, nil
}

// tryCompileFieldSer returns a userfn for fields that can be fully handled
// via unsafe pointer access. Returns nil for complex types that must use
// the reflect-based slow path.
func tryCompileFieldSer(f *serRecordField, goType reflect.Type) userfn {
	// Custom types need the reflect slow path for the conversion wrapper.
	if f.meta != nil && (f.meta.hasCustomType || (f.meta.inner != nil && f.meta.inner.hasCustomType)) {
		return nil
	}
	k := goType.Kind()

	// Regular unions need the reflect slow path.
	if f.avroType == "union" {
		return nil
	}

	// Null-union: *T mapped to ["null", T] or [T, "null"].
	if f.avroType == "nullunion" {
		if k != reflect.Pointer {
			return nil
		}
		if f.meta == nil || f.meta.inner == nil {
			return nil
		}
		nullByte, valByte := nullUnionBytes(f.meta.nullSecond)
		inner := f.meta.inner
		if inner.hasCustomType {
			return nil
		}
		innerGoType := goType.Elem()
		if inner.serRecord != nil {
			return usNullUnionRecord(inner.serRecord, innerGoType, nullByte, valByte)
		}
		innerFn := tryCompileFieldSer(&serRecordField{avroType: inner.avroType, meta: inner}, innerGoType)
		if innerFn != nil {
			return usNullUnionPtr(innerFn, nullByte, valByte)
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
			if elemGoType.Kind() != reflect.Pointer {
				return nil
			}
			nullByte, valByte := nullUnionBytes(inner.nullSecond)
			if inner.inner != nil && inner.inner.hasCustomType {
				return nil
			}
			if inner.inner != nil && inner.inner.serRecord != nil {
				return usArrayNullUnionRecord(inner.inner.serRecord, elemGoType.Elem(), nullByte, valByte)
			}
			innerFn := tryCompileFieldSer(&serRecordField{avroType: inner.inner.avroType, meta: inner.inner}, elemGoType.Elem())
			if innerFn != nil {
				return usArrayNullUnionPtr(innerFn, nullByte, valByte)
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
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			if fast := rec.fast.Load(); fast != nil && fast.typ == goType && fast.allFast {
				return serRecordFastPtr(dst, fast, p, depth+1)
			}
			return rec.ser(dst, reflect.NewAt(goType, p).Elem(), depth+1)
		}
	}

	if k == reflect.Pointer {
		inner := tryCompileFieldSer(f, goType.Elem())
		if inner == nil {
			return nil
		}
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			pp := *(*unsafe.Pointer)(p)
			if pp == nil {
				return nil, errIndirectNil
			}
			return inner(dst, pp, depth+1)
		}
	}

	// Logical type fast paths for time.Time and time.Duration.
	if f.meta != nil && f.meta.logical != "" {
		return tryCompileLogicalSer(f.meta.logical, f.avroType, goType)
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
	if f.meta != nil && (f.meta.hasCustomType || (f.meta.inner != nil && f.meta.inner.hasCustomType)) {
		return nil
	}
	k := goType.Kind()

	if f.avroType == "union" {
		return nil
	}

	// Null-union: *T mapped to ["null", T] or [T, "null"].
	if f.avroType == "nullunion" {
		if k != reflect.Pointer {
			return nil
		}
		if f.meta == nil || f.meta.inner == nil {
			return nil
		}
		nullByte, valByte := nullUnionBytes(f.meta.nullSecond)
		valIdx := 1
		if f.meta.nullSecond {
			valIdx = 0
		}
		inner := f.meta.inner
		if inner.hasCustomType {
			return nil
		}
		innerGoType := goType.Elem()
		if inner.deserRecord != nil {
			return udNullUnionRecord(inner.deserRecord, innerGoType, valIdx, nullByte, valByte)
		}
		innerFn := tryCompileFieldDeser(&deserRecordField{avroType: inner.avroType, meta: inner}, innerGoType)
		if innerFn != nil {
			return udNullUnionPtr(innerFn, innerGoType, valIdx, nullByte, valByte)
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
		if inner.hasCustomType {
			return nil
		}
		elemGoType := goType.Elem()
		switch inner.avroType {
		case "record":
			if inner.deserRecord != nil && elemGoType.Kind() == reflect.Pointer {
				return udArrayPtrRecord(inner.deserRecord, elemGoType.Elem(), goType, inner.minBytes)
			}
		default:
			innerFn := tryCompileFieldDeser(&deserRecordField{avroType: inner.avroType, meta: inner}, elemGoType)
			if innerFn != nil {
				return udArrayDirect(innerFn, elemGoType.Size(), goType, inner.minBytes)
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
	if k == reflect.Pointer {
		return nil
	}

	// Logical type fast paths for time.Time and time.Duration.
	if f.meta != nil && f.meta.logical != "" {
		return tryCompileLogicalDeser(f.meta.logical, f.avroType, goType)
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

func tryCompileLogicalSer(logical, avroType string, goType reflect.Type) userfn {
	switch logical {
	case "timestamp-millis":
		if goType == timeType {
			return usTimestampMillis
		}
		return usLong(goType.Kind())
	case "timestamp-micros":
		if goType == timeType {
			return usTimestampMicros
		}
		return usLong(goType.Kind())
	case "timestamp-nanos":
		if goType == timeType {
			return usTimestampNanos
		}
		return usLong(goType.Kind())
	case "local-timestamp-millis":
		if goType == timeType {
			return usLocalTimestampMillis
		}
		return usLong(goType.Kind())
	case "local-timestamp-micros":
		if goType == timeType {
			return usLocalTimestampMicros
		}
		return usLong(goType.Kind())
	case "local-timestamp-nanos":
		if goType == timeType {
			return usLocalTimestampNanos
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
		if goType == timeType {
			return usTimeMillisTime
		}
		return usInt(goType.Kind())
	case "time-micros":
		if goType == durationType {
			return usTimeMicros
		}
		if goType == timeType {
			return usTimeMicrosTime
		}
		return usLong(goType.Kind())
	case "duration":
		if goType == avroDurationType {
			return usDuration
		}
		return nil
	case "uuid":
		if avroType == "fixed" {
			if goType.Kind() == reflect.String {
				return usFixedUUIDString
			}
			return nil // [16]byte, []byte handled by default fixed ser
		}
		if isUUIDType(goType) {
			return usUUID
		}
		if goType.Kind() == reflect.String {
			return usString
		}
	}
	return nil
}

func tryCompileLogicalDeser(logical, avroType string, goType reflect.Type) udeserfn {
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
	case "timestamp-nanos", "local-timestamp-nanos":
		if goType == timeType {
			return udTimestampNanos
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
		if goType == timeType {
			return udTimeMillisTime
		}
		return udInt(goType.Kind())
	case "time-micros":
		if goType == durationType {
			return udTimeMicros
		}
		if goType == timeType {
			return udTimeMicrosTime
		}
		return udLong(goType.Kind())
	case "duration":
		if goType == avroDurationType {
			return udDuration
		}
		return nil
	case "uuid":
		if avroType == "fixed" {
			if isUUIDType(goType) {
				return udFixedUUID
			}
			if goType.Kind() == reflect.String {
				return udFixedUUIDString
			}
			return nil // any, []byte etc. handled by reflect path
		}
		if isUUIDType(goType) {
			return udUUID
		}
		if goType.Kind() == reflect.String {
			return udStringDeser
		}
	}
	return nil
}

// usTimeAsLong is the shared body of the six unsafe time-logical "long"
// serializers — direct *(*time.Time)(p) read, conv, SemanticError-wrap.
func usTimeAsLong(dst []byte, p unsafe.Pointer, conv func(time.Time) (int64, error)) ([]byte, error) {
	n, err := conv(*(*time.Time)(p))
	if err != nil {
		return nil, &SemanticError{GoType: timeType, AvroType: "long", Err: err}
	}
	return appendVarlong(dst, n), nil
}

func usTimestampMillis(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	return usTimeAsLong(dst, p, timeToTimestampMillis)
}

func usTimestampMicros(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	return usTimeAsLong(dst, p, timeToTimestampMicros)
}

func usTimestampNanos(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	return usTimeAsLong(dst, p, timeToTimestampNanos)
}

// Local-timestamp unsafe serializers: see logical.go for rationale.

func usLocalTimestampMillis(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	return usTimeAsLong(dst, p, timeToLocalTimestampMillis)
}

func usLocalTimestampMicros(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	return usTimeAsLong(dst, p, timeToLocalTimestampMicros)
}

func usLocalTimestampNanos(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	return usTimeAsLong(dst, p, timeToLocalTimestampNanos)
}

func usDate(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	d, err := timeToDate(*(*time.Time)(p))
	if err != nil {
		return nil, &SemanticError{GoType: timeType, AvroType: "date", Err: err}
	}
	return appendVarint(dst, d), nil
}

func usTimeMillis(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	ms, err := durationToTimeMillis(*(*time.Duration)(p))
	if err != nil {
		return nil, &SemanticError{GoType: durationType, AvroType: "time-millis", Err: err}
	}
	return appendVarint(dst, ms), nil
}

// usTimeMillisTime is the time.Time variant of usTimeMillis — extracts
// time-of-day fields and encodes as time-millis, mirroring the
// serTimeMillis(timeType) safe-path arm.
func usTimeMillisTime(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	t := *(*time.Time)(p)
	d := time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute + time.Duration(t.Second())*time.Second + time.Duration(t.Nanosecond())
	ms, err := durationToTimeMillis(d)
	if err != nil {
		return nil, &SemanticError{GoType: timeType, AvroType: "time-millis", Err: err}
	}
	return appendVarint(dst, ms), nil
}

func usTimeMicros(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	return appendVarlong(dst, (*(*time.Duration)(p)).Microseconds()), nil
}

// usTimeMicrosTime mirrors serTimeMicros(timeType).
func usTimeMicrosTime(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	t := *(*time.Time)(p)
	d := time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute + time.Duration(t.Second())*time.Second + time.Duration(t.Nanosecond())
	return appendVarlong(dst, d.Microseconds()), nil
}

func udTimestampMillis(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	*(*time.Time)(p) = timestampMillisToTime(val)
	return src, nil
}

func udTimestampMicros(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	*(*time.Time)(p) = timestampMicrosToTime(val)
	return src, nil
}

func udTimestampNanos(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	*(*time.Time)(p) = timestampNanosToTime(val)
	return src, nil
}

func udDate(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	val, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	*(*time.Time)(p) = dateToTime(val)
	return src, nil
}

func udTimeMillis(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	val, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	*(*time.Duration)(p) = timeMillisToDuration(val)
	return src, nil
}

// udTimeMillisTime is the time.Time variant of udTimeMillis, materializing
// the time-of-day duration at epoch midnight (UTC). Mirrors
// deserTimeMillis's timeType safe-path arm.
func udTimeMillisTime(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	val, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	*(*time.Time)(p) = timeOfDayToTime(timeMillisToDuration(val))
	return src, nil
}

func udTimeMicros(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	d, err := timeMicrosToDuration(val)
	if err != nil {
		return nil, err
	}
	*(*time.Duration)(p) = d
	return src, nil
}

// udTimeMicrosTime is the time.Time variant of udTimeMicros.
func udTimeMicrosTime(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	d, err := timeMicrosToDuration(val)
	if err != nil {
		return nil, err
	}
	*(*time.Time)(p) = timeOfDayToTime(d)
	return src, nil
}

func usDuration(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
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

func usUUID(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	u := *(*[16]byte)(p)
	return doSerString(dst, uuidToString(u)), nil
}

func udUUID(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	n, src, err := readLength(src, "string")
	if err != nil {
		return nil, err
	}
	u, err := parseUUIDBytes(src[:n])
	if err != nil {
		return nil, err
	}
	*(*[16]byte)(p) = u
	return src[n:], nil
}

// udFixedUUID reads 16 raw bytes from a fixed(16) UUID and writes a [16]byte.
// Used when the target is [16]byte or any (interface).
func udFixedUUID(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	if len(src) < 16 {
		return nil, &ShortBufferError{Type: "uuid", Need: 16, Have: len(src)}
	}
	*(*[16]byte)(p) = [16]byte(src[:16])
	return src[16:], nil
}

// udFixedUUIDString reads 16 raw bytes from a fixed(16) UUID and writes a
// formatted UUID string (e.g. "550e8400-e29b-41d4-a716-446655440000").
func udFixedUUIDString(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	if len(src) < 16 {
		return nil, &ShortBufferError{Type: "uuid", Need: 16, Have: len(src)}
	}
	*(*string)(p) = uuidToString([16]byte(src[:16]))
	return src[16:], nil
}

// usFixedUUIDString serializes a UUID string to 16 raw fixed bytes.
func usFixedUUIDString(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	s := *(*string)(p)
	u, err := parseUUID(s)
	if err != nil {
		return nil, err
	}
	return append(dst, u[:]...), nil
}

// ---- Unsafe serializers ----
// These read values directly via unsafe.Pointer. No string→[]byte
// conversions; all reads go through typed pointer dereferences.

func usBool(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	if *(*bool)(p) {
		return append(dst, 1), nil
	}
	return append(dst, 0), nil
}

func usInt(k reflect.Kind) userfn {
	switch k {
	case reflect.Int:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			n := *(*int)(p)
			if n < math.MinInt32 || n > math.MaxInt32 {
				return nil, &SemanticError{AvroType: "int", Err: fmt.Errorf("value %d overflows int32", n)}
			}
			return appendVarint(dst, int32(n)), nil
		}
	case reflect.Int8:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			return appendVarint(dst, int32(*(*int8)(p))), nil
		}
	case reflect.Int16:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			return appendVarint(dst, int32(*(*int16)(p))), nil
		}
	case reflect.Int32:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			return appendVarint(dst, *(*int32)(p)), nil
		}
	case reflect.Int64:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			n := *(*int64)(p)
			if n < math.MinInt32 || n > math.MaxInt32 {
				return nil, &SemanticError{AvroType: "int", Err: fmt.Errorf("value %d overflows int32", n)}
			}
			return appendVarint(dst, int32(n)), nil
		}
	case reflect.Uint:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			n := *(*uint)(p)
			if n > math.MaxInt32 {
				return nil, &SemanticError{AvroType: "int", Err: fmt.Errorf("value %d overflows int32", n)}
			}
			return appendVarint(dst, int32(n)), nil
		}
	case reflect.Uint8:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			return appendVarint(dst, int32(*(*uint8)(p))), nil
		}
	case reflect.Uint16:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			return appendVarint(dst, int32(*(*uint16)(p))), nil
		}
	case reflect.Uint32:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			n := *(*uint32)(p)
			if n > math.MaxInt32 {
				return nil, &SemanticError{AvroType: "int", Err: fmt.Errorf("value %d overflows int32", n)}
			}
			return appendVarint(dst, int32(n)), nil
		}
	case reflect.Uint64:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			n := *(*uint64)(p)
			if n > math.MaxInt32 {
				return nil, &SemanticError{AvroType: "int", Err: fmt.Errorf("value %d overflows int32", n)}
			}
			return appendVarint(dst, int32(n)), nil
		}
	default:
		return nil
	}
}

func usLong(k reflect.Kind) userfn {
	switch k {
	case reflect.Int:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			return appendVarlong(dst, int64(*(*int)(p))), nil
		}
	case reflect.Int8:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			return appendVarlong(dst, int64(*(*int8)(p))), nil
		}
	case reflect.Int16:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			return appendVarlong(dst, int64(*(*int16)(p))), nil
		}
	case reflect.Int32:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			return appendVarlong(dst, int64(*(*int32)(p))), nil
		}
	case reflect.Int64:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			return appendVarlong(dst, *(*int64)(p)), nil
		}
	case reflect.Uint:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			n := *(*uint)(p)
			if uint64(n) > math.MaxInt64 {
				return nil, &SemanticError{AvroType: "long", Err: fmt.Errorf("value %d overflows int64", n)}
			}
			return appendVarlong(dst, int64(n)), nil
		}
	case reflect.Uint8:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			return appendVarlong(dst, int64(*(*uint8)(p))), nil
		}
	case reflect.Uint16:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			return appendVarlong(dst, int64(*(*uint16)(p))), nil
		}
	case reflect.Uint32:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			return appendVarlong(dst, int64(*(*uint32)(p))), nil
		}
	case reflect.Uint64:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			n := *(*uint64)(p)
			if n > math.MaxInt64 {
				return nil, &SemanticError{AvroType: "long", Err: fmt.Errorf("value %d overflows int64", n)}
			}
			return appendVarlong(dst, int64(n)), nil
		}
	default:
		return nil
	}
}

func usFloat(k reflect.Kind) userfn {
	switch k {
	case reflect.Float32:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			return appendUint32(dst, math.Float32bits(*(*float32)(p))), nil
		}
	case reflect.Float64:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			f := *(*float64)(p)
			// Match serFloat: reject silent narrowing to ±Inf for finite inputs.
			if finiteFloat32Overflows(f) {
				return nil, &SemanticError{AvroType: "float", Err: fmt.Errorf("value %g overflows float32", f)}
			}
			return appendUint32(dst, math.Float32bits(float32(f))), nil
		}
	default:
		return nil
	}
}

func usDouble(k reflect.Kind) userfn {
	switch k {
	case reflect.Float32:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			return appendUint64(dst, math.Float64bits(float64(*(*float32)(p)))), nil
		}
	case reflect.Float64:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			return appendUint64(dst, math.Float64bits(*(*float64)(p))), nil
		}
	default:
		return nil
	}
}

// usString reads the string header directly from memory.
func usString(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	return doSerString(dst, *(*string)(p)), nil
}

// usBytes reads the slice header directly from memory.
func usBytes(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
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
	*(*bool)(p) = src[0] == 1
	return src[1:], nil
}

func udInt(k reflect.Kind) udeserfn {
	switch k {
	case reflect.Int:
		// int32 always fits in int (int is int32 or int64).
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
			if v < math.MinInt8 || v > math.MaxInt8 {
				return nil, &SemanticError{AvroType: "int", Err: fmt.Errorf("value %d overflows int8", v)}
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
			if v < math.MinInt16 || v > math.MaxInt16 {
				return nil, &SemanticError{AvroType: "int", Err: fmt.Errorf("value %d overflows int16", v)}
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
			if v < 0 {
				return nil, &SemanticError{AvroType: "int", Err: fmt.Errorf("value %d overflows uint", v)}
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
			if v < 0 || v > math.MaxUint8 {
				return nil, &SemanticError{AvroType: "int", Err: fmt.Errorf("value %d overflows uint8", v)}
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
			if v < 0 || v > math.MaxUint16 {
				return nil, &SemanticError{AvroType: "int", Err: fmt.Errorf("value %d overflows uint16", v)}
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
			if v < 0 {
				return nil, &SemanticError{AvroType: "int", Err: fmt.Errorf("value %d overflows uint32", v)}
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
			if v < 0 {
				return nil, &SemanticError{AvroType: "int", Err: fmt.Errorf("value %d overflows uint64", v)}
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
			// On 32-bit platforms int is int32; bound-check.
			if v < math.MinInt || v > math.MaxInt {
				return nil, &SemanticError{AvroType: "long", Err: fmt.Errorf("value %d overflows int", v)}
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
			if v < math.MinInt8 || v > math.MaxInt8 {
				return nil, &SemanticError{AvroType: "long", Err: fmt.Errorf("value %d overflows int8", v)}
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
			if v < math.MinInt16 || v > math.MaxInt16 {
				return nil, &SemanticError{AvroType: "long", Err: fmt.Errorf("value %d overflows int16", v)}
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
			if v < math.MinInt32 || v > math.MaxInt32 {
				return nil, &SemanticError{AvroType: "long", Err: fmt.Errorf("value %d overflows int32", v)}
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
			// On 64-bit uint can hold any non-negative int64; on 32-bit
			// uint = uint32, so additionally cap at MaxUint32 via MaxUint.
			if v < 0 || uint64(v) > math.MaxUint {
				return nil, &SemanticError{AvroType: "long", Err: fmt.Errorf("value %d overflows uint", v)}
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
			if v < 0 || v > math.MaxUint8 {
				return nil, &SemanticError{AvroType: "long", Err: fmt.Errorf("value %d overflows uint8", v)}
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
			if v < 0 || v > math.MaxUint16 {
				return nil, &SemanticError{AvroType: "long", Err: fmt.Errorf("value %d overflows uint16", v)}
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
			if v < 0 || v > math.MaxUint32 {
				return nil, &SemanticError{AvroType: "long", Err: fmt.Errorf("value %d overflows uint32", v)}
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
			if v < 0 {
				return nil, &SemanticError{AvroType: "long", Err: fmt.Errorf("value %d overflows uint64", v)}
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
			f := math.Float64frombits(u)
			// Match deserDouble (safe path): reject silent narrowing of a
			// finite float64 to ±Inf when the destination is float32.
			if finiteFloat32Overflows(f) {
				return nil, &SemanticError{AvroType: "double", Err: fmt.Errorf("value %g overflows float32", f)}
			}
			*(*float32)(p) = float32(f)
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
	n, src, err := readLength(src, "string")
	if err != nil {
		return nil, err
	}
	*(*string)(p) = sl.string(src, n)
	return src[n:], nil
}

// udBytesDeser writes the byte slice directly via typed pointer store.
// *(*[]byte)(p) = b triggers GC write barriers automatically.
// make+copy ensures the decoded slice owns its memory.
func udBytesDeser(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	n, src, err := readLength(src, "bytes")
	if err != nil {
		return nil, err
	}
	b := make([]byte, n)
	copy(b, src[:n])
	*(*[]byte)(p) = b
	return src[n:], nil
}

// ---- Null-union unsafe ser/deser ----

// nullUnionBytes returns the single-byte varint-encoded union index for
// the null and value branches. Zigzag varint: index 0 encodes as 0x00,
// index 1 encodes as 0x02. So ["null",T] → null=0x00, val=0x02;
// ["T","null"] → val=0x00, null=0x02.
func nullUnionBytes(nullSecond bool) (nullByte, valByte byte) {
	if nullSecond {
		return 2, 0
	}
	return 0, 2
}

// usNullUnionPtr handles null-union ser for *T where T has a primitive unsafe serializer.
// nullByte/valByte are the varint-encoded index bytes for null and value branches.
func usNullUnionPtr(inner userfn, nullByte, valByte byte) userfn {
	return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
		pp := *(*unsafe.Pointer)(p)
		if pp == nil {
			return append(dst, nullByte), nil
		}
		return inner(append(dst, valByte), pp, depth+1)
	}
}

// usNullUnionRecord handles null-union ser for *T where T is a record.
func usNullUnionRecord(rec *serRecord, innerType reflect.Type, nullByte, valByte byte) userfn {
	return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
		pp := *(*unsafe.Pointer)(p)
		if pp == nil {
			return append(dst, nullByte), nil
		}
		dst = append(dst, valByte)
		if fast := rec.fast.Load(); fast != nil && fast.typ == innerType && fast.allFast {
			return serRecordFastPtr(dst, fast, pp, depth+1)
		}
		return rec.ser(dst, reflect.NewAt(innerType, pp).Elem(), depth+1)
	}
}

// udNullUnionPtr handles null-union deser for *T where T has a primitive unsafe deserializer.
func udNullUnionPtr(inner udeserfn, innerType reflect.Type, valIdx int, nullByte, valByte byte) udeserfn {
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		isVal, src, err := readNullUnionIndex(src, valIdx, nullByte, valByte)
		if err != nil {
			return nil, err
		}
		if !isVal {
			*(*unsafe.Pointer)(p) = nil
			return src, nil
		}
		pp := *(*unsafe.Pointer)(p)
		if pp == nil {
			v := reflect.New(innerType)
			pp = v.UnsafePointer()
			*(*unsafe.Pointer)(p) = pp
		}
		return inner(src, pp, sl)
	}
}

// udNullUnionRecord handles null-union deser for *T where T is a record.
//
// Depth bookkeeping note: this function does NOT bump sl.depth itself.
// It relies on the inner record-entry — deserRecordFastPtr (fast path)
// or rec.deser (slow path) — to bump on the way in and decrement on
// the way out. Both currently do. If a future change adds a third
// record-entry path that's invoked here, that path MUST also bump
// sl.depth, or recursive ["null", T] schemas will silently lose depth
// tracking.
func udNullUnionRecord(rec *deserRecord, innerType reflect.Type, valIdx int, nullByte, valByte byte) udeserfn {
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		isVal, src, err := readNullUnionIndex(src, valIdx, nullByte, valByte)
		if err != nil {
			return nil, err
		}
		if !isVal {
			*(*unsafe.Pointer)(p) = nil
			return src, nil
		}
		pp := *(*unsafe.Pointer)(p)
		if pp == nil {
			v := reflect.New(innerType)
			pp = v.UnsafePointer()
			*(*unsafe.Pointer)(p) = pp
		}
		if fast := rec.fast.Load(); fast != nil && fast.typ == innerType && fast.allFast {
			return deserRecordFastPtr(src, fast, pp, sl)
		}
		return rec.deser(src, reflect.NewAt(innerType, pp).Elem(), sl)
	}
}

// ---- Array unsafe ser/deser ----

// usArrayRecord handles array ser for []T or []*T where items are records.
func usArrayRecord(rec *serRecord, elemGoType reflect.Type) userfn {
	if elemGoType.Kind() == reflect.Pointer {
		return usArrayPtrRecord(rec, elemGoType.Elem())
	}
	elemSize := elemGoType.Size()
	return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
		if depth >= maxDepth {
			return nil, errTooDeep
		}
		bs := *(*[]byte)(p)
		n := len(bs)
		dst = appendVarlong(dst, int64(n))
		if n == 0 {
			return dst, nil
		}
		data := unsafe.Pointer(unsafe.SliceData(bs))
		fast := rec.fast.Load()
		useFast := fast != nil && fast.typ == elemGoType && fast.allFast
		var err error
		for i := range n {
			elemP := unsafe.Add(data, uintptr(i)*elemSize)
			if useFast {
				dst, err = serRecordFastPtr(dst, fast, elemP, depth+1)
			} else {
				dst, err = rec.ser(dst, reflect.NewAt(elemGoType, elemP).Elem(), depth+1)
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
	return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
		if depth >= maxDepth {
			return nil, errTooDeep
		}
		s := *(*[]unsafe.Pointer)(p)
		n := len(s)
		dst = appendVarlong(dst, int64(n))
		if n == 0 {
			return dst, nil
		}
		fast := rec.fast.Load()
		useFast := fast != nil && fast.typ == innerType && fast.allFast
		var err error
		for _, pp := range s {
			if pp == nil {
				return nil, errIndirectNil
			}
			if useFast {
				dst, err = serRecordFastPtr(dst, fast, pp, depth+1)
			} else {
				dst, err = rec.ser(dst, reflect.NewAt(innerType, pp).Elem(), depth+1)
			}
			if err != nil {
				return nil, err
			}
		}
		return append(dst, 0), nil
	}
}

// usArrayNullUnionRecord handles array ser for []*T where items are ["null", Record].
func usArrayNullUnionRecord(rec *serRecord, innerType reflect.Type, nullByte, valByte byte) userfn {
	return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
		if depth >= maxDepth {
			return nil, errTooDeep
		}
		s := *(*[]unsafe.Pointer)(p)
		n := len(s)
		dst = appendVarlong(dst, int64(n))
		if n == 0 {
			return dst, nil
		}
		fast := rec.fast.Load()
		useFast := fast != nil && fast.typ == innerType && fast.allFast
		var err error
		for _, pp := range s {
			if pp == nil {
				dst = append(dst, nullByte)
			} else {
				dst = append(dst, valByte)
				if useFast {
					dst, err = serRecordFastPtr(dst, fast, pp, depth+1)
				} else {
					dst, err = rec.ser(dst, reflect.NewAt(innerType, pp).Elem(), depth+1)
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
func usArrayNullUnionPtr(inner userfn, nullByte, valByte byte) userfn {
	return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
		if depth >= maxDepth {
			return nil, errTooDeep
		}
		s := *(*[]unsafe.Pointer)(p)
		n := len(s)
		dst = appendVarlong(dst, int64(n))
		if n == 0 {
			return dst, nil
		}
		var err error
		for _, pp := range s {
			if pp == nil {
				dst = append(dst, nullByte)
			} else {
				dst, err = inner(append(dst, valByte), pp, depth+1)
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
	return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
		if depth >= maxDepth {
			return nil, errTooDeep
		}
		bs := *(*[]byte)(p)
		n := len(bs)
		dst = appendVarlong(dst, int64(n))
		if n == 0 {
			return dst, nil
		}
		data := unsafe.Pointer(unsafe.SliceData(bs))
		var err error
		for i := range n {
			dst, err = inner(dst, unsafe.Add(data, uintptr(i)*elemSize), depth+1)
			if err != nil {
				return nil, err
			}
		}
		return append(dst, 0), nil
	}
}

// udArrayPtrRecord handles array deser for []*T where items are records.
// Uses reflect for slice management, unsafe for per-element record deser.
func udArrayPtrRecord(rec *deserRecord, innerType, sliceType reflect.Type, minItemBytes int) udeserfn {
	innerSize := innerType.Size()
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		if sl.depth >= maxDepth {
			return nil, errTooDeep
		}
		sl.depth++
		defer func() { sl.depth-- }()
		v := reflect.NewAt(sliceType, p).Elem()
		v.SetLen(0)
		var err error
		var totalItems int64
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
			if err := checkArrayBlockBounds(count, totalItems, len(src), minItemBytes); err != nil {
				return nil, err
			}
			totalItems += count
			n := int(count)
			start := v.Len()
			if start > math.MaxInt-n {
				return nil, fmt.Errorf("array length overflows int: start=%d count=%d", start, n)
			}
			newLen := start + n
			if v.Cap() < newLen {
				ns := reflect.MakeSlice(sliceType, newLen, newLen)
				reflect.Copy(ns, v)
				v.Set(ns)
			} else {
				v.SetLen(newLen)
			}
			s := *(*[]unsafe.Pointer)(p)
			// Batch-allocate backing memory for nil pointer slots in
			// one contiguous slice, then distribute pointers into the
			// individual slots. This is much cheaper than allocating
			// each element separately.
			var need int
			for i := range n {
				if s[start+i] == nil {
					need++
				}
			}
			if need > 0 {
				backing := reflect.MakeSlice(reflect.SliceOf(innerType), need, need)
				backingBase := backing.Index(0).Addr().UnsafePointer()
				j := 0
				for i := range n {
					if s[start+i] == nil {
						s[start+i] = unsafe.Add(backingBase, uintptr(j)*innerSize)
						j++
					}
				}
			}
			// Deserialize each element.
			fast := rec.fast.Load()
			useFast := fast != nil && fast.typ == innerType && fast.allFast
			for i := range n {
				elemP := s[start+i]
				if useFast {
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
// minItemBytes is the schema-derived per-item wire-byte minimum (1 for
// varint primitives, 4 for float, 8 for double, etc.) — bounds block
// count to len(src)/minItemBytes, mirroring deserArray.deser. Without
// it the loose count > len(src) check would let a hostile float-array
// stream drive a 4× MakeSlice allocation per wire byte before the
// per-element readUint32 loop fails on short buffer.
func udArrayDirect(inner udeserfn, elemSize uintptr, sliceType reflect.Type, minItemBytes int) udeserfn {
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		if sl.depth >= maxDepth {
			return nil, errTooDeep
		}
		sl.depth++
		defer func() { sl.depth-- }()
		v := reflect.NewAt(sliceType, p).Elem()
		v.SetLen(0)
		var err error
		var totalItems int64
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
			if err := checkArrayBlockBounds(count, totalItems, len(src), minItemBytes); err != nil {
				return nil, err
			}
			totalItems += count
			n := int(count)
			start := v.Len()
			if start > math.MaxInt-n {
				return nil, fmt.Errorf("array length overflows int: start=%d count=%d", start, n)
			}
			newLen := start + n
			if v.Cap() < newLen {
				ns := reflect.MakeSlice(sliceType, newLen, newLen)
				reflect.Copy(ns, v)
				v.Set(ns)
			} else {
				v.SetLen(newLen)
			}
			// Access data pointer after potential reallocation. v.UnsafePointer
			// returns the slice's underlying-array pointer for any element
			// type — avoids the type-pun via *(*[]byte)(p).
			data := v.UnsafePointer()
			for i := start; i < newLen; i++ {
				src, err = inner(src, unsafe.Add(data, uintptr(i)*elemSize), sl)
				if err != nil {
					return nil, err
				}
			}
		}
	}
}
