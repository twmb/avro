package avro

import (
	"fmt"
	"math"
	"math/bits"
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

// usTimestampLogicals maps the six long-typed timestamp logicals to
// their time.Time-target unsafe serializer. Non-time.Time targets fall
// back to usLong(kind) at the dispatch site below.
var usTimestampLogicals = map[string]userfn{
	"timestamp-millis":       usTimestampMillis,
	"timestamp-micros":       usTimestampMicros,
	"timestamp-nanos":        usTimestampNanos,
	"local-timestamp-millis": usLocalTimestampMillis,
	"local-timestamp-micros": usLocalTimestampMicros,
	"local-timestamp-nanos":  usLocalTimestampNanos,
}

func tryCompileLogicalSer(logical, avroType string, goType reflect.Type) userfn {
	if fn, ok := usTimestampLogicals[logical]; ok {
		if goType == timeType {
			return fn
		}
		return usLong(goType.Kind())
	}
	switch logical {
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

// udTimestampLogicals maps the six long-typed timestamp logicals to
// their time.Time-target unsafe deserializer (local-timestamp-* and
// timestamp-* decode identically; the wire long is interpreted the same
// way — see logical.go for the encode-side wall-clock vs instant note).
var udTimestampLogicals = map[string]udeserfn{
	"timestamp-millis":       udTimestampMillis,
	"timestamp-micros":       udTimestampMicros,
	"timestamp-nanos":        udTimestampNanos,
	"local-timestamp-millis": udTimestampMillis,
	"local-timestamp-micros": udTimestampMicros,
	"local-timestamp-nanos":  udTimestampNanos,
}

func tryCompileLogicalDeser(logical, avroType string, goType reflect.Type) udeserfn {
	if fn, ok := udTimestampLogicals[logical]; ok {
		if goType == timeType {
			return fn
		}
		return udLong(goType.Kind())
	}
	switch logical {
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
	ms, err := durationToTimeMillis(timeOfDay(*(*time.Time)(p)))
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
	return appendVarlong(dst, timeOfDay(*(*time.Time)(p)).Microseconds()), nil
}

// udTimeFromVarint reads a varint and stores conv(val) into *T.
func udTimeFromVarint[T any](conv func(int32) T) udeserfn {
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		val, src, err := readVarint(src)
		if err != nil {
			return nil, err
		}
		*(*T)(p) = conv(val)
		return src, nil
	}
}

// udTimeFromVarlong is udTimeFromVarint's varlong sibling.
func udTimeFromVarlong[T any](conv func(int64) T) udeserfn {
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		val, src, err := readVarlong(src)
		if err != nil {
			return nil, err
		}
		*(*T)(p) = conv(val)
		return src, nil
	}
}

// udTimeFromVarlongChecked is udTimeFromVarlong for fallible converters.
func udTimeFromVarlongChecked[T any](conv func(int64) (T, error)) udeserfn {
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		val, src, err := readVarlong(src)
		if err != nil {
			return nil, err
		}
		out, err := conv(val)
		if err != nil {
			return nil, err
		}
		*(*T)(p) = out
		return src, nil
	}
}

// *Time variants materialize the time-of-day count as a time.Time at
// epoch midnight (UTC) — used when a struct field is typed as time.Time
// but the schema is time-millis/time-micros.
var (
	udTimestampMillis = udTimeFromVarlong(timestampMillisToTime)
	udTimestampMicros = udTimeFromVarlong(timestampMicrosToTime)
	udTimestampNanos  = udTimeFromVarlong(timestampNanosToTime)
	udDate            = udTimeFromVarint(dateToTime)
	udTimeMillis      = udTimeFromVarint(timeMillisToDuration)
	udTimeMillisTime  = udTimeFromVarint(func(v int32) time.Time {
		return timeOfDayToTime(timeMillisToDuration(v))
	})
	udTimeMicros     = udTimeFromVarlongChecked(timeMicrosToDuration)
	udTimeMicrosTime = udTimeFromVarlongChecked(func(v int64) (time.Time, error) {
		d, err := timeMicrosToDuration(v)
		if err != nil {
			return time.Time{}, err
		}
		return timeOfDayToTime(d), nil
	})
)

func usDuration(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	b := (*(*Duration)(p)).Bytes()
	return append(dst, b[:]...), nil
}

func udDuration(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	if len(src) < 12 {
		return nil, &ShortBufferError{Type: "duration", Need: 12, Have: len(src)}
	}
	*(*Duration)(p) = DurationFromBytes(src[:12])
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

// readFixedUUID validates len(src) ≥ 16 and returns the next UUID bytes
// plus the advanced source slice. Shared body of udFixedUUID (writes
// [16]byte) and udFixedUUIDString (writes the canonical string form).
func readFixedUUID(src []byte) ([16]byte, []byte, error) {
	if len(src) < 16 {
		return [16]byte{}, nil, &ShortBufferError{Type: "uuid", Need: 16, Have: len(src)}
	}
	return [16]byte(src[:16]), src[16:], nil
}

// udFixedUUID reads 16 raw bytes from a fixed(16) UUID and writes a [16]byte.
// Used when the target is [16]byte or any (interface).
func udFixedUUID(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	u, src, err := readFixedUUID(src)
	if err == nil {
		*(*[16]byte)(p) = u
	}
	return src, err
}

// udFixedUUIDString reads 16 raw bytes from a fixed(16) UUID and writes a
// formatted UUID string (e.g. "550e8400-e29b-41d4-a716-446655440000").
func udFixedUUIDString(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	u, src, err := readFixedUUID(src)
	if err == nil {
		*(*string)(p) = uuidToString(u)
	}
	return src, err
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

// usVarintFrom is udVarintTo's serialize-side mirror: reads a typed
// integer via unsafe pointer, range-checks, writes as Avro int.
func usVarintFrom[T intLike](lo, hi int64) userfn {
	return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
		n := int64(*(*T)(p))
		if n < lo || n > hi {
			return nil, &SemanticError{AvroType: "int", Err: fmt.Errorf("value %d overflows int32", n)}
		}
		return appendVarint(dst, int32(n)), nil
	}
}

// usVarlongFrom is usVarintFrom's varlong (Avro long) sibling.
func usVarlongFrom[T intLike](lo, hi int64) userfn {
	return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
		n := int64(*(*T)(p))
		if n < lo || n > hi {
			return nil, &SemanticError{AvroType: "long", Err: fmt.Errorf("value %d overflows int64", n)}
		}
		return appendVarlong(dst, n), nil
	}
}

// usVarlongFromUnsigned is usVarlongFrom for unsigned T. The upper
// bound is checked in uint64 space since uint64 > MaxInt64 can't
// round-trip through int64.
func usVarlongFromUnsigned[T uint | uint8 | uint16 | uint32 | uint64]() userfn {
	return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
		n := uint64(*(*T)(p))
		if n > math.MaxInt64 {
			return nil, &SemanticError{AvroType: "long", Err: fmt.Errorf("value %d overflows int64", n)}
		}
		return appendVarlong(dst, int64(n)), nil
	}
}

func usInt(k reflect.Kind) userfn {
	switch k {
	case reflect.Int:
		// On 64-bit int can exceed int32; bound check applies.
		return usVarintFrom[int](math.MinInt32, math.MaxInt32)
	case reflect.Int8:
		return usVarintFrom[int8](math.MinInt64, math.MaxInt64)
	case reflect.Int16:
		return usVarintFrom[int16](math.MinInt64, math.MaxInt64)
	case reflect.Int32:
		return usVarintFrom[int32](math.MinInt64, math.MaxInt64)
	case reflect.Int64:
		return usVarintFrom[int64](math.MinInt32, math.MaxInt32)
	case reflect.Uint:
		// uint as int64 max — bound applies on 64-bit too (uint > MaxInt32 possible).
		return usVarintFrom[uint](0, math.MaxInt32)
	case reflect.Uint8:
		return usVarintFrom[uint8](math.MinInt64, math.MaxInt64)
	case reflect.Uint16:
		return usVarintFrom[uint16](math.MinInt64, math.MaxInt64)
	case reflect.Uint32:
		return usVarintFrom[uint32](0, math.MaxInt32)
	case reflect.Uint64:
		// uint64 > MaxInt64 can't be represented as int64 — but the
		// usVarintFrom int64 cast already truncates negative-when-
		// reinterpreted values. Match the prior behavior by using
		// the unsigned-aware bound directly.
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
		return usVarlongFrom[int](math.MinInt64, math.MaxInt64)
	case reflect.Int8:
		return usVarlongFrom[int8](math.MinInt64, math.MaxInt64)
	case reflect.Int16:
		return usVarlongFrom[int16](math.MinInt64, math.MaxInt64)
	case reflect.Int32:
		return usVarlongFrom[int32](math.MinInt64, math.MaxInt64)
	case reflect.Int64:
		return usVarlongFrom[int64](math.MinInt64, math.MaxInt64)
	case reflect.Uint:
		return usVarlongFromUnsigned[uint]()
	case reflect.Uint8:
		return usVarlongFromUnsigned[uint8]()
	case reflect.Uint16:
		return usVarlongFromUnsigned[uint16]()
	case reflect.Uint32:
		return usVarlongFromUnsigned[uint32]()
	case reflect.Uint64:
		return usVarlongFromUnsigned[uint64]()
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

// intLike covers all signed and unsigned integer kinds.
type intLike interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

// udVarintTo reads a varint (int32 wire), range-checks against [lo, hi]
// in int64 space, and stores the narrowed result into *T. lo=MinInt64 /
// hi=MaxInt64 disables the range check.
func udVarintTo[T intLike](lo, hi int64, avroType, targetName string) udeserfn {
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		v, src, err := readVarint(src)
		if err != nil {
			return nil, err
		}
		if int64(v) < lo || int64(v) > hi {
			return nil, &SemanticError{AvroType: avroType, Err: fmt.Errorf("value %d overflows %s", v, targetName)}
		}
		*(*T)(p) = T(v)
		return src, nil
	}
}

// udVarlongTo is udVarintTo's varlong (int64 wire) sibling.
func udVarlongTo[T intLike](lo, hi int64, avroType, targetName string) udeserfn {
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		v, src, err := readVarlong(src)
		if err != nil {
			return nil, err
		}
		if v < lo || v > hi {
			return nil, &SemanticError{AvroType: avroType, Err: fmt.Errorf("value %d overflows %s", v, targetName)}
		}
		*(*T)(p) = T(v)
		return src, nil
	}
}

func udInt(k reflect.Kind) udeserfn {
	switch k {
	case reflect.Int:
		// int32 wire always fits in int (int is int32 or int64).
		return udVarintTo[int](math.MinInt64, math.MaxInt64, "int", "int")
	case reflect.Int8:
		return udVarintTo[int8](math.MinInt8, math.MaxInt8, "int", "int8")
	case reflect.Int16:
		return udVarintTo[int16](math.MinInt16, math.MaxInt16, "int", "int16")
	case reflect.Int32:
		return udVarintTo[int32](math.MinInt64, math.MaxInt64, "int", "int32")
	case reflect.Int64:
		return udVarintTo[int64](math.MinInt64, math.MaxInt64, "int", "int64")
	case reflect.Uint:
		// Varint wire is int32; uint is always wide enough — only
		// the lower-bound (v < 0) check matters. hi=MaxInt64 is
		// effectively unbounded.
		return udVarintTo[uint](0, math.MaxInt64, "int", "uint")
	case reflect.Uint8:
		return udVarintTo[uint8](0, math.MaxUint8, "int", "uint8")
	case reflect.Uint16:
		return udVarintTo[uint16](0, math.MaxUint16, "int", "uint16")
	case reflect.Uint32:
		return udVarintTo[uint32](0, math.MaxInt64, "int", "uint32")
	case reflect.Uint64:
		return udVarintTo[uint64](0, math.MaxInt64, "int", "uint64")
	default:
		return nil
	}
}

func udLong(k reflect.Kind) udeserfn {
	switch k {
	case reflect.Int:
		// On 64-bit int holds any int64; on 32-bit int is int32 so
		// the bound check is real. math.MinInt/MaxInt resolve per
		// platform.
		return udVarlongTo[int](math.MinInt, math.MaxInt, "long", "int")
	case reflect.Int8:
		return udVarlongTo[int8](math.MinInt8, math.MaxInt8, "long", "int8")
	case reflect.Int16:
		return udVarlongTo[int16](math.MinInt16, math.MaxInt16, "long", "int16")
	case reflect.Int32:
		return udVarlongTo[int32](math.MinInt32, math.MaxInt32, "long", "int32")
	case reflect.Int64:
		return udVarlongTo[int64](math.MinInt64, math.MaxInt64, "long", "int64")
	case reflect.Uint:
		// On 64-bit uint = uint64 holds any non-negative int64; on
		// 32-bit uint = uint32, so cap at MaxUint32. bits.UintSize
		// is a compile-time constant so the branch resolves to
		// dead code on the non-matching platform.
		hi := int64(math.MaxInt64)
		if bits.UintSize == 32 {
			hi = math.MaxUint32
		}
		return udVarlongTo[uint](0, hi, "long", "uint")
	case reflect.Uint8:
		return udVarlongTo[uint8](0, math.MaxUint8, "long", "uint8")
	case reflect.Uint16:
		return udVarlongTo[uint16](0, math.MaxUint16, "long", "uint16")
	case reflect.Uint32:
		return udVarlongTo[uint32](0, math.MaxUint32, "long", "uint32")
	case reflect.Uint64:
		return udVarlongTo[uint64](0, math.MaxInt64, "long", "uint64")
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

// udNullUnionEnter is the shared preamble for udNullUnionPtr / udNullUnionRecord:
// read the union index, nil-out *T on the null branch, allocate inner
// storage on the value branch. Returns (pp, src, isNull, err). When
// isNull is true the caller should return src directly (the pointer
// field has already been zeroed). Otherwise pp points at freshly-
// allocated inner storage that the caller's per-branch decoder fills.
func udNullUnionEnter(src []byte, p unsafe.Pointer, innerType reflect.Type, valIdx int, nullByte, valByte byte) (pp unsafe.Pointer, rest []byte, isNull bool, err error) {
	isVal, src, err := readNullUnionIndex(src, valIdx, nullByte, valByte)
	if err != nil {
		return nil, nil, false, err
	}
	if !isVal {
		*(*unsafe.Pointer)(p) = nil
		return nil, src, true, nil
	}
	pp = *(*unsafe.Pointer)(p)
	if pp == nil {
		v := reflect.New(innerType)
		pp = v.UnsafePointer()
		*(*unsafe.Pointer)(p) = pp
	}
	return pp, src, false, nil
}

// udNullUnionPtr handles null-union deser for *T where T has a primitive unsafe deserializer.
func udNullUnionPtr(inner udeserfn, innerType reflect.Type, valIdx int, nullByte, valByte byte) udeserfn {
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		pp, src, isNull, err := udNullUnionEnter(src, p, innerType, valIdx, nullByte, valByte)
		if err != nil || isNull {
			return src, err
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
		pp, src, isNull, err := udNullUnionEnter(src, p, innerType, valIdx, nullByte, valByte)
		if err != nil || isNull {
			return src, err
		}
		if fast := rec.fast.Load(); fast != nil && fast.typ == innerType && fast.allFast {
			return deserRecordFastPtr(src, fast, pp, sl)
		}
		return rec.deser(src, reflect.NewAt(innerType, pp).Elem(), sl)
	}
}

// ---- Array unsafe ser/deser ----
//
// R4 attempted a usSliceFrame[Elem] generic factor of the (depth-check +
// length-prefix + early-exit + per-element body + terminator) sequence.
// Benchstat against the inline-five-copies baseline showed +16% on
// BenchmarkLargeArrayEncode (the []*Record hot path) — beyond the 5%
// audit threshold. The extra closure call per slice (body func passed
// to usSliceFrame) combined with the Go compiler choosing not to inline
// the generic at the call site costs measurable per-array work. Reverted.
// See DRY_AUDIT.md R4 for the proposal that was rejected on perf grounds.

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
				continue
			}
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
				continue
			}
			dst, err = inner(append(dst, valByte), pp, depth+1)
			if err != nil {
				return nil, err
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

// udArrayBlocks drives the outer block loop for the unsafe array
// deserializers. onBlock fills slots [start, start+n) of v's backing
// array; src→advancedSrc is returned. Shared by udArrayPtrRecord and
// udArrayDirect so the depth bookkeeping, length overflow guard,
// MakeSlice/SetLen growth, and block-bounds check live in one place.
//
// minItemBytes is the schema-derived per-item wire-byte minimum (1 for
// varint primitives, 4 for float, 8 for double, etc.) — bounds block
// count to len(src)/minItemBytes, mirroring deserArray.deser. Without
// it the loose count > len(src) check would let a hostile float-array
// stream drive a 4× MakeSlice allocation per wire byte before the
// per-element readUint32 loop fails on short buffer.
//
// Benchstat verified perf-neutral on BenchmarkLargeArrayDecode (sister
// of LargeArrayEncode, exercising []*Record fast-path decode).
func udArrayBlocks(
	src []byte, p unsafe.Pointer, sl *slab,
	sliceType reflect.Type, minItemBytes int,
	onBlock func(src []byte, v reflect.Value, p unsafe.Pointer, start, n int, sl *slab) ([]byte, error),
) ([]byte, error) {
	if sl.depth >= maxDepth {
		return nil, errTooDeep
	}
	sl.depth++
	defer func() { sl.depth-- }()
	v := reflect.NewAt(sliceType, p).Elem()
	v.SetLen(0)
	var totalItems int64
	for {
		count, _, rest, end, err := readBlockHeader(src, "array", false)
		if err != nil {
			return nil, err
		}
		src = rest
		if end {
			return src, nil
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
		if src, err = onBlock(src, v, p, start, n, sl); err != nil {
			return nil, err
		}
	}
}

// udArrayPtrRecord handles array deser for []*T where items are records.
// Uses reflect for slice management, unsafe for per-element record deser.
func udArrayPtrRecord(rec *deserRecord, innerType, sliceType reflect.Type, minItemBytes int) udeserfn {
	innerSize := innerType.Size()
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		return udArrayBlocks(src, p, sl, sliceType, minItemBytes,
			func(src []byte, _ reflect.Value, p unsafe.Pointer, start, n int, sl *slab) ([]byte, error) {
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
				var err error
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
				return src, nil
			})
	}
}

// udArrayDirect handles array deser for []T where items are primitives.
// Uses reflect for slice management, unsafe for per-element deser.
func udArrayDirect(inner udeserfn, elemSize uintptr, sliceType reflect.Type, minItemBytes int) udeserfn {
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		return udArrayBlocks(src, p, sl, sliceType, minItemBytes,
			func(src []byte, v reflect.Value, _ unsafe.Pointer, start, n int, sl *slab) ([]byte, error) {
				// Access data pointer after the slice growth in
				// udArrayBlocks. v.UnsafePointer returns the slice's
				// underlying-array pointer for any element type — avoids
				// the type-pun via *(*[]byte)(p).
				data := v.UnsafePointer()
				var err error
				for i := start; i < start+n; i++ {
					src, err = inner(src, unsafe.Add(data, uintptr(i)*elemSize), sl)
					if err != nil {
						return nil, err
					}
				}
				return src, nil
			})
	}
}
