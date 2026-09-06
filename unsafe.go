package avro

import (
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"
	"unsafe"
)

// userfn serializes the value at p into dst. p points straight at the Go
// field's memory, and we only ever read through it.
type userfn func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error)

type udeserfn func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error)

type fastRecordSer struct {
	typ     reflect.Type
	allFast bool
	fields  []fastFieldSer
}

// fastFieldSer is a hybrid: primitive fields take the unsafe fast path
// (ser != nil), complex and union fields take reflect FieldByIndex
// (slowFn != nil). Mixing keeps the primitive subset off reflect.NewAt.
type fastFieldSer struct {
	offset   uintptr
	name     string
	ser      userfn // non-nil for unsafe-optimized fields (primitives)
	slowFn   serfn  // non-nil for reflect-based fields (complex types)
	slowIdx  []int  // field index path for FieldByIndex (used with slowFn)
	omitzero bool   // true when omitzero acts on this field (fills a default or null)
	// When omitzero fires on a zero value, the bytes we emit: the field's
	// default (ozDefault) or the null-branch index byte (ozNull). Precomputed
	// at compile from omitzeroAction so we match the reflect path.
	omitzeroBytes []byte
	// omitzeroErr carries serRecordField.defaultErr through the compile, so
	// this path refuses where the reflect path refuses.
	omitzeroErr error
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
	skip    skipfn   // non-nil when SkipUnknown found no Go field for this one
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
		// An omitzero field that actually acts (fills a default or a null
		// branch; see omitzeroAction) needs a runtime zero check the unsafe
		// path can't do, so we fall back to reflect. A no-op omitzero
		// (non-nullable, no default) just encodes the value and stays fast.
		oz := mapping.omitzero[i] && f.omitzeroAction() != ozNoop
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
				switch f.omitzeroAction() {
				case ozDefault:
					ffs.omitzeroBytes = f.defaultBytes
					ffs.omitzeroErr = f.defaultErr
				case ozNull:
					nb, _ := nullUnionBytes(f.meta != nil && f.meta.nullSecond)
					ffs.omitzeroBytes = []byte{nb}
				}
			}
			fast.fields[i] = ffs
		}
	}
	fast.allFast = allFast
	return fast
}

func compileFastDeser(rec *deserRecord, t reflect.Type, skipUnknown bool) *fastRecordDeser {
	fields := rec.fields
	mapping, err := typeFieldMappingSkip(rec.names, &rec.cache, t, skipUnknown)
	if err != nil {
		return nil
	}
	fast := &fastRecordDeser{typ: t, fields: make([]fastFieldDeser, len(fields))}
	allFast := true
	var skips []skipfn
	for i := range fields {
		f := &fields[i]
		if mapping.unmapped(i) {
			if skips == nil {
				skips = rec.fieldSkips()
			}
			allFast = false
			fast.fields[i] = fastFieldDeser{name: f.name, skip: skips[i]}
			continue
		}
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

// computeFieldOffset flattens a struct field index path into one byte offset.
// We return false for a path through a pointer: that needs a runtime deref,
// so there is nothing to precompute.
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
			fv := fieldByIndexZero(v, f.slowIdx)
			if f.omitzero && valueIsZero(fv) {
				// We populated f.omitzeroBytes at compile (compileFastSer)
				// from omitzeroAction: the field's default, or the
				// null-branch byte (0x00 for ["null",T], 0x02 for
				// ["T","null"]).
				if f.omitzeroErr != nil {
					return nil, recordFieldError(fast.typ, f.name, f.omitzeroErr)
				}
				dst = append(dst, f.omitzeroBytes...)
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

// deserRecordFast is the unsafe fast body for one record node. Its only
// caller, deserRecord.deser, already bumped sl.depth for this node, so we do
// not bump again: a record costs one depth unit on every path.
// deserRecordFastPtr, the other fast entry, is reached on child edges that
// bypass deserRecord.deser, so it keeps its own bump.
func deserRecordFast(src []byte, fast *fastRecordDeser, v reflect.Value, sl *slab) ([]byte, error) {
	base := v.Addr().UnsafePointer()
	var err error
	for i := range fast.fields {
		f := &fast.fields[i]
		if f.skip != nil {
			src, err = f.skip(src, sl)
		} else if f.deser != nil {
			src, err = f.deser(src, unsafe.Add(base, f.offset), sl)
		} else {
			fv, ferr := fieldByIndex(v, f.slowIdx)
			if ferr != nil {
				return nil, recordFieldError(fast.typ, f.name, ferr)
			}
			src, err = f.slowFn(src, fv, sl)
		}
		if err != nil {
			return nil, recordFieldError(fast.typ, f.name, err)
		}
	}
	return src, nil
}

// serRecordFastPtr serializes a record whose fields all have unsafe ser fns.
// We need only a raw pointer to the struct base, no reflect.Value.
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

// deserRecordFastPtr deserializes a record whose fields all have unsafe
// deser fns.
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

// serRecordVia encodes one record through the all-fast unsafe path when we
// have one, else through reflect. You pass fast pre-resolved (rec.fastFor) so
// a per-element loop looks it up once, outside the loop.
//
// Do *not* inline this into per-element array loops: the helper blows the Go
// inline budget (cost ~290 vs 80) because reflect.NewAt+Elem counts as
// interface method calls, and the t/rec parameters escape to heap. The four
// array sites in this file (usArrayRecord, usArrayPtrRecord,
// usArrayNullUnionRecord, udArrayPtrRecord) keep the if/else open-coded with
// useFast hoisted out of the loop.
func serRecordVia(dst []byte, fast *fastRecordSer, rec *serRecord, t reflect.Type, p unsafe.Pointer, depth int) ([]byte, error) {
	if fast != nil && fast.allFast {
		return serRecordFastPtr(dst, fast, p, depth)
	}
	return rec.ser(dst, reflect.NewAt(t, p).Elem(), depth)
}

// deserRecordVia is serRecordVia's decode-side mirror. The same inline-budget
// caveat applies: do not use it in per-element loops.
func deserRecordVia(src []byte, fast *fastRecordDeser, rec *deserRecord, t reflect.Type, p unsafe.Pointer, sl *slab) ([]byte, error) {
	if fast != nil && fast.allFast {
		return deserRecordFastPtr(src, fast, p, sl)
	}
	return rec.deser(src, reflect.NewAt(t, p).Elem(), sl)
}

// tryCompileFieldSer returns a userfn for a field we can handle entirely
// through unsafe pointer access, or nil for one that must take the reflect
// slow path.
func tryCompileFieldSer(f *serRecordField, goType reflect.Type) userfn {
	// Custom types need the reflect slow path for the conversion wrapper.
	if f.meta != nil && (f.meta.hasCustomType || (f.meta.inner != nil && f.meta.inner.hasCustomType)) {
		return nil
	}
	k := goType.Kind()

	// Regular unions need the reflect slow path.
	if f.avroType() == "union" {
		return nil
	}

	// Null-union: *T mapped to ["null", T] or [T, "null"].
	if f.avroType() == "nullunion" {
		if k != reflect.Pointer {
			return nil
		}
		if f.meta.inner == nil {
			return nil
		}
		nullByte, valByte := nullUnionBytes(f.meta.nullSecond)
		inner := f.meta.inner
		if inner.hasCustomType {
			return nil
		}
		innerGoType := goType.Elem()
		// A non-nil *T wrapping a nil pointer, slice, map, or interface is
		// nil-equivalent per isNilValue, so the reflect path encodes the
		// null branch. usNullUnionEnter tests only the outer pointer and
		// would commit to the value branch, so we decline every nilable
		// inner kind to reflect.
		if isNilableKind(innerGoType.Kind()) {
			return nil
		}
		if inner.serRecord != nil {
			return usNullUnionRecord(inner.serRecord, innerGoType, nullByte, valByte)
		}
		innerFn := tryCompileFieldSer(&serRecordField{meta: inner}, innerGoType)
		if innerFn != nil {
			return usNullUnionPtr(innerFn, nullByte, valByte)
		}
		return nil
	}

	// Array: []T or []*T.
	if f.avroType() == "array" {
		if k != reflect.Slice {
			return nil
		}
		if f.meta.inner == nil {
			return nil
		}
		inner := f.meta.inner
		elemGoType := goType.Elem()
		switch inner.avroType {
		case "nullunion":
			if elemGoType.Kind() != reflect.Pointer {
				return nil
			}
			// Same rule as the field nullunion case above: a nilable inner
			// kind is nil-equivalent per isNilValue, so it declines to
			// reflect.
			if isNilableKind(elemGoType.Elem().Kind()) {
				return nil
			}
			nullByte, valByte := nullUnionBytes(inner.nullSecond)
			if inner.inner != nil && inner.inner.hasCustomType {
				return nil
			}
			if inner.inner != nil && inner.inner.serRecord != nil {
				return usArrayNullUnionRecord(inner.inner.serRecord, elemGoType.Elem(), nullByte, valByte)
			}
			innerFn := tryCompileFieldSer(&serRecordField{meta: inner.inner}, elemGoType.Elem())
			if innerFn != nil {
				return usArrayNullUnionPtr(innerFn, nullByte, valByte)
			}
		case "record":
			if inner.serRecord != nil {
				return usArrayRecord(inner.serRecord, elemGoType)
			}
		default:
			innerFn := tryCompileFieldSer(&serRecordField{meta: inner}, elemGoType)
			if innerFn != nil {
				return usArrayDirect(innerFn, elemGoType.Size())
			}
		}
		return nil
	}

	// Record: struct T.
	if f.avroType() == "record" {
		if k != reflect.Struct {
			return nil
		}
		if f.meta.serRecord == nil {
			return nil
		}
		rec := f.meta.serRecord
		// depth, not depth+1: the field-pass call already charged the edge
		// from parent record to this one, and the field is the child record
		// node.
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			return serRecordVia(dst, rec.fastFor(goType), rec, goType, p, depth)
		}
	}

	if k == reflect.Pointer {
		// Bound the pointer-chain peel: a cyclic pointer type (type P *P)
		// would recurse here forever at compile time, and the reflect
		// encoder peels at most maxIndirectDepth levels, so deeper chains
		// route to reflect, which errors uniformly.
		levels := 1
		for e := goType.Elem(); e.Kind() == reflect.Pointer; e = e.Elem() {
			levels++
			if levels > maxIndirectDepth {
				return nil
			}
		}
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
		return tryCompileLogical(usLogicalFast, f.meta.logical, f.avroType(), goType)
	}

	switch f.avroType() {
	case "boolean":
		if k == reflect.Bool {
			return usBool
		}
	case "int":
		return usInteger[int32](goType)
	case "long":
		return usInteger[int64](goType)
	case "float":
		return usFloat(k)
	case "double":
		return usDouble(k)
	case "string":
		if k == reflect.String {
			// json.Number and text-method string kinds take the safe path;
			// the raw *(*string)(p) read would bypass appendAvroString's
			// json.Number reject and text-out arm.
			if !stringFastPathEligibleEncode(goType) {
				return nil
			}
			return usString
		}
	case "bytes":
		if k == reflect.Slice && goType.Elem().Kind() == reflect.Uint8 {
			return usBytes
		}
	}

	return nil
}

// tryCompileFieldDeser returns a udeserfn for a field we can write directly
// through unsafe, or nil for one that must take the reflect slow path.
func tryCompileFieldDeser(f *deserRecordField, goType reflect.Type) udeserfn {
	if f.meta != nil && (f.meta.hasCustomType || (f.meta.inner != nil && f.meta.inner.hasCustomType)) {
		return nil
	}
	k := goType.Kind()

	if f.avroType() == "union" {
		return nil
	}

	// Null-union: *T mapped to ["null", T] or [T, "null"].
	if f.avroType() == "nullunion" {
		if k != reflect.Pointer {
			return nil
		}
		if f.meta.inner == nil {
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
		// A multi-level pointer target (**T) declines to reflect: the
		// unsafe path consumes the outer pointer and then indirectAllocs
		// the rest, which would accept one level more than the
		// maxIndirectDepth every other path enforces. The record inner has
		// its own branch below, so this guard must precede it.
		if innerGoType.Kind() == reflect.Pointer {
			return nil
		}
		if inner.deserRecord != nil {
			return udNullUnionRecord(inner.deserRecord, innerGoType, valIdx, nullByte, valByte)
		}
		innerFn := tryCompileFieldDeser(&deserRecordField{meta: inner}, innerGoType)
		if innerFn != nil {
			return udNullUnionPtr(innerFn, innerGoType, valIdx, nullByte, valByte)
		}
		return nil
	}

	// Array: []T or []*T.
	if f.avroType() == "array" {
		if k != reflect.Slice {
			return nil
		}
		if f.meta.inner == nil {
			return nil
		}
		inner := f.meta.inner
		if inner.hasCustomType {
			return nil
		}
		elemGoType := goType.Elem()
		switch inner.avroType {
		case "record":
			// Single-pointer elements only ([]*record): udArrayPtrRecord
			// peels one level inline and rec.deser peels a further
			// maxIndirectDepth, so []**record would accept one level more
			// than every other path. It falls to default and declines.
			if inner.deserRecord != nil && elemGoType.Kind() == reflect.Pointer && elemGoType.Elem().Kind() != reflect.Pointer {
				return udArrayPtrRecord(inner.deserRecord, elemGoType.Elem(), goType, inner.minBytes)
			}
		default:
			innerFn := tryCompileFieldDeser(&deserRecordField{meta: inner}, elemGoType)
			if innerFn != nil {
				return udArrayDirect(innerFn, elemGoType.Size(), goType, inner.minBytes)
			}
		}
		return nil
	}

	// Record: struct T.
	if f.avroType() == "record" {
		if k != reflect.Struct {
			return nil
		}
		if f.meta.deserRecord == nil {
			return nil
		}
		rec := f.meta.deserRecord
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			return deserRecordVia(src, rec.fastFor(goType, sl.skipUnknown), rec, goType, p, sl)
		}
	}

	// Pointer fields need GC write barriers to allocate, so we take reflect.
	if k == reflect.Pointer {
		return nil
	}

	// Logical type fast paths for time.Time and time.Duration.
	if f.meta != nil && f.meta.logical != "" {
		return tryCompileLogical(udLogicalFast, f.meta.logical, f.avroType(), goType)
	}

	switch f.avroType() {
	case "boolean":
		if k == reflect.Bool {
			return udBool
		}
	case "int":
		return udInteger[int32](goType)
	case "long":
		return udInteger[int64](goType)
	case "float":
		return udFloat(k)
	case "double":
		return udDouble(goType)
	case "string":
		if k == reflect.String {
			// json.Number and TextUnmarshaler string kinds take the safe
			// path; the raw store would bypass setStringValue's json.Number
			// guard and UnmarshalText arm.
			if !stringFastPathEligibleDecode(goType) {
				return nil
			}
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

// usTimestampLogicals maps the six long-typed timestamp logicals to their
// time.Time-target unsafe serializer. A non-time.Time target falls back to
// usLong(kind) at the dispatch site below.
var usTimestampLogicals = map[string]userfn{
	"timestamp-millis":       usTimestampMillis,
	"timestamp-micros":       usTimestampMicros,
	"timestamp-nanos":        usTimestampNanos,
	"local-timestamp-millis": usLocalTimestampMillis,
	"local-timestamp-micros": usLocalTimestampMicros,
	"local-timestamp-nanos":  usLocalTimestampNanos,
}

// logicalFast is one direction's unsafe fast paths for the logical types,
// picked by tryCompileLogical. The ser and deser tables differ only in the
// functions they hold, so the two directions choose from one rule.
type logicalFast[F any] struct {
	timestamps                                                   map[string]F // the six long timestamps, time.Time target
	date, timeMillis, timeMillisTime, timeMicros, timeMicrosTime F
	duration                                                     F
	uuid, uuidString                                             F // uuid on a string schema: [16]byte target, string target
	// fixedUUID picks the target arm for uuid on a fixed schema. The two
	// directions cover different targets there: decode handles [16]byte,
	// encode leaves it to the default fixed serializer.
	fixedUUID func(goType reflect.Type) F
	// intFn and longFn are the integer fast paths a non-time target of an
	// int- or long-carried logical takes.
	intFn, longFn func(t reflect.Type) F
	// stringEligible reports whether a string-kind target may take the raw
	// string fast path; a json.Number or text-method string takes the safe
	// path instead.
	stringEligible func(t reflect.Type) bool
}

// tryCompileLogical returns fast's unsafe function for a logical type and
// Go target, or the zero F for a pairing that takes the reflect path.
func tryCompileLogical[F any](fast *logicalFast[F], logical, avroType string, goType reflect.Type) F {
	var none F
	if fn, ok := fast.timestamps[logical]; ok {
		if goType == timeType {
			return fn
		}
		return fast.longFn(goType)
	}
	switch logical {
	case "date":
		if goType == timeType {
			return fast.date
		}
		return fast.intFn(goType)
	case "time-millis":
		if goType == durationType {
			return fast.timeMillis
		}
		if goType == timeType {
			return fast.timeMillisTime
		}
		return fast.intFn(goType)
	case "time-micros":
		if goType == durationType {
			return fast.timeMicros
		}
		if goType == timeType {
			return fast.timeMicrosTime
		}
		return fast.longFn(goType)
	case "duration":
		if goType == avroDurationType {
			return fast.duration
		}
	case "uuid":
		if avroType == "fixed" {
			return fast.fixedUUID(goType)
		}
		if isUUIDType(goType) {
			return fast.uuid
		}
		if goType.Kind() == reflect.String {
			// The raw string fast path applies no validation and no text
			// method; the safe path's serUUID and setStringValue do both.
			if !fast.stringEligible(goType) {
				return none
			}
			return fast.uuidString
		}
	}
	return none
}

var usLogicalFast = &logicalFast[userfn]{
	timestamps:     usTimestampLogicals,
	date:           usDate,
	timeMillis:     usTimeMillis,
	timeMillisTime: usTimeMillisTime,
	timeMicros:     usTimeMicros,
	timeMicrosTime: usTimeMicrosTime,
	duration:       usDuration,
	uuid:           usUUID,
	uuidString:     usString,
	fixedUUID: func(goType reflect.Type) userfn {
		if goType.Kind() == reflect.String && stringFastPathEligibleEncode(goType) {
			return usFixedUUIDString
		}
		return nil // [16]byte and []byte take the default fixed ser
	},
	intFn:          usInteger[int32],
	longFn:         usInteger[int64],
	stringEligible: stringFastPathEligibleEncode,
}

// udTimestampLogicals maps the six long-typed timestamp logicals to their
// time.Time-target unsafe deserializer. local-timestamp-* and timestamp-*
// decode identically, since we read the wire long the same way either way;
// see logical.go for the encode-side wall-clock vs instant note.
var udTimestampLogicals = map[string]udeserfn{
	"timestamp-millis":       udTimestampMillis,
	"timestamp-micros":       udTimestampMicros,
	"timestamp-nanos":        udTimestampNanos,
	"local-timestamp-millis": udTimestampMillis,
	"local-timestamp-micros": udTimestampMicros,
	"local-timestamp-nanos":  udTimestampNanos,
}

var udLogicalFast = &logicalFast[udeserfn]{
	timestamps:     udTimestampLogicals,
	date:           udDate,
	timeMillis:     udTimeMillis,
	timeMillisTime: udTimeMillisTime,
	timeMicros:     udTimeMicros,
	timeMicrosTime: udTimeMicrosTime,
	duration:       udDuration,
	uuid:           udUUID,
	uuidString:     udStringDeser,
	fixedUUID: func(goType reflect.Type) udeserfn {
		if isUUIDType(goType) {
			return udFixedUUID
		}
		if goType.Kind() == reflect.String && stringFastPathEligibleDecode(goType) {
			return udFixedUUIDString
		}
		return nil // any, []byte etc. take the reflect path
	},
	intFn:          udInteger[int32],
	longFn:         udInteger[int64],
	stringEligible: stringFastPathEligibleDecode,
}

// usTimeAsLong is the shared body of the six unsafe time-logical "long"
// serializers: read *(*time.Time)(p), convert, wrap in a SemanticError.
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

// usTimeMillisTime is usTimeMillis for a time.Time: we take the time-of-day
// fields and encode those, mirroring serTimeMillis's timeType arm.
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

// udTimeFrom reads a wire W and stores conv of it into *T. The reader is
// picked by W's width, which folds per instantiation.
func udTimeFrom[W int32 | int64, T any](conv func(W) T) udeserfn {
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		var val W
		var err error
		if unsafe.Sizeof(val) == 4 {
			var v32 int32
			v32, src, err = readVarint(src)
			val = W(v32)
		} else {
			var v64 int64
			v64, src, err = readVarlong(src)
			val = W(v64)
		}
		if err != nil {
			return nil, err
		}
		*(*T)(p) = conv(val)
		return src, nil
	}
}

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

// The *Time variants materialize the time-of-day count as a time.Time at
// epoch midnight (UTC). We use them when your struct field is a time.Time
// but the schema is time-millis or time-micros.
var (
	udTimestampMillis = udTimeFrom(timestampMillisToTime)
	udTimestampMicros = udTimeFrom(timestampMicrosToTime)
	udTimestampNanos  = udTimeFrom(timestampNanosToTime)
	udDate            = udTimeFrom(dateToTime)
	udTimeMillis      = udTimeFrom(timeMillisToDuration)
	udTimeMillisTime  = udTimeFrom(func(v int32) time.Time {
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
	if err := needLen(src, 12, "duration"); err != nil {
		return nil, err
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

// readFixedUUID checks that src holds 16 bytes and returns them with the
// advanced source. Shared by udFixedUUID (writes [16]byte) and
// udFixedUUIDString (writes the canonical string form).
func readFixedUUID(src []byte) ([16]byte, []byte, error) {
	if err := needLen(src, 16, "uuid"); err != nil {
		return [16]byte{}, nil, err
	}
	return [16]byte(src[:16]), src[16:], nil
}

// udFixedUUID is the fixed(16) UUID decoder for a [16]byte or any target.
func udFixedUUID(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	u, src, err := readFixedUUID(src)
	if err == nil {
		*(*[16]byte)(p) = u
	}
	return src, err
}

// udFixedUUIDString writes the formatted form,
// "550e8400-e29b-41d4-a716-446655440000".
func udFixedUUIDString(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	u, src, err := readFixedUUID(src)
	if err == nil {
		*(*string)(p) = uuidToString(u)
	}
	return src, err
}

func usFixedUUIDString(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	s := *(*string)(p)
	u, err := parseUUID(s)
	if err != nil {
		return nil, err
	}
	return append(dst, u[:]...), nil
}

// ---- Unsafe serializers ----
// These read values directly through unsafe.Pointer. No string-to-[]byte
// conversions: every read is a typed pointer dereference.

func usBool(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	if *(*bool)(p) {
		return append(dst, 1), nil
	}
	return append(dst, 0), nil
}

// intLike is the set of Go integer kinds the unsafe int and long paths
// read through a pointer.
type intLike interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

// intRange returns T's value range in int64 space, an unsigned T's top
// end clamped to MaxInt64 (the decode side needs no more, since no wire
// value exceeds it; the encode side asks intFits instead). Both ends derive
// from unsafe.Sizeof and the sign of a complemented zero, which fold per
// instantiation, so int and uint come out right on 32-bit as well.
func intRange[T intLike]() (lo, hi int64) {
	bits := unsafe.Sizeof(T(0)) * 8
	if ^T(0) > 0 {
		if bits == 64 {
			return 0, math.MaxInt64
		}
		return 0, int64(uint64(1)<<bits - 1)
	}
	hi = int64(uint64(1)<<(bits-1) - 1)
	return -hi - 1, hi
}

// intFits reports whether every T value fits the signed wire integer W: a
// signed T no wider than W, or an unsigned T strictly narrower.
func intFits[W int32 | int64, T intLike]() bool {
	if ^T(0) > 0 {
		return unsafe.Sizeof(T(0)) < unsafe.Sizeof(W(0))
	}
	return unsafe.Sizeof(T(0)) <= unsafe.Sizeof(W(0))
}

// wireName returns the Avro and Go type names of a wire integer width, for
// error messages.
func wireName[W int32 | int64]() (avroType, goType string) {
	if unsafe.Sizeof(W(0)) == 4 {
		return "int", "int32"
	}
	return "long", "int64"
}

// usIntegerFrom serializes a Go T read through an unsafe pointer as the
// wire integer W. Where every T fits W the closure only converts; otherwise
// it range-checks first, an unsigned T in uint64 space since a uint64 above
// MaxInt64 has no int64 form. t is the Go field type, set on the
// SemanticError so this path reports the same GoType as the reflect path.
//
// Each closure selects the width's append by unsafe.Sizeof inline, which
// folds per instantiation to the one direct call the hand-written closures
// made. A helper wrapping that choice would not inline into the closure and
// would cost a second call per value.
func usIntegerFrom[W int32 | int64, T intLike](t reflect.Type) userfn {
	if intFits[W, T]() {
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			if unsafe.Sizeof(W(0)) == 4 {
				return appendVarint(dst, int32(*(*T)(p))), nil
			}
			return appendVarlong(dst, int64(*(*T)(p))), nil
		}
	}
	wlo, whi := intRange[W]()
	avroType, wireGo := wireName[W]()
	return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
		n := *(*T)(p)
		if ^T(0) > 0 {
			if uint64(n) > uint64(whi) {
				return nil, &SemanticError{GoType: t, AvroType: avroType, Err: fmt.Errorf("value %d overflows %s", n, wireGo)}
			}
		} else if int64(n) < wlo || int64(n) > whi {
			return nil, &SemanticError{GoType: t, AvroType: avroType, Err: fmt.Errorf("value %d overflows %s", n, wireGo)}
		}
		if unsafe.Sizeof(W(0)) == 4 {
			return appendVarint(dst, int32(n)), nil
		}
		return appendVarlong(dst, int64(n)), nil
	}
}

// usInteger picks the usIntegerFrom instantiation for t's kind, or nil for a
// non-integer kind.
func usInteger[W int32 | int64](t reflect.Type) userfn {
	switch t.Kind() {
	case reflect.Int:
		return usIntegerFrom[W, int](t)
	case reflect.Int8:
		return usIntegerFrom[W, int8](t)
	case reflect.Int16:
		return usIntegerFrom[W, int16](t)
	case reflect.Int32:
		return usIntegerFrom[W, int32](t)
	case reflect.Int64:
		return usIntegerFrom[W, int64](t)
	case reflect.Uint:
		return usIntegerFrom[W, uint](t)
	case reflect.Uint8:
		return usIntegerFrom[W, uint8](t)
	case reflect.Uint16:
		return usIntegerFrom[W, uint16](t)
	case reflect.Uint32:
		return usIntegerFrom[W, uint32](t)
	case reflect.Uint64:
		return usIntegerFrom[W, uint64](t)
	}
	return nil
}

func usFloat(k reflect.Kind) userfn {
	switch k {
	case reflect.Float32:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			return appendUint32(dst, math.Float32bits(*(*float32)(p))), nil
		}
	case reflect.Float64:
		return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
			// Lossy by destination: a finite float64 narrowed to float32
			// silently becomes +/-Inf when out of range, matching
			// appendAvroFloat32 and Java.
			return appendUint32(dst, math.Float32bits(float32(*(*float64)(p)))), nil
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

func usString(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	return doSerString(dst, *(*string)(p)), nil
}

func usBytes(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
	b := *(*[]byte)(p)
	dst = appendVarlong(dst, int64(len(b)))
	return append(dst, b...), nil
}

// ---- Unsafe deserializers ----
// For types without GC pointers (bool, ints, floats), we write directly
// through unsafe. For types holding GC pointers (string, []byte), a typed
// pointer store triggers the GC write barrier for us. Every decoded value is
// a fresh copy: we never alias src.

func udBool(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	if len(src) < 1 {
		return nil, &ShortBufferError{Type: "boolean"}
	}
	*(*bool)(p) = src[0] == 1
	return src[1:], nil
}

// udIntegerTo reads the wire integer W and stores it into *T. Where every
// W fits T the closure only converts; otherwise it range-checks against T's
// own range first, in int64 space.
func udIntegerTo[W int32 | int64, T intLike](t reflect.Type) udeserfn {
	lo, hi := intRange[T]()
	if wlo, whi := intRange[W](); lo <= wlo && hi >= whi {
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			var v W
			var err error
			if unsafe.Sizeof(v) == 4 {
				var v32 int32
				v32, src, err = readVarint(src)
				v = W(v32)
			} else {
				var v64 int64
				v64, src, err = readVarlong(src)
				v = W(v64)
			}
			if err != nil {
				return nil, err
			}
			*(*T)(p) = T(v)
			return src, nil
		}
	}
	avroType, _ := wireName[W]()
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		var v W
		var err error
		if unsafe.Sizeof(v) == 4 {
			var v32 int32
			v32, src, err = readVarint(src)
			v = W(v32)
		} else {
			var v64 int64
			v64, src, err = readVarlong(src)
			v = W(v64)
		}
		if err != nil {
			return nil, err
		}
		if int64(v) < lo || int64(v) > hi {
			return nil, &SemanticError{GoType: t, AvroType: avroType, Err: fmt.Errorf("value %d overflows %s", v, t)}
		}
		*(*T)(p) = T(v)
		return src, nil
	}
}

// udInteger picks the udIntegerTo instantiation for t's kind, or nil for a
// non-integer kind.
func udInteger[W int32 | int64](t reflect.Type) udeserfn {
	switch t.Kind() {
	case reflect.Int:
		return udIntegerTo[W, int](t)
	case reflect.Int8:
		return udIntegerTo[W, int8](t)
	case reflect.Int16:
		return udIntegerTo[W, int16](t)
	case reflect.Int32:
		return udIntegerTo[W, int32](t)
	case reflect.Int64:
		return udIntegerTo[W, int64](t)
	case reflect.Uint:
		return udIntegerTo[W, uint](t)
	case reflect.Uint8:
		return udIntegerTo[W, uint8](t)
	case reflect.Uint16:
		return udIntegerTo[W, uint16](t)
	case reflect.Uint32:
		return udIntegerTo[W, uint32](t)
	case reflect.Uint64:
		return udIntegerTo[W, uint64](t)
	}
	return nil
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

func udDouble(t reflect.Type) udeserfn {
	switch t.Kind() {
	case reflect.Float32:
		return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
			u, src, err := readUint64(src)
			if err != nil {
				return nil, err
			}
			f := math.Float64frombits(u)
			// Match deserDouble, the safe path: we reject a silent narrowing
			// of a finite float64 to +/-Inf when the target is float32.
			if finiteFloat32Overflows(f) {
				return nil, &SemanticError{GoType: t, AvroType: "double", Err: fmt.Errorf("value %g overflows float32", f)}
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

// udStringDeser stores the string through a typed pointer, so *(*string)(p) = s
// triggers the GC write barrier for us. string(src[:n]) always copies, so the
// decoded string owns its memory.
func udStringDeser(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	n, src, err := readLength(src, "string")
	if err != nil {
		return nil, err
	}
	*(*string)(p) = sl.string(src, n)
	return src[n:], nil
}

// udBytesDeser stores the byte slice through a typed pointer, so the store
// triggers the GC write barrier. The decoded slice owns its memory unless
// [AliasInput] is on, the same choice setBytesValue makes on the reflect
// path.
func udBytesDeser(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
	n, src, err := readLength(src, "bytes")
	if err != nil {
		return nil, err
	}
	if sl.aliases() {
		*(*[]byte)(p) = sl.bytes(src, n)
		return src[n:], nil
	}
	b := make([]byte, n)
	copy(b, src[:n])
	*(*[]byte)(p) = b
	return src[n:], nil
}

// ---- Null-union unsafe ser/deser ----

// nullUnionBytes returns the single-byte varint union index for the null and
// value branches. Zigzag varint: index 0 encodes as 0x00, index 1 as 0x02. So
// ["null",T] gives null=0x00, val=0x02, and ["T","null"] gives val=0x00,
// null=0x02.
func nullUnionBytes(nullSecond bool) (nullByte, valByte byte) {
	if nullSecond {
		return 2, 0
	}
	return 0, 2
}

// usNullUnionEnter is the encode-side mirror of udNullUnionEnter: we emit the
// null-branch byte when *(*unsafe.Pointer)(p) is nil, the value-branch byte
// otherwise. Returns (pp, dst-after-tag, isNull). On isNull you return dst
// directly; otherwise pp is the inner *T address your branch ser fn fills.
func usNullUnionEnter(dst []byte, p unsafe.Pointer, nullByte, valByte byte) (pp unsafe.Pointer, _ []byte, isNull bool) {
	pp = *(*unsafe.Pointer)(p)
	if pp == nil {
		return nil, append(dst, nullByte), true
	}
	return pp, append(dst, valByte), false
}

// usNullUnionPtr handles null-union ser for *T where T has a primitive unsafe
// serializer.
func usNullUnionPtr(inner userfn, nullByte, valByte byte) userfn {
	return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
		// Guard at the union node, mirroring the decode-side udNullUnionPtr
		// and the reflect serNullUnionAt. The union is a schema node, so it
		// must charge its edge (depth+1 into the branch below) and guard.
		// See the depth-uniformity invariant in deserNullUnionAt.
		if depth >= maxDepth {
			return nil, errTooDeep
		}
		pp, dst, isNull := usNullUnionEnter(dst, p, nullByte, valByte)
		if isNull {
			return dst, nil
		}
		return inner(dst, pp, depth+1)
	}
}

// usNullUnionRecord handles null-union ser for *T where T is a record.
func usNullUnionRecord(rec *serRecord, innerType reflect.Type, nullByte, valByte byte) userfn {
	return func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
		// Guard at the union node, mirroring the decode-side
		// udNullUnionRecord and the reflect serNullUnionAt. This guard
		// charges the union node, the depth+1 below charges its edge to the
		// inner record, and the record node self-charges on entry, so a
		// ["null", Record] link costs two depth units on every path. See the
		// depth-uniformity invariant in deserNullUnionAt.
		if depth >= maxDepth {
			return nil, errTooDeep
		}
		pp, dst, isNull := usNullUnionEnter(dst, p, nullByte, valByte)
		if isNull {
			return dst, nil
		}
		return serRecordVia(dst, rec.fastFor(innerType), rec, innerType, pp, depth+1)
	}
}

// udNullUnionEnter is the shared preamble for udNullUnionPtr and
// udNullUnionRecord: read the union index, nil out *T on the null branch,
// allocate inner storage on the value branch. On isNull you return src
// directly, since we already zeroed the pointer field; otherwise pp points at
// fresh inner storage for your branch decoder to fill.
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

// udNullUnionPtr handles null-union deser for *T where T has a primitive
// unsafe deserializer.
//
// The union is a schema node and bumps sl.depth once, like the reflect
// deserNullUnionAt, the general deserUnion.deser, and the encode side
// (usNullUnionPtr passes its branch at depth+1). See the depth-uniformity
// invariant in deserNullUnionAt. We decrement inline rather than defer, for the
// same hot-path reason as udNullUnionRecord below.
func udNullUnionPtr(inner udeserfn, innerType reflect.Type, valIdx int, nullByte, valByte byte) udeserfn {
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		if sl.depth >= maxDepth {
			return nil, errTooDeep
		}
		sl.depth++
		pp, src, isNull, err := udNullUnionEnter(src, p, innerType, valIdx, nullByte, valByte)
		if err != nil || isNull {
			sl.depth--
			return src, err
		}
		src, err = inner(src, pp, sl)
		sl.depth--
		return src, err
	}
}

// udNullUnionRecord handles null-union deser for *T where T is a record.
//
// The union node bumps sl.depth once here and the inner record bumps on its own
// entry, so a ["null", Record] link costs two depth units on every path. See
// the depth-uniformity invariant in deserNullUnionAt.
//
// We decrement inline rather than defer, here and in udNullUnionPtr. This is
// the recursive-decode hot path, and benchstat showed the open-coded defer
// costs ~10% per link on a recursive-record decode. Every post-bump return
// decrements first, so depth is restored on success and error alike, and the
// over-bound abort returns before the bump.
func udNullUnionRecord(rec *deserRecord, innerType reflect.Type, valIdx int, nullByte, valByte byte) udeserfn {
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		if sl.depth >= maxDepth {
			return nil, errTooDeep
		}
		sl.depth++
		pp, src, isNull, err := udNullUnionEnter(src, p, innerType, valIdx, nullByte, valByte)
		if err != nil || isNull {
			sl.depth--
			return src, err
		}
		src, err = deserRecordVia(src, rec.fastFor(innerType, sl.skipUnknown), rec, innerType, pp, sl)
		sl.depth--
		return src, err
	}
}

// ---- Array unsafe ser/deser ----
//
// We inline the five per-element loops (depth check, length prefix, early
// exit, per-element body, terminator) rather than factor them into a generic
// helper. A usSliceFrame[Elem] factoring regressed the large-array encode,
// the []*Record hot path, by ~16%: the extra closure call per slice, plus the
// compiler declining to inline the generic at each call site, costs measurable
// per-array work.

// usArrayRecord handles array ser for []T or []*T where items are records.
func usArrayRecord(rec *serRecord, elemGoType reflect.Type) userfn {
	if elemGoType.Kind() == reflect.Pointer {
		// Single-pointer elements only: usArrayPtrRecord peels one level
		// inline and rec.ser peels a further maxIndirectDepth, so []**record
		// would accept one level more than every other path.
		if elemGoType.Elem().Kind() == reflect.Pointer {
			return nil
		}
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
		bodyStart := len(dst)
		data := unsafe.Pointer(unsafe.SliceData(bs))
		fast := rec.fastFor(elemGoType)
		useFast := fast != nil && fast.allFast
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
		if err := arrayZeroByteEncodeCompliance(len(dst) == bodyStart, n); err != nil {
			return nil, err
		}
		return append(dst, 0), nil
	}
}

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
		bodyStart := len(dst)
		fast := rec.fastFor(innerType)
		useFast := fast != nil && fast.allFast
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
		if err := arrayZeroByteEncodeCompliance(len(dst) == bodyStart, n); err != nil {
			return nil, err
		}
		return append(dst, 0), nil
	}
}

// usArrayNullUnionRecord handles array ser for []*T where items are
// ["null", Record]. The union between the array and the record is a schema
// node and costs one depth unit, as on every decode and JSON path: the array
// at depth, each element's union at depth+1, the record at depth+2. The
// per-element union guard is loop-invariant, so it is hoisted before the
// loop; only the empty array, which enters no element union, escapes it.
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
		// We enter each element's union node at depth+1; charge it.
		if depth+1 >= maxDepth {
			return nil, errTooDeep
		}
		fast := rec.fastFor(innerType)
		useFast := fast != nil && fast.allFast
		var err error
		for _, pp := range s {
			if pp == nil {
				dst = append(dst, nullByte)
				continue
			}
			dst = append(dst, valByte)
			if useFast {
				dst, err = serRecordFastPtr(dst, fast, pp, depth+2)
			} else {
				dst, err = rec.ser(dst, reflect.NewAt(innerType, pp).Elem(), depth+2)
			}
			if err != nil {
				return nil, err
			}
		}
		return append(dst, 0), nil
	}
}

// usArrayNullUnionPtr handles array ser for []*T where items are
// ["null", primitive], with the same depth accounting as
// usArrayNullUnionRecord.
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
		// We enter each element's union node at depth+1; charge it.
		if depth+1 >= maxDepth {
			return nil, errTooDeep
		}
		var err error
		for _, pp := range s {
			if pp == nil {
				dst = append(dst, nullByte)
				continue
			}
			dst, err = inner(append(dst, valByte), pp, depth+2)
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
		bodyStart := len(dst)
		data := unsafe.Pointer(unsafe.SliceData(bs))
		var err error
		for i := range n {
			dst, err = inner(dst, unsafe.Add(data, uintptr(i)*elemSize), depth+1)
			if err != nil {
				return nil, err
			}
		}
		if err := arrayZeroByteEncodeCompliance(len(dst) == bodyStart, n); err != nil {
			return nil, err
		}
		return append(dst, 0), nil
	}
}

// udArrayBlocks drives the outer block loop for the unsafe array
// deserializers. onBlock fills slots [start, start+n) of v's backing array
// and returns the advanced src. minItemBytes bounds the block count to
// len(src)/minItemBytes, as deserArray.deser does, so a hostile float-array
// stream cannot drive a 4x MakeSlice allocation per wire byte.
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
			// An empty array gives you a non-nil empty slice, as on every
			// other decode path.
			if v.IsNil() {
				v.Set(reflect.MakeSlice(sliceType, 0, 0))
			}
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

// udArrayPtrRecord handles array deser for []*T where items are records. We
// manage the slice with reflect and decode each element through unsafe.
func udArrayPtrRecord(rec *deserRecord, innerType, sliceType reflect.Type, minItemBytes int) udeserfn {
	innerSize := innerType.Size()
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		return udArrayBlocks(src, p, sl, sliceType, minItemBytes,
			func(src []byte, _ reflect.Value, p unsafe.Pointer, start, n int, sl *slab) ([]byte, error) {
				s := *(*[]unsafe.Pointer)(p)
				// One contiguous allocation backs every nil pointer slot.
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
				fast := rec.fastFor(innerType, sl.skipUnknown)
				useFast := fast != nil && fast.allFast
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

// udArrayDirect handles array deser for []T where items are primitives. We
// manage the slice with reflect and decode each element through unsafe.
func udArrayDirect(inner udeserfn, elemSize uintptr, sliceType reflect.Type, minItemBytes int) udeserfn {
	return func(src []byte, p unsafe.Pointer, sl *slab) ([]byte, error) {
		return udArrayBlocks(src, p, sl, sliceType, minItemBytes,
			func(src []byte, v reflect.Value, _ unsafe.Pointer, start, n int, sl *slab) ([]byte, error) {
				// Read the data pointer after udArrayBlocks grew the slice.
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
