package avro

import (
	"math"
	"reflect"
	"time"
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
		return src, setIface(v, reflect.ValueOf(int64(val)), "long")
	}
	return src, setLongValue(v, int64(val))
}

func promoteIntToFloat(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
	val, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	return src, setFloatValue(indirectAlloc(v), float64(val), "float", 32)
}

func promoteIntToDouble(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
	val, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	return src, setFloatValue(indirectAlloc(v), float64(val), "double", 64)
}

func promoteLongToFloat(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	return src, setFloatValue(indirectAlloc(v), float64(val), "float", 32)
}

func promoteLongToDouble(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	return src, setFloatValue(indirectAlloc(v), float64(val), "double", 64)
}

func promoteFloatToDouble(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
	u, src, err := readUint32(src)
	if err != nil {
		return nil, err
	}
	return src, setFloatValue(indirectAlloc(v), float64(math.Float32frombits(u)), "double", 64)
}

func promoteStringToBytes(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
	length, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, &SemanticError{AvroType: "bytes"}
	}
	if length > int64(len(src)) {
		return nil, &ShortBufferError{Type: "string", Need: int(length), Have: len(src)}
	}
	n := int(length)
	b := make([]byte, n)
	copy(b, src[:n])
	if err := setBytesValue(indirectAlloc(v), b, "bytes"); err != nil {
		return nil, err
	}
	return src[n:], nil
}

// promotionDeserForLogical returns a deserfn that reads the writer's
// wire type AND applies the reader's logical-type conversion, or nil
// if the reader has no logical type (or none that's reachable via the
// promotion paths). Without this, a writer int → reader
// {"long","logicalType":"timestamp-millis"} resolution would produce
// raw int64 instead of time.Time — the basic promotion deser uses
// setLongValue / setBytesValue / setStringValue, which know nothing
// about logical conversions. The wrappers below read the writer's
// wire (varint for int→long, length-prefixed for string↔bytes) and
// dispatch through the same target arms the natural logical
// deserializers use.
func promotionDeserForLogical(writerKind string, r *schemaNode) deserfn {
	if r.logical == "" {
		return nil
	}
	switch r.kind {
	case "long":
		if writerKind != "int" {
			// long→float, long→double, float→double don't have
			// logical-type readers (no logicals on float/double).
			return nil
		}
		switch r.logical {
		case "timestamp-millis", "local-timestamp-millis":
			return promoteIntToLongTime(timestampMillisToTime)
		case "timestamp-micros", "local-timestamp-micros":
			return promoteIntToLongTime(timestampMicrosToTime)
		case "timestamp-nanos", "local-timestamp-nanos":
			return promoteIntToLongTime(timestampNanosToTime)
		case "time-micros":
			return promoteIntToLongTimeMicros
		}
	case "bytes":
		if writerKind != "string" {
			return nil
		}
		switch r.logical {
		case "decimal":
			return promoteStringToBytesDecimal(r.scale)
		case "big-decimal":
			return promoteStringToBytesBigDecimal
		}
	case "string":
		if writerKind != "bytes" {
			return nil
		}
		switch r.logical {
		case "uuid":
			return promoteBytesToStringUUID
		}
	}
	return nil
}

func promoteIntToLongTime(conv func(int64) time.Time) deserfn {
	return func(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
		val, src, err := readVarint(src)
		if err != nil {
			return nil, err
		}
		return src, setTimeAsLongTarget(indirectAlloc(v), int64(val), conv)
	}
}

// promoteIntToLongTimeMicros mirrors deserTimeMicros but reads the
// writer's varint (int) and widens to int64 before applying the
// duration conversion. time-micros has its own overflow check inside
// timeMicrosToDuration; we preserve it here.
func promoteIntToLongTimeMicros(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
	val, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	v = indirectAlloc(v)
	if v.Type() == durationType || v.Type() == timeType || v.Kind() == reflect.Interface {
		d, err := timeMicrosToDuration(int64(val))
		if err != nil {
			return nil, err
		}
		switch {
		case v.Type() == durationType:
			v.Set(reflect.ValueOf(d))
		case v.Type() == timeType:
			v.Set(reflect.ValueOf(timeOfDayToTime(d)))
		default:
			return src, setIface(v, reflect.ValueOf(d), "long")
		}
		return src, nil
	}
	return src, setLongValue(v, int64(val))
}

// promoteStringToBytesDecimal reads the writer's varlong-length-
// prefixed string bytes and applies the reader's decimal conversion
// at the given schema scale. Mirrors deserBytesDecimal but with the
// length-read shape of promoteStringToBytes.
func promoteStringToBytesDecimal(scale int) deserfn {
	return func(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
		length, src, err := readVarlong(src)
		if err != nil {
			return nil, err
		}
		if length < 0 {
			return nil, &SemanticError{AvroType: "bytes"}
		}
		if length > int64(len(src)) {
			return nil, &ShortBufferError{Type: "string", Need: int(length), Have: len(src)}
		}
		n := int(length)
		b := make([]byte, n)
		copy(b, src[:n])
		v = indirectAlloc(v)
		ok, err := setDecimalValue(v, b, scale)
		if err != nil {
			return nil, err
		}
		if ok {
			return src[n:], nil
		}
		// Fall through to plain bytes target.
		if err := setBytesValue(v, b, "bytes"); err != nil {
			return nil, err
		}
		return src[n:], nil
	}
}

// promoteStringToBytesBigDecimal reads the writer's varlong-length-
// prefixed bytes and dispatches to the same arms as deserBigDecimal
// (parse as structured big-decimal payload, fall back to raw bytes
// for opaque-pass-through targets).
func promoteStringToBytesBigDecimal(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
	length, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, &SemanticError{AvroType: "bytes"}
	}
	if length > int64(len(src)) {
		return nil, &ShortBufferError{Type: "string", Need: int(length), Have: len(src)}
	}
	n := int(length)
	payload := make([]byte, n)
	copy(payload, src[:n])
	v = indirectAlloc(v)
	if r, displayScale, perr := parseBigDecimalPayload(payload); perr == nil {
		if ok, err := setDecimalRat(v, r, displayScale); ok {
			if err != nil {
				return nil, err
			}
			return src[n:], nil
		}
	} else if v.Kind() != reflect.Slice && v.Kind() != reflect.String && v.Kind() != reflect.Array {
		return nil, perr
	}
	if _, err := assignBytesTarget(v, payload, "big-decimal"); err != nil {
		return nil, err
	}
	return src[n:], nil
}

// promoteBytesToStringUUID reads the writer's varlong-length-prefixed
// bytes and parses them as a canonical UUID string, dispatching to
// the same target arms as deserFixedUUIDReflect (string / [16]byte /
// []byte / interface). Mirrors how the natural string+uuid logical
// deserializer would handle the bytes if they'd been written as the
// 36-char hex-dash form.
func promoteBytesToStringUUID(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	length, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, &SemanticError{AvroType: "string"}
	}
	if length > int64(len(src)) {
		return nil, &ShortBufferError{Type: "bytes", Need: int(length), Have: len(src)}
	}
	n := int(length)
	v = indirectAlloc(v)
	// [16]byte target wants the parsed UUID bytes; everything else
	// gets the canonical-string view (interface, string, []byte).
	if isUUIDType(v.Type()) {
		s := string(src[:n])
		u, err := parseUUID(s)
		if err != nil {
			return nil, err
		}
		reflect.Copy(v, reflect.ValueOf(u))
		return src[n:], nil
	}
	if err := setStringValue(v, src, n, sl); err != nil {
		return nil, err
	}
	return src[n:], nil
}

func promoteBytesToString(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	length, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, &SemanticError{AvroType: "string"}
	}
	if length > int64(len(src)) {
		return nil, &ShortBufferError{Type: "bytes", Need: int(length), Have: len(src)}
	}
	n := int(length)
	if err := setStringValue(indirectAlloc(v), src, n, sl); err != nil {
		return nil, err
	}
	return src[n:], nil
}
