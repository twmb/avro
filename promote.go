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

// promoteRead wraps a wire read + per-target setter into a deserfn. Each
// promote* function below is a one-liner using this helper.
func promoteRead[Wire any](
	read func([]byte) (Wire, []byte, error),
	apply func(reflect.Value, Wire) error,
) deserfn {
	return func(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
		val, src, err := read(src)
		if err != nil {
			return nil, err
		}
		return src, apply(indirectAlloc(v), val)
	}
}

var (
	// setLongValue handles the Interface arm internally, so no special-case
	// needed here (the prior promoteIntToLong's separate Interface arm was
	// redundant with setLongValue's first branch).
	promoteIntToLong = promoteRead(readVarint,
		func(v reflect.Value, n int32) error { return setLongValue(v, int64(n)) })
	promoteIntToFloat = promoteRead(readVarint,
		func(v reflect.Value, n int32) error { return setFloatValue(v, float64(n), "float", 32) })
	promoteIntToDouble = promoteRead(readVarint,
		func(v reflect.Value, n int32) error { return setFloatValue(v, float64(n), "double", 64) })
	promoteLongToFloat = promoteRead(readVarlong,
		func(v reflect.Value, n int64) error { return setFloatValue(v, float64(n), "float", 32) })
	promoteLongToDouble = promoteRead(readVarlong,
		func(v reflect.Value, n int64) error { return setFloatValue(v, float64(n), "double", 64) })
	promoteFloatToDouble = promoteRead(readUint32,
		func(v reflect.Value, u uint32) error {
			return setFloatValue(v, float64(math.Float32frombits(u)), "double", 64)
		})
)

// readBytesPrefix reads a varlong length prefix and validates it against
// the remaining buffer. Shared by the four promote*-with-length-prefix
// helpers (promoteStringToBytes, promoteStringToBytesDecimal,
// promoteStringToBytesBigDecimal, promoteBytesToStringUUID,
// promoteBytesToString) so the trio of error shapes (varlong, negative,
// overrun) is in one place. destAvroType labels the SemanticError for a
// negative length; wireTypeName labels the ShortBufferError for buffer
// overrun. They differ across promotion directions: a string→bytes
// promotion tags negative-length as "bytes" (destination) and short-
// buffer as "string" (writer's wire type), and vice versa for bytes→
// string.
func readBytesPrefix(src []byte, destAvroType, wireTypeName string) (n int, rest []byte, err error) {
	length, rest, err := readVarlong(src)
	if err != nil {
		return 0, nil, err
	}
	if length < 0 {
		return 0, nil, &SemanticError{AvroType: destAvroType}
	}
	if length > int64(len(rest)) {
		return 0, nil, &ShortBufferError{Type: wireTypeName, Need: int(length), Have: len(rest)}
	}
	return int(length), rest, nil
}

func promoteStringToBytes(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
	n, src, err := readBytesPrefix(src, "bytes", "string")
	if err != nil {
		return nil, err
	}
	if err := setBytesValue(indirectAlloc(v), src[:n], "bytes"); err != nil {
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
	return src, setTimeMicrosTarget(indirectAlloc(v), int64(val))
}

// promoteStringToBytesDecimal reads the writer's varlong-length-
// prefixed string bytes and applies the reader's decimal conversion
// at the given schema scale. Mirrors deserBytesDecimal but with the
// length-read shape of promoteStringToBytes.
func promoteStringToBytesDecimal(scale int) deserfn {
	return func(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
		n, src, err := readBytesPrefix(src, "bytes", "string")
		if err != nil {
			return nil, err
		}
		b := src[:n]
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
	n, src, err := readBytesPrefix(src, "bytes", "string")
	if err != nil {
		return nil, err
	}
	payload := src[:n]
	v = indirectAlloc(v)
	done, err := applyBigDecimalPayload(v, payload)
	if !done {
		err = setBytesValue(v, payload, "big-decimal")
	}
	if err != nil {
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
	n, src, err := readBytesPrefix(src, "string", "bytes")
	if err != nil {
		return nil, err
	}
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
	n, src, err := readBytesPrefix(src, "string", "bytes")
	if err != nil {
		return nil, err
	}
	if err := setStringValue(indirectAlloc(v), src, n, sl); err != nil {
		return nil, err
	}
	return src[n:], nil
}
