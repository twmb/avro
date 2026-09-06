package avro

import (
	"math"
	"reflect"
	"time"
)

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

// promoteIntFloatMantissa is the int->float conversion shared by the four
// int/long->float/double promotion arms. A float or double reader schema opts
// into IEEE precision, so we round any wire magnitude it cannot hold exactly,
// as Java's ResolvingDecoder.readDouble, fastavro's maybe_promote and hamba's
// createDoubleConverter all do. Note that the same-schema decode is stricter:
// decoding a long schema into a float64 still rejects, because there only
// your Go type is lossy, not the reader schema.
func promoteIntFloatMantissa(v reflect.Value, n int64, avroType string, bitSize int) error {
	var f float64
	if bitSize == 32 {
		f = float64(float32(n))
	} else {
		f = float64(n)
	}
	return setFloatValue(v, f, avroType, bitSize)
}

var (
	// setLongValue handles the Interface arm internally, so no special-case
	// needed here.
	promoteIntToLong = promoteRead(readVarint,
		func(v reflect.Value, n int32) error { return setLongValue(v, int64(n)) })
	promoteIntToFloat = promoteRead(readVarint,
		func(v reflect.Value, n int32) error { return promoteIntFloatMantissa(v, int64(n), "float", 32) })
	promoteIntToDouble = promoteRead(readVarint,
		func(v reflect.Value, n int32) error { return promoteIntFloatMantissa(v, int64(n), "double", 64) })
	promoteLongToFloat = promoteRead(readVarlong,
		func(v reflect.Value, n int64) error { return promoteIntFloatMantissa(v, n, "float", 32) })
	promoteLongToDouble = promoteRead(readVarlong,
		func(v reflect.Value, n int64) error { return promoteIntFloatMantissa(v, n, "double", 64) })
	promoteFloatToDouble = promoteRead(readUint32,
		func(v reflect.Value, u uint32) error {
			return setFloatValue(v, float64(math.Float32frombits(u)), "double", 64)
		})
)

// readBytesPrefix reads a varlong length prefix and validates it against the
// remaining buffer. Every length-prefixed promotion comes through here, so the
// three error shapes (varlong, negative, overrun) stay in one place.
//
// destAvroType labels the negative-length SemanticError, wireTypeName the
// overrun ShortBufferError. The two swap by direction: string->bytes tags
// negative as "bytes" (destination) and short-buffer as "string" (the writer's
// wire type).
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

func promoteStringToBytes(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	n, src, err := readBytesPrefix(src, "bytes", "string")
	if err != nil {
		return nil, err
	}
	if err := setBytesValue(indirectAlloc(v), src[:n], "bytes", sl); err != nil {
		return nil, err
	}
	return src[n:], nil
}

// promotionDeserForLogical returns a deserfn that reads the writer's wire type
// *and* applies the reader's logical conversion, or nil when the reader has no
// logical reachable through a promotion. The bare promotion desers know nothing
// about logicals, so without this a writer int -> reader {"long",
// "logicalType":"timestamp-millis"} yields raw int64 instead of time.Time.
func promotionDeserForLogical(writerKind string, r *schemaNode) deserfn {
	if r.logical == "" {
		return nil
	}
	// We key on the writer>reader pair, the same key the promotions table
	// uses, not on the reader kind alone: each wrapper reads the writer's
	// wire form (a varint for int->long, a length prefix for string<->bytes)
	// and is correct only for its pair. A promotion added to the table with
	// no arm here falls through to the bare widening deser. long->float,
	// long->double and float->double end on kinds that carry no logical
	// type, so they have no arm.
	switch writerKind + ">" + r.kind {
	case "int>long":
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
	case "string>bytes":
		switch r.logical {
		case "decimal":
			return promoteStringToBytesDecimal(r.scale)
		case "big-decimal":
			return promoteStringToBytesBigDecimal
		}
	case "bytes>string":
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

// promoteIntToLongTimeMicros mirrors deserTimeMicros, but we read the writer's
// varint (int) and widen to int64 before converting. time-micros has its own
// overflow check inside timeMicrosToDuration; we keep it.
func promoteIntToLongTimeMicros(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
	val, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	return src, setTimeMicrosTarget(indirectAlloc(v), int64(val))
}

// promoteStringToBytesDecimal is deserBytesDecimal with the length-read shape
// of promoteStringToBytes: we read the writer's string bytes, then convert at
// the reader's schema scale.
func promoteStringToBytesDecimal(scale int) deserfn {
	return func(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
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
		if err := setBytesValue(v, b, "bytes", sl); err != nil {
			return nil, err
		}
		return src[n:], nil
	}
}

// promoteStringToBytesBigDecimal reads the writer's length-prefixed bytes and
// dispatches to deserBigDecimal's arms: parse as a structured big-decimal
// payload, fall back to raw bytes for opaque-pass-through targets.
func promoteStringToBytesBigDecimal(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	n, src, err := readBytesPrefix(src, "bytes", "string")
	if err != nil {
		return nil, err
	}
	payload := src[:n]
	v = indirectAlloc(v)
	done, err := applyBigDecimalPayload(v, payload)
	if !done {
		err = setBytesValue(v, payload, "big-decimal", sl)
	}
	if err != nil {
		return nil, err
	}
	return src[n:], nil
}

// promoteBytesToStringUUID reads the writer's length-prefixed bytes as a
// canonical UUID string, dispatching to deserFixedUUIDReflect's target arms.
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
		copyBytesToArray(v, u[:])
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
