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

// promoteIntFloatMantissa is the int→float conversion shared by the
// four int/long→float/double promotion arms. The READER schema is the
// user's evolved-to type — by writing a reader schema of float/double
// the user explicitly opted into IEEE-precision semantics, so wire
// magnitudes the reader can't represent exactly silently IEEE-round.
// Matches Java's ResolvingDecoder.readDouble's `(double) in.readLong()`
// (`lang/java/avro/src/main/java/org/apache/avro/io/ResolvingDecoder.
// java:192`), fastavro's `maybe_promote` returning `float(data)`
// (`fastavro/_read_py.py:619-621`), and hamba's createDoubleConverter
// `float64(r.ReadLong())` (`hamba/avro/converter.go:28`).
//
// Asymmetric with the natural same-schema decode case: `s.Decode(wire,
// &f float64)` against `s = MustParse("long")` still rejects via
// setLongValue's CanFloat arm because there the READER schema IS long
// (exact) — the user did NOT evolve the schema, only chose a Go type
// the wire doesn't fit. The principle: lossiness on decode is
// acceptable when the READER SCHEMA (the user's contract) is lossy;
// when the reader schema is exact and only the Go type is lossy, the
// wire preserved a value the user shouldn't silently lose.
//
// Float32 narrowing for the bitSize=32 arm happens at setFloatValue's
// `v.SetFloat(f)` when the Go target is *float32; the float64 cast
// here preserves the long's full value before the assignment narrows
// it to the reader-schema's float32 precision.
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
