package avro

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

func decodeLogicalInt(val int32, node *schemaNode) any {
	switch node.logical {
	case "date":
		return dateToTime(val)
	case "time-millis":
		return timeMillisToDuration(val)
	}
	return val
}

// timestampToTimeConv returns the wire-int64 to time.Time converter for the
// six long-typed timestamp logicals, or (nil, false) for anything else. Every
// long target arm reads this one mapping.
func timestampToTimeConv(logical string) (func(int64) time.Time, bool) {
	switch logical {
	case "timestamp-millis", "local-timestamp-millis":
		return timestampMillisToTime, true
	case "timestamp-micros", "local-timestamp-micros":
		return timestampMicrosToTime, true
	case "timestamp-nanos", "local-timestamp-nanos":
		return timestampNanosToTime, true
	}
	return nil, false
}

// decodeLogicalLong converts long-backed logicals for an *any target. It errors
// only for time-micros, when val * time.Microsecond would wrap; the timestamp
// conversions are total.
func decodeLogicalLong(val int64, node *schemaNode) (any, error) {
	if conv, ok := timestampToTimeConv(node.logical); ok {
		return conv(val), nil
	}
	if node.logical == "time-micros" {
		return timeMicrosToDuration(val)
	}
	return val, nil
}

// decodeLogicalBytes converts bytes-backed logicals for an *any target,
// erroring on a malformed payload.
func decodeLogicalBytes(b []byte, node *schemaNode) (any, error) {
	if node.logical == "decimal" {
		// The into-any path bypasses setDecimalValue's bound.
		if err := checkDecimalUnscaledLen(b); err != nil {
			return nil, err
		}
		return bytesToRat(b, node.scale), nil
	}
	if node.logical == "big-decimal" {
		r, _, err := parseBigDecimalPayload(b)
		if err != nil {
			return nil, err
		}
		return r, nil
	}
	return b, nil
}

// jsonDecodeAppliesLogical reports whether decodeKind would transform the raw
// Avro-native value into an enriched Go type for this node's logical type. We
// probe the decodeLogical functions decodeKind itself uses rather than keep a
// second list. Only applyCustomTypes consults this, at parse time.
func jsonDecodeAppliesLogical(node *schemaNode) bool {
	if node.logical == "" {
		return false
	}
	switch node.kind {
	case "int":
		_, raw := decodeLogicalInt(0, node).(int32)
		return !raw
	case "long":
		v, err := decodeLogicalLong(0, node)
		if err != nil { // only a transforming arm (time-micros) can error
			return true
		}
		_, raw := v.(int64)
		return !raw
	case "bytes":
		v, err := decodeLogicalBytes(nil, node)
		if err != nil { // only a transforming arm (big-decimal) can error on empty input
			return true
		}
		_, raw := v.([]byte)
		return !raw
	case "fixed":
		// No fixed logical inspects a length above maxFixedLogicalLen, so we
		// cap the probe buffer just past it: a schema-controlled huge size
		// would otherwise drive a huge make here at parse time, and a capped
		// length gives the same answer.
		probeLen := node.size
		if probeLen > maxFixedLogicalLen {
			probeLen = maxFixedLogicalLen + 1
		}
		_, raw := decodeLogicalFixed(make([]byte, probeLen), node).([]byte)
		return !raw
	case "string":
		// uuid-on-string transforms only into a typed [16]byte target, which
		// the *any probe cannot see; no other string logical transforms.
		return node.logical == "uuid"
	}
	return false
}

// maxFixedLogicalLen is the largest fixed length any decodeLogicalFixed arm
// inspects: uuid at 16, duration at 12. A new fixed-backed logical that
// converts at a longer length must raise this.
const maxFixedLogicalLen = 16

func decodeLogicalFixed(b []byte, node *schemaNode) any {
	switch node.logical {
	case "decimal":
		return bytesToRat(b, node.scale)
	case "duration":
		if len(b) == 12 {
			return DurationFromBytes(b)
		}
	case "uuid":
		if len(b) == 16 {
			return [16]byte(b)
		}
	}
	return b
}

// assignAny sets a native Go value on a reflect.Value target. A nil val zeros
// v; a val not assignable to a non-empty interface target returns a
// SemanticError.
func assignAny(v reflect.Value, val any, avroType string) error {
	if val == nil {
		setZero(v)
		return nil
	}
	rv := reflect.ValueOf(val)
	if v.Kind() == reflect.Interface && v.Type().NumMethod() != 0 && !rv.Type().AssignableTo(v.Type()) {
		return &SemanticError{GoType: v.Type(), AvroType: avroType}
	}
	v.Set(rv)
	return nil
}

// consumeSlabString consumes a JSON string and copies it into the slab
// allocator, batching small string allocations into one backing buffer.
func (ctx *jsonDecoder) consumeSlabString() (string, error) {
	start, end, hasEscapes, err := ctx.scanner.consumeStringRaw()
	if err != nil {
		return "", err
	}
	if hasEscapes {
		return resolveJSONEscapes(ctx.scanner.data[start:end])
	}
	return ctx.slab.string(ctx.scanner.data[start:], end-start), nil
}

// jsonDecoder is the state for schema-guided JSON decoding.
type jsonDecoder struct {
	scanner *jsonScanner
	// slab carries the decode options as well as the string arena, since a
	// field filled from its default routes through the binary deser fn, which
	// reads the options off the slab.
	slab *slab
	// suppressLogical makes the next decodeKind hand the raw Avro-native
	// value to its leaf decoder rather than the logical-transformed one.
	// decodeKind captures and clears it on entry so it scopes to one node.
	suppressLogical bool
}

// decodeValue reads the next JSON value off the scanner, guided by the schema
// node, and assigns it to v. A node carrying a custom-decoder wrapper
// dispatches through it.
func (ctx *jsonDecoder) decodeValue(v reflect.Value, node *schemaNode) error {
	if ctx.slab.depth >= maxDepth {
		return errTooDeep
	}
	ctx.slab.depth++
	defer func() { ctx.slab.depth-- }()
	if node.decodeJSON != nil {
		return node.decodeJSON(ctx, v, node)
	}
	return ctx.decodeKind(v, node)
}

// decodeKind is decodeValue minus the depth guard and the custom-decoder
// dispatch.
func (ctx *jsonDecoder) decodeKind(v reflect.Value, node *schemaNode) error {
	// suppressLogical applies to this node's leaf only, never to children.
	raw := ctx.suppressLogical
	ctx.suppressLogical = false

	// Unions handle pointer/nil targets specially (before indirectAlloc), so
	// we dispatch early.
	if node.kind == "union" {
		return ctx.decodeUnion(v, node)
	}

	v = indirectAlloc(v)
	toAny := v.Kind() == reflect.Interface

	switch node.kind {
	case "null":
		return ctx.decodeNull(v, toAny)
	case "boolean":
		return ctx.decodeBool(v, toAny)
	case "int":
		return ctx.decodeInt(v, node, toAny, raw)
	case "long":
		return ctx.decodeLong(v, node, toAny, raw)
	case "float":
		return ctx.decodeFloat(v)
	case "double":
		return ctx.decodeDouble(v)
	case "string":
		return ctx.decodeString(v, node, toAny, raw)
	case "enum":
		return ctx.decodeEnum(v, node)
	case "bytes":
		return ctx.decodeBytes(v, node, toAny, raw)
	case "fixed":
		return ctx.decodeFixed(v, node, toAny, raw)
	case "array":
		return ctx.decodeArray(v, node, toAny)
	case "map":
		return ctx.decodeMap(v, node, toAny)
	case "record":
		return ctx.decodeRecord(v, node, toAny)
	default:
		return fmt.Errorf("avro json: unsupported schema kind %q", node.kind)
	}
}

// wrapDecodeJSONWithCustomDecoders builds a per-node JSON decode closure that
// captures the custom decoder chain, the JSON parallel of
// wrapDeserWithCustomDecoders.
func wrapDecodeJSONWithCustomDecoders(decoders []func(any, *SchemaNode) (any, error), sn *SchemaNode, suppressLogical bool) jsonDecodeFn {
	return func(ctx *jsonDecoder, v reflect.Value, node *schemaNode) error {
		// A no-match ancestor is re-decoding the subtree without customs.
		if ctx.slab.bypassCustom {
			ctx.suppressLogical = suppressLogical
			return ctx.decodeKind(v, node)
		}
		if len(decoders) == 0 {
			// No Decode callback: decode straight into the target through the
			// raw arms, as the binary raw deser does.
			ctx.suppressLogical = suppressLogical
			return ctx.decodeKind(v, node)
		}
		// A nil interface target takes decodeKind's output as the chain input
		// directly, keeping a parent probe to a single pass. A non-nil
		// interface would reuse the held value in place, so it takes the
		// probe and re-decode path below.
		if v.Kind() == reflect.Interface && v.IsNil() {
			ctx.suppressLogical = suppressLogical
			if err := ctx.decodeKind(v, node); err != nil {
				return err
			}
			chainVal := v.Interface()
			for _, dec := range decoders {
				out, err := dec(chainVal, sn)
				if err != nil {
					if errors.Is(err, ErrSkipCustomType) {
						continue
					}
					return err
				}
				ctx.slab.customMatches++
				return setCustomResult(v, out, node.kind)
			}
			return nil // all-skip: v already holds the no-custom value
		}
		// Typed target: we probe into a throwaway any for the chain, and if
		// every decoder skips we rewind and re-decode into v, the same decode
		// a no-custom schema performs.
		var tmp any
		tmpV := reflect.ValueOf(&tmp).Elem()
		savedPos := ctx.scanner.pos
		savedMatches := ctx.slab.customMatches
		ctx.suppressLogical = suppressLogical
		if err := ctx.decodeKind(tmpV, node); err != nil {
			return err
		}
		for _, dec := range decoders {
			out, err := dec(tmp, sn)
			if err != nil {
				if errors.Is(err, ErrSkipCustomType) {
					continue
				}
				return err
			}
			// The un-indirected v lets a *T result go into a *T target.
			ctx.slab.customMatches++
			return setCustomResult(v, out, node.kind)
		}
		// Every decoder skipped. If no nested custom matched we bypass customs
		// for the re-decode, otherwise we re-decode with them active to
		// reproduce the nested match.
		ctx.scanner.pos = savedPos
		ctx.suppressLogical = suppressLogical
		if ctx.slab.customMatches == savedMatches {
			ctx.slab.bypassCustom = true
			err := ctx.decodeKind(v, node)
			ctx.slab.bypassCustom = false
			return err
		}
		return ctx.decodeKind(v, node)
	}
}

func (ctx *jsonDecoder) decodeNull(v reflect.Value, _ bool) error {
	if err := ctx.scanner.consumeNull(); err != nil {
		return err
	}
	setZero(v)
	return nil
}

func (ctx *jsonDecoder) decodeBool(v reflect.Value, toAny bool) error {
	b, err := ctx.scanner.consumeBool()
	if err != nil {
		return err
	}
	if toAny {
		return setIface(v, reflect.ValueOf(b), "boolean")
	}
	if v.Kind() == reflect.Bool {
		v.SetBool(b)
		return nil
	}
	return semErr(v, "boolean")
}

func (ctx *jsonDecoder) decodeInt(v reflect.Value, node *schemaNode, toAny, raw bool) error {
	nb, err := ctx.scanner.consumeNumberBytes()
	if err != nil {
		return err
	}
	val, err := parseJSONInt32(nb)
	if err != nil {
		return err
	}
	if toAny {
		if raw {
			return setIface(v, reflect.ValueOf(val), "int")
		}
		return setIface(v, reflect.ValueOf(decodeLogicalInt(val, node)), "int")
	}
	if raw {
		// A matching no-Decode CustomType suppressed the logical: assign the
		// raw int32 and skip the typed-target arms, as binary's raw deserInt
		// does.
		return setIntValue(v, val)
	}
	// Every DecodeJSON entry point produces addressable values.
	if v.Type() == timeType {
		switch node.logical {
		case "date":
			*(*time.Time)(v.Addr().UnsafePointer()) = dateToTime(val)
			return nil
		case "time-millis":
			// Mirrors serTimeMillis's timeType arm.
			*(*time.Time)(v.Addr().UnsafePointer()) = timeOfDayToTime(timeMillisToDuration(val))
			return nil
		}
	}
	if v.Type() == durationType && node.logical == "time-millis" {
		*(*time.Duration)(v.Addr().UnsafePointer()) = timeMillisToDuration(val)
		return nil
	}
	// A string target takes the date formatted, as deserDate does.
	// formatToStringKindTarget excludes json.Number, which falls through to
	// the raw integer.
	if node.logical == "date" {
		if wrote, err := formatToStringKindTarget(v, dateToTime(val).Format(time.DateOnly), "int"); wrote {
			return err
		}
	}
	return setIntValue(v, val)
}

func (ctx *jsonDecoder) decodeLong(v reflect.Value, node *schemaNode, toAny, raw bool) error {
	nb, err := ctx.scanner.consumeNumberBytes()
	if err != nil {
		return err
	}
	val, err := parseJSONInt64(nb)
	if err != nil {
		return err
	}
	if toAny {
		if raw {
			return setIface(v, reflect.ValueOf(val), "long")
		}
		logical, err := decodeLogicalLong(val, node)
		if err != nil {
			return err
		}
		return setIface(v, reflect.ValueOf(logical), "long")
	}
	if raw {
		// Suppressed logical: assign the raw int64, as in decodeInt.
		return setLongValue(v, val)
	}
	// All DecodeJSON entry points produce addressable values (see decodeInt).
	if v.Type() == timeType {
		p := (*time.Time)(v.Addr().UnsafePointer())
		if conv, ok := timestampToTimeConv(node.logical); ok {
			*p = conv(val)
			return nil
		}
		if node.logical == "time-micros" {
			// Mirrors serTimeMicros's timeType arm.
			d, err := timeMicrosToDuration(val)
			if err != nil {
				return err
			}
			*p = timeOfDayToTime(d)
			return nil
		}
	}
	if v.Type() == durationType && node.logical == "time-micros" {
		d, err := timeMicrosToDuration(val)
		if err != nil {
			return err
		}
		*(*time.Duration)(v.Addr().UnsafePointer()) = d
		return nil
	}
	// A string target takes the timestamp in RFC 3339, as deserTimeAsLong
	// does; json.Number falls through to the raw integer.
	conv, ok := timestampToTimeConv(node.logical)
	if !ok {
		return setLongValue(v, val)
	}
	if wrote, err := formatToStringKindTarget(v, conv(val).Format(time.RFC3339Nano), "long"); wrote {
		return err
	}
	return setLongValue(v, val)
}

// isJSONNullStart reports whether the next token is the JSON "null" literal.
// The peeked first byte 'n' could also start a bare lowercase "nan", so we
// check the second byte: null is the only token starting with "nu".
func isJSONNullStart(s *jsonScanner, p byte) bool {
	return p == 'n' && s.peekAt(1) == 'u'
}

// isBareSpecialFloatStart reports whether the next token could begin a bare
// NaN, Infinity, -Infinity, Inf or INF token; parseSpecialFloat applies the
// exact match after consumption. Java, fastavro, and goavro all reject a
// lowercase first letter, and a lowercase 'n' would collide with null.
func isBareSpecialFloatStart(s *jsonScanner, p byte) bool {
	switch p {
	case 'N', 'I':
		return true
	case '-':
		return s.peekAt(1) == 'I'
	}
	return false
}

// decodeJSONFloat decodes the next JSON token into a float64, accepting the
// four producer conventions: a quoted "NaN" or "Infinity" (Java's form and
// ours), a bare null for NaN (goavro), a bare NaN or Infinity token
// (fastavro), and a numeric literal, which goes to +/-Inf on overflow
// (goavro's 1e999).
func (ctx *jsonDecoder) decodeJSONFloat(bitSize int, typ string) (float64, error) {
	p := ctx.scanner.peek()
	switch {
	case p == '"':
		s, err := ctx.scanner.consumeStringZeroCopy()
		if err != nil {
			return 0, err
		}
		return parseSpecialFloat(s)
	case isJSONNullStart(ctx.scanner, p):
		if err := ctx.scanner.consumeNull(); err != nil {
			return 0, err
		}
		return math.NaN(), nil
	case isBareSpecialFloatStart(ctx.scanner, p):
		tok, err := ctx.scanner.consumeBareSpecialFloat()
		if err != nil {
			return 0, err
		}
		return parseSpecialFloat(tok)
	default:
		nb, err := ctx.scanner.consumeNumberBytes()
		if err != nil {
			return 0, err
		}
		// parseJSONNumberAsFloat applies the JSON number grammar, rejecting
		// forms like "5." that strconv.ParseFloat accepts, and parses a
		// "float" schema at float32 precision so there is a single rounding.
		f, err := parseJSONNumberAsFloat(unsafe.String(unsafe.SliceData(nb), len(nb)), bitSize)
		if err != nil {
			return 0, fmt.Errorf("avro json: %s: %w", typ, err)
		}
		return f, nil
	}
}

func (ctx *jsonDecoder) decodeFloat(v reflect.Value) error {
	f, err := ctx.decodeJSONFloat(32, "float")
	if err != nil {
		return err
	}
	return setFloatValue(v, f, "float", 32)
}

func (ctx *jsonDecoder) decodeDouble(v reflect.Value) error {
	f, err := ctx.decodeJSONFloat(64, "double")
	if err != nil {
		return err
	}
	return setFloatValue(v, f, "double", 64)
}

func (ctx *jsonDecoder) decodeString(v reflect.Value, node *schemaNode, toAny, raw bool) error {
	s, err := ctx.consumeSlabString()
	if err != nil {
		return err
	}
	// A [16]byte target parses the uuid, as deserUUID does; a suppressed
	// logical skips this and errors below, as deserString does.
	if node.logical == "uuid" && !toAny && !raw && isUUIDType(v.Type()) {
		u, err := parseUUID(s)
		if err != nil {
			return err
		}
		copyBytesToArray(v, u[:])
		return nil
	}
	if toAny {
		return setIface(v, reflect.ValueOf(s), "string")
	}
	// TextUnmarshaler wins over the string kind, as in setStringValue.
	if v.CanAddr() && v.Addr().Type().Implements(textUnmarshalerType) {
		_, err := tryTextUnmarshal(v, []byte(s))
		return err
	}
	if v.Kind() == reflect.String {
		return setStringTarget(v, s, "string")
	}
	if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		v.SetBytes([]byte(s))
		return nil
	}
	return semErr(v, "string")
}

func (ctx *jsonDecoder) decodeEnum(v reflect.Value, node *schemaNode) error {
	s, err := ctx.consumeSlabString()
	if err != nil {
		return err
	}
	idx, ok := node.symbolIndex(s)
	if !ok {
		return fmt.Errorf("avro json: unknown enum symbol %q", truncForError(s))
	}
	return setEnumTarget(v, idx, s)
}

func (ctx *jsonDecoder) decodeBytes(v reflect.Value, node *schemaNode, toAny, raw bool) error {
	// A decimal takes a bare JSON number as well as the byte string, so
	// hand-edited JSON stays convenient. A suppressed logical skips this: a
	// bare number has no raw-bytes form.
	if !raw && hasDecimalBareNumberArm(node) {
		if handled, err := ctx.decodeBareDecimal(v, node, toAny); handled {
			return err
		}
	}
	start, end, _, err := ctx.scanner.consumeStringRaw()
	if err != nil {
		return err
	}
	b, err := scanAvroJSONBytes(ctx.scanner.data[start:end])
	if err != nil {
		return err
	}
	if toAny {
		if raw {
			return setIface(v, reflect.ValueOf(b), "bytes")
		}
		val, err := decodeLogicalBytes(b, node)
		if err != nil {
			return err
		}
		return setIface(v, reflect.ValueOf(val), "bytes")
	}
	return assignBytes(v, b, node, raw, ctx.slab)
}

func (ctx *jsonDecoder) decodeFixed(v reflect.Value, node *schemaNode, toAny, raw bool) error {
	// A decimal takes a bare JSON number, as in decodeBytes.
	if !raw && hasDecimalBareNumberArm(node) {
		if handled, err := ctx.decodeBareDecimal(v, node, toAny); handled {
			return err
		}
	}
	start, end, _, err := ctx.scanner.consumeStringRaw()
	if err != nil {
		return err
	}
	b, err := scanAvroJSONBytes(ctx.scanner.data[start:end])
	if err != nil {
		return err
	}
	// The JSON string for a fixed must have exactly node.size code points;
	// Java enforces this too.
	if len(b) != node.size {
		return fmt.Errorf("avro json: fixed value has %d bytes, schema declares %d", len(b), node.size)
	}
	if !raw && node.logical == "decimal" {
		// decodeLogicalFixed has no error return, so the unscaled bound goes
		// here.
		if err := checkDecimalUnscaledLen(b); err != nil {
			return err
		}
	}
	if toAny {
		if raw {
			return setIface(v, reflect.ValueOf(b), "fixed")
		}
		return setIface(v, reflect.ValueOf(decodeLogicalFixed(b, node)), "fixed")
	}
	return assignBytes(v, b, node, raw, ctx.slab)
}

// assignBytes assigns decoded bytes to a typed target, handling the decimal,
// duration and uuid logical types. A logical arm that does not return falls
// through to the generic byte targets. raw means a matching no-Decode
// CustomType suppressed the logical, so every arm is skipped, as in the
// binary raw deserBytes and deserFixed.
func assignBytes(v reflect.Value, b []byte, node *schemaNode, raw bool, sl *slab) error {
	if raw {
		return setBytesValue(v, b, node.kind, sl)
	}
	// Each arm fires only on the kind its logical is valid on, matching the
	// *any path: a logical on the wrong kind exists only through a CustomType
	// resurrection, whose contract is the raw value.
	switch node.logical {
	case "decimal":
		if ok, err := setDecimalValue(v, b, node.scale); ok {
			return err
		}
	case "big-decimal":
		if node.kind == "bytes" {
			if done, err := applyBigDecimalPayload(v, b); done {
				return err
			}
		}
	case "duration":
		// DurationFromBytes reads exactly 12 bytes, so any other size falls
		// through raw, as decodeLogicalFixed does.
		if node.kind == "fixed" && len(b) == 12 && v.Type() == avroDurationType {
			v.Set(reflect.ValueOf(DurationFromBytes(b)))
			return nil
		}
	case "uuid":
		if node.kind == "fixed" && len(b) == 16 {
			var u [16]byte
			copy(u[:], b)
			// [16]byte trusts the raw bytes before any TextUnmarshaler, as
			// deserFixedUUIDReflect does.
			if isUUIDType(v.Type()) {
				copyBytesToArray(v, u[:])
				return nil
			}
			if v.CanAddr() && v.Addr().Type().Implements(textUnmarshalerType) {
				_, err := tryTextUnmarshal(v, []byte(uuidToString(u)))
				return err
			}
			if v.Kind() == reflect.String {
				return setStringTarget(v, uuidToString(u), "fixed")
			}
		}
	}
	return setBytesValue(v, b, node.kind, sl)
}

// hasDecimalBareNumberArm reports whether node accepts the bare-number JSON
// form on decode: decimal on bytes and fixed, big-decimal on bytes only, the
// same kind gate assignBytes applies.
func hasDecimalBareNumberArm(node *schemaNode) bool {
	switch node.logical {
	case "decimal":
		return true
	case "big-decimal":
		return node.kind == "bytes"
	}
	return false
}

// decodeBareDecimal handles the bare-number JSON arm for decimal and
// big-decimal. handled is false when the next token is a quoted string, so
// the caller falls through to the spec form.
func (ctx *jsonDecoder) decodeBareDecimal(v reflect.Value, node *schemaNode, toAny bool) (handled bool, err error) {
	c := ctx.scanner.peek()
	if c == '"' || c == 0 {
		return false, nil
	}
	nb, perr := ctx.scanner.consumeNumberBytes()
	if perr != nil {
		return true, perr
	}
	// boundedRatFromString neither writes nor retains the string.
	r, ok, perr := boundedRatFromString(unsafe.String(unsafe.SliceData(nb), len(nb)))
	if perr != nil {
		return true, fmt.Errorf("avro json: %s %q: %w", node.logical, truncBytesForError(nb), perr)
	}
	if !ok {
		return true, fmt.Errorf("avro json: invalid %s number %q", node.logical, truncBytesForError(nb))
	}
	if toAny {
		return true, setIface(v, reflect.ValueOf(r), node.kind)
	}
	// Big-decimal has no schema-level scale, so we derive it from the rat.
	scale := node.scale
	if node.logical == "big-decimal" {
		s, ok := finiteScale(r)
		if !ok {
			return true, fmt.Errorf("avro json: big-decimal value %s has no finite decimal expansion", truncRatForError(r))
		}
		scale = s
	}
	if applied, err := setDecimalRat(v, r, scale); applied {
		return true, err
	}
	return true, &SemanticError{GoType: v.Type(), AvroType: node.kind}
}

func (ctx *jsonDecoder) decodeArray(v reflect.Value, node *schemaNode, toAny bool) error {
	if err := ctx.scanner.expect('['); err != nil {
		return err
	}
	if toAny {
		var arr []any
		if ctx.scanner.peek() != ']' {
			var elem any
			elemV := reflect.ValueOf(&elem).Elem()
			for {
				if err := ctx.decodeValue(elemV, node.items); err != nil {
					return err
				}
				arr = append(arr, elem)
				elem = nil
				if ctx.scanner.peek() != ',' {
					break
				}
				ctx.scanner.pos++ // consume comma
			}
		}
		if arr == nil {
			arr = []any{}
		}
		if err := ctx.scanner.expect(']'); err != nil {
			return err
		}
		return setIface(v, reflect.ValueOf(arr), "array")
	}
	// Typed array target ([N]T): we require exactly len(v) elements, as the
	// binary side does.
	if v.Kind() == reflect.Array {
		arrLen := v.Len()
		idx := 0
		if ctx.scanner.peek() != ']' {
			for {
				if idx >= arrLen {
					return &SemanticError{GoType: v.Type(), AvroType: "array", Err: fmt.Errorf("expected %d elements, got more", arrLen)}
				}
				if err := ctx.decodeValue(v.Index(idx), node.items); err != nil {
					return err
				}
				idx++
				if ctx.scanner.peek() != ',' {
					break
				}
				ctx.scanner.pos++
			}
		}
		if idx != arrLen {
			return &SemanticError{GoType: v.Type(), AvroType: "array", Err: fmt.Errorf("expected %d elements, got %d", arrLen, idx)}
		}
		return ctx.scanner.expect(']')
	}
	// Typed slice target.
	if v.Kind() != reflect.Slice {
		return semErr(v, "array")
	}
	// Native fast path for a plain primitive item into an unnamed []V, which
	// drops the per-element reflect.Append.
	if node.items.logical == "" && node.items.decodeJSON == nil {
		if handled, err := decodeJSONNativeSliceDispatch(ctx, v, node.items); handled {
			return err
		}
	}
	v.Set(reflect.MakeSlice(v.Type(), 0, 0))
	if ctx.scanner.peek() != ']' {
		elem := reflect.New(v.Type().Elem()).Elem()
		for {
			if err := ctx.decodeValue(elem, node.items); err != nil {
				return err
			}
			v.Set(reflect.Append(v, elem))
			elem.SetZero()
			if ctx.scanner.peek() != ',' {
				break
			}
			ctx.scanner.pos++
		}
	}
	return ctx.scanner.expect(']')
}

func (ctx *jsonDecoder) decodeMap(v reflect.Value, node *schemaNode, toAny bool) error {
	if err := ctx.scanner.expect('{'); err != nil {
		return err
	}
	if toAny {
		m := make(map[string]any)
		if ctx.scanner.peek() != '}' {
			var val any
			valV := reflect.ValueOf(&val).Elem()
			for {
				key, err := ctx.consumeSlabString()
				if err != nil {
					return err
				}
				if err := ctx.scanner.expect(':'); err != nil {
					return err
				}
				if err := ctx.decodeValue(valV, node.values); err != nil {
					return err
				}
				m[key] = val
				val = nil
				if ctx.scanner.peek() != ',' {
					break
				}
				ctx.scanner.pos++
			}
		}
		if err := ctx.scanner.expect('}'); err != nil {
			return err
		}
		return setIface(v, reflect.ValueOf(m), "map")
	}
	// Typed map target.
	if v.Kind() != reflect.Map || v.Type().Key().Kind() != reflect.String {
		return semErr(v, "map")
	}
	if v.IsNil() {
		v.Set(reflect.MakeMap(v.Type()))
	}
	keyType := v.Type().Key()
	valType := v.Type().Elem()
	// Native fast path for a plain primitive value under a string key, which
	// drops the per-entry reflect SetMapIndex.
	if node.values.logical == "" && node.values.decodeJSON == nil && keyType == stringType && v.CanInterface() {
		if handled, err := decodeJSONNativeMap(ctx, v, node.values); handled {
			return err
		}
	}
	if ctx.scanner.peek() != '}' {
		elem := reflect.New(valType).Elem()
		keyVal := reflect.New(keyType).Elem() // typed for a named string key

		for {
			key, err := ctx.consumeSlabString()
			if err != nil {
				return err
			}
			if err := validateJSONNumberMapKey(key, keyType, "map"); err != nil {
				return err
			}
			if err := ctx.scanner.expect(':'); err != nil {
				return err
			}
			if err := ctx.decodeValue(elem, node.values); err != nil {
				return err
			}
			keyVal.SetString(key)
			v.SetMapIndex(keyVal, elem)
			elem.SetZero()
			if ctx.scanner.peek() != ',' {
				break
			}
			ctx.scanner.pos++
		}
	}
	return ctx.scanner.expect('}')
}

// The jsonRead leaves parse one JSON token straight into a Go value with no
// reflect.Value, sharing the token parsing the reflect decoders use.
func jsonReadString(c *jsonDecoder) (string, error) { return c.consumeSlabString() }
func jsonReadBool(c *jsonDecoder) (bool, error)     { return c.scanner.consumeBool() }
func jsonReadInt32(c *jsonDecoder) (int32, error) {
	nb, err := c.scanner.consumeNumberBytes()
	if err != nil {
		return 0, err
	}
	return parseJSONInt32(nb)
}
func jsonReadInt64(c *jsonDecoder) (int64, error) {
	nb, err := c.scanner.consumeNumberBytes()
	if err != nil {
		return 0, err
	}
	return parseJSONInt64(nb)
}

// jsonReadInt is only reached when int is 64-bit; on 32-bit the long-into-int
// native arms fall back to the overflow-checked reflect path.
func jsonReadInt(c *jsonDecoder) (int, error) { n, err := jsonReadInt64(c); return int(n), err }
func jsonReadFloat32(c *jsonDecoder) (float32, error) {
	f, err := c.decodeJSONFloat(32, "float")
	return float32(f), err
}
func jsonReadFloat64(c *jsonDecoder) (float64, error) { return c.decodeJSONFloat(64, "double") }

func decodeJSONNativeStringMap[V any](ctx *jsonDecoder, m map[string]V, readOne func(*jsonDecoder) (V, error)) error {
	if ctx.scanner.peek() != '}' {
		for {
			key, err := ctx.consumeSlabString()
			if err != nil {
				return err
			}
			if err := ctx.scanner.expect(':'); err != nil {
				return err
			}
			val, err := readOne(ctx)
			if err != nil {
				return err
			}
			m[key] = val
			if ctx.scanner.peek() != ',' {
				break
			}
			ctx.scanner.pos++
		}
	}
	return ctx.scanner.expect('}')
}

func decodeJSONNativeSlice[V any](ctx *jsonDecoder, v reflect.Value, readOne func(*jsonDecoder) (V, error)) error {
	var s []V
	if ctx.scanner.peek() != ']' {
		for {
			val, err := readOne(ctx)
			if err != nil {
				return err
			}
			s = append(s, val)
			if ctx.scanner.peek() != ',' {
				break
			}
			ctx.scanner.pos++
		}
	}
	if s == nil {
		s = []V{}
	}
	v.Set(reflect.ValueOf(s))
	return ctx.scanner.expect(']')
}

// decodeJSONNativeMap routes an unnamed map[string]V of a plain primitive to
// the native loop. A named map or value type falls back to reflect with the
// scanner untouched.
func decodeJSONNativeMap(ctx *jsonDecoder, v reflect.Value, valNode *schemaNode) (bool, error) {
	switch et := v.Type().Elem(); {
	case valNode.kind == "string" && et == stringType:
		if m, ok := v.Interface().(map[string]string); ok {
			return true, decodeJSONNativeStringMap(ctx, m, jsonReadString)
		}
	case valNode.kind == "int" && et == int32Type:
		if m, ok := v.Interface().(map[string]int32); ok {
			return true, decodeJSONNativeStringMap(ctx, m, jsonReadInt32)
		}
	case valNode.kind == "long" && et == int64Type:
		if m, ok := v.Interface().(map[string]int64); ok {
			return true, decodeJSONNativeStringMap(ctx, m, jsonReadInt64)
		}
	// int(int64) narrows on 32-bit, so that platform falls back to the
	// overflow-checked reflect path.
	case valNode.kind == "long" && et == intType && strconv.IntSize == 64:
		if m, ok := v.Interface().(map[string]int); ok {
			return true, decodeJSONNativeStringMap(ctx, m, jsonReadInt)
		}
	case valNode.kind == "float" && et == float32Type:
		if m, ok := v.Interface().(map[string]float32); ok {
			return true, decodeJSONNativeStringMap(ctx, m, jsonReadFloat32)
		}
	case valNode.kind == "double" && et == float64Type:
		if m, ok := v.Interface().(map[string]float64); ok {
			return true, decodeJSONNativeStringMap(ctx, m, jsonReadFloat64)
		}
	case valNode.kind == "boolean" && et == boolType:
		if m, ok := v.Interface().(map[string]bool); ok {
			return true, decodeJSONNativeStringMap(ctx, m, jsonReadBool)
		}
	}
	return false, nil
}

// decodeJSONNativeSliceDispatch routes an unnamed []V of a plain primitive to
// the native loop. A named slice type cannot take v.Set([]V), and a named
// element type misses the exact-type case; both fall back.
func decodeJSONNativeSliceDispatch(ctx *jsonDecoder, v reflect.Value, itemNode *schemaNode) (bool, error) {
	if v.Type().Name() != "" {
		return false, nil
	}
	switch et := v.Type().Elem(); {
	case itemNode.kind == "string" && et == stringType:
		return true, decodeJSONNativeSlice(ctx, v, jsonReadString)
	case itemNode.kind == "int" && et == int32Type:
		return true, decodeJSONNativeSlice(ctx, v, jsonReadInt32)
	case itemNode.kind == "long" && et == int64Type:
		return true, decodeJSONNativeSlice(ctx, v, jsonReadInt64)
	case itemNode.kind == "long" && et == intType && strconv.IntSize == 64:
		return true, decodeJSONNativeSlice(ctx, v, jsonReadInt)
	case itemNode.kind == "float" && et == float32Type:
		return true, decodeJSONNativeSlice(ctx, v, jsonReadFloat32)
	case itemNode.kind == "double" && et == float64Type:
		return true, decodeJSONNativeSlice(ctx, v, jsonReadFloat64)
	case itemNode.kind == "boolean" && et == boolType:
		return true, decodeJSONNativeSlice(ctx, v, jsonReadBool)
	}
	return false, nil
}

func (ctx *jsonDecoder) decodeRecord(v reflect.Value, node *schemaNode, toAny bool) error {
	if err := ctx.scanner.expect('{'); err != nil {
		return err
	}
	if toAny {
		return ctx.decodeRecordAny(v, node)
	}
	// Typed target: struct or map[string]T.
	if v.Kind() == reflect.Map && v.Type().Key().Kind() == reflect.String {
		return ctx.decodeRecordMap(v, node)
	}
	if v.Kind() == reflect.Struct {
		return ctx.decodeRecordStruct(v, node)
	}
	return semErr(v, "record")
}

// iterateRecordFields drives the record field loop: we dispatch each key to
// handle, skip unknown keys, then invoke fillDefault for any absent field that
// has a schema default, erroring otherwise. fillDefault may be nil.
func (ctx *jsonDecoder) iterateRecordFields(node *schemaNode, handle func(idx int, key string) error, fillDefault func(idx int) error) error {
	seen := make([]bool, len(node.fields))
	// We track which JSON key claimed each field, so a name and its alias
	// both appearing in one object error rather than silently overwrite.
	seenKey := make([]string, len(node.fields))
	if ctx.scanner.peek() != '}' {
		for {
			// The key is only looked up and quoted in errors, never stored.
			key, err := ctx.scanner.consumeStringZeroCopy()
			if err != nil {
				return err
			}
			if err := ctx.scanner.expect(':'); err != nil {
				return err
			}
			idx := -1
			if node.fieldIdx != nil {
				if i, ok := node.fieldIdx[key]; ok {
					idx = i
				}
			}
			if idx < 0 {
				if err := ctx.scanner.skipValue(); err != nil {
					return err
				}
			} else {
				// The same key appearing twice is last-wins, matching Java,
				// fastavro, and encoding/json on duplicate keys.
				if seen[idx] && seenKey[idx] != key {
					return fmt.Errorf("avro json: record %q field %q resolved from both %q and %q in the same JSON object",
						truncForError(node.name), truncForError(node.fields[idx].name), truncForError(seenKey[idx]), truncForError(key))
				}
				seen[idx] = true
				seenKey[idx] = key
				if err := handle(idx, key); err != nil {
					return err
				}
			}
			if ctx.scanner.peek() != ',' {
				break
			}
			ctx.scanner.pos++
		}
	}
	if err := ctx.scanner.expect('}'); err != nil {
		return err
	}
	for i, f := range node.fields {
		if seen[i] {
			continue
		}
		if !f.hasDefault {
			return fmt.Errorf("avro json: record %q missing required field %q", truncForError(node.name), truncForError(f.name))
		}
		if fillDefault != nil {
			if err := fillDefault(i); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ctx *jsonDecoder) decodeRecordAny(v reflect.Value, node *schemaNode) error {
	// Fail before decoding on a target that cannot hold the result, as
	// deserRecord.deser does.
	if v.Type().NumMethod() != 0 && !mapStringAnyType.AssignableTo(v.Type()) {
		return semErr(v, "record")
	}
	m := reuseOrMakeStringAnyMap(v, len(node.fields))
	var val any
	valV := reflect.ValueOf(&val).Elem()
	err := ctx.iterateRecordFields(node,
		func(idx int, key string) error {
			f := &node.fields[idx]
			if err := ctx.decodeValue(valV, f.node); err != nil {
				return recordFieldError(v.Type(), f.name, err)
			}
			m[f.name] = val
			val = nil
			return nil
		},
		func(idx int) error {
			f := &node.fields[idx]
			var defVal any
			defValV := reflect.ValueOf(&defVal).Elem()
			if err := ctx.applyFieldDefault(defValV, node, idx); err != nil {
				return fmt.Errorf("field %q default: %w", truncForError(f.name), err)
			}
			m[f.name] = defVal
			return nil
		},
	)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(m))
	return nil
}

func (ctx *jsonDecoder) decodeRecordMap(v reflect.Value, node *schemaNode) error {
	if v.IsNil() {
		v.Set(reflect.MakeMapWithSize(v.Type(), len(node.fields)))
	}
	elem := reflect.New(v.Type().Elem()).Elem()
	mapType := v.Type()
	keyType := mapType.Key()
	return ctx.iterateRecordFields(node,
		func(idx int, key string) error {
			f := &node.fields[idx]
			if err := validateJSONNumberMapKey(f.name, keyType, "record"); err != nil {
				return err
			}
			if err := ctx.decodeValue(elem, f.node); err != nil {
				return recordFieldError(v.Type(), f.name, err)
			}
			v.SetMapIndex(mapKeyAs(mapType, f.nameVal), elem)
			elem.SetZero()
			return nil
		},
		func(idx int) error {
			f := &node.fields[idx]
			if err := validateJSONNumberMapKey(f.name, keyType, "record"); err != nil {
				return err
			}
			if err := ctx.applyFieldDefault(elem, node, idx); err != nil {
				return fmt.Errorf("field %q default: %w", truncForError(f.name), err)
			}
			v.SetMapIndex(mapKeyAs(mapType, f.nameVal), elem)
			elem.SetZero()
			return nil
		},
	)
}

func (ctx *jsonDecoder) decodeRecordStruct(v reflect.Value, node *schemaNode) error {
	dr := node.deserRecord
	if dr == nil {
		return &SemanticError{GoType: v.Type(), AvroType: "record", Err: errors.New("no record metadata")}
	}
	mapping, err := typeFieldMappingSkip(dr.names, &dr.cache, v.Type(), ctx.slab.skipUnknown)
	if err != nil {
		return err
	}
	return ctx.iterateRecordFields(node,
		func(idx int, key string) error {
			f := &node.fields[idx]
			if mapping.unmapped(idx) {
				return ctx.scanner.skipValue()
			}
			fv, err := fieldByIndex(v, mapping.indices[idx])
			if err != nil {
				return recordFieldError(v.Type(), f.name, err)
			}
			if err := ctx.decodeValue(fv, f.node); err != nil {
				return recordFieldError(v.Type(), f.name, err)
			}
			return nil
		},
		func(idx int) error {
			f := &node.fields[idx]
			if mapping.unmapped(idx) {
				return nil // no struct field to fill
			}
			fv, err := fieldByIndex(v, mapping.indices[idx])
			if err != nil {
				return fmt.Errorf("field %q default: %w", truncForError(f.name), err)
			}
			if err := ctx.applyFieldDefault(fv, node, idx); err != nil {
				return fmt.Errorf("field %q default: %w", truncForError(f.name), err)
			}
			return nil
		},
	)
}

// applyFieldDefault decodes the field's pre-encoded binary default into target
// through the record's wrapped binary deserfn, the same one a present field
// uses, so a registered CustomType.Decode fires for default-filled fields.
// A zero-length default is valid for any field whose wire encoding is
// naturally empty.
func (ctx *jsonDecoder) applyFieldDefault(target reflect.Value, node *schemaNode, idx int) error {
	if node.serRecord == nil || idx >= len(node.serRecord.fields) {
		return fmt.Errorf("record has no pre-encoded default for field %d", idx)
	}
	// The schema's own buffer is the src, uncopied; see
	// defaultOp.encodedDefault. DecodeJSON ignores [AliasInput], so nothing
	// aliases it.
	enc := node.serRecord.fields[idx].defaultBytes
	_, err := node.deserRecord.fields[idx].fn(enc, target, ctx.slab)
	return err
}

// unionBranchRecurses reports whether a union branch kind decodes a nested
// value that can recurse back into the union. The JSON union decoders commit
// to the first such branch rather than re-decode the subtree as a later
// container branch, since backtracking across recursive branches is
// 2^depth. Scalar branches cannot recurse, so they keep their bounded
// backtrack.
func unionBranchRecurses(kind string) bool {
	return kind == "record" || kind == "array" || kind == "map"
}

func (ctx *jsonDecoder) decodeUnion(v reflect.Value, node *schemaNode) error {
	p := ctx.scanner.peek()

	// JSON null takes the null branch, if the union has one, before any
	// indirectAlloc so a *T target stays nil. Java and fastavro both reject
	// null when the union has no null branch. isJSONNullStart tells null
	// from a bare special-float token.
	if isJSONNullStart(ctx.scanner, p) {
		hasNull := false
		for _, br := range node.branches {
			if br.kind == "null" {
				hasNull = true
				break
			}
		}
		if !hasNull {
			return fmt.Errorf("avro json: union has no null branch but value is null at offset %d", ctx.scanner.pos)
		}
		if err := ctx.scanner.consumeNull(); err != nil {
			return err
		}
		setZero(v)
		return nil
	}

	// Indirection is per-branch (see unionTarget), as in the binary
	// deserUnion.deser. Pre-indirecting here would dereference a reused *T
	// held in an interface and reject a custom Decode returning a pointer.

	// A JSON object is either a tagged union {"type": value} or a bare
	// record/map.
	if p == '{' {
		return ctx.decodeUnionObject(v, node)
	}

	// Bare non-object value: match by JSON token type.
	return ctx.decodeUnionBare(v, node, p)
}

func (ctx *jsonDecoder) decodeUnionObject(v reflect.Value, node *schemaNode) error {
	savedPos := ctx.scanner.pos
	// We keep the tagged interpretation's decode error so the final message
	// names the real reason when the bare fallback also fails.
	var taggedErr error

	// Try tagged union: {"branchName": value}.
	ctx.scanner.pos++ // consume '{'
	ctx.scanner.skipWhitespace()
	if ctx.scanner.peek() != '}' {
		key, err := ctx.scanner.consumeStringZeroCopy()
		if err == nil {
			if branch := findUnionBranch(node, key); branch != nil {
				if err := ctx.scanner.expect(':'); err == nil {
					target, toAny := unionTarget(v, branch)
					if toAny {
						// We decode into a temporary so the target stays
						// untouched until the close brace arrives; a malformed
						// `{"long": 42,` would otherwise write it and then
						// backtrack to the bare fallback.
						var val any
						err := ctx.decodeValue(reflect.ValueOf(&val).Elem(), branch)
						if err == nil {
							if ctx.scanner.peek() == '}' {
								ctx.scanner.pos++
								return assignAny(target, ctx.wrapUnion(target, val, node, branch), branch.kind)
							}
						} else if errors.Is(err, errTooDeep) {
							return err
						} else if unionBranchRecurses(branch.kind) {
							// A key matching a container branch commits to
							// the tagged form: the bare retry would re-decode
							// the subtree, 2^depth when a record field name
							// collides with a branch name.
							return err
						} else {
							taggedErr = err
						}
					} else {
						// The typed path writes target directly. The only
						// backtrack trigger is a missing close brace on
						// otherwise valid JSON, and the bare fallback
						// overwrites if it matches.
						err := ctx.decodeValue(target, branch)
						if err == nil {
							if ctx.scanner.peek() == '}' {
								ctx.scanner.pos++
								return nil
							}
						} else if errors.Is(err, errTooDeep) {
							return err
						} else if unionBranchRecurses(branch.kind) {
							return err
						} else {
							taggedErr = err
						}
					}
				}
			}
		}
	}

	// The tagged interpretation failed: backtrack and try bare.
	ctx.scanner.pos = savedPos
	if err := ctx.decodeUnionBare(v, node, '{'); err != nil {
		if taggedErr != nil {
			return fmt.Errorf("%w (tagged-form: %v)", err, taggedErr)
		}
		return err
	}
	return nil
}

// decodeBranchInto decodes the next JSON value as the given union branch and
// writes the result into v.
func (ctx *jsonDecoder) decodeBranchInto(rawV reflect.Value, union, branch *schemaNode) error {
	v, toAny := unionTarget(rawV, branch)
	if toAny {
		var val any
		if err := ctx.decodeValue(reflect.ValueOf(&val).Elem(), branch); err != nil {
			return err
		}
		// wrapUnion returns nil for a null branch, so we go through
		// assignAny, which sets a typed nil for an interface target.
		return assignAny(v, ctx.wrapUnion(v, val, union, branch), branch.kind)
	}
	return ctx.decodeValue(v, branch)
}

// unionTarget selects the decode target and toAny flag for a matched union
// branch, with the same per-branch indirection as the binary
// deserUnion.deser. A custom branch keeps the raw target so its
// setCustomResult can put a pointer result into a reused interface or a
// concrete *T field. An interface target is peeled and assigned directly. A
// concrete target returns the un-peeled rawV, since the branch decode runs
// its own indirectAlloc; returning the peeled value would let a union
// pointer target accept twice maxIndirectDepth levels.
func unionTarget(rawV reflect.Value, branch *schemaNode) (reflect.Value, bool) {
	if branch.decodeJSON != nil {
		return rawV, rawV.Kind() == reflect.Interface
	}
	iv := indirectAlloc(rawV)
	if iv.Kind() == reflect.Interface {
		return iv, true
	}
	return rawV, false
}

func (ctx *jsonDecoder) decodeUnionBare(v reflect.Value, node *schemaNode, p byte) error {
	// Match by JSON token type against branch kinds. We keep the last
	// branch's decode error so the final message names the concrete reason.
	var lastErr error
	for _, branch := range node.branches {
		// decodeUnion already routed JSON null, so a peek byte of 'n' here is
		// a bare special-float start and must not match the null branch.
		if branch.kind == "null" {
			continue
		}
		if !jsonTokenMatchesBranch(p, branch) {
			continue
		}
		savedPos := ctx.scanner.pos
		if err := ctx.decodeBranchInto(v, node, branch); err == nil {
			return nil
		} else if errors.Is(err, errTooDeep) {
			return err
		} else {
			lastErr = err
			// We commit to the first container branch the token matches, since
			// backtracking is 2^depth on a recursive union; use the tagged
			// form to name a later container branch. Scalar branches keep
			// their backtrack, so ["int","long"] still falls through to long
			// for an int-overflowing value.
			if unionBranchRecurses(branch.kind) {
				break
			}
			ctx.scanner.pos = savedPos
		}
	}
	if lastErr != nil {
		return fmt.Errorf("avro json: no union branch matched at offset %d: %w", ctx.scanner.pos, lastErr)
	}
	return fmt.Errorf("avro json: no union branch matched at offset %d", ctx.scanner.pos)
}

func (ctx *jsonDecoder) wrapUnion(v reflect.Value, val any, union, branch *schemaNode) any {
	if !ctx.slab.taggedUnions || val == nil {
		return val
	}
	// As in the binary deserUnion.maybeWrap, the envelope applies only to an
	// interface target that map[string]any is assignable to; a non-empty
	// interface receives the bare value.
	if v.Kind() == reflect.Interface && !mapStringAnyType.AssignableTo(v.Type()) {
		return val
	}
	// unionEmitTag keeps the decode-side key byte-identical to the encode
	// side's.
	return map[string]any{unionEmitTag(union, branch, ctx.slab.tagLogicalTypes): val}
}

func jsonTokenMatchesBranch(p byte, branch *schemaNode) bool {
	switch p {
	case '"':
		switch branch.kind {
		case "string", "enum", "bytes", "fixed":
			return true
		case "float", "double":
			return true // NaN/Infinity strings
		}
	case 't', 'f':
		return branch.kind == "boolean"
	case '[':
		return branch.kind == "array"
	case '{':
		return branch.kind == "record" || branch.kind == "map"
	default: // digit or '-'
		switch branch.kind {
		case "int", "long", "float", "double":
			return true
		case "bytes", "fixed":
			// Lenient decode: a hand-edited or alternate-tool JSON
			// producer may emit a decimal-like-typed bytes/fixed
			// branch as a bare number. decodeBytes and decodeFixed
			// accept both forms via decodeBareDecimal, so dispatch has
			// to offer the branch for the number form to reach them.
			// Big-decimal is bytes-only per spec, hence not eligible
			// on a fixed branch.
			return hasDecimalBareNumberArm(branch)
		}
	}
	return false
}
