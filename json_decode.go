package avro

import (
	"encoding"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

// decodeLogicalInt applies logical type conversion for int-backed logical types
// when decoding to *any targets.
func decodeLogicalInt(val int32, node *schemaNode) any {
	switch node.logical {
	case "date":
		return dateToTime(val)
	case "time-millis":
		return timeMillisToDuration(val)
	}
	return val
}

// decodeLogicalLong applies logical type conversion for long-backed logical types
// when decoding to *any targets. Returns an error only for time-micros when
// val * time.Microsecond would wrap; the timestamp conversions are total.
func decodeLogicalLong(val int64, node *schemaNode) (any, error) {
	switch node.logical {
	case "timestamp-millis", "local-timestamp-millis":
		return timestampMillisToTime(val), nil
	case "timestamp-micros", "local-timestamp-micros":
		return timestampMicrosToTime(val), nil
	case "timestamp-nanos", "local-timestamp-nanos":
		return timestampNanosToTime(val), nil
	case "time-micros":
		return timeMicrosToDuration(val)
	}
	return val, nil
}

// decodeLogicalBytes applies logical type conversion for bytes-backed
// logical types when decoding to *any targets. Errors on malformed
// payloads.
func decodeLogicalBytes(b []byte, node *schemaNode) (any, error) {
	if node.logical == "decimal" {
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

// decodeLogicalFixed applies logical type conversion for fixed-backed logical types
// when decoding to *any targets.
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

// assignAny sets a native Go value on a reflect.Value target.
// For nil val + nilable v (interface, pointer, map, slice), zeros v.
// For non-nil val, returns a SemanticError if val's type isn't assignable
// to v's type — guarding against decode targets like *interface{Foo()}
// that the produced value doesn't satisfy. Skips the AssignableTo lookup
// for the empty-interface (any) target — the hot decode-into-*any path.
func assignAny(v reflect.Value, val any, avroType string) error {
	if val == nil {
		switch v.Kind() {
		case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice:
			v.Set(reflect.Zero(v.Type()))
		}
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
	scanner        *jsonScanner
	slab           *slab
	wrapUnions     bool
	qualifyLogical bool
}

// decodeValue is the core recursive decoder. It reads the next JSON
// value from the scanner, guided by the schema node, and assigns to v.
//
// For interface (any) targets, it produces JSON-native or enriched Go
// values. For typed targets (struct, int, string, etc.), it assigns
// directly.
//
// When the node carries a custom-decoder wrapper (decodeJSON), the
// wrapper handles the dispatch — it captures the decoder chain at
// schema build, calls decodeKind for the inner value, then applies
// each custom decoder in turn. No runtime map lookup, no recursion
// guard. Concurrency safety is structural: the schema graph is
// read-only at decode time and the jsonDecoder is per-call.
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

// decodeKind is decodeValue minus the depth guard and the
// custom-decoder dispatch — just the kind switch. Called directly by
// decodeValue for nodes without custom decoders, and by the
// custom-decoder closure to produce the inner *any value before
// applying the decoder chain.
func (ctx *jsonDecoder) decodeKind(v reflect.Value, node *schemaNode) error {
	// Unions handle pointer/nil targets specially (before indirectAlloc),
	// so dispatch early.
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
		return ctx.decodeInt(v, node, toAny)
	case "long":
		return ctx.decodeLong(v, node, toAny)
	case "float":
		return ctx.decodeFloat(v)
	case "double":
		return ctx.decodeDouble(v)
	case "string":
		return ctx.decodeString(v, node, toAny)
	case "enum":
		return ctx.decodeEnum(v, node, toAny)
	case "bytes":
		return ctx.decodeBytes(v, node, toAny)
	case "fixed":
		return ctx.decodeFixed(v, node, toAny)
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

// wrapDecodeJSONWithCustomDecoders builds a per-node JSON decode
// closure that captures the custom decoder chain — the JSON parallel
// of wrapDeserWithCustomDecoders (custom_type.go). The inner value is
// produced via decodeKind so we don't re-enter the wrapper for the
// same node.
func wrapDecodeJSONWithCustomDecoders(decoders []func(any, *SchemaNode) (any, error), sn *SchemaNode) jsonDecodeFn {
	return func(ctx *jsonDecoder, v reflect.Value, node *schemaNode) error {
		var tmp any
		tmpV := reflect.ValueOf(&tmp).Elem()
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
			return assignAny(indirectAlloc(v), out, node.kind)
		}
		return assignAny(indirectAlloc(v), tmp, node.kind)
	}
}

func (ctx *jsonDecoder) decodeNull(v reflect.Value, toAny bool) error {
	if err := ctx.scanner.consumeNull(); err != nil {
		return err
	}
	if toAny {
		v.Set(reflect.Zero(v.Type()))
	} else {
		switch v.Kind() {
		case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Interface:
			v.Set(reflect.Zero(v.Type()))
		}
	}
	return nil
}

func (ctx *jsonDecoder) decodeBool(v reflect.Value, toAny bool) error {
	b, err := ctx.scanner.consumeBool()
	if err != nil {
		return err
	}
	if toAny {
		if v.Type().NumMethod() == 0 {
			v.Set(reflect.ValueOf(b))
			return nil
		}
		rv := reflect.ValueOf(b)
		if !rv.Type().AssignableTo(v.Type()) {
			return &SemanticError{GoType: v.Type(), AvroType: "boolean"}
		}
		v.Set(rv)
	} else if v.Kind() == reflect.Bool {
		v.SetBool(b)
	} else {
		return &SemanticError{GoType: v.Type(), AvroType: "boolean"}
	}
	return nil
}

func (ctx *jsonDecoder) decodeInt(v reflect.Value, node *schemaNode, toAny bool) error {
	nb, err := ctx.scanner.consumeNumberBytes()
	if err != nil {
		return err
	}
	val, err := parseJSONInt32(nb)
	if err != nil {
		return err
	}
	if toAny {
		logical := decodeLogicalInt(val, node)
		if v.Type().NumMethod() == 0 {
			v.Set(reflect.ValueOf(logical))
			return nil
		}
		rv := reflect.ValueOf(logical)
		if !rv.Type().AssignableTo(v.Type()) {
			return &SemanticError{GoType: v.Type(), AvroType: "int"}
		}
		v.Set(rv)
		return nil
	}
	// All DecodeJSON entry points produce addressable values
	// (Schema.DecodeJSON requires a pointer; recursive paths use
	// reflect.New().Elem() or addressable struct fields).
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
	// String target for date: mirrors json_codec.go's "int" date arm
	// which accepts a date-string on encode (tryParseDateString).
	// Parity with deserDate on the binary side.
	if v.Kind() == reflect.String && node.logical == "date" {
		v.SetString(dateToTime(val).Format(time.DateOnly))
		return nil
	}
	return setIntValue(v, val)
}

func (ctx *jsonDecoder) decodeLong(v reflect.Value, node *schemaNode, toAny bool) error {
	nb, err := ctx.scanner.consumeNumberBytes()
	if err != nil {
		return err
	}
	val, err := parseJSONInt64(nb)
	if err != nil {
		return err
	}
	if toAny {
		logical, err := decodeLogicalLong(val, node)
		if err != nil {
			return err
		}
		if v.Type().NumMethod() == 0 {
			v.Set(reflect.ValueOf(logical))
			return nil
		}
		rv := reflect.ValueOf(logical)
		if !rv.Type().AssignableTo(v.Type()) {
			return &SemanticError{GoType: v.Type(), AvroType: "long"}
		}
		v.Set(rv)
		return nil
	}
	// All DecodeJSON entry points produce addressable values (see decodeInt).
	if v.Type() == timeType {
		p := (*time.Time)(v.Addr().UnsafePointer())
		switch node.logical {
		case "timestamp-millis", "local-timestamp-millis":
			*p = timestampMillisToTime(val)
			return nil
		case "timestamp-micros", "local-timestamp-micros":
			*p = timestampMicrosToTime(val)
			return nil
		case "timestamp-nanos", "local-timestamp-nanos":
			*p = timestampNanosToTime(val)
			return nil
		case "time-micros":
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
	// String target for the six long-typed time logicals: mirrors the
	// JSON encoder's "long" arm (json_codec.go), which accepts an RFC
	// 3339 string via extractTime. Parity with deserTimeAsLong on the
	// binary side.
	if v.Kind() == reflect.String {
		var t time.Time
		switch node.logical {
		case "timestamp-millis", "local-timestamp-millis":
			t = timestampMillisToTime(val)
		case "timestamp-micros", "local-timestamp-micros":
			t = timestampMicrosToTime(val)
		case "timestamp-nanos", "local-timestamp-nanos":
			t = timestampNanosToTime(val)
		default:
			return setLongValue(v, val)
		}
		v.SetString(t.Format(time.RFC3339Nano))
		return nil
	}
	return setLongValue(v, val)
}

// isJSONNullStart reports whether the next token is the JSON "null"
// literal. The peeked first byte 'n' is ambiguous (could also start a
// bare lowercase "nan"); we disambiguate by checking the second byte
// — null is the only token starting with "nu".
func isJSONNullStart(s *jsonScanner, p byte) bool {
	return p == 'n' && s.peekAt(1) == 'u'
}

// isBareSpecialFloatStart reports whether the next token could begin
// a bare NaN / Infinity / -Infinity / Inf / INF token in the canonical
// Java/fastavro casings (uppercase first letter; parseSpecialFloat
// applies the exact-match gate after consumption). Lowercase first
// letters ('n', 'i') are rejected — Java's JsonDecoder, fastavro's
// Python json, and goavro all reject lowercase, and the lowercase 'n'
// in particular collided with the JSON null literal in the union
// dispatcher (F1 finding).
func isBareSpecialFloatStart(s *jsonScanner, p byte) bool {
	switch p {
	case 'N', 'I':
		return true
	case '-':
		return s.peekAt(1) == 'I'
	}
	return false
}

// decodeJSONFloat decodes the next JSON token into a float64,
// dispatching across the four producer conventions twmb accepts:
//
//   - quoted-string "NaN"/"Infinity"/"-Infinity"/"INF"/"-INF"/"Inf"/"-Inf"
//     (Java JsonEncoder form, twmb's default emit form). parseSpecialFloat
//     gates exact-match (Java/fastavro/goavro parity — see its docstring).
//   - bare null → NaN (goavro convention). isJSONNullStart disambiguates
//     from bare special-float tokens whose first byte is unambiguously
//     uppercase post-tightening.
//   - bare NaN/Infinity/-Infinity/INF/-INF/Inf/-Inf (fastavro / Python json.dumps
//     with allow_nan=True). Routed through parseSpecialFloat for consistency
//     with the quoted-string arm — same exact-match acceptance set.
//   - numeric literal, with ±Inf accept on overflow (goavro's 1e999 / -1e999
//     convention, and any over-range literal that strconv.ParseFloat
//     produces as Inf with ErrRange).
//
// Shared by decodeFloat (bitSize=32) and decodeDouble (bitSize=64);
// the per-target narrowing is applied via setFloatValue downstream.
// typ is "float" or "double" for the syntax-error message.
func (ctx *jsonDecoder) decodeJSONFloat(bitSize int, typ string) (float64, error) {
	p := ctx.scanner.peek()
	switch {
	case p == '"':
		s, err := ctx.scanner.consumeString()
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
		// strconv.ParseFloat is read-only; alias nb to avoid the copy.
		f, err := strconv.ParseFloat(unsafe.String(unsafe.SliceData(nb), len(nb)), bitSize)
		if err != nil {
			// Accept ±Inf from overflow (e.g. 1e999, goavro convention).
			if math.IsInf(f, 0) {
				return f, nil
			}
			return 0, fmt.Errorf("avro json: invalid %s: %w", typ, err)
		}
		return f, nil
	}
}

func (ctx *jsonDecoder) decodeFloat(v reflect.Value) error {
	f, err := ctx.decodeJSONFloat(32, "float")
	if err != nil {
		return err
	}
	// setFloatValue's interface arm subsumes what the toAny branch
	// would otherwise do — single point of truth for the float-target
	// matrix shared with deserFloat. The float32 narrowing happens
	// inside setFloatValue for typed float32 targets.
	return setFloatValue(v, f, "float", 32)
}

func (ctx *jsonDecoder) decodeDouble(v reflect.Value) error {
	f, err := ctx.decodeJSONFloat(64, "double")
	if err != nil {
		return err
	}
	return setFloatValue(v, f, "double", 64)
}

func (ctx *jsonDecoder) decodeString(v reflect.Value, node *schemaNode, toAny bool) error {
	s, err := ctx.consumeSlabString()
	if err != nil {
		return err
	}
	// UUID logical type: [16]byte target parses the hex-dash string
	// into raw bytes, matching deserUUID on the binary side.
	if node.logical == "uuid" && !toAny && isUUIDType(v.Type()) {
		u, err := parseUUID(s)
		if err != nil {
			return err
		}
		reflect.Copy(v, reflect.ValueOf(u))
		return nil
	}
	if toAny {
		if v.Type().NumMethod() == 0 {
			v.Set(reflect.ValueOf(s))
			return nil
		}
		rv := reflect.ValueOf(s)
		if !rv.Type().AssignableTo(v.Type()) {
			return &SemanticError{GoType: v.Type(), AvroType: "string"}
		}
		v.Set(rv)
		return nil
	}
	if v.Kind() == reflect.String {
		v.SetString(s)
		return nil
	}
	// TextUnmarshaler before []byte: named []byte subtypes like net.IP
	// should use their text parsing, not raw byte assignment. Mirrors
	// deserString's order so binary and JSON decoders agree on which
	// Go target types accept Avro string.
	if v.CanAddr() && v.Addr().Type().Implements(textUnmarshalerType) {
		// s borrows the slab; allocate fresh storage for the callee.
		if err := v.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText([]byte(s)); err != nil {
			return err
		}
		return nil
	}
	if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		v.SetBytes([]byte(s))
		return nil
	}
	return &SemanticError{GoType: v.Type(), AvroType: "string"}
}

func (ctx *jsonDecoder) decodeEnum(v reflect.Value, node *schemaNode, toAny bool) error {
	s, err := ctx.consumeSlabString()
	if err != nil {
		return err
	}
	idx := -1
	for i, sym := range node.symbols {
		if sym == s {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("avro json: unknown enum symbol %q", s)
	}
	switch {
	case toAny:
		return setIface(v, reflect.ValueOf(s), "enum")
	case v.Kind() == reflect.String:
		v.SetString(s)
	case v.CanInt():
		// Mirrors deserEnum's int target arm: set the symbol's
		// ordinal so a struct with an int-typed enum field round-
		// trips identically through binary and JSON. Java's
		// JsonDecoder.readEnum and fastavro's read_enum both return
		// the index — twmb's JSON path used to reject this shape.
		if v.OverflowInt(int64(idx)) {
			return &SemanticError{GoType: v.Type(), AvroType: "enum", Err: fmt.Errorf("ordinal %d overflows %s", idx, v.Type())}
		}
		v.SetInt(int64(idx))
	case v.CanUint():
		if v.OverflowUint(uint64(idx)) {
			return &SemanticError{GoType: v.Type(), AvroType: "enum", Err: fmt.Errorf("ordinal %d overflows %s", idx, v.Type())}
		}
		v.SetUint(uint64(idx))
	default:
		return &SemanticError{GoType: v.Type(), AvroType: "enum"}
	}
	return nil
}

func (ctx *jsonDecoder) decodeBytes(v reflect.Value, node *schemaNode, toAny bool) error {
	// Decimal / big-decimal logical types: accept JSON numbers (e.g. 0.33
	// or 1.5) in addition to Avro JSON byte strings, for round-trip with
	// EncodeJSON output and convenience for hand-edited JSON. Big-decimal
	// is bytes-only per spec (rejected in decodeFixed).
	if hasDecimalBareNumberArm(node) {
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
		val, err := decodeLogicalBytes(b, node)
		if err != nil {
			return err
		}
		return setIface(v, reflect.ValueOf(val), "bytes")
	}
	return assignBytes(v, b, node)
}

func (ctx *jsonDecoder) decodeFixed(v reflect.Value, node *schemaNode, toAny bool) error {
	// Decimal logical type: accept JSON numbers, same as decodeBytes.
	// Big-decimal is bytes-only per spec, so it never reaches here.
	if hasDecimalBareNumberArm(node) {
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
	// Per spec, the JSON string for a fixed value must have exactly
	// node.size code points (= bytes after code-point semantics).
	// Java's JsonDecoder.readFixed enforces this; the JSON encoder
	// produces exactly that length. Reject mismatches symmetrically.
	if len(b) != node.size {
		return fmt.Errorf("avro json: fixed value has %d bytes, schema declares %d", len(b), node.size)
	}
	if toAny {
		return setIface(v, reflect.ValueOf(decodeLogicalFixed(b, node)), "fixed")
	}
	return assignBytes(v, b, node)
}

// assignBytes assigns decoded bytes to a typed target, handling decimal,
// duration, and uuid logical types. Logical-arm fall-through (the arm
// fires but doesn't return) lands on the generic byte/string/array
// targets below.
func assignBytes(v reflect.Value, b []byte, node *schemaNode) error {
	switch node.logical {
	case "decimal":
		// Share setDecimalRat with the binary path so the JSON
		// decoder accepts the same target types (*big.Rat, big.Rat,
		// json.Number, *float32, *float64, *string) with the same
		// float-overflow guards.
		if ok, err := setDecimalRat(v, bytesToRat(b, node.scale), node.scale); ok {
			return err
		}
	case "big-decimal":
		// b is the inner big-decimal payload (length-prefixed unscaled
		// + zigzag scale); the outer codepoint-string decode has
		// already stripped the JSON quoting. Mirrors the binary
		// deserBigDecimal post-readLength path INCLUDING its opaque-
		// bytes pass-through: when the payload can't be parsed AND
		// the target is byte-like (slice / string / array), fall
		// through to setBytesValue below so a raw payload that the
		// encoder accepts via serBigDecimal's serBytes fall-through
		// round-trips. Only surface the parse error when the target
		// is a structured big.Rat / json.Number / float / etc., which
		// can't take raw bytes meaningfully.
		if r, displayScale, perr := parseBigDecimalPayload(b); perr == nil {
			if ok, err := setDecimalRat(v, r, displayScale); ok {
				return err
			}
		} else if v.Kind() != reflect.Slice && v.Kind() != reflect.String && v.Kind() != reflect.Array {
			return perr
		}
	case "duration":
		if v.Type() == avroDurationType {
			v.Set(reflect.ValueOf(DurationFromBytes(b)))
			return nil
		}
	case "uuid":
		// UUID into a string target: format as RFC 4122 hex-dash,
		// matching deserFixedUUIDReflect on the binary side. Plain
		// []byte / [16]byte targets fall through to the generic
		// byte-copy paths below.
		if len(b) == 16 && v.Kind() == reflect.String {
			var u [16]byte
			copy(u[:], b)
			v.SetString(uuidToString(u))
			return nil
		}
	}
	// Generic byte-target fall-through shared with the binary path.
	// setBytesValue handles slice/array/string (and would handle the
	// interface arm too; the toAny branch upstream already covered
	// interface targets for this call site).
	return setBytesValue(v, b, node.kind)
}

// hasDecimalBareNumberArm reports whether node is a logical-typed bytes/
// fixed schema that accepts the bare-number JSON form on decode. Both
// decimal and big-decimal qualify; the union-dispatch sibling
// jsonTokenMatchesBranch uses the same rule.
func hasDecimalBareNumberArm(node *schemaNode) bool {
	return node.logical == "decimal" || node.logical == "big-decimal"
}

// decodeBareDecimal handles the bare-number JSON arm for decimal-like
// logical types (decimal, big-decimal). Returns handled=true when the
// next token was a bare number (and the value was assigned or an error
// produced); handled=false when the next token is a quoted string and
// the caller should fall through to the spec-form path. Shared by
// decodeBytes (decimal + big-decimal) and decodeFixed (decimal only;
// big-decimal is bytes-only per spec) so all three sites agree on
// scale derivation and target-set dispatch.
func (ctx *jsonDecoder) decodeBareDecimal(v reflect.Value, node *schemaNode, toAny bool) (handled bool, err error) {
	c := ctx.scanner.peek()
	if c == '"' || c == 0 {
		return false, nil
	}
	nb, perr := ctx.scanner.consumeNumberBytes()
	if perr != nil {
		return true, perr
	}
	// boundedRatFromString and its callees (json.Valid, strconv.ParseInt,
	// big.Rat.SetString, fmt.Errorf) treat the string read-only and don't
	// retain it past the call, so alias nb instead of copying.
	r, ok, perr := boundedRatFromString(unsafe.String(unsafe.SliceData(nb), len(nb)))
	if perr != nil {
		return true, fmt.Errorf("avro json: %s %q: %w", node.logical, nb, perr)
	}
	if !ok {
		return true, fmt.Errorf("avro json: invalid %s number %q", node.logical, nb)
	}
	if toAny {
		return true, setIface(v, reflect.ValueOf(r), node.kind)
	}
	// Decimal uses the schema-declared node.scale; big-decimal has no
	// schema-level scale (it's encoded inline on the wire), so derive
	// the natural scale from the rat. Scale is consulted only by
	// setDecimalRat's json.Number / string target arms; for big.Rat,
	// float, and interface targets the value is unchanged.
	scale := node.scale
	if node.logical == "big-decimal" {
		s, ok := finiteScale(r)
		if !ok {
			return true, fmt.Errorf("avro json: big-decimal value %s has no finite decimal expansion", r.RatString())
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
	// Typed slice target.
	if v.Kind() != reflect.Slice {
		return &SemanticError{GoType: v.Type(), AvroType: "array"}
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
		return &SemanticError{GoType: v.Type(), AvroType: "map"}
	}
	if v.IsNil() {
		v.Set(reflect.MakeMap(v.Type()))
	}
	valType := v.Type().Elem()
	if ctx.scanner.peek() != '}' {
		elem := reflect.New(valType).Elem()
		// Reusable key Value typed to match the user's map key type
		// (handles `type UserID string; map[UserID]V` without panic).
		keyVal := reflect.New(v.Type().Key()).Elem()
		for {
			key, err := ctx.consumeSlabString()
			if err != nil {
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
	return &SemanticError{GoType: v.Type(), AvroType: "record"}
}

// iterateRecordFields drives the JSON object field loop for records:
// dispatch each key to handle, skip unknown keys, and after the loop
// invoke fillDefault for any absent field that has a schema default
// (errors otherwise). fillDefault may be nil.
func (ctx *jsonDecoder) iterateRecordFields(node *schemaNode, handle func(idx int, key string) error, fillDefault func(idx int) error) error {
	seen := make([]bool, len(node.fields))
	if ctx.scanner.peek() != '}' {
		for {
			// Zero-copy: key is used only for fieldIdx lookup and
			// error messages, never stored in output.
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
				seen[idx] = true
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
			return fmt.Errorf("avro json: record %q missing required field %q", node.name, f.name)
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
	// Fail fast on a target that can't hold the result, mirroring
	// deserRecord.deser. The caller (decodeRecord) has already
	// consumed the leading '{', so this advances at most one byte
	// before erroring — but it avoids the much larger waste of
	// iterating each field, allocating a map, and decoding values
	// only to throw them away on assignment.
	if v.Type().NumMethod() != 0 && !mapStringAnyType.AssignableTo(v.Type()) {
		return &SemanticError{GoType: v.Type(), AvroType: "record"}
	}
	// Reuse the existing map[string]any if v already wraps one — the
	// streaming pattern (DecodeJSON repeatedly into the same *any).
	// Mirrors the equivalent reuse in deserRecord for binary decode,
	// including the same stale-key semantics: keys not present in the
	// schema are retained (matches encoding/json into a non-empty map).
	var m map[string]any
	if inner := v.Elem(); inner.IsValid() && inner.Type() == mapStringAnyType {
		m = inner.Interface().(map[string]any)
	} else {
		m = make(map[string]any, len(node.fields))
	}
	var val any
	valV := reflect.ValueOf(&val).Elem()
	err := ctx.iterateRecordFields(node,
		func(idx int, key string) error {
			f := &node.fields[idx]
			if err := ctx.decodeValue(valV, f.node); err != nil {
				return fmt.Errorf("field %q: %w", key, err)
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
				return fmt.Errorf("field %q default: %w", f.name, err)
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
	return ctx.iterateRecordFields(node,
		func(idx int, key string) error {
			f := &node.fields[idx]
			if err := ctx.decodeValue(elem, f.node); err != nil {
				return fmt.Errorf("field %q: %w", key, err)
			}
			v.SetMapIndex(mapKeyAs(mapType, f.nameVal), elem)
			elem.SetZero()
			return nil
		},
		func(idx int) error {
			f := &node.fields[idx]
			if err := ctx.applyFieldDefault(elem, node, idx); err != nil {
				return fmt.Errorf("field %q default: %w", f.name, err)
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
	mapping, err := typeFieldMapping(dr.names, &dr.cache, v.Type())
	if err != nil {
		return err
	}
	return ctx.iterateRecordFields(node,
		func(idx int, key string) error {
			f := &node.fields[idx]
			fv := fieldByIndex(v, mapping.indices[idx])
			if err := ctx.decodeValue(fv, f.node); err != nil {
				return fmt.Errorf("field %q: %w", key, err)
			}
			return nil
		},
		func(idx int) error {
			f := &node.fields[idx]
			if len(mapping.indices[idx]) == 0 {
				// Struct has no field for this Avro field — nothing to fill.
				// Mirrors decodeRecord's tolerance of struct-field omission.
				return nil
			}
			fv := fieldByIndex(v, mapping.indices[idx])
			if err := ctx.applyFieldDefault(fv, node, idx); err != nil {
				return fmt.Errorf("field %q default: %w", f.name, err)
			}
			return nil
		},
	)
}

// applyFieldDefault decodes the field's pre-encoded binary default
// into target via the field's binary deserfn.
//
// A zero-length defaultBytes is a *valid* default for any field whose
// wire encoding is naturally 0 bytes — null-typed fields, empty-record
// fields, and records whose every field is null-typed. The caller
// (iterateRecordFields) gates on f.hasDefault before invoking us, so
// presence of a default is already authoritative; the structural check
// below only guards a malformed schema where serRecord is missing.
func (ctx *jsonDecoder) applyFieldDefault(target reflect.Value, node *schemaNode, idx int) error {
	if node.serRecord == nil || idx >= len(node.serRecord.fields) {
		return fmt.Errorf("record has no pre-encoded default for field %d", idx)
	}
	enc := node.serRecord.fields[idx].defaultBytes
	// Copy the encoded bytes — deserfns may slab-substring into src
	// and we don't want them to reach into the schema's shared default.
	src := append([]byte(nil), enc...)
	_, err := node.fields[idx].node.deser(src, target, ctx.slab)
	return err
}

func (ctx *jsonDecoder) decodeUnion(v reflect.Value, node *schemaNode) error {
	p := ctx.scanner.peek()

	// JSON null → null branch (only if the union has one). Handle
	// before indirectAlloc so *T pointer targets stay nil. Java's
	// JsonDecoder.readIndex and fastavro's read_index both reject
	// null when no "null" label is in the union; we match.
	//
	// Use isJSONNullStart to disambiguate from bare special-float
	// tokens. Currently parseSpecialFloat rejects lowercase 'n'-start
	// (parity tightening with Java/fastavro/goavro), so a bare 'n'
	// here is unambiguous as "null". The helper is still used
	// defensively: if future leniency re-accepts lowercase nan, this
	// dispatcher must NOT hijack it into the null arm. Sibling
	// dispatchers decodeFloat (json_decode.go) and decodeDouble use
	// the helper for the same reason.
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
		switch v.Kind() {
		case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Interface:
			v.Set(reflect.Zero(v.Type()))
		}
		return nil
	}

	v = indirectAlloc(v)
	toAny := v.Kind() == reflect.Interface

	// JSON object → could be tagged union {"type": value} or bare record/map.
	if p == '{' {
		return ctx.decodeUnionObject(v, node, toAny)
	}

	// Bare non-object value — match by JSON token type.
	return ctx.decodeUnionBare(v, node, toAny, p)
}

func (ctx *jsonDecoder) decodeUnionObject(v reflect.Value, node *schemaNode, toAny bool) error {
	// Save position for backtracking.
	savedPos := ctx.scanner.pos
	// Preserve the deepest concrete decode error from a matched branch
	// so a failed tagged interpretation surfaces the real reason (e.g.
	// "cannot assign float to map[string]any") rather than being masked
	// by the bare-fallback's generic "no union branch matched".
	var taggedErr error

	// Try tagged union: {"branchName": value}.
	ctx.scanner.pos++ // consume '{'
	ctx.scanner.skipWhitespace()
	if ctx.scanner.peek() != '}' {
		// Zero-copy: key is used only for branch name lookup, never stored.
		key, err := ctx.scanner.consumeStringZeroCopy()
		if err == nil {
			if branch := findUnionBranch(node, key); branch != nil {
				if err := ctx.scanner.expect(':'); err == nil {
					if toAny {
						var val any
						err := ctx.decodeValue(reflect.ValueOf(&val).Elem(), branch)
						if err == nil {
							if ctx.scanner.peek() == '}' {
								ctx.scanner.pos++
								// wrapUnion returns nil for null branches;
								// reflect.ValueOf(nil) is the invalid zero
								// Value, so use assignAny which sets a typed
								// nil for interface targets.
								return assignAny(v, ctx.wrapUnion(val, branch), branch.kind)
							}
						} else if errors.Is(err, errTooDeep) {
							// Don't fall through to bare-union retry; the
							// recursion limit applies regardless of how the
							// branch is matched, so masking it as "no
							// branch matched" would be wrong.
							return err
						} else {
							taggedErr = err
						}
					} else {
						err := ctx.decodeValue(v, branch)
						if err == nil {
							if ctx.scanner.peek() == '}' {
								ctx.scanner.pos++
								return nil
							}
						} else if errors.Is(err, errTooDeep) {
							return err
						} else {
							taggedErr = err
						}
					}
				}
			}
		}
	}

	// Tagged interpretation failed — backtrack and try bare. Pass the
	// tagged-side concrete error so it can be surfaced if bare also
	// fails to match.
	ctx.scanner.pos = savedPos
	if err := ctx.decodeUnionBare(v, node, toAny, '{'); err != nil {
		if taggedErr != nil {
			return fmt.Errorf("%w (tagged-form: %v)", err, taggedErr)
		}
		return err
	}
	return nil
}

func (ctx *jsonDecoder) decodeUnionBare(v reflect.Value, node *schemaNode, toAny bool, p byte) error {
	// Match by JSON token type against branch kinds. The last branch's
	// decode error (if any) is preserved so the final message names the
	// concrete reason — typically a target-type mismatch like the binary
	// path reports ("cannot use map[string]any with Avro type float").
	// Without this, callers saw a generic "no union branch matched at
	// offset N" that hid the actual root cause.
	var lastErr error
	for _, branch := range node.branches {
		if branch.kind == "null" {
			continue
		}
		if !jsonTokenMatchesBranch(p, branch) {
			continue
		}
		savedPos := ctx.scanner.pos
		var err error
		if toAny {
			var val any
			err = ctx.decodeValue(reflect.ValueOf(&val).Elem(), branch)
			if err == nil {
				return assignAny(v, ctx.wrapUnion(val, branch), branch.kind)
			}
		} else {
			err = ctx.decodeValue(v, branch)
			if err == nil {
				return nil
			}
		}
		if errors.Is(err, errTooDeep) {
			return err
		}
		lastErr = err
		ctx.scanner.pos = savedPos
	}
	if lastErr != nil {
		return fmt.Errorf("avro json: no union branch matched at offset %d: %w", ctx.scanner.pos, lastErr)
	}
	return fmt.Errorf("avro json: no union branch matched at offset %d", ctx.scanner.pos)
}

func (ctx *jsonDecoder) wrapUnion(val any, branch *schemaNode) any {
	if !ctx.wrapUnions || val == nil {
		return val
	}
	name := unionBranchName(branch)
	if ctx.qualifyLogical && branch.logical != "" {
		name = branch.kind + "." + branch.logical
	}
	return map[string]any{name: val}
}

// jsonTokenMatchesBranch returns true if a JSON token type could
// potentially match a given schema branch kind.
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
			// branch as a bare number. decodeBytes / decodeFixed
			// accept both forms via decodeBareDecimal; dispatch must
			// offer the branch so the number-form reaches them.
			// Big-decimal is bytes-only per spec, hence not eligible
			// on a fixed branch.
			return hasDecimalBareNumberArm(branch)
		}
	}
	return false
}
