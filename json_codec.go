package avro

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
	"unsafe"
)

// Opt configures encoding and decoding behavior. See each option's
// documentation for which functions it affects. Inapplicable options
// are silently ignored.
type Opt interface{ opt() }

type taggedUnions struct{}

func (taggedUnions) opt() {}

// TaggedUnions wraps non-null union values as {"type_name": value}.
//
// In [Schema.EncodeJSON], this produces tagged JSON union output.
// In [Schema.Decode] and [Schema.DecodeJSON], this wraps union
// values as map[string]any{branchName: value} — but only when the
// decode target is *any (the common case). For typed targets — a
// concrete struct field, *T, or any non-empty Go interface — the
// wrapper would not be assignable to the target, so the bare branch
// value is assigned without the envelope.
//
// [Schema.DecodeJSON] and [Schema.Encode] always accept both tagged
// and bare union input regardless of this option.
//
// Spec note: the Avro 1.12 JSON-encoding section defines non-null
// union values as {"type_name": value}. The library's default
// (without TaggedUnions) emits bare values, which Java's stock
// JsonDecoder and fastavro's JSON decoder both REJECT — they throw
// "Expected start-union" / equivalent on the first non-null union
// field. Pass TaggedUnions when interop with Java, fastavro, or
// avro-tools fromjson is required. The bare default is for
// round-trips through goavro and for the natural Go map[string]any
// shape; see Apache Avro Jira issue AVRO-2899 for the long-standing
// upstream discussion.
func TaggedUnions() Opt { return taggedUnions{} }

type tagLogicalTypes struct{}

func (tagLogicalTypes) opt() {}

// TagLogicalTypes qualifies union branch names with their logical type
// (e.g. "long.timestamp-millis" instead of "long"). This applies to
// [Schema.EncodeJSON] with [TaggedUnions] and to [Schema.Decode] with
// [TaggedUnions]. Without this option, branch names use the base Avro
// type per the specification. This option has no effect without
// [TaggedUnions].
func TagLogicalTypes() Opt { return tagLogicalTypes{} }

type linkedinFloats struct{}

func (linkedinFloats) opt() {}

// LinkedinFloats encodes NaN as JSON null and ±Infinity as ±1e999
// in [Schema.EncodeJSON], matching the linkedin/goavro convention.
// Without this option, NaN is encoded as the JSON string "NaN" and
// ±Infinity as "Infinity"/"-Infinity", following the Java Avro
// convention.
//
// [Schema.DecodeJSON] accepts both conventions for a float/double decoded
// directly or as a tagged union branch ({"float":null} → NaN). The one
// exception is a NaN that is a member of a bare (untagged) union: NaN
// encodes as a bare null, and on decode a bare null is claimed by the
// union's null branch — or rejected when the union has none — before the
// float branch is considered, so the NaN does not round-trip. This is an
// inherent ambiguity of the null-for-NaN convention when null is also a
// structural union value; use [TaggedUnions] for a round-trip-safe NaN
// union member. ±Infinity (±1e999) is a number token and round-trips in a
// bare union regardless.
func LinkedinFloats() Opt { return linkedinFloats{} }

type optConfig struct {
	tagged     bool
	tagLogical bool
	linkedin   bool
}

func parseOpts(opts []Opt) optConfig {
	var cfg optConfig
	for _, o := range opts {
		switch o.(type) {
		case taggedUnions:
			cfg.tagged = true
		case tagLogicalTypes:
			cfg.tagLogical = true
		case linkedinFloats:
			cfg.linkedin = true
		}
	}
	return cfg
}

// EncodeJSON encodes v as JSON using the schema for type-aware encoding.
// By default, union values are written as bare JSON values and bytes/fixed
// fields use \uXXXX escapes for non-ASCII bytes. Options can modify the
// output format; see [Opt] for details.
//
// NaN and Infinity float values are encoded as JSON strings "NaN",
// "Infinity", and "-Infinity" by default (Java Avro convention), or as
// null/±1e999 with [LinkedinFloats]. A generic JSON encoder rejects
// non-finite floats outright; EncodeJSON encodes them via the strings
// above so the result is valid JSON and round-trippable through any
// strict parser.
//
// EncodeJSON accepts the same Go types as [Schema.Encode]. Map key order
// in the output is non-deterministic — Go's map iteration order is
// randomized and the encoder does not sort keys.
//
// Interop note: the default bare-union output is NOT readable by Java's
// org.apache.avro.io.JsonDecoder, fastavro's JSON decoder, or
// avro-tools fromjson — they all require the spec-compliant
// {"type_name": value} envelope and reject bare values with
// "Expected start-union" / equivalent. Pass [TaggedUnions] to produce
// the wrapped form when interop with those tools is required. See
// the [TaggedUnions] doc and Apache Avro Jira issue AVRO-2899 for the
// long-standing upstream discussion of this divergence.
func (s *Schema) EncodeJSON(v any, opts ...Opt) ([]byte, error) {
	return s.AppendEncodeJSON(nil, v, opts...)
}

// AppendEncodeJSON is like [Schema.EncodeJSON] but appends to dst.
func (s *Schema) AppendEncodeJSON(dst []byte, v any, opts ...Opt) ([]byte, error) {
	cfg := parseOpts(opts)
	return appendAvroJSON(dst, reflect.ValueOf(v), s.node, &cfg, s.custom, 0)
}

// DecodeJSON decodes Avro JSON from src into v. It unwraps union wrappers,
// converts bytes/fixed strings, and coerces numeric types to match the
// schema. When v is *any, the result is returned directly. For typed
// targets (structs, etc.), the value is round-tripped through binary
// encode/decode.
//
// DecodeJSON also accepts the non-standard union branch naming used by
// linkedin/goavro (e.g. "long.timestamp-millis" instead of "long").
//
// DecodeJSON accepts all input formats (tagged and bare unions, Java and
// goavro NaN/Infinity conventions). Pass [TaggedUnions] to wrap decoded
// union values when the target is *any.
func (s *Schema) DecodeJSON(src []byte, v any, opts ...Opt) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return errors.New("avro: DecodeJSON requires a non-nil pointer")
	}
	cfg := parseOpts(opts)
	sl := slabPool.Get().(*slab)
	ctx := &jsonDecoder{
		scanner:        &jsonScanner{data: src},
		slab:           sl,
		wrapUnions:     cfg.tagged,
		qualifyLogical: cfg.tagLogical,
	}
	err := ctx.decodeValue(rv.Elem(), s.node)
	if err == nil {
		// One DecodeJSON call decodes exactly one value and returns no
		// offset, so any trailing non-whitespace content is rejected
		// (matching encoding/json.Unmarshal and fastavro).
		ctx.scanner.skipWhitespace()
		if ctx.scanner.pos < len(ctx.scanner.data) {
			err = fmt.Errorf("avro json: unexpected trailing content at offset %d", ctx.scanner.pos)
		}
	}
	sl.put()
	return err
}

// appendAvroJSON is the single-pass Avro JSON encoder. It walks
// the Go value via reflect and the schema tree simultaneously, writing
// JSON directly without an intermediate binary encoding step. Handles
// structs, maps, all numeric coercions, time.Time, etc.
func appendAvroJSON(buf []byte, v reflect.Value, node *schemaNode, cfg *optConfig, custom map[*schemaNode]*customWiring, depth int) ([]byte, error) {
	if depth >= maxDepth {
		return nil, errTooDeep
	}
	// Handle nil / invalid values.
	if !v.IsValid() {
		if node.kind == "null" {
			return append(buf, "null"...), nil
		}
		if node.kind == "union" {
			for _, br := range node.branches {
				if br.kind == "null" {
					return append(buf, "null"...), nil
				}
			}
			// Union without a null branch can't represent nil; reject
			// rather than emit "null", matching the binary path
			// (serUnion.ser → tryAll → "no matching branch") and
			// Java's UnresolvedUnionException / fastavro's
			// "do not match" rejection. The library's own DecodeJSON
			// also rejects null against a no-null union (see
			// TestRegression_UnionWithoutNullBranchAcceptsJsonNull).
			return nil, fmt.Errorf("avro json: nil value for union without a null branch")
		}
		return nil, fmt.Errorf("avro json: nil value for non-nullable type %q", node.kind)
	}
	// Dereference pointers and interfaces. Capped at maxIndirectDepth
	// so a self-referential interface (var p any; p = &p) terminates;
	// the deeply-wrapped value falls through to the type switch which
	// will return a SemanticError for the unmatched kind.
	for range maxIndirectDepth {
		if v.Kind() != reflect.Pointer && v.Kind() != reflect.Interface {
			break
		}
		if v.IsNil() {
			return appendAvroJSON(buf, reflect.Value{}, node, cfg, custom, depth+1)
		}
		v = v.Elem()
	}

	// Apply custom type encode conversion before the type switch.
	if w := custom[node]; w != nil && w.encode != nil {
		var err error
		v, err = w.encode(v)
		if err != nil {
			return nil, err
		}
	}

	switch node.kind {
	case "null":
		// Validate v is nil-equivalent, mirroring serNull on the binary
		// side. A non-nil value into a "null" schema is rejected with
		// errNonNil — both at top level (Schema.EncodeJSON(42, "null")),
		// as a null-typed record field, and via tagged-union dispatch
		// ({"null": 42} against ["null", T]). Binary serNull (ser.go)
		// returns errNonNil for the same shapes; matching that here
		// keeps EncodeJSON ↔ AppendEncode parity and prevents silent
		// input loss.
		switch v.Kind() {
		case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
			if !v.IsNil() {
				return nil, errNonNil
			}
		default:
			return nil, errNonNil
		}
		return append(buf, "null"...), nil

	case "boolean":
		if v.Kind() == reflect.Bool {
			return strconv.AppendBool(buf, v.Bool()), nil
		}
		return nil, fmt.Errorf("avro json: expected bool, got %s", v.Type())

	case "int":
		if v.Type() == timeType {
			t := v.Interface().(time.Time)
			switch node.logical {
			case "date":
				d, err := timeToDate(t)
				if err != nil {
					return nil, err
				}
				return strconv.AppendInt(buf, int64(d), 10), nil
			case "time-millis":
				// Time-of-day ms (< 86.4M) never overflows int32.
				return strconv.AppendInt(buf, timeOfDay(t).Milliseconds(), 10), nil
			}
		}
		if v.Type() == durationType {
			d := v.Interface().(time.Duration)
			switch node.logical {
			case "time-millis":
				ms, err := durationToTimeMillis(d)
				if err != nil {
					return nil, err
				}
				return strconv.AppendInt(buf, int64(ms), 10), nil
			}
		}
		if node.logical == "date" {
			if t, ok := tryParseDateString(v); ok {
				d, err := timeToDate(t)
				if err != nil {
					return nil, err
				}
				return strconv.AppendInt(buf, int64(d), 10), nil
			}
		}
		n, err := jsonCoerceToInt32(v)
		if err != nil {
			return nil, err
		}
		return strconv.AppendInt(buf, int64(n), 10), nil

	case "long":
		if conv := timeLogicalToInt64(node.logical); conv != nil {
			if t, ok := extractTime(v); ok {
				n, err := conv(t)
				if err != nil {
					return nil, err
				}
				return strconv.AppendInt(buf, n, 10), nil
			}
		}
		if node.logical == "time-micros" {
			if v.Type() == timeType {
				return strconv.AppendInt(buf, timeOfDay(v.Interface().(time.Time)).Microseconds(), 10), nil
			}
			if v.Type() == durationType {
				return strconv.AppendInt(buf, v.Interface().(time.Duration).Microseconds(), 10), nil
			}
		}
		n, err := jsonCoerceToInt64(v)
		if err != nil {
			return nil, err
		}
		return strconv.AppendInt(buf, n, 10), nil

	case "float":
		f, err := jsonCoerceToFloat64(v)
		if err != nil {
			return nil, err
		}
		return appendJSONFloat(buf, f, 32, cfg), nil

	case "double":
		f, err := jsonCoerceToFloat64(v)
		if err != nil {
			return nil, err
		}
		return appendJSONFloat(buf, f, 64, cfg), nil

	case "string":
		// UUID logical type: [16]byte input canonicalizes to the RFC 4122
		// hex-dash string, matching serUUID on the binary side.
		if node.logical == "uuid" {
			if u, ok := uuidBytes(v); ok {
				return appendJSONString(buf, uuidToString(u)), nil
			}
		}
		// Resolution order is shared with the binary encoder via
		// avroStringValue: json.Number rejected, reflect.String,
		// TextAppender, TextMarshaler, []byte slice. Both encoders
		// must stay in lockstep on precedence.
		s, err := avroStringValue(v)
		if err != nil {
			return nil, err
		}
		return appendJSONString(buf, s), nil

	case "bytes":
		// Decimal logical type: emit the spec form — the underlying bytes
		// (two's-complement big-endian unscaled integer) as an Avro JSON
		// byte string with code points 0-255 mapped to byte values 0-255.
		// Per Avro 1.12 spec ("Logical Types": "always serialized using
		// its underlying Avro type") + the bytes/fixed JSON rule. Matches
		// Java's JsonEncoder and fastavro's AvroJSONEncoder.write_bytes.
		// The decoder accepts both this form and bare numbers, so users
		// who hand-edit JSON can still feed 0.33 into DecodeJSON.
		//
		// Logical-arm fall-through (no decimalRatFor match) lands on
		// the generic string/slice/array targets below. big-decimal
		// (AVRO-4124) wraps the binary inner payload (length-prefixed
		// unscaled + zigzag scale, via buildBigDecimalPayload) in the
		// spec codepoint-string form; binary and JSON share the
		// helper to stay in lockstep.
		switch node.logical {
		case "decimal":
			r, ok, err := decimalRatFor(v)
			if err != nil {
				return nil, semErrW(v, "bytes", err)
			}
			if ok {
				b, err := decimalUnscaledBytes(r, node.scale, node.precision, "bytes", v.Type())
				if err != nil {
					return nil, err
				}
				return appendAvroJSONBytes(buf, b), nil
			}
		case "big-decimal":
			r, ok, err := decimalRatFor(v)
			if err != nil {
				return nil, semErrW(v, "bytes", err)
			}
			if ok {
				inner, err := buildBigDecimalPayload(r)
				if err != nil {
					return nil, semErrW(v, "bytes", err)
				}
				return appendAvroJSONBytes(buf, inner), nil
			}
		}
		if v.Kind() == reflect.String {
			if err := rejectJSONNumberRawTarget(v, "bytes"); err != nil {
				return nil, err
			}
			// Treat the Go string as raw UTF-8 bytes, matching serBytes
			// (ser.go's string arm appends the string bytes verbatim).
			// Binary↔JSON parity: "é" encodes as c3 a9 on both paths.
			// Defaults don't reach this arm — convertDefaultBytes
			// (schema.go) already turns JSON-parsed default strings into
			// []byte, so only runtime user input lands here, where the
			// Go convention is UTF-8. appendAvroJSONBytes then handles
			// the codepoint↔byte mapping on the wire form.
			// appendAvroJSONBytes iterates byte-by-byte without retaining;
			// alias v's string data instead of allocating a copy.
			s := v.String()
			return appendAvroJSONBytes(buf, unsafe.Slice(unsafe.StringData(s), len(s))), nil
		}
		if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
			return appendAvroJSONBytes(buf, v.Bytes()), nil
		}
		if v.Kind() == reflect.Array && v.Type().Elem().Kind() == reflect.Uint8 {
			// reflect.Value.Bytes() panics on Array kinds, so materialize
			// the bytes via Copy. Mirrors the "fixed" arm below and
			// serBytes (ser.go:460) which accepts Array alongside Slice.
			raw := make([]byte, v.Len())
			reflect.Copy(reflect.ValueOf(raw), v)
			return appendAvroJSONBytes(buf, raw), nil
		}
		return nil, fmt.Errorf("avro json: expected []byte or string, got %s", v.Type())

	case "fixed":
		// Decimal: spec form padded / sign-extended to the fixed
		// schema size (mirrors serFixedDecimal.serRat). UUID: hex-
		// dash string input parses to 16 bytes (matches
		// serFixedUUIDReflect), checked before the generic raw
		// extraction so a 36-char string isn't rejected as size != 16.
		// Logical-arm fall-through lands on the generic string/slice/
		// array targets below.
		switch node.logical {
		case "decimal":
			r, ok, err := decimalRatFor(v)
			if err != nil {
				return nil, semErrW(v, "fixed", err)
			}
			if ok {
				b, err := decimalUnscaledBytes(r, node.scale, node.precision, "fixed", v.Type())
				if err != nil {
					return nil, err
				}
				padded, err := appendDecimalFixed(nil, b, node.size, v.Type())
				if err != nil {
					return nil, err
				}
				return appendAvroJSONBytes(buf, padded), nil
			}
		case "duration":
			if v.Type() == avroDurationType {
				raw := v.Interface().(Duration).Bytes()
				return appendAvroJSONBytes(buf, raw[:]), nil
			}
		case "uuid":
			// [16]byte trusts its bytes (uuidBytes-first, matching binary
			// serFixedUUIDReflect): the raw 16 bytes are the wire form, with
			// no MarshalText→parseUUID round trip. Without this, a [16]byte
			// type that also implements TextMarshaler (e.g. google/uuid.UUID)
			// diverged from the binary path.
			if u, ok := uuidBytes(v); ok {
				return appendAvroJSONBytes(buf, u[:]), nil
			}
			// TextMarshaler / AppendText before the reflect.String arm
			// (parity with serFixedUUIDReflect). MarshalText must produce a
			// UUID hex-dash string, which parseUUID validates into 16 bytes.
			if text, ok, err := textValue(v, "fixed"); err != nil {
				return nil, err
			} else if ok {
				u, err := parseUUID(text)
				if err != nil {
					return nil, err
				}
				return appendAvroJSONBytes(buf, u[:]), nil
			}
			if v.Kind() == reflect.String {
				if err := rejectJSONNumberRawTarget(v, "fixed"); err != nil {
					return nil, err
				}
				u, err := parseUUID(v.String())
				if err != nil {
					return nil, err
				}
				return appendAvroJSONBytes(buf, u[:]), nil
			}
		}
		var raw []byte
		if v.Kind() == reflect.String {
			if err := rejectJSONNumberRawTarget(v, "fixed"); err != nil {
				return nil, err
			}
			// Go string → raw UTF-8 bytes, matching serSize on the
			// binary side. See the bytes-string arm above for the full
			// rationale on why codepoint mapping was wrong here.
			// Alias v's bytes; downstream consumers iterate read-only.
			s := v.String()
			raw = unsafe.Slice(unsafe.StringData(s), len(s))
		} else if v.Kind() == reflect.Array && v.Type().Elem().Kind() == reflect.Uint8 {
			raw = make([]byte, v.Len())
			reflect.Copy(reflect.ValueOf(raw), v)
		} else if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
			raw = v.Bytes()
		} else {
			return nil, fmt.Errorf("avro json: expected []byte, [N]byte, or string, got %s", v.Type())
		}
		if len(raw) != node.size {
			return nil, fmt.Errorf("avro json: fixed size mismatch: got %d bytes, need %d", len(raw), node.size)
		}
		return appendAvroJSONBytes(buf, raw), nil

	case "enum":
		// Builtin string fast path (parity with serEnum): unnamed string is
		// text-less, so it IS the symbol — skip the textValue probe. Named
		// string types fall through to textValue, so uniformity holds.
		if v.Type() == stringType {
			needle := v.String()
			if slices.Contains(node.symbols, needle) {
				return appendJSONString(buf, needle), nil
			}
			return nil, fmt.Errorf("avro json: unknown enum symbol %q", truncForError(needle))
		}
		// Text-out first (uniformity / name-based matching), then named string
		// without a text method, then the int-ordinal arm.
		if text, ok, err := textValue(v, "enum"); err != nil {
			return nil, err
		} else if ok {
			if slices.Contains(node.symbols, text) {
				return appendJSONString(buf, text), nil
			}
			return nil, fmt.Errorf("avro json: unknown enum symbol %q", truncForError(text))
		}
		if v.Kind() == reflect.String {
			needle := v.String()
			if slices.Contains(node.symbols, needle) {
				return appendJSONString(buf, needle), nil
			}
			return nil, fmt.Errorf("avro json: unknown enum symbol %q", truncForError(needle))
		}
		if v.CanInt() || v.CanUint() {
			n, err := enumOrdinalIndex(v, len(node.symbols))
			if err != nil {
				return nil, fmt.Errorf("avro json: enum %w", err)
			}
			return appendJSONString(buf, node.symbols[n]), nil
		}
		return nil, fmt.Errorf("avro json: expected string or integer for enum, got %s", v.Type())

	case "array":
		if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
			return nil, fmt.Errorf("avro json: expected slice/array, got %s", v.Type())
		}
		// Native concrete fast path: plain primitive item + unnamed []V slice.
		// Logical items, [N]T arrays, named slice/elem types fall through.
		if node.items.logical == "" && custom[node.items] == nil && v.Kind() == reflect.Slice && v.CanInterface() {
			if out, ok := appendAvroJSONNativeArray(buf, v, node.items.kind, cfg); ok {
				return out, nil
			}
		}
		buf = append(buf, '[')
		for i := range v.Len() {
			if i > 0 {
				buf = append(buf, ',')
			}
			var err error
			buf, err = appendAvroJSON(buf, v.Index(i), node.items, cfg, custom, depth+1)
			if err != nil {
				return nil, err
			}
		}
		return append(buf, ']'), nil

	case "map":
		if v.Kind() != reflect.Map || v.Type().Key().Kind() != reflect.String {
			// Avro spec: "Map keys are assumed to be strings."
			// Without this guard, iter.Key().String() returns
			// reflect's <int Value>-style placeholder for non-string
			// keys, producing invalid Avro JSON. Mirrors
			// serMapPreamble's check on the binary side.
			return nil, semErr(v, "map")
		}
		// Native concrete fast path: a plain (non-logical) primitive value and
		// an exactly-string key mean the whole map is a known unnamed type, so
		// assert it and emit natively (no reflect.MapRange). Logical-typed
		// values (date/time/uuid serialize specially), named map/value types,
		// and non-interfaceable maps fall through to the reflect path.
		if node.values.logical == "" && custom[node.values] == nil && v.Type().Key() == stringType && v.CanInterface() {
			if out, ok := appendAvroJSONNativeMap(buf, v, node.values.kind, cfg); ok {
				return out, nil
			}
		}
		buf = append(buf, '{')
		first := true
		keyType := v.Type().Key()
		// Reused addressable Values avoid the per-entry alloc of
		// iter.Key()/iter.Value() — see appendMapPrimitive (ser.go).
		keyV := reflect.New(keyType).Elem()
		valV := reflect.New(v.Type().Elem()).Elem()
		iter := v.MapRange()
		for iter.Next() {
			keyV.SetIterKey(iter)
			key := keyV.String()
			if err := validateJSONNumberMapKey(key, keyType, "map"); err != nil {
				return nil, err
			}
			if !first {
				buf = append(buf, ',')
			}
			first = false
			buf = appendJSONString(buf, key)
			buf = append(buf, ':')
			valV.SetIterValue(iter)
			var err error
			buf, err = appendAvroJSON(buf, valV, node.values, cfg, custom, depth+1)
			if err != nil {
				return nil, err
			}
		}
		return append(buf, '}'), nil

	case "record":
		return appendAvroJSONRecord(buf, v, node, cfg, custom, depth+1)

	case "union":
		return appendAvroJSONUnion(buf, v, node, cfg, custom, depth+1)

	default:
		return nil, fmt.Errorf("avro json: unsupported schema kind %q", node.kind)
	}
}

// appendJSONNativeStringMap ranges a concrete map[string]V natively (no
// reflect.MapRange / per-entry Value), emitting each value via emit. The
// native range dominates the win, so the emit func-param's indirection is
// negligible on this (cooler than binary) path — and it keeps one loop shape
// for all value types.
func appendJSONNativeStringMap[V any](buf []byte, m map[string]V, emit func([]byte, V) []byte) []byte {
	buf = append(buf, '{')
	first := true
	for k, val := range m {
		if !first {
			buf = append(buf, ',')
		}
		first = false
		buf = appendJSONString(buf, k)
		buf = append(buf, ':')
		buf = emit(buf, val)
	}
	return append(buf, '}')
}

// appendAvroJSONNativeMap emits a plain-primitive-valued map[string]V the same
// way appendAvroJSON would, but natively. ok is false (buf untouched) when v's
// dynamic type isn't the unnamed map[string]V for kind — the caller falls back
// to the reflect path. Only reached when node.values has no logical type
// (logical values serialize specially) and no custom codec (custom values
// route through the per-element path), and the key is exactly string.
func appendAvroJSONNativeMap(buf []byte, v reflect.Value, kind string, cfg *optConfig) ([]byte, bool) {
	switch et := v.Type().Elem(); {
	case kind == "string" && et == stringType:
		if m, ok := v.Interface().(map[string]string); ok {
			return appendJSONNativeStringMap(buf, m, appendJSONString), true
		}
	case kind == "int" && et == int32Type:
		if m, ok := v.Interface().(map[string]int32); ok {
			return appendJSONNativeStringMap(buf, m, func(b []byte, x int32) []byte { return strconv.AppendInt(b, int64(x), 10) }), true
		}
	case kind == "long" && et == int64Type:
		if m, ok := v.Interface().(map[string]int64); ok {
			return appendJSONNativeStringMap(buf, m, func(b []byte, x int64) []byte { return strconv.AppendInt(b, x, 10) }), true
		}
	case kind == "long" && et == intType:
		if m, ok := v.Interface().(map[string]int); ok {
			return appendJSONNativeStringMap(buf, m, func(b []byte, x int) []byte { return strconv.AppendInt(b, int64(x), 10) }), true
		}
	case kind == "float" && et == float32Type:
		if m, ok := v.Interface().(map[string]float32); ok {
			return appendJSONNativeStringMap(buf, m, func(b []byte, x float32) []byte { return appendJSONFloat(b, float64(x), 32, cfg) }), true
		}
	case kind == "double" && et == float64Type:
		if m, ok := v.Interface().(map[string]float64); ok {
			return appendJSONNativeStringMap(buf, m, func(b []byte, x float64) []byte { return appendJSONFloat(b, x, 64, cfg) }), true
		}
	case kind == "boolean" && et == boolType:
		if m, ok := v.Interface().(map[string]bool); ok {
			return appendJSONNativeStringMap(buf, m, strconv.AppendBool), true
		}
	}
	return buf, false
}

// appendJSONNativeSlice ranges a concrete []V natively (no per-element
// reflect.Value / appendAvroJSON dispatch), emitting each value via emit.
func appendJSONNativeSlice[V any](buf []byte, s []V, emit func([]byte, V) []byte) []byte {
	buf = append(buf, '[')
	for i, val := range s {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = emit(buf, val)
	}
	return append(buf, ']')
}

// appendAvroJSONNativeArray is appendAvroJSONNativeMap's slice sibling. ok is
// false (buf untouched) when v's dynamic type isn't the unnamed []V for kind.
// Only reached when node.items has no logical type and no custom codec, and
// v is a slice (not [N]T).
func appendAvroJSONNativeArray(buf []byte, v reflect.Value, kind string, cfg *optConfig) ([]byte, bool) {
	switch et := v.Type().Elem(); {
	case kind == "string" && et == stringType:
		if s, ok := v.Interface().([]string); ok {
			return appendJSONNativeSlice(buf, s, appendJSONString), true
		}
	case kind == "int" && et == int32Type:
		if s, ok := v.Interface().([]int32); ok {
			return appendJSONNativeSlice(buf, s, func(b []byte, x int32) []byte { return strconv.AppendInt(b, int64(x), 10) }), true
		}
	case kind == "long" && et == int64Type:
		if s, ok := v.Interface().([]int64); ok {
			return appendJSONNativeSlice(buf, s, func(b []byte, x int64) []byte { return strconv.AppendInt(b, x, 10) }), true
		}
	case kind == "long" && et == intType:
		if s, ok := v.Interface().([]int); ok {
			return appendJSONNativeSlice(buf, s, func(b []byte, x int) []byte { return strconv.AppendInt(b, int64(x), 10) }), true
		}
	case kind == "float" && et == float32Type:
		if s, ok := v.Interface().([]float32); ok {
			return appendJSONNativeSlice(buf, s, func(b []byte, x float32) []byte { return appendJSONFloat(b, float64(x), 32, cfg) }), true
		}
	case kind == "double" && et == float64Type:
		if s, ok := v.Interface().([]float64); ok {
			return appendJSONNativeSlice(buf, s, func(b []byte, x float64) []byte { return appendJSONFloat(b, x, 64, cfg) }), true
		}
	case kind == "boolean" && et == boolType:
		if s, ok := v.Interface().([]bool); ok {
			return appendJSONNativeSlice(buf, s, strconv.AppendBool), true
		}
	}
	return buf, false
}

// appendJSONFieldDefault appends a missing record field's default value
// to buf — JSON `null` for nil defaultVal, otherwise recursive
// appendAvroJSON. Errors with "missing required field" when the field
// has no default. Shared by the map[string]any fast path and the
// generic-map arm in appendAvroJSONRecord so the missing-required /
// nil-default-to-null / default-via-appendAvroJSON sequence agrees
// across both. Defaults route through appendAvroJSON (not a pre-
// marshalled splice) so encoder options apply equally to defaults.
//
// The recursive appendAvroJSON entries pass nil for the custom map so
// CustomType.Encode is bypassed for default values, matching binary's
// encodeDefault. Encoders convert user-Go-type → Avro-native; defaults
// are stored in parsed Avro-native form (json.Number / []byte / string)
// and have no Go-domain-type representation, so the directional
// contract has nothing to apply. The custom encoder fires for
// user-supplied values (encode arm of appendAvroJSONRecord) but not
// for the library-inserted defaults handled here.
//
// Union defaults dispatch with a declaration-order try-each that mirrors
// the binary side's encodeDefault (resolve.go). The runtime
// appendAvroJSONUnion dispatcher uses unionTypeNameForValue (a kind-match
// fast path) — correct for user-supplied values (the Go type names the
// user's intended branch) but wrong for stored defaults: parse time
// already chose a branch via the declaration-order accept rule, and the
// JSON tagged-form wrap must name that same branch for parity with the
// binary defaultBytes. Without this, [enum, string] default "A" emits
// {"v":{"string":"A"}} under TaggedUnions while validate chose enum and
// the binary defaultBytes encode against the enum branch.
//
// firstUnionBranchAcceptingDefault isn't reusable here because
// convertDefaultBytes has already rewritten string→[]byte for bytes/fixed
// branches, and validateDefault's bytes/fixed arm only accepts string.
// Declaration-order try-each on appendAvroJSON itself has the right
// post-convert acceptance set (the bytes/fixed appendAvroJSON arms accept
// []byte) and matches encodeDefault's new try-each loop on the binary
// side branch-by-branch.
func appendJSONFieldDefault(buf []byte, recordName string, f fieldNode, cfg *optConfig, depth int) ([]byte, error) {
	if !f.hasDefault {
		return nil, fmt.Errorf("avro json: record %q missing required field %q", recordName, f.name)
	}
	if f.defaultVal == nil {
		return append(buf, "null"...), nil
	}
	// Defaults bypass the CustomType.Encode wrap by passing nil for
	// the custom map at the recursive appendAvroJSON entries. Encoders
	// convert user-Go-type → Avro-native; the parsed default value is
	// already in Avro-native form (json.Number / []byte / string per
	// the schema's type), so the directional contract has nothing to
	// apply. Binary's encodeDefault (resolve.go) takes no custom
	// parameter and never reaches the wiring hook for the same reason;
	// matching it here keeps Encode/EncodeJSON parity for default-fill.
	if f.node != nil && f.node.kind == "union" {
		v := reflect.ValueOf(f.defaultVal)
		for _, branch := range f.node.branches {
			encoded, err := appendAvroJSON(nil, v, branch, cfg, nil, depth+1)
			if err == nil {
				return appendUnionBranch(buf, branch, encoded, cfg), nil
			}
			if errors.Is(err, errTooDeep) {
				return nil, err
			}
		}
		return nil, fmt.Errorf("avro json: union default for field %q does not match any branch", f.name)
	}
	return appendAvroJSON(buf, reflect.ValueOf(f.defaultVal), f.node, cfg, nil, depth+1)
}

// appendAvroJSONRecord handles record encoding for both structs and maps.
func appendAvroJSONRecord(buf []byte, v reflect.Value, node *schemaNode, cfg *optConfig, custom map[*schemaNode]*customWiring, depth int) ([]byte, error) {
	buf = append(buf, '{')
	if v.Kind() == reflect.Map {
		if v.Type().Key().Kind() != reflect.String {
			return nil, semErr(v, "record")
		}
		// Input keys must match the schema's canonical field names —
		// aliases are a reader-side / decode concept, not relevant on
		// encode (we are the writer and our output uses our schema's
		// canonical names). map[string]any fast path: MapIndex
		// allocates via reflect.copyVal for each interface{} element;
		// direct lookup skips that.
		if v.Type() == mapStringAnyType {
			m := v.Interface().(map[string]any)
			for i, f := range node.fields {
				if i > 0 {
					buf = append(buf, ',')
				}
				buf = appendJSONString(buf, f.name)
				buf = append(buf, ':')
				value, exists := m[f.name]
				var err error
				if !exists {
					buf, err = appendJSONFieldDefault(buf, node.name, f, cfg, depth)
				} else {
					buf, err = appendAvroJSON(buf, reflect.ValueOf(value), f.node, cfg, custom, depth+1)
				}
				if err != nil {
					return nil, err
				}
			}
			return append(buf, '}'), nil
		}
		mapType := v.Type()
		keyType := mapType.Key()
		for i, f := range node.fields {
			if err := validateJSONNumberMapKey(f.name, keyType, "record"); err != nil {
				return nil, err
			}
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = appendJSONString(buf, f.name)
			buf = append(buf, ':')
			value := v.MapIndex(mapKeyAs(mapType, f.nameVal))
			var err error
			if !value.IsValid() {
				buf, err = appendJSONFieldDefault(buf, node.name, f, cfg, depth)
			} else {
				buf, err = appendAvroJSON(buf, value, f.node, cfg, custom, depth+1)
			}
			if err != nil {
				return nil, err
			}
		}
	} else if v.Kind() == reflect.Struct {
		mapping, err := typeFieldMapping(node.serRecord.names, &node.serRecord.cache, v.Type())
		if err != nil {
			return nil, err
		}
		for i, f := range node.fields {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = appendJSONString(buf, f.name)
			buf = append(buf, ':')
			fv := v.FieldByIndex(mapping.indices[i])
			// Honor omitzero: mirrors ser.go's slow-path check
			// at the binary site so a value-typed zero-value
			// null-union field renders as JSON `null` rather
			// than the value's zero literal. Position-agnostic on
			// the JSON side — only the binary path needs
			// nullUnionBytes for the branch-index byte. avroType
			// lives on serRecord.fields[i] (parallel-indexed to
			// node.fields[i]).
			if mapping.omitzero[i] && node.serRecord.fields[i].avroType == "nullunion" && valueIsZero(fv) {
				buf = append(buf, "null"...)
				continue
			}
			buf, err = appendAvroJSON(buf, fv, f.node, cfg, custom, depth+1)
			if err != nil {
				return nil, err
			}
		}
	} else {
		return nil, fmt.Errorf("avro json: expected struct or map for record, got %s", v.Type())
	}
	return append(buf, '}'), nil
}

// appendUnionBranch appends encoded bytes for the chosen union branch,
// wrapping in tagged form {type_name: value} when cfg.tagged is set AND
// the branch is non-null. Centralizes the "wrap iff non-null" invariant
// so the four dispatcher sites in appendAvroJSONUnion (tagged-form,
// nil-first, type-name, try-each) can't drift on it.
//
// Mirrors Java's JsonEncoder.writeIndex
// (lang/java/avro/src/main/java/org/apache/avro/io/JsonEncoder.java):
// `if (symbol != Symbol.NULL && includeNamespace) { writeStartObject;
// writeFieldName; }`. The Avro JSON spec defines a union null value as
// bare `null`; TaggedUnions's own doc commits to "wraps non-null union
// values," so the null branch must stay bare even under cfg.tagged.
// Without this guard, EncodeJSON((*int)(nil), ...) (which the entry
// peel at appendAvroJSON converts to invalid → early-null at lines
// 168-172 → bare "null") and EncodeJSON([]byte(nil), ...) (which
// reaches appendAvroJSONUnion as a still-valid nil Slice and is routed
// to the null branch by isNilValue) produced different outputs under
// TaggedUnions: bare `null` vs `{"null":null}` for the same conceptual
// input.
func appendUnionBranch(buf []byte, branch *schemaNode, encoded []byte, cfg *optConfig) []byte {
	if cfg.tagged && branch.kind != "null" {
		return appendTaggedUnion(buf, branch, encoded, cfg.tagLogical)
	}
	return append(buf, encoded...)
}

// appendAvroJSONUnion handles union encoding.
func appendAvroJSONUnion(buf []byte, v reflect.Value, node *schemaNode, cfg *optConfig, custom map[*schemaNode]*customWiring, depth int) ([]byte, error) {
	if depth >= maxDepth {
		return nil, errTooDeep
	}

	// Accept tagged union maps: {"typeName": value}. This matches the
	// Avro JSON convention and the behavior of Encode (binary).
	if v.Kind() == reflect.Map && v.Len() == 1 {
		iter := v.MapRange()
		iter.Next()
		key := iter.Key()
		if key.Kind() == reflect.String {
			if branch := findUnionBranch(node, key.String()); branch != nil {
				inner := iter.Value()
				encoded, err := appendAvroJSON(nil, inner, branch, cfg, custom, depth+1)
				if err != nil {
					if errors.Is(err, errTooDeep) {
						return nil, err
					}
					// Fall through to try-each-branch loop,
					// matching Encode's serUnion behavior.
					goto tryAll
				}
				return appendUnionBranch(buf, branch, encoded, cfg), nil
			}
		}
	}

	// Nil-first dispatch: if v is nil-equivalent and the union has a
	// null branch, pick null regardless of arity. Mirrors the binary
	// 2-branch optimization serNullUnionAt (ser.go) and the
	// corresponding serUnion.ser nil-first check; generalizes the
	// "Go nil = absent → null branch" semantic uniformly across all
	// union arities so 2-branch and N-branch behavior agree on what
	// counts as null. Without this, nil []byte against ["null","bytes"]
	// routes via unionTypeNameForValue → "bytes" → bytes branch
	// (emitting empty bytes) while binary 2-branch picks null —
	// producing a binary↔JSON parity gap for the 2-branch case and a
	// binary 2-branch↔3-branch inconsistency for the N-branch case.
	if isNilValue(v) {
		for _, branch := range node.branches {
			if branch.kind == "null" {
				return appendUnionBranch(buf, branch, []byte("null"), cfg), nil
			}
		}
	}

	// Type-name dispatch (Java/fastavro/hamba parity): if v's Go type
	// has a canonical Avro primitive name and exactly one branch
	// matches, prefer it over try-each. Mirrors serUnion.ser. Falls
	// through to try-each on no-match or on encode failure (e.g. a
	// numeric value that needs promotion via the encoder's lenient
	// arms — try-each preserves those paths).
	if name := unionTypeNameForValue(v); name != "" {
		for _, branch := range node.branches {
			if branch.kind != name {
				continue
			}
			encoded, err := appendAvroJSON(nil, v, branch, cfg, custom, depth+1)
			if err == nil {
				return appendUnionBranch(buf, branch, encoded, cfg), nil
			}
			if errors.Is(err, errTooDeep) {
				return nil, err
			}
			break // only one branch has this kind; don't waste cycles
		}
	}

tryAll:
	// Try every branch including null, mirroring serUnion.ser.
	// The case "null" arm of appendAvroJSON rejects non-nil values with
	// errNonNil, so a non-nil v cleanly falls through to the next branch.
	// A nil Map / nil Slice / nil Chan / nil Func (none of which the peel
	// loop above converts to invalid, and none of which unionTypeNameForValue
	// names except []byte → "bytes") lands on case "null" here and emits
	// "null"; without trying the null branch, binary↔JSON parity breaks
	// for those shapes the way the binary serUnion.ser try-each handles via
	// serNull. The 2-branch [null,T] fast path on the binary side uses
	// serNullUnionAt → isNilValue, which covers the same shapes.
	//
	// Keep the last concrete error so the final message names the closest
	// reason a branch failed (mirrors decodeUnionBare's lastErr plumbing
	// on the decode side and serUnion.ser on the binary encode side).
	var lastErr error
	for _, branch := range node.branches {
		encoded, err := appendAvroJSON(nil, v, branch, cfg, custom, depth+1)
		if err == nil {
			return appendUnionBranch(buf, branch, encoded, cfg), nil
		}
		// Propagate too-deep without trying further branches; the
		// trial loop would otherwise mask the recursion limit error
		// behind a misleading "no branch matched".
		if errors.Is(err, errTooDeep) {
			return nil, err
		}
		lastErr = err
	}
	if lastErr != nil {
		return nil, fmt.Errorf("avro json: no union branch matched value of type %s: %w", v.Type(), lastErr)
	}
	return nil, fmt.Errorf("avro json: no union branch matched value of type %s", v.Type())
}

// unionBranchName returns the Avro JSON type name for a union branch.
func unionBranchName(node *schemaNode) string {
	switch node.kind {
	case "record", "enum", "fixed":
		return node.name
	default:
		return node.kind
	}
}

// unionBranchNames returns the standard and logical branch names for a
// union branch node. The logical name includes the logical type qualifier
// (e.g. "long.timestamp-millis") when present, otherwise it equals the
// standard name.
func unionBranchNames(node *schemaNode) (standard, logical string) {
	standard = unionBranchName(node)
	if node.logical != "" {
		logical = node.kind + "." + node.logical
	} else {
		logical = standard
	}
	return standard, logical
}

// appendTaggedUnion appends the Avro JSON tagged-union wrapping
// `{"<branch>":<encoded>}` for the given branch and pre-encoded body.
func appendTaggedUnion(buf []byte, branch *schemaNode, encoded []byte, tagLogical bool) []byte {
	bn, ln := unionBranchNames(branch)
	name := bn
	if tagLogical {
		name = ln
	}
	buf = append(buf, '{')
	buf = appendJSONString(buf, name)
	buf = append(buf, ':')
	buf = append(buf, encoded...)
	return append(buf, '}')
}

// findUnionBranch finds a union branch by type name.
//
// We accept three tag conventions on input for cross-implementation
// interop, in order:
//
//  1. Exact match against the spec/Java fullname (e.g. "long" or
//     "com.example.User"). This is what we emit on output.
//  2. goavro's "type.logicalType" form (e.g. "long.timestamp-millis"):
//     match the base primitive before the dot.
//  3. fastavro's unqualified short-name form for named types (e.g.
//     "User" instead of "com.example.User"). Only applied when the
//     input has no namespace AND exactly one branch matches by short
//     name; ambiguous cases return no match rather than guess.
func findUnionBranch(union *schemaNode, name string) *schemaNode {
	for _, b := range union.branches {
		if unionBranchName(b) == name {
			return b
		}
	}
	// Fallback (goavro / TagLogicalTypes): "type.logicalType" → match a
	// branch whose kind == base AND whose logicalType == suffix. Includes
	// primitives and "fixed" (the only named type that can carry a
	// logical type — duration/decimal/uuid are valid on fixed; enum
	// can't have a logical type per spec). Matching on the (kind, logical)
	// pair — not just kind — prevents silently misrouting a tagged
	// branch when a union contains two same-kind branches that differ
	// only by logical type (e.g. [long, {"type":"long","logicalType":
	// "timestamp-millis"}]). Pre-tightening, this fallback returned the
	// first kind-match, which lost the logical-type distinction.
	if base, suffix, ok := strings.Cut(name, "."); ok {
		for _, b := range union.branches {
			switch b.kind {
			case "null", "boolean", "int", "long", "float", "double", "string", "bytes", "fixed":
				if b.kind == base && b.logical == suffix {
					return b
				}
			}
		}
		return nil
	}
	// Fallback (fastavro): unqualified short name. The ambiguity guard
	// prevents silent misrouting when two namespaces share a short name.
	var match *schemaNode
	for _, b := range union.branches {
		switch b.kind {
		case "record", "enum", "fixed":
			if unqualified(b.name) == name {
				if match != nil {
					return nil // ambiguous
				}
				match = b
			}
		}
	}
	return match
}

// parseSpecialFloat parses NaN/Infinity string forms. Accepts the
// Java/fastavro set {"NaN", "Infinity", "INF", "-Infinity", "-INF"}
// plus Go-strconv-style "Inf"/"-Inf". Java/fastavro/goavro all reject
// lowercase; the lowercase 'n' would collide with the JSON null
// literal in the union dispatcher, so case-strict matters here. The goavro null→NaN and ±1e999→±Inf conventions are handled
// separately by the bare-token/number paths in decodeJSONFloat.
func parseSpecialFloat(s string) (float64, error) {
	switch s {
	case "NaN":
		return math.NaN(), nil
	case "Infinity", "INF", "Inf":
		return math.Inf(1), nil
	case "-Infinity", "-INF", "-Inf":
		return math.Inf(-1), nil
	}
	return 0, fmt.Errorf("avro json: unknown float value %q", truncForError(s))
}

// jsonEscapeShort returns the second byte of the 2-byte short JSON
// escape for c (so '"' → '"', '\n' → 'n', etc.) or 0 if no short
// escape applies. Shared by appendAvroJSONBytes and appendJSONString
// so the two hot-path escape switches can't drift on the set of
// short-form bytes the JSON spec defines.
func jsonEscapeShort(c byte) byte {
	switch c {
	case '"', '\\':
		return c
	case '\b':
		return 'b'
	case '\t':
		return 't'
	case '\n':
		return 'n'
	case '\f':
		return 'f'
	case '\r':
		return 'r'
	}
	return 0
}

// appendAvroJSONBytes encodes raw bytes as an Avro JSON string using
// ISO-8859-1 encoding, matching the Java canonical implementation.
// Printable ASCII bytes (0x20-0x7E, except " and \) are written as
// literal characters. All other bytes use \uXXXX escapes.
func appendAvroJSONBytes(buf []byte, b []byte) []byte {
	buf = append(buf, '"')
	for _, c := range b {
		if esc := jsonEscapeShort(c); esc != 0 {
			buf = append(buf, '\\', esc)
		} else if c >= 0x20 && c <= 0x7E {
			buf = append(buf, c)
		} else {
			buf = append(buf, '\\', 'u', '0', '0', jsonHex[c>>4], jsonHex[c&0xf])
		}
	}
	return append(buf, '"')
}

const jsonHex = "0123456789abcdef"

// appendJSONString appends a JSON-encoded string to buf, escaping as needed.
// Avoids the allocation a generic string-marshal call would require. Escapes
// control characters, U+2028/U+2029 (for JavaScript safety), and replaces
// invalid UTF-8 with U+FFFD.
func appendJSONString(buf []byte, s string) []byte {
	buf = append(buf, '"')
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			// ASCII fast path.
			if esc := jsonEscapeShort(c); esc != 0 {
				buf = append(buf, '\\', esc)
			} else if c < 0x20 {
				buf = append(buf, '\\', 'u', '0', '0', jsonHex[c>>4], jsonHex[c&0xf])
			} else {
				buf = append(buf, c)
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			// Replace invalid UTF-8 byte with raw U+FFFD bytes
			// (efbfbd) rather than the literal `�` escape, so
			// encode is idempotent: a re-decoded escape produces
			// the U+FFFD codepoint, which would then re-encode as
			// raw UTF-8 — different from the escape. Using raw
			// here makes both paths converge.
			buf = utf8.AppendRune(buf, utf8.RuneError)
			i++
			continue
		}
		// Escape U+2028 and U+2029 for JavaScript safety.
		if r == '\u2028' || r == '\u2029' {
			buf = append(buf, '\\', 'u', '2', '0', '2', jsonHex[byte(r)&0xf])
			i += size
			continue
		}
		buf = append(buf, s[i:i+size]...)
		i += size
	}
	return append(buf, '"')
}

// avroJSONBytesToBytes decodes an Avro JSON bytes string (\uXXXX per byte)
// to raw bytes. Inverse pair with [bytesToAvroJSONString]: round-trip
// `avroJSONBytesToBytes(bytesToAvroJSONString(b)) == b` for every []byte.
func avroJSONBytesToBytes(s string) ([]byte, error) {
	// The string from json.Unmarshal has already decoded \uXXXX escapes
	// to Unicode code points. Each code point 0-255 maps to a byte.
	b := make([]byte, 0, len(s))
	for _, r := range s {
		if r > 255 {
			return nil, fmt.Errorf("avro json: bytes string contains code point U+%04X, max U+00FF", r)
		}
		b = append(b, byte(r))
	}
	return b, nil
}

// bytesToAvroJSONString encodes raw bytes into the Avro JSON bytes
// codepoint-string form: each byte 0x00-0xFF becomes a rune at the same
// codepoint (a 1-byte UTF-8 sequence for 0x00-0x7F, a 2-byte sequence
// for 0x80-0xFF). The Go string this returns serializes as either
// `\uXXXX` (for control chars) or the literal UTF-8 bytes (for printable
// codepoints) — both forms re-parse back to the original codepoint,
// which [avroJSONBytesToBytes] then maps back to the original byte.
//
// Inverse pair with [avroJSONBytesToBytes]: round-trip
// `avroJSONBytesToBytes(bytesToAvroJSONString(b)) == b` for every []byte.
// Used by [jsonSerializableValue] (schema_node.go) to re-emit []byte
// SchemaField.Default / SchemaNode.Props values in the Avro JSON spec's
// codepoint form rather than the default base64 a generic []byte
// marshal would produce (which the Avro parser would silently re-read
// as the literal ASCII bytes of the base64 alphabet).
func bytesToAvroJSONString(b []byte) string {
	// strings.Builder.WriteRune calls utf8.EncodeRune internally and
	// uses unsafe to return the underlying buffer as a string without
	// a final copy. Grow(len(b)) is the all-ASCII lower bound; bytes
	// ≥ 0x80 take 2 UTF-8 bytes, so the worst-case output is 2*len(b)
	// — append's geometric growth absorbs that with at most one realloc.
	var sb strings.Builder
	sb.Grow(len(b))
	for _, v := range b {
		sb.WriteRune(rune(v))
	}
	return sb.String()
}

// appendJSONFloat formats a float for JSON output, handling special values.
// With LinkedinFloats, NaN encodes as null and ±Infinity as ±1e999 (goavro
// convention). Otherwise NaN/Infinity encode as JSON strings (Java convention).
//
// When bits==32 and f is a finite float64 that overflows float32's range,
// the value narrows to ±Inf and emits as "Infinity"/"-Infinity" per the
// lossy-destination policy (matches Java's writeFloat(Number.floatValue())
// silent narrowing).
func appendJSONFloat(buf []byte, f float64, bits int, cfg *optConfig) []byte {
	if bits == 32 {
		f = float64(float32(f))
	}
	if math.IsNaN(f) {
		if cfg.linkedin {
			return append(buf, "null"...)
		}
		return append(buf, `"NaN"`...)
	}
	if math.IsInf(f, 1) {
		if cfg.linkedin {
			return append(buf, "1e999"...)
		}
		return append(buf, `"Infinity"`...)
	}
	if math.IsInf(f, -1) {
		if cfg.linkedin {
			return append(buf, "-1e999"...)
		}
		return append(buf, `"-Infinity"`...)
	}
	return strconv.AppendFloat(buf, f, 'g', -1, bits)
}

// jsonCoerceToFloat64 converts a reflect.Value to float64, accepting
// float, int, uint, and json.Number types. Encoding into a float/double
// schema is lossy by destination: integer inputs exceeding the mantissa
// precision silently IEEE-round, and float32-target overflows silently
// narrow to ±Inf at the caller's float64 → float32 cast. Matches Java's
// GenericDatumWriter.writeFloat / writeDouble and fastavro's encoder.
func jsonCoerceToFloat64(v reflect.Value) (float64, error) {
	switch {
	case v.CanFloat():
		return v.Float(), nil
	case v.CanInt():
		return float64(v.Int()), nil
	case v.CanUint():
		return float64(v.Uint()), nil
	case v.Type() == jsonNumberType:
		// Route through the shared json.Number → float64 helper —
		// same predicate used by binary encode (jsonNumberToFloat) and
		// schema-parse default validation (defaultAsFloat).
		f, err := parseJSONNumberAsFloat(string(v.Interface().(json.Number)), 64)
		if err != nil {
			return 0, fmt.Errorf("avro json: %w", err)
		}
		return f, nil
	}
	return 0, fmt.Errorf("avro json: expected numeric, got %s", v.Type())
}

// jsonCoerceInt converts a reflect.Value to an integer T, with overflow
// and whole-number checks. Mirrors Encode's serInt/serLong coercion.
// Shared body of jsonCoerceToInt32 / jsonCoerceToInt64.
//
// hi bounds the signed int range (MaxInt32 for int32 target, MaxInt64
// for int64 target); the CanInt path applies a symmetric -hi-1 lo
// bound (math.MinInt32 / math.MinInt64). floatFits and parseLenient
// are the matching int-narrowed helpers.
func jsonCoerceInt[T int32 | int64](v reflect.Value, hi int64,
	floatFits func(float64, int) (T, error),
	parseLenient func(string) (T, error),
) (T, error) {
	if v.CanInt() {
		n := v.Int()
		if n < -hi-1 || n > hi {
			return 0, fmt.Errorf("avro json: value %d overflows %T", n, T(0))
		}
		return T(n), nil
	}
	if v.CanUint() {
		n := v.Uint()
		if n > uint64(hi) {
			return 0, fmt.Errorf("avro json: value %d overflows %T", n, T(0))
		}
		return T(n), nil
	}
	if v.CanFloat() {
		n, err := floatFits(v.Float(), v.Type().Bits())
		if err != nil {
			return 0, fmt.Errorf("avro json: %w", err)
		}
		return n, nil
	}
	if v.Type() == jsonNumberType {
		n, err := parseLenient(string(v.Interface().(json.Number)))
		if err != nil {
			return 0, fmt.Errorf("avro json: %w", err)
		}
		return n, nil
	}
	return 0, fmt.Errorf("avro json: expected integer, got %s", v.Type())
}

func jsonCoerceToInt32(v reflect.Value) (int32, error) {
	return jsonCoerceInt(v, math.MaxInt32, floatFitsInt32From, parseInt32Lenient)
}

func jsonCoerceToInt64(v reflect.Value) (int64, error) {
	return jsonCoerceInt(v, math.MaxInt64, floatFitsInt64From, parseInt64Lenient)
}
