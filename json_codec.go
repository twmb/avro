package avro

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/twmb/avro/internal/optmark"
)

// Opt configures encoding and decoding. Each option's doc says which functions
// it affects; we ignore an option that does not apply.
type Opt interface{ opt() }

type taggedUnions struct{}

func (taggedUnions) opt() {}

// TaggedUnions wraps non-null union values as {"type_name": value}, overriding
// the default of bare values.
//
// [Schema.EncodeJSON] emits the tagged form. [Schema.Decode] and
// [Schema.DecodeJSON] wrap union values as map[string]any{branchName: value},
// but only when your decode target is *any. A typed target (a concrete struct
// field, *T, or a non-empty interface) cannot hold the wrapper, so it gets the
// bare branch value.
//
// [Schema.DecodeJSON] and [Schema.Encode] take both tagged and bare union
// input either way.
//
// The tagged form is what the Avro spec defines for JSON. Java's JsonDecoder
// and fastavro's JSON decoder reject our bare default on the first non-null
// union field, so pass TaggedUnions if Java, fastavro, or avro-tools fromjson
// reads your output. The bare default is for goavro's bare-JSON codecs
// (NewCodecForStandardJSON and NewCodecForStandardJSONFull) and for plain
// map[string]any consumers. See AVRO-2899 for the upstream discussion.
//
// Note that a bare union value does not name its branch, so [Schema.DecodeJSON]
// cannot tell which branch the writer used when several share a JSON token
// class: a bare 7 matches int, long, float and double, and a bare "x" matches
// string, bytes, fixed and enum. We use the *first* such branch in declaration
// order. That can differ from the writer's branch, and it bypasses a
// [CustomType] registered on a later branch of the same class: its Decode
// never runs, and a typed target is filled by plain coercion from the first
// branch. Binary [Schema.Decode] is unaffected, since the wire carries the
// branch index. If branch identity or a branch-bound CustomType matters to
// you, encode and decode with TaggedUnions.
func TaggedUnions() Opt { return taggedUnions{} }

type tagLogicalTypes struct{}

func (tagLogicalTypes) opt() {}

// TagLogicalTypes qualifies union branch names with their logical type,
// "long.timestamp-millis" rather than the spec's "long". This is the
// linkedin/goavro convention. It applies to [Schema.EncodeJSON] and
// [Schema.Decode], and only alongside [TaggedUnions]; without that option it
// does nothing.
func TagLogicalTypes() Opt { return tagLogicalTypes{} }

type skipUnknown struct{}

func (skipUnknown) opt() {}

// SkipUnknown allows decoding into a struct that maps only some of a record's
// fields, skipping the fields your struct lacks rather than erroring as we do
// by default. Nested records follow the same rule. This applies to
// [Schema.Decode], [Schema.DecodeJSON] and [Schema.DecodeSingleObject]. It is
// decode only: encoding from a struct that does not cover the record still
// errors, since the missing fields would go out as zero values.
//
// Note that an ambiguous field name (two same-depth fields claiming it) still
// errors. Your type has fields for it, so there is nothing to skip.
func SkipUnknown() Opt { return skipUnknown{} }

type aliasInput struct{}

func (aliasInput) opt() {}

// AvroOptAliasesInput marks this option for [optmark.AliasesInput], so a host
// decoding from a buffer it reuses can drop it without introspecting the type.
func (aliasInput) AvroOptAliasesInput() {}

// AliasInput makes decoded strings and byte slices point into the bytes they
// were read from, rather than copying them as we do by default. This applies
// to [Schema.Decode] and [Schema.DecodeSingleObject]. [Schema.DecodeJSON]
// ignores it, because a JSON string with an escape cannot alias.
//
// You must not modify src after the decode, and you must not modify anything
// the decode returns. An aliased value *is* the memory it was read from: if
// you write to src, every string and byte slice decoded from it changes too,
// even though Go otherwise guarantees strings are immutable. Usually that
// memory is your src, but a field filled from a schema default aliases the
// parsed [Schema] itself, which every decode of that schema shares.
//
// One aliased field keeps the whole buffer it points into alive for as long as
// you hold it. Do not use this option if you reuse the buffer, or if you keep
// one small field of a large message around.
//
// We alias string and []byte targets of the string, bytes and fixed kinds,
// including inside an any, under a uuid logical type, and as map keys. We
// still copy for [N]byte, [encoding.TextUnmarshaler], and any logical type
// that builds a new Go value (decimal, the timestamps, and the hex-dash uuid
// form).
//
// This is an [Opt] rather than a [SchemaOpt] on purpose: ocf.WithSchemaOpts
// forwards SchemaOpts into an OCF reader, and a reader must not alias its
// block buffer. ocf.WithDecodeOpts drops this option for the same reason.
func AliasInput() Opt { return aliasInput{} }

type linkedinFloats struct{}

func (linkedinFloats) opt() {}

// LinkedinFloats encodes NaN as JSON null and +/-Infinity as +/-1e999 in
// [Schema.EncodeJSON], the linkedin/goavro convention, overriding our default
// of the JSON strings "NaN", "Infinity" and "-Infinity" (the Java convention).
//
// [Schema.DecodeJSON] accepts both conventions for a float or double decoded
// directly or as a tagged union branch ({"float":null} decodes to NaN). Note
// that a NaN inside a *bare* union does not round-trip: it encodes as a bare
// null, and on decode the union's null branch claims that null (or the union
// rejects it if it has no null branch) before we try the float branch. Use
// [TaggedUnions] if you need NaN to round-trip through a union. +/-Infinity is
// a number token and round-trips in a bare union regardless.
func LinkedinFloats() Opt { return linkedinFloats{} }

type optConfig struct {
	tagged      bool
	tagLogical  bool
	linkedin    bool
	skipUnknown bool
	alias       bool
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
		case skipUnknown:
			cfg.skipUnknown = true
		case aliasInput:
			cfg.alias = true
		}
	}
	return cfg
}

// EncodeJSON encodes v as JSON, using the schema for type-aware encoding. By
// default we write union values bare and escape non-ASCII bytes/fixed bytes as
// \uXXXX; see [Opt] for the options that change the output.
//
// We encode NaN and Infinity as the JSON strings "NaN", "Infinity" and
// "-Infinity" (the Java convention), or as null and +/-1e999 with
// [LinkedinFloats]. A generic JSON encoder rejects non-finite floats outright;
// these forms keep the output valid JSON for any strict parser.
//
// We replace each invalid byte of non-UTF-8 string content with U+FFFD, for
// string values and map keys at any depth. A JSON string cannot carry
// arbitrary bytes, so JSON is lossy for such content where [Schema.Encode]
// preserves it verbatim. Java behaves the same.
//
// EncodeJSON accepts the same Go types as [Schema.Encode]. We do not sort map
// keys, so their output order is non-deterministic.
//
// Note that Java's JsonDecoder, fastavro's JSON decoder and avro-tools
// fromjson all require the {"type_name": value} envelope and reject bare
// union values. Pass [TaggedUnions] for those tools; see its doc and
// AVRO-2899.
func (s *Schema) EncodeJSON(v any, opts ...Opt) ([]byte, error) {
	return s.AppendEncodeJSON(nil, v, opts...)
}

// AppendEncodeJSON is like [Schema.EncodeJSON] but appends to dst.
func (s *Schema) AppendEncodeJSON(dst []byte, v any, opts ...Opt) ([]byte, error) {
	cfg := parseOpts(opts)
	return appendAvroJSON(dst, reflect.ValueOf(v), s.node, &cfg, s.custom, 0)
}

// DecodeJSON decodes Avro JSON from src into v. We unwrap union wrappers,
// convert bytes/fixed strings, and coerce numeric types to match the schema.
// When v is *any, you get the natural Go value directly.
//
// We accept every input format: tagged and bare unions, the Java and goavro
// NaN/Infinity conventions, and the linkedin/goavro union branch naming
// ("long.timestamp-millis" instead of "long"). Pass [TaggedUnions] to wrap
// decoded union values when the target is *any.
//
// A bare union value whose JSON token class matches several branches (a bare
// number against ["long","int"], say) decodes via the first matching branch
// in declaration order, since the bare form does not name the writer's
// branch. See [TaggedUnions] for the consequences.
//
// On a schema returned by [Resolve], src is JSON in the writer's shape, as a
// producer using the writer schema would emit it. We apply full
// writer-to-reader resolution: promotion, enum-symbol remapping to the reader
// default, field add and drop, and aliases, matching Java's ResolvingDecoder
// over a JsonDecoder. Note that this path decodes the writer's JSON and then
// resolves through a binary decode, so it is slower than a plain DecodeJSON.
func (s *Schema) DecodeJSON(src []byte, v any, opts ...Opt) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return errors.New("avro: DecodeJSON requires a non-nil pointer")
	}
	if s.resolveWriter != nil {
		return s.decodeJSONResolved(src, rv, opts...)
	}
	cfg := parseOpts(opts)
	sl := slabPool.Get().(*slab)
	// A record field filled from its schema default routes through the
	// binary deser fn (applyFieldDefault), which reads the slab's
	// taggedUnions and tagLogicalTypes flags, so we set them here as
	// Schema.Decode does. Without this, a present union field wraps in its
	// {branch: value} envelope but a default-filled one emits the bare
	// value, a JSON-vs-binary and intra-call inconsistency on the option.
	sl.taggedUnions = cfg.tagged
	sl.tagLogicalTypes = cfg.tagLogical
	sl.skipUnknown = cfg.skipUnknown
	ctx := &jsonDecoder{
		scanner: &jsonScanner{data: src},
		slab:    sl,
	}
	err := ctx.decodeValue(rv.Elem(), s.node)
	if err == nil {
		// One DecodeJSON call decodes exactly one value and returns no
		// offset, so we reject any trailing non-whitespace content,
		// matching encoding/json.Unmarshal and fastavro.
		ctx.scanner.skipWhitespace()
		if ctx.scanner.pos < len(ctx.scanner.data) {
			err = fmt.Errorf("avro json: unexpected trailing content at offset %d", ctx.scanner.pos)
		}
	}
	sl.put()
	return err
}

// decodeJSONResolved composes already-validated paths: we decode the JSON with
// the writer schema, re-encode that to writer binary, then run the resolving
// binary decode. Java does the same, wrapping a JsonDecoder built with the
// writer schema in a ResolvingDecoder. Resolution is not throughput-critical,
// so reusing the binary resolver keeps the code small and correct by
// construction.
func (s *Schema) decodeJSONResolved(src []byte, rv reflect.Value, opts ...Opt) error {
	w := s.resolveWriterRaw
	// The custom-free writer view: this is a wire-shape transform, so the
	// intermediate must hold raw Avro-native values; the reader's customs
	// and your opts apply only in the final resolving s.Decode. TaggedUnions
	// on the intermediate keeps the branch envelope, the only carrier of the
	// writer's branch choice: bare, the re-encode would re-derive the branch
	// by first match and rewrite branch identity whenever a later branch's
	// value also satisfies an earlier one.
	var inter any
	if err := w.DecodeJSON(src, &inter, TaggedUnions()); err != nil {
		return err
	}
	wb, err := w.Encode(inter)
	if err != nil {
		return fmt.Errorf("avro: re-encoding resolved JSON intermediate: %w", err)
	}
	_, err = s.Decode(wb, rv.Interface(), dropAliasingOpts(opts)...)
	return err
}

// dropAliasingOpts returns opts without any that would make decoded values
// reference the decode input. The buffer this hands Decode is a re-encoded
// intermediate, not your src, so honoring one here would point the result at
// memory you never gave and DecodeJSON never promised. We return opts
// untouched when there is nothing to drop, so the common call allocates
// nothing.
func dropAliasingOpts(opts []Opt) []Opt {
	for i, o := range opts {
		if _, ok := o.(optmark.AliasesInput); !ok {
			continue
		}
		kept := make([]Opt, i, len(opts)-1)
		copy(kept, opts[:i])
		for _, o := range opts[i+1:] {
			if _, ok := o.(optmark.AliasesInput); !ok {
				kept = append(kept, o)
			}
		}
		return kept
	}
	return opts
}

// appendAvroJSON is the single-pass Avro JSON encoder: we walk the Go value and
// the schema tree together, writing JSON directly with no intermediate binary
// encoding step.
func appendAvroJSON(buf []byte, v reflect.Value, node *schemaNode, cfg *optConfig, custom map[*schemaNode]*customWiring, depth int) ([]byte, error) {
	if depth >= maxDepth {
		return nil, errTooDeep
	}
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
			// A union without a null branch cannot represent nil, so we
			// reject rather than emit null, as the binary path, Java and
			// fastavro do. An untyped nil carries *SemanticError identity
			// like the binary entry guard; a typed nil pointer never reaches
			// here.
			return nil, &SemanticError{AvroType: "union", Err: errors.New("avro json: nil value for union without a null branch")}
		}
		return nil, &SemanticError{AvroType: node.kind, Err: fmt.Errorf("avro json: nil value for non-nullable type %q", node.kind)}
	}
	// We dispatch unions before dereferencing and the custom hook, as the
	// binary serUnion does: the branch encoders must receive the un-peeled
	// value so a branch's custom encoder with a pointer GoType matches. A
	// custom type never matches a union node itself.
	if node.kind == "union" {
		// depth passes unchanged: appendAvroJSONUnion is a same-level
		// dispatch hop and recurses into branches at depth+1.
		return appendAvroJSONUnion(buf, v, node, cfg, custom, depth)
	}
	// The custom encode conversion runs before dereferencing, so a custom
	// type with a pointer GoType matches; customEncode peels and checks
	// GoType at each level itself and returns either the encoded result or
	// a pass-through value, as on the binary path.
	if w := custom[node]; w != nil && w.encode != nil {
		var err error
		v, err = w.encode(v)
		if err != nil {
			return nil, err
		}
	}
	// We dereference pointers and interfaces, capped at maxIndirectDepth
	// so a self-referential interface (var p any; p = &p) terminates. The
	// deeply-wrapped value falls through to the type switch, which returns
	// a SemanticError for the unmatched kind.
	for range maxIndirectDepth {
		if v.Kind() != reflect.Pointer && v.Kind() != reflect.Interface {
			break
		}
		if v.IsNil() {
			// A nil pointer or interface layer inside a typed value: the
			// "null" schema takes it as JSON null, and every other kind
			// rejects it with the plain indirection sentinel the binary
			// encoders return.
			if node.kind == "null" {
				return append(buf, "null"...), nil
			}
			return nil, errIndirectNil
		}
		v = v.Elem()
	}

	switch node.kind {
	case "null":
		// A non-nil value into a "null" schema gets errNonNil, as binary
		// serNull returns for the same shapes.
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
		return nil, semErr(v, "boolean")

	case "int":
		if v.Type() == timeType {
			t := v.Interface().(time.Time)
			switch node.logical {
			case "date":
				d, err := timeToDate(t)
				if err != nil {
					// Same identity as serDate: a range failure of your
					// value carries *SemanticError on both wires.
					return nil, &SemanticError{GoType: timeType, AvroType: "date", Err: err}
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
					// Same identity as serTimeMillis' duration arm.
					return nil, &SemanticError{GoType: durationType, AvroType: "time-millis", Err: err}
				}
				return strconv.AppendInt(buf, int64(ms), 10), nil
			}
		}
		if node.logical == "date" {
			if t, ok := tryParseDateString(v); ok {
				d, err := timeToDate(t)
				if err != nil {
					// Same identity as serDate's date-string arm. The
					// 4-digit-year formats tryParseDateString accepts
					// cannot express a date outside timeToDate's range,
					// so this arm is not reachable today; it mirrors the
					// binary twin so the two cannot drift.
					return nil, semErrW(v, "date", err)
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
					// Same identity as serTimeAsLong: a timestamp range
					// failure carries *SemanticError on both wires.
					return nil, semErrW(v, "long", err)
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
		f, err := jsonCoerceToFloat64(v, "float")
		if err != nil {
			return nil, err
		}
		return appendJSONFloat(buf, f, 32, cfg), nil

	case "double":
		f, err := jsonCoerceToFloat64(v, "double")
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
		// must agree on precedence.
		s, err := avroStringValue(v)
		if err != nil {
			return nil, err
		}
		return appendJSONString(buf, s), nil

	case "bytes":
		// Decimal emits the spec form: the two's-complement big-endian
		// unscaled integer as an Avro JSON byte string, as Java and fastavro
		// do. No decimalRatFor match falls through to the generic targets
		// below. We skip the decimal arm exactly when the binary build
		// replaced the logical serializer with the base one, which is when a
		// non-wildcard matching CustomType has an Encode (encodeSuppresses);
		// the runtime proxy custom[node].encode != nil would wrongly skip it
		// for a wildcard with Encode.
		noCustomEnc := custom[node] == nil || !custom[node].encodeSuppresses
		switch node.logical {
		case "decimal":
			if noCustomEnc {
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
				// A non-numeric string carrier is not a valid decimal, so we
				// reject it (numeric-text-only, symmetric with decode and with
				// the binary serBytesDecimal path) rather than fall through to
				// the opaque raw-bytes string arm below. []byte keeps the opaque
				// fall-through; big-decimal (next case) stays opaque-symmetric.
				if err := rejectNonNumericStructuredString(v, "bytes", "decimal"); err != nil {
					return nil, err
				}
				if err := chargeOpaqueDecimalBytes(v, "bytes", "decimal"); err != nil {
					return nil, err
				}
			}
		case "big-decimal":
			if noCustomEnc {
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
				// A non-numeric string carrier is not a valid big-decimal, so we
				// reject it (numeric-text-only, symmetric with decode and the
				// binary serBigDecimal path) rather than fall through to the
				// opaque raw string arm below. A crafted string whose bytes form
				// valid framing would otherwise decode to a different value.
				// []byte keeps the opaque fall-through.
				if err := rejectNonNumericStructuredString(v, "bytes", "big-decimal"); err != nil {
					return nil, err
				}
				if err := chargeOpaqueDecimalBytes(v, "bytes", "big-decimal"); err != nil {
					return nil, err
				}
			}
		}
		if v.Kind() == reflect.String {
			if err := rejectJSONNumberRawTarget(v, "bytes"); err != nil {
				return nil, err
			}
			// A Go string is raw UTF-8 bytes, as in serBytes. Defaults never
			// reach this arm, since convertDefaultBytes already turned them
			// into []byte. appendAvroJSONBytes iterates without retaining, so
			// we alias the string data.
			s := v.String()
			return appendAvroJSONBytes(buf, unsafe.Slice(unsafe.StringData(s), len(s))), nil
		}
		if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
			return appendAvroJSONBytes(buf, v.Bytes()), nil
		}
		if v.Kind() == reflect.Array && v.Type().Elem().Kind() == reflect.Uint8 {
			// reflect.Value.Bytes() panics on Array kinds, so we materialize
			// the bytes via byteArrayToSlice (element-agnostic so a named byte
			// element [N]B does not panic). This mirrors the "fixed" arm below
			// and serBytes (ser.go:460), which takes Array alongside Slice.
			return appendAvroJSONBytes(buf, byteArrayToSlice(v)), nil
		}
		return nil, semErr(v, "bytes")

	case "fixed":
		// Decimal: the spec form padded and sign-extended to the schema size.
		// UUID: a hex-dash string parses to 16 bytes, checked before the
		// generic extraction so a 36-char string is not rejected as size
		// != 16. We skip every logical arm exactly when the binary fixed
		// build replaced the logical serializer with serSize, which is when
		// a non-wildcard matching CustomType has an Encode (encodeSuppresses).
		if custom[node] == nil || !custom[node].encodeSuppresses {
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
				// A non-numeric string carrier is not a valid decimal, so we
				// reject it (numeric-text-only, symmetric with decode and with
				// the binary serFixedDecimal path) rather than fall through to
				// the size-checked opaque raw arm below. []byte keeps the opaque
				// fall-through.
				if err := rejectNonNumericStructuredString(v, "fixed", "decimal"); err != nil {
					return nil, err
				}
				// The opaque arm below writes exactly node.size bytes, which is
				// what the decoder charges, the same condition
				// appendDecimalFixed applies to the numeric arm above.
				if err := checkDecimalUnscaledSize(node.size); err != nil {
					return nil, &SemanticError{GoType: v.Type(), AvroType: "fixed", Err: err}
				}
			case "duration":
				// Duration.Bytes() always emits 12 bytes, so it is only correct
				// for the spec-required size-12 fixed. A CustomType-resurrected
				// wrong-size duration (mirroring the binary fixed build) falls
				// through to the size-checked raw path below, so JSON encode stays
				// raw and self-readable.
				if node.size == 12 && v.Type() == avroDurationType {
					raw := v.Interface().(Duration).Bytes()
					return appendAvroJSONBytes(buf, raw[:]), nil
				}
			case "uuid":
				// Only emit the 16-byte UUID form for the spec-required size-16
				// fixed; a CustomType-resurrected wrong-size uuid (mirroring the
				// binary fixed build) breaks to the size-checked raw path below,
				// so JSON encode stays raw and self-readable.
				if node.size != 16 {
					break
				}
				// [16]byte trusts its bytes (uuidBytes-first, matching binary
				// serFixedUUIDReflect): the raw 16 bytes are the wire form, with
				// no MarshalText and parseUUID round trip. Without this, a
				// [16]byte type that also implements TextMarshaler, such as
				// google/uuid.UUID, diverged from the binary path.
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
		}
		var raw []byte
		if v.Kind() == reflect.String {
			if err := rejectJSONNumberRawTarget(v, "fixed"); err != nil {
				return nil, err
			}
			// A Go string is raw UTF-8 bytes here, matching serSize on
			// the binary side. See the bytes-string arm above for the
			// full rationale on why codepoint mapping was wrong here.
			// We alias v's bytes; downstream consumers iterate read-only.
			s := v.String()
			raw = unsafe.Slice(unsafe.StringData(s), len(s))
		} else if v.Kind() == reflect.Array && v.Type().Elem().Kind() == reflect.Uint8 {
			raw = byteArrayToSlice(v)
		} else if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
			raw = v.Bytes()
		} else {
			return nil, semErr(v, "fixed")
		}
		if len(raw) != node.size {
			// The same failure of your value serSize rejects on binary; both
			// wires carry *SemanticError identity, and the JSON message keeps
			// the got/need detail in the chain.
			return nil, &SemanticError{GoType: v.Type(), AvroType: "fixed", Err: fmt.Errorf("size mismatch: got %d bytes, need %d", len(raw), node.size)}
		}
		return appendAvroJSONBytes(buf, raw), nil

	case "enum":
		// Builtin string fast path, parity with serEnum: an unnamed string is
		// text-less, so it *is* the symbol and we skip the textValue probe. A
		// named string type falls through to textValue, so uniformity holds.
		if v.Type() == stringType {
			needle := v.String()
			if _, ok := node.symbolIndex(needle); ok {
				return appendJSONString(buf, needle), nil
			}
			// A value naming no symbol is the same user-value failure the
			// binary encoder rejects (serEnum); both wires report it as an
			// errors.As-able *SemanticError so you get one error identity per
			// failure regardless of wire format. Decode-side wire-content
			// errors, a bad ordinal or an unknown wire symbol, are plain on
			// both wires, a separate family.
			return nil, &SemanticError{GoType: v.Type(), AvroType: "enum", Err: fmt.Errorf("unknown enum symbol %q", truncForError(needle))}
		}
		// We try text-out first, for uniformity and name-based matching, then
		// a named string without a text method, then the int-ordinal arm.
		if text, ok, err := textValue(v, "enum"); err != nil {
			return nil, err
		} else if ok {
			if _, ok := node.symbolIndex(text); ok {
				return appendJSONString(buf, text), nil
			}
			return nil, &SemanticError{GoType: v.Type(), AvroType: "enum", Err: fmt.Errorf("unknown enum symbol %q", truncForError(text))}
		}
		if v.Kind() == reflect.String {
			// See serEnum.ser: json.Number (Kind reflect.String) is a numeric
			// carrier, and a stringy enum target is a type mismatch, rejected
			// on both wire formats, symmetric with the decoder.
			if err := rejectJSONNumberRawTarget(v, "enum"); err != nil {
				return nil, err
			}
			needle := v.String()
			if _, ok := node.symbolIndex(needle); ok {
				return appendJSONString(buf, needle), nil
			}
			return nil, &SemanticError{GoType: v.Type(), AvroType: "enum", Err: fmt.Errorf("unknown enum symbol %q", truncForError(needle))}
		}
		if v.CanInt() || v.CanUint() {
			n, err := enumOrdinalIndex(v, len(node.symbols))
			if err != nil {
				return nil, &SemanticError{GoType: v.Type(), AvroType: "enum", Err: err}
			}
			return appendJSONString(buf, node.symbols[n]), nil
		}
		return nil, semErr(v, "enum")

	case "array":
		if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
			return nil, semErr(v, "array")
		}
		// Native concrete fast path: a plain primitive item and an unnamed []V
		// slice. We fall through for logical items, [N]T arrays and named slice
		// or elem types.
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
		// Native concrete fast path: a plain, non-logical primitive value and
		// an exactly-string key mean the whole map is a known unnamed type, so
		// we assert it and emit natively, with no reflect.MapRange. We fall
		// through to reflect for logical-typed values (date, time and uuid
		// serialize specially), named map or value types, and non-interfaceable
		// maps.
		if node.values.logical == "" && custom[node.values] == nil && v.Type().Key() == stringType && v.CanInterface() {
			if out, ok := appendAvroJSONNativeMap(buf, v, node.values.kind, cfg); ok {
				return out, nil
			}
		}
		buf = append(buf, '{')
		first := true
		keyType := v.Type().Key()
		// We reuse addressable Values to avoid the per-entry alloc of
		// iter.Key() and iter.Value(); see appendMapPrimitive (ser.go).
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
		// depth unchanged: same-level dispatch hop (see the union case).
		return appendAvroJSONRecord(buf, v, node, cfg, custom, depth)

	// "union" is dispatched before the peel loop above (un-peeled value).

	default:
		return nil, fmt.Errorf("avro json: unsupported schema kind %q", node.kind)
	}
}

// appendJSONNativeStringMap ranges a concrete map[string]V with no
// reflect.MapRange and no per-entry Value. The native range dominates the win,
// so emit's indirection is negligible here, cooler than binary, and it keeps
// one loop shape for all value types.
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

// appendAvroJSONNativeMap emits a plain-primitive-valued map[string]V natively,
// the same output appendAvroJSON would produce. ok is false, buf untouched,
// when v's dynamic type isn't the unnamed map[string]V for kind, and the caller
// falls back to reflect. We only reach it when node.values has no logical type
// (logical values serialize specially) and no custom codec (custom values route
// through the per-element path), and the key is exactly string.
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
// false, buf untouched, when v's dynamic type isn't the unnamed []V for kind.
// We only reach it when node.items has no logical type and no custom codec,
// and v is a slice, not [N]T.
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

// appendJSONFieldDefault appends a missing record field's default: JSON null
// for a nil defaultVal, otherwise recursive appendAvroJSON, so encoder
// options apply to defaults too. It errors when the field has no default.
// The recursion passes a nil custom map, bypassing CustomType.Encode as the
// binary encodeDefault does, since defaults are already Avro-native. A
// union default dispatches with the same declaration-order try-each as
// encodeDefault, so the tagged form names the branch the binary
// defaultBytes encode against; the runtime type-name fast path would name
// a different one for [enum, string] default "A".
func appendJSONFieldDefault(buf []byte, f fieldNode, cfg *optConfig, depth int) ([]byte, error) {
	if !f.hasDefault {
		// The callers wrap through recordFieldError, so this renders with
		// the field path and *SemanticError identity, the same "missing
		// key" construction the binary record loops build.
		return nil, errors.New("missing key")
	}
	if f.defaultVal == nil {
		return append(buf, "null"...), nil
	}
	// Binary's encodeDefault takes no custom parameter for the same reason;
	// matching that keeps Encode/EncodeJSON parity for default-fill.
	if f.node != nil && f.node.kind == "union" {
		v := reflect.ValueOf(f.defaultVal)
		for _, branch := range f.node.branches {
			// We select the branch as binary encodeDefault does.
			// appendAvroJSON success alone is too lenient: its bytes/fixed
			// arm writes a default string as raw UTF-8, so it would claim a
			// codepoint>255 default that binary passes to a later branch.
			if _, err := encodeDefault(nil, f.defaultVal, branch); err != nil {
				continue
			}
			encoded, err := appendAvroJSON(nil, v, branch, cfg, nil, depth+1)
			if err != nil {
				return nil, err
			}
			return appendUnionBranch(buf, f.node, branch, encoded, cfg), nil
		}
		return nil, fmt.Errorf("avro json: union default for field %q does not match any branch", truncForError(f.name))
	}
	return appendAvroJSON(buf, reflect.ValueOf(f.defaultVal), f.node, cfg, nil, depth+1)
}

func appendAvroJSONRecord(buf []byte, v reflect.Value, node *schemaNode, cfg *optConfig, custom map[*schemaNode]*customWiring, depth int) ([]byte, error) {
	buf = append(buf, '{')
	if v.Kind() == reflect.Map {
		if v.Type().Key().Kind() != reflect.String {
			return nil, semErr(v, "record")
		}
		// Input keys must match the schema's canonical field names.
		// Aliases are a reader-side decode concept, not relevant on
		// encode: we are the writer and our output uses our schema's
		// canonical names. map[string]any fast path: MapIndex allocates
		// via reflect.copyVal for each interface{} element, and we skip
		// that with a direct lookup.
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
					buf, err = appendJSONFieldDefault(buf, f, cfg, depth)
					if err != nil {
						return nil, recordFieldError(v.Type(), f.name, err)
					}
				} else {
					buf, err = appendAvroJSON(buf, reflect.ValueOf(value), f.node, cfg, custom, depth+1)
					if err != nil {
						return nil, recordFieldError(v.Type(), f.name, err)
					}
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
				buf, err = appendJSONFieldDefault(buf, f, cfg, depth)
				if err != nil {
					return nil, recordFieldError(v.Type(), f.name, err)
				}
			} else {
				buf, err = appendAvroJSON(buf, value, f.node, cfg, custom, depth+1)
				if err != nil {
					return nil, recordFieldError(v.Type(), f.name, err)
				}
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
			fv := fieldByIndexZero(v, mapping.indices[i])
			// We honor omitzero through the shared omitzeroAction, the same
			// decision ser.go's binary site makes. A zero or IsZero value
			// emits the field's default, else JSON `null` for a nullable
			// field, else nothing, falling through to encode the zero.
			// avroType and hasDefault live on serRecord.fields[i],
			// parallel-indexed to node.fields[i].
			if mapping.omitzero[i] && valueIsZero(fv) {
				switch node.serRecord.fields[i].omitzeroAction() {
				case ozDefault:
					buf, err = appendJSONFieldDefault(buf, f, cfg, depth)
					if err != nil {
						return nil, recordFieldError(v.Type(), f.name, err)
					}
					continue
				case ozNull:
					buf = append(buf, "null"...)
					continue
				}
				// ozNoop: fall through to the normal field encoder.
			}
			buf, err = appendAvroJSON(buf, fv, f.node, cfg, custom, depth+1)
			if err != nil {
				return nil, recordFieldError(v.Type(), f.name, err)
			}
		}
	} else {
		return nil, semErr(v, "record")
	}
	return append(buf, '}'), nil
}

// appendUnionBranch wraps encoded in the tagged form {type_name: value} when
// cfg.tagged is set and the branch is non-null, holding the "wrap iff
// non-null" rule in one place for the four dispatcher sites. The spec
// defines a union null as bare null, as Java's JsonEncoder.writeIndex does.
func appendUnionBranch(buf []byte, union, branch *schemaNode, encoded []byte, cfg *optConfig) []byte {
	if cfg.tagged && branch.kind != "null" {
		return appendTaggedUnion(buf, union, branch, encoded, cfg.tagLogical)
	}
	return append(buf, encoded...)
}

func appendAvroJSONUnion(buf []byte, v reflect.Value, node *schemaNode, cfg *optConfig, custom map[*schemaNode]*customWiring, depth int) ([]byte, error) {
	if depth >= maxDepth {
		return nil, errTooDeep
	}

	// We peel pointer and interface layers for the tagged-map detection below,
	// mirroring binary serUnion.tryUnwrapTagged. v itself stays un-peeled. The
	// try-each loop then hands the original value to branch encoders, so a
	// branch's custom encoder with a pointer GoType matches at the pointer
	// level. isNilValue and unionTypeNameForValue peel internally on their own,
	// so only the tagged-map check needs a peeled view here.
	tv := v
	for range maxIndirectDepth {
		if tv.Kind() != reflect.Pointer && tv.Kind() != reflect.Interface {
			break
		}
		if tv.IsNil() {
			break
		}
		tv = tv.Elem()
	}

	// We accept tagged union maps, {"typeName": value}, matching the Avro JSON
	// convention and what binary Encode does.
	if tv.Kind() == reflect.Map && tv.Len() == 1 {
		iter := tv.MapRange()
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
				return appendUnionBranch(buf, node, branch, encoded, cfg), nil
			}
		}
	}

	// Nil-first dispatch: a nil-equivalent v takes the null branch whatever
	// the arity, as the binary serNullUnionAt and serUnion.ser do, so a nil
	// []byte against ["null","bytes"] emits null rather than empty bytes.
	if isNilValue(v) {
		if branch := unionBranchOfKind(node, "null"); branch != nil {
			return appendUnionBranch(buf, node, branch, []byte("null"), cfg), nil
		}
	}

	// Type-name dispatch, for parity with Java, fastavro and hamba: if v's Go
	// type has a canonical Avro primitive name and exactly one branch matches,
	// we prefer it over try-each, mirroring serUnion.ser. We fall through to
	// try-each on no-match or on encode failure, such as a numeric value that
	// needs promotion via the encoder's lenient arms, since try-each preserves
	// those paths.
	if name := unionTypeNameForValue(v); name != "" {
		if branch := unionBranchOfKind(node, name); branch != nil {
			// Only one branch has this kind, so there is nothing to fall
			// through *to*: a failure here goes straight to try-each.
			encoded, err := appendAvroJSON(nil, v, branch, cfg, custom, depth+1)
			if err == nil {
				return appendUnionBranch(buf, node, branch, encoded, cfg), nil
			}
			if errors.Is(err, errTooDeep) {
				return nil, err
			}
		}
	}

tryAll:
	// We try each branch, mirroring serUnion.ser; v is non-nil here, since
	// the nil-first dispatch ran. Null handling is arity-dependent so a
	// wildcard custom encode hook fires the same number of times on both
	// paths: the binary 2-branch fast path never trials null for a non-nil
	// value, so we skip it too, while the N>=3 try-each does trial it. We
	// keep the last concrete error so the final message names the closest
	// reason.
	var lastErr error
	for _, branch := range node.branches {
		if len(node.branches) == 2 && branch.kind == "null" {
			continue
		}
		encoded, err := appendAvroJSON(nil, v, branch, cfg, custom, depth+1)
		if err == nil {
			return appendUnionBranch(buf, node, branch, encoded, cfg), nil
		}
		// We propagate too-deep without trying further branches. The trial
		// loop would otherwise mask the recursion limit error behind a
		// misleading "no branch matched".
		if errors.Is(err, errTooDeep) {
			return nil, err
		}
		lastErr = err
	}
	// No-match identity mirrors the binary dispatch split: a 2-branch null
	// union returns the value branch's own error unwrapped, as
	// serNullUnionAt does, and every other shape wraps in the union's own
	// *SemanticError, as serUnion.ser does.
	if len(node.branches) == 2 &&
		(node.branches[0].kind == "null" || node.branches[1].kind == "null") &&
		lastErr != nil {
		return nil, lastErr
	}
	e := &SemanticError{GoType: v.Type(), AvroType: "union"}
	if lastErr != nil {
		e.Err = fmt.Errorf("avro json: no union branch matched: %w", lastErr)
	} else {
		e.Err = errors.New("avro json: no union branch matched")
	}
	return nil, e
}

// unionBranchName returns the Avro JSON type name for a union branch: a
// named type answers to its declared name, everything else to its kind.
// [branchIsNamedKind] is the one place that set of kinds is written down.
func unionBranchName(node *schemaNode) string {
	if branchIsNamedKind(node) {
		return node.name
	}
	return node.kind
}

// unionBranchNames returns a branch's standard and logical names. The
// "<kind>.<logicalType>" qualifier applies only to a primitive-backed
// logical; a named fixed carrying a logical keeps its declared name as both,
// as goavro and Java emit it.
func unionBranchNames(node *schemaNode) (standard, logical string) {
	standard = unionBranchName(node)
	// standard != node.kind exactly when the branch is a named type
	// (record/enum/fixed), whose name supersedes any logical qualifier.
	if node.logical != "" && standard == node.kind {
		logical = node.kind + "." + node.logical
	} else {
		logical = standard
	}
	return standard, logical
}

// unionLogicalTagOwnedElsewhere reports whether ln, the "<kind>.<logicalType>"
// qualifier computed for branch i, is some other branch's exact name: a
// fixed named "decimal" in namespace "bytes" has the fullname
// "bytes.decimal". An exact branch name outranks a logical qualifier, so
// emitting the qualifier would hand the decoder a tag it routes to the other
// branch; the colliding branch falls back to its unqualified name instead.
func unionLogicalTagOwnedElsewhere(standard []string, i int, ln string) bool {
	for j, bn := range standard {
		if j != i && bn == ln {
			return true
		}
	}
	return false
}

func unionStandardNames(branches []*schemaNode) []string {
	out := make([]string, len(branches))
	for i, b := range branches {
		if b != nil {
			out[i] = unionBranchName(b)
		}
	}
	return out
}

// unionEmitTag returns the tag branch emits inside union, under tagLogical.
// It is the one source of truth for the JSON encoder; the binary side reads
// the equivalent value out of deserUnion.logicalNames, which fillUnionTagTables
// builds with this same rule.
func unionEmitTag(union, branch *schemaNode, tagLogical bool) string {
	bn, ln := unionBranchNames(branch)
	if !tagLogical || ln == bn {
		return bn
	}
	for _, b := range union.branches {
		if b != nil && b != branch && unionBranchName(b) == ln {
			return bn
		}
	}
	return ln
}

func appendTaggedUnion(buf []byte, union, branch *schemaNode, encoded []byte, tagLogical bool) []byte {
	name := unionEmitTag(union, branch, tagLogical)
	buf = append(buf, '{')
	buf = appendJSONString(buf, name)
	buf = append(buf, ':')
	buf = append(buf, encoded...)
	return append(buf, '}')
}

// unionTagTier is one tier of the union tag namespace: the rule by which a
// tag you wrote names a branch. findUnionBranch resolves a name through the
// tiers and fillUnionTagTables builds the binary tagged-map lookup through
// them, so adding a tier here reaches both.
type unionTagTier struct {
	name string
	// guarded marks a tier where a name two branches could claim resolves
	// *nowhere* instead of to the first of them. Silently picking one is a
	// coin flip between two branches you may have meant either of.
	guarded bool
	// sep joins head and tail into the claimed name. Splitting the name into
	// pieces instead of returning it built is what keeps resolution
	// allocation-free: matching compares the pieces in place, and only the
	// parse-time table build ever joins them. Returning a built string, or
	// appending into your buffer, both put an allocation on a per-value JSON
	// path, the buffer because it escapes through this indirect call.
	sep string
	// claim reports the name this tier makes b answer to, as head+sep+tail,
	// and whether the tier applies to b at all. The empty string is a legal
	// branch name, an empty-named record under a [WithLaxNames] validator,
	// so applicability cannot be signalled by an empty result.
	claim func(b *schemaNode) (head, tail string, ok bool)
}

func tierMatches(tier unionTagTier, b *schemaNode, name string) bool {
	head, tail, ok := tier.claim(b)
	if !ok || len(name) != len(head)+len(tier.sep)+len(tail) {
		return false
	}
	return name[:len(head)] == head &&
		name[len(head):len(head)+len(tier.sep)] == tier.sep &&
		name[len(head)+len(tier.sep):] == tail
}

// tierClaim builds b's claim under tier. Parse-time only.
func tierClaim(tier unionTagTier, b *schemaNode) (string, bool) {
	head, tail, ok := tier.claim(b)
	if !ok {
		return "", false
	}
	return head + tier.sep + tail, true
}

// unionTagTiers is the tag namespace, most specific first.
var unionTagTiers = []unionTagTier{
	{
		// The branch's own name: its fullname for a named type, its kind
		// otherwise. Two branches cannot share one, since that is a
		// duplicate union type and the parse refuses it, so first match
		// is exact.
		name: "exact name",
		claim: func(b *schemaNode) (string, string, bool) {
			return unionBranchName(b), "", true
		},
	},
	{
		// "<kind>.<logicalType>", the goavro spelling TagLogicalTypes emits
		// for a primitive-backed logical. We accept it for a named fixed
		// carrying a logical too. Matching the (kind, logicalType) pair
		// rather than the kind alone keeps [long, {long, timestamp-millis}]
		// from misrouting.
		name:    "logical qualifier",
		guarded: true,
		sep:     ".",
		claim: func(b *schemaNode) (string, string, bool) {
			if b.logical == "" {
				return "", "", false
			}
			switch b.kind {
			case "null", "boolean", "int", "long", "float", "double", "string", "bytes", "fixed":
				return b.kind, b.logical, true
			}
			return "", "", false
		},
	},
	{
		// Unqualified short name for a namespaced named type, a leniency of
		// ours for hand-written input; no reference emits or reads it.
		// Only a named type has a short form of a name; the set is
		// [branchIsNamedKind]'s, the same one the exact tier reaches
		// through unionBranchName.
		name:    "unqualified short name",
		guarded: true,
		claim: func(b *schemaNode) (string, string, bool) {
			if !branchIsNamedKind(b) {
				return "", "", false
			}
			return unqualified(b.name), "", true
		},
	},
}

// findUnionBranch finds a union branch by type name, accepting three tag
// conventions in order: the exact fullname, which is what we emit and the
// only form Java and fastavro read; goavro's "type.logicalType" form; and an
// unqualified short name for a named type, a leniency of ours for
// hand-written JSON, applied only when exactly one branch matches.
func findUnionBranch(union *schemaNode, name string) *schemaNode {
	// unionTags.byName is the tier walk below applied once at parse time,
	// which keeps this constant per value. A hand-assembled node with no
	// table falls through to the walk.
	if union.tags != nil {
		i, ok := union.tags.byName[name]
		if !ok {
			return nil
		}
		return union.branches[i]
	}
	return scanUnionBranch(union, name)
}

// unionBranchOfKind returns the union's first branch of the given Avro kind, or
// nil. It reads the same table serUnion.ser dispatches through
// (unionTags.byKind), so the two encoders route a Go value's canonical type
// name to one branch; a table-less node takes the scan for the same answer.
func unionBranchOfKind(union *schemaNode, kind string) *schemaNode {
	// Same shape as findUnionBranch: a table, when present, *is* the answer.
	// A miss there is a miss, not a reason to scan.
	if union.tags != nil {
		i, ok := union.tags.byKind[kind]
		if !ok {
			return nil
		}
		return union.branches[i]
	}
	for _, b := range union.branches {
		if b != nil && b.kind == kind {
			return b
		}
	}
	return nil
}

// scanUnionBranch answers findUnionBranch's question by walking unionTagTiers
// directly. It is the tier rule in executable form, the table
// fillUnionTagTables builds being this walk's result, and it stays the fallback
// for a table-less node.
func scanUnionBranch(union *schemaNode, name string) *schemaNode {
	for _, tier := range unionTagTiers {
		var match *schemaNode
		for _, b := range union.branches {
			if b == nil || !tierMatches(tier, b, name) {
				continue
			}
			if !tier.guarded {
				return b
			}
			if match != nil {
				return nil // ambiguous within this tier
			}
			match = b
		}
		if match != nil {
			return match
		}
	}
	return nil
}

// parseSpecialFloat parses the NaN and Infinity string forms: Java's exact
// set of "NaN", "Infinity", "INF", "-Infinity" and "-INF", plus Go's "Inf"
// and "-Inf". Every implementation rejects lowercase, and a lowercase 'n'
// would collide with the JSON null literal in the union dispatcher. The
// goavro conventions of null and +/-1e999 are handled in decodeJSONFloat.
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

// jsonEscapeShort returns the second byte of the 2-byte short JSON escape for
// c, so '"' gives '"' and '\n' gives 'n', or 0 if no short escape applies.
// appendAvroJSONBytes and appendJSONString share it so the two hot-path escape
// switches can't drift on the set of short-form bytes the JSON spec defines.
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

// appendAvroJSONBytes encodes raw bytes as an Avro JSON string using ISO-8859-1
// encoding, matching the Java canonical implementation. Printable ASCII bytes
// (0x20-0x7E, except " and \) are written as literal characters; all other
// bytes use \uXXXX escapes.
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

// appendJSONString appends a JSON-encoded string to buf, avoiding the
// allocation a generic string-marshal call would need.
func appendJSONString(buf []byte, s string) []byte {
	buf = append(buf, '"')
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
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
			// Replace an invalid byte with the raw U+FFFD bytes rather than
			// the six-byte escape, so encode is idempotent: a re-decoded
			// escape produces the U+FFFD codepoint, which would re-encode as
			// raw UTF-8.
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

// bytesToAvroJSONString encodes raw bytes into the Avro JSON codepoint form:
// each byte becomes the rune at the same codepoint, so 0x80-0xFF become
// 2-byte UTF-8 sequences. avroJSONBytesToBytes is the inverse for every
// []byte. The metadata render uses it for []byte Props and Defaults, since
// the base64 a generic marshal would produce re-reads as its ASCII bytes.
func bytesToAvroJSONString(b []byte) string {
	// strings.Builder.WriteRune calls utf8.EncodeRune internally and
	// uses unsafe to return the underlying buffer as a string without
	// a final copy. Grow(len(b)) is the all-ASCII lower bound; bytes
	// >= 0x80 take 2 UTF-8 bytes, so the worst-case output is 2*len(b),
	// which append's geometric growth absorbs with at most one realloc.
	var sb strings.Builder
	sb.Grow(len(b))
	for _, v := range b {
		sb.WriteRune(rune(v))
	}
	return sb.String()
}

// appendJSONFloat formats a float for JSON output. NaN and Infinity encode
// as JSON strings, or as null and +/-1e999 with LinkedinFloats. When bits is
// 32, a finite float64 past float32's range narrows to +/-Inf, as Java's
// writeFloat does.
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

// jsonCoerceToFloat64 converts a reflect.Value to float64, accepting float,
// int, uint and json.Number. Encoding into a float schema is lossy by
// destination, as in Java and fastavro. avroType tags failures as a
// SemanticError carrying the field path, as the binary encoder does.
func jsonCoerceToFloat64(v reflect.Value, avroType string) (float64, error) {
	switch {
	case v.CanFloat():
		return v.Float(), nil
	case v.CanInt():
		return float64(v.Int()), nil
	case v.CanUint():
		return float64(v.Uint()), nil
	case v.Type() == jsonNumberType:
		// Route through the shared json.Number to float64 helper, the
		// same predicate binary encode (jsonNumberToFloat) and
		// schema-parse default validation (defaultAsFloat) use.
		f, err := parseJSONNumberAsFloat(string(v.Interface().(json.Number)), 64)
		if err != nil {
			return 0, semErrW(v, avroType, err)
		}
		return f, nil
	}
	return 0, semErr(v, avroType)
}

// jsonCoerceInt converts a reflect.Value to an integer T with the overflow
// and whole-number checks of serInt and serLong. hi bounds the signed range
// and the CanInt path applies the symmetric -hi-1 low bound. avroType tags
// failures as a SemanticError carrying the field path.
func jsonCoerceInt[T int32 | int64](v reflect.Value, hi int64, avroType string,
	floatFits func(float64, int) (T, error),
	parseLenient func(string) (T, error),
) (T, error) {
	if v.CanInt() {
		n := v.Int()
		if n < -hi-1 || n > hi {
			return 0, semErrW(v, avroType, fmt.Errorf("value %d overflows %T", n, T(0)))
		}
		return T(n), nil
	}
	if v.CanUint() {
		n := v.Uint()
		if n > uint64(hi) {
			return 0, semErrW(v, avroType, fmt.Errorf("value %d overflows %T", n, T(0)))
		}
		return T(n), nil
	}
	if v.CanFloat() {
		n, err := floatFits(v.Float(), v.Type().Bits())
		if err != nil {
			return 0, semErrW(v, avroType, err)
		}
		return n, nil
	}
	if v.Type() == jsonNumberType {
		n, err := parseLenient(string(v.Interface().(json.Number)))
		if err != nil {
			return 0, semErrW(v, avroType, err)
		}
		return n, nil
	}
	return 0, semErr(v, avroType)
}

func jsonCoerceToInt32(v reflect.Value) (int32, error) {
	return jsonCoerceInt(v, math.MaxInt32, "int", floatFitsInt32From, parseInt32Lenient)
}

func jsonCoerceToInt64(v reflect.Value) (int64, error) {
	return jsonCoerceInt(v, math.MaxInt64, "long", floatFitsInt64From, parseInt64Lenient)
}
