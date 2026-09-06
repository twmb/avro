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
// it affects; we silently ignore an option that does not apply.
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

// LinkedinFloats encodes NaN as JSON null and ±Infinity as ±1e999 in
// [Schema.EncodeJSON], the linkedin/goavro convention, overriding our default
// of the JSON strings "NaN", "Infinity" and "-Infinity" (the Java convention).
//
// [Schema.DecodeJSON] accepts both conventions for a float or double decoded
// directly or as a tagged union branch ({"float":null} decodes to NaN). Note
// that a NaN inside a *bare* union does not round-trip: it encodes as a bare
// null, and on decode the union's null branch claims that null (or the union
// rejects it if it has no null branch) before we try the float branch. Use
// [TaggedUnions] if you need NaN to round-trip through a union. ±Infinity is
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
// "-Infinity" (the Java convention), or as null and ±1e999 with
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
	// taggedUnions and tagLogicalTypes flags, so we set them here exactly as
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
// so reusing the binary resolver keeps the surface small and correct by
// construction.
func (s *Schema) decodeJSONResolved(src []byte, rv reflect.Value, opts ...Opt) error {
	w := s.resolveWriterRaw
	// The custom-free writer view: this is a pure wire-shape transform, so the
	// intermediate must hold *raw* Avro-native values. The writer's own
	// CustomType decoders would produce Go-domain values the re-encode cannot
	// invert, since a Decode-only custom has no Encode. The reader's customs
	// and your opts apply only in the final resolving s.Decode.
	//
	// TaggedUnions on the intermediate keeps the {"branch": value} envelope, so
	// the re-encode routes each value back to its exact branch. The envelope is
	// the only carrier of the writer's branch choice. Bare, the re-encode
	// re-derives the branch by first-match and silently rewrites branch
	// identity whenever a later branch's value also satisfies an earlier one
	// (two records, two enums sharing a symbol, enum vs string). That changes
	// the decoded value wherever resolution differs per branch. Java's
	// JsonDecoder.readIndex reads the tag straight to the branch index, and
	// this envelope round-trip is the composed-path equivalent. Bare,
	// untagged, writer JSON is unaffected in substance: its decode commits
	// to the documented first-match branch and the envelope then pins that
	// same branch through the re-encode.
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
			// A union without a null branch can't represent nil, so we
			// reject rather than emit "null". So does the binary path
			// (serUnion.ser to tryAll to "no matching branch"), Java
			// (UnresolvedUnionException), fastavro ("do not match") and
			// our own DecodeJSON. An untyped nil is a failure of your
			// value, so it carries *SemanticError identity exactly like
			// the binary entry guard (AppendEncode) and serUnion's
			// no-match wrap. A *typed* nil pointer never reaches here:
			// the peel loop hands it to the branch encoders, which
			// surface the plain indirection sentinel on both wires.
			return nil, &SemanticError{AvroType: "union", Err: errors.New("avro json: nil value for union without a null branch")}
		}
		return nil, &SemanticError{AvroType: node.kind, Err: fmt.Errorf("avro json: nil value for non-nullable type %q", node.kind)}
	}
	// We dispatch unions *before* dereferencing and the custom hook: the
	// branch encoders must receive the un-peeled value so a branch's custom
	// encoder with a pointer or interface GoType matches at the pointer level.
	// Binary serUnion (ser.go) does the same, passing the un-peeled value to
	// the branch serializers, whose customEncode peels and GoType-checks at
	// each level, as does the decode side (decodeKind), which dispatches union
	// before indirectAlloc. A custom type never matches a union container node
	// (applyCustomTypes skips unions), so we do not bypass the custom hook
	// below for unions; unionTypeNameForValue and isNilValue inside
	// appendAvroJSONUnion peel internally for the branch-selection decision.
	if node.kind == "union" {
		// We pass depth unchanged: appendAvroJSONUnion is a same-level
		// dispatch hop, a function split rather than a schema-nesting
		// level, and it recurses into branches at depth+1. Incrementing
		// here too would cost a union 2 depth units per level, halving
		// the bound against binary encode, decode and parse, which all
		// count 1 per level (see ser.go's serUnion). That asymmetry
		// breaks JSON round-trips for values decode takes but encode
		// then rejects.
		return appendAvroJSONUnion(buf, v, node, cfg, custom, depth)
	}
	// We apply the custom encode conversion *before* dereferencing, so a
	// custom type with a pointer GoType (*url.URL, say) matches before we
	// strip the pointer. customEncode (schema.go) peels and checks GoType
	// at each level itself. It returns either the encoded result or a
	// pass-through value dereferenced as far as it peeled. The loop below
	// then turns nil into null and handles any remaining indirection on a
	// pass-through value. The binary path does the same, wrapping the
	// serializer with this same customEncode closure on the un-peeled
	// value (schema.go).
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
			// A nil pointer or interface layer inside a *typed* value:
			// the "null" schema takes it as JSON null, mirroring
			// serNull's isNilValue accept. Every other kind rejects it
			// with the same plain indirection sentinel the binary
			// encoders surface from indirect(), *not* the *SemanticError
			// the untyped-nil entry arms above carry. Unions never reach
			// this loop, dispatching before the peel, so the two arms
			// cannot disagree on a union's nil handling.
			if node.kind == "null" {
				return append(buf, "null"...), nil
			}
			return nil, errIndirectNil
		}
		v = v.Elem()
	}

	switch node.kind {
	case "null":
		// We validate v is nil-equivalent, mirroring serNull on the
		// binary side. A non-nil value into a "null" schema gets
		// errNonNil at top level (Schema.EncodeJSON(42, "null")), as a
		// null-typed record field, and via tagged-union dispatch
		// ({"null": 42} against ["null", T]). Binary serNull (ser.go)
		// returns errNonNil for the same shapes; matching it here keeps
		// EncodeJSON and AppendEncode in parity and prevents silent
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
		// must stay in lockstep on precedence.
		s, err := avroStringValue(v)
		if err != nil {
			return nil, err
		}
		return appendJSONString(buf, s), nil

	case "bytes":
		// Decimal emits the spec form: the underlying two's-complement
		// big-endian unscaled integer as an Avro JSON byte string, code points
		// 0-255 mapping to byte values (Avro 1.12 "Logical Types" plus the
		// bytes/fixed JSON rule), matching Java's JsonEncoder and fastavro's
		// write_bytes. We also take bare numbers on decode, so a 0.33 you
		// hand-edited still feeds DecodeJSON.
		//
		// No decimalRatFor match falls through to the generic
		// string/slice/array targets below. big-decimal (AVRO-4124) wraps the
		// binary inner payload in the same codepoint-string form; binary and
		// JSON share buildBigDecimalPayload to stay in lockstep.
		//
		// We skip the decimal and big-decimal coercion arm exactly when the
		// binary build replaced serBytesDecimal/serBigDecimal with the
		// base-bytes serializer, that is, when a non-wildcard matching
		// CustomType has an Encode (encodeSuppresses =
		// hasMatchingCustomTypeWithEncode). Then we write a value matching the
		// custom GoType as its raw []byte, and the base-bytes targets below
		// reject a non-matching pass-through such as *big.Rat. We gate on the
		// threaded predicate, *not* the runtime proxy custom[node].encode !=
		// nil. A wildcard CustomType (empty LogicalType and AvroType) has an
		// Encode wrapper but is excluded from the binary gate, so it keeps the
		// decimal arm, which accepts *big.Rat, on both paths.
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
			// We treat the Go string as raw UTF-8 bytes, matching serBytes
			// (ser.go's string arm appends the string bytes verbatim).
			// Binary and JSON agree: "é" encodes as c3 a9 on both paths.
			// Defaults don't reach this arm, because convertDefaultBytes
			// (schema.go) already turns JSON-parsed default strings into
			// []byte, so only your runtime input lands here, where the Go
			// convention is UTF-8. appendAvroJSONBytes then maps between
			// codepoint and byte on the wire form. It iterates
			// byte-by-byte without retaining, so we alias v's string data
			// instead of copying.
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
		// Decimal: spec form padded and sign-extended to the schema size.
		// UUID: hex-dash string input parses to 16 bytes, checked before
		// the generic raw extraction so a 36-char string is not rejected
		// as size != 16. Fall-through lands on the generic targets below.
		//
		// We skip *all* logical coercion arms exactly when the binary fixed
		// build replaced serFixedDecimal, serDuration or serFixedUUIDReflect
		// with the base serSize, that is, when a non-wildcard matching
		// CustomType has an Encode (encodeSuppresses =
		// hasMatchingCustomTypeWithEncode). Then we write a value matching the
		// custom GoType as its raw bytes, and a non-matching pass-through falls
		// through to the size-checked base targets below. We gate on the
		// threaded predicate, *not* the runtime proxy custom[node].encode !=
		// nil. A wildcard CustomType has an Encode wrapper but is excluded from
		// the binary gate, so it keeps the logical arm on both paths. Non-fixed
		// logicals (uuid-on-string, timestamp and so on) keep their logical
		// serializer wrapped by the custom encoder on the binary side, so only
		// the fixed arms are gated here.
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
			// binary encoder rejects (serEnum); both wires surface it as an
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
// for a nil defaultVal, otherwise recursive appendAvroJSON. It errors with
// "missing key" when the field has no default. Both map arms of
// appendAvroJSONRecord share it, so the missing-required, nil-default-to-null
// and default sequence agrees. Defaults route through appendAvroJSON rather
// than a pre-marshalled splice, so encoder options apply to them too.
//
// The recursive entries pass a nil custom map, bypassing CustomType.Encode as
// binary's encodeDefault does. Encoders convert a user Go type to Avro-native,
// and defaults are already stored Avro-native, so there is nothing to apply.
//
// Union defaults dispatch with a declaration-order try-each that mirrors the
// binary side's encodeDefault (resolve.go). The runtime appendAvroJSONUnion
// dispatcher uses unionTypeNameForValue, a kind-match fast path. That is right
// for a value you supply, where the Go type names the intended branch, and
// wrong for a stored default. Parse time already chose a branch via the
// declaration-order accept rule, and the JSON tagged-form wrap must name that
// same branch for parity with the binary defaultBytes. Without this,
// [enum, string] default "A" emits {"v":{"string":"A"}} under TaggedUnions
// while validate chose enum and the binary defaultBytes encode against the enum
// branch.
//
// firstUnionBranchAcceptingDefault isn't reusable here because
// convertDefaultBytes has already rewritten string to []byte for bytes and
// fixed branches, and validateDefault's bytes/fixed arm only accepts string.
// Declaration-order try-each on appendAvroJSON itself has the right
// post-convert acceptance set (the bytes/fixed appendAvroJSON arms accept
// []byte) and matches encodeDefault's try-each loop on the binary side
// branch-by-branch.
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
			// We select the branch exactly as binary encodeDefault does, so
			// the JSON wire names the branch Encode, Decode-fill and the
			// metadata API name. appendAvroJSON success alone is too lenient
			// a test. Its bytes/fixed arm writes a default string as raw
			// UTF-8, so it would claim a codepoint>255 default that binary
			// correctly passes to a later branch. A bytes/fixed JSON default
			// maps each codepoint 0-255 to one byte. encodeDefault applies
			// that rule, and accepts both the converted ([]byte) and raw
			// (string) forms.
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
// cfg.tagged is set and the branch is non-null. It holds the "wrap iff
// non-null" invariant in one place, so the four dispatcher sites in
// appendAvroJSONUnion (tagged-form, nil-first, type-name, try-each) can't drift
// on it.
//
// This mirrors Java's JsonEncoder.writeIndex. The spec defines a union null as
// bare `null`, and TaggedUnions documents "wraps non-null union values", so the
// null branch stays bare even under cfg.tagged. Without the guard,
// EncodeJSON((*int)(nil)) emits {"null":null} instead of bare null.
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

	// Nil-first dispatch: if v is nil-equivalent and the union has a null
	// branch, we pick null regardless of arity. This mirrors the binary
	// 2-branch optimization serNullUnionAt (ser.go) and the corresponding
	// serUnion.ser nil-first check. It generalizes "Go nil means absent, so
	// take the null branch" across all union arities, so 2-branch and N-branch
	// agree on what counts as null. Without it, a nil []byte against
	// ["null","bytes"] routes via unionTypeNameForValue to "bytes" and then the
	// bytes branch, emitting empty bytes, while binary 2-branch picks null.
	// That is a binary-vs-JSON parity gap for the 2-branch case and a binary
	// 2-branch-vs-3-branch inconsistency for the N-branch case.
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
	// We try each branch, mirroring serUnion.ser. The case "null" arm of
	// appendAvroJSON rejects non-nil values with errNonNil, so a non-nil v
	// cleanly falls through to the next branch. The nil-first dispatch above
	// already routed nil-equivalent values to the null branch, so v here is
	// non-nil and the null branch can never succeed for a value.
	//
	// Null handling is arity-dependent, mirroring the binary side exactly so a
	// wildcard custom encode hook, installed on every branch including null,
	// fires the same number of times on both paths:
	//   - 2-branch [null,T] or [T,null]: binary dispatches via the
	//     serNullUnionAt fast path (ser.go), which for a non-nil value goes
	//     straight to the non-null branch and *never* trials null. We skip
	//     null here too. Otherwise the wildcard hook on the null node fires
	//     spuriously, and a side-effecting wildcard Encode (the logging or
	//     property-dispatch pattern) runs an extra time on EncodeJSON vs
	//     Encode.
	//   - N>=3: binary uses serUnion.ser's try-each (ser.go), which does trial
	//     the null branch, firing the hook on it, before rejecting the non-nil
	//     value, so JSON must trial null too for parity.
	//
	// We keep the last concrete error so the final message names the closest
	// reason a branch failed, mirroring decodeUnionBare's lastErr plumbing on
	// the decode side and serUnion.ser on the binary encode side.
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
	// No-match identity mirrors the binary dispatch split exactly. A 2-branch
	// null union takes serNullUnionAt on the binary side, which hands a non-nil
	// value straight to the value branch and returns *that* branch's error
	// unwrapped, with AvroType being the branch's own type, so we surface
	// lastErr bare here. Every other union shape goes through serUnion.ser,
	// whose no-match verdict wraps unconditionally in the union's own
	// *SemanticError, never inherited from lastErr's chain: a typed nil's
	// per-branch failure is the plain indirection sentinel, which must not
	// leave the no-match plain.
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
// "<kind>.<logicalType>" qualifier applies only to a primitive-backed logical,
// a branch whose standard name is its kind. A *named* type carrying a logical,
// only a fixed with uuid, decimal or duration, keeps its declared name as both.
// Both tagged-union-producing references emit the fixed's name rather than
// "fixed.<logicalType>": goavro keys the envelope by the codec's
// typeName.fullName, and Java's JsonEncoder by getFullName().
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
// qualifier computed for branch i, is some *other* branch's exact name. The two
// spellings share one tag namespace: "bytes.decimal" is the qualifier for a
// decimal-on-bytes branch and also the fullname of a fixed named "decimal" in
// namespace "bytes". findUnionBranch resolves an exact name before it tries
// the qualifier fallback. Emitting the qualifier there hands the decoder a tag
// it routes to the other branch, and the schema's own output stops decoding
// against the schema that produced it.
//
// The rule this expresses, applied identically by every tag table and by the
// JSON emitter: an exact branch name outranks a logical qualifier. A branch
// whose qualifier is taken falls back to its unqualified name, which is what
// TagLogicalTypes emits for that branch anyway when no logical type is present.
// Nothing else about the union changes: the schema still parses, since it is
// legal Avro and Java accepts it too, and only the tag spelling degrades, only
// for the colliding branch.
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

// unionTagTier is one tier of the union tag namespace: the rule by which a tag
// you wrote names a branch. A tier answers one question: which name it makes
// this branch answer to. Both consumers of the namespace walk this same slice
// in this same order, so we ask the set of tiers rather than restate it.
// findUnionBranch resolves a name through it, and fillUnionTagTables
// (schema.go) builds the binary tagged-map lookup through it. Adding a tier
// here reaches both; adding one by hand inside either reaches neither.
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
		// "<kind>.<logicalType>", the goavro-interop spelling that
		// TagLogicalTypes emits for a primitive-backed logical. We accept it
		// for a named fixed carrying a logical too, where the emitted tag is
		// the fixed's *name* instead: the decoder has always taken this legacy
		// form and the encoder has to take the same input shape.
		//
		// Matching the (kind, logicalType) pair rather than the kind alone is
		// what keeps [long, {long, timestamp-millis}] from misrouting. We keep
		// the kind list here and nowhere else: it is the one set of kinds this
		// spelling is defined for, and `fixed` is in it as the only named kind
		// that can carry a logical type.
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

// findUnionBranch finds a union branch by type name.
//
// We accept three tag conventions on input for cross-implementation interop,
// in order:
//
//  1. Exact match against the spec and Java fullname, "long" or
//     "com.example.User". This is what we emit on output, and the only form
//     the references emit or read. Java's JsonEncoder keys by getFullName().
//     fastavro 1.12.2's json_writer emits the fullname, and its
//     AvroJSONDecoder.read_index exact-matches branch labels, where a
//     short-name tag raises, observed.
//  2. goavro's "type.logicalType" form, "long.timestamp-millis": match the
//     base primitive before the dot.
//  3. Unqualified short-name form for named types, "User" instead of
//     "com.example.User", a leniency of ours for hand-written JSON; no
//     reference implementation emits or reads it, and fastavro's short-name
//     matching exists only in schema *resolution*, match_schemas'
//     unqualified-name tier, not in union-tag decoding. Applied only when the
//     input has no namespace and exactly one branch matches by short name; an
//     ambiguous case returns no match rather than guessing.
func findUnionBranch(union *schemaNode, name string) *schemaNode {
	// The tier walk below is the rule; unionTags.byName is that rule applied
	// once at parse time. Asking the table keeps this O(1) per value: branch
	// count is schema-chosen and this is asked per union value, so a scan
	// multiplies two numbers you picked.
	//
	// A table-less node, hand-assembled and never parsed, falls through to the
	// walk for the same answer computed slowly. No parsed or resolved node
	// reaches that fallback.
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

// parseSpecialFloat parses the NaN and Infinity string forms. We accept Java's
// exact set {"NaN", "Infinity", "INF", "-Infinity", "-INF"}, from JsonDecoder's
// isNaNString and is*InfinityString equality checks, which only ever see
// *quoted* strings. We add Go strconv-style "Inf"/"-Inf". fastavro's accept set
// is the bare-token subset Python json takes: NaN, Infinity, -Infinity, not
// INF/-INF, and it does not parse the quoted forms at all (observed 1.12.2).
// Everything rejects lowercase (Java exact-equals, Python json, goavro), and a
// lowercase 'n' would collide with the JSON null literal in the union
// dispatcher, so case-strictness matters here. The goavro conventions of null
// for NaN and ±1e999 for ±Inf are handled separately by the bare-token and
// number paths in decodeJSONFloat.
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
			// Replace an invalid UTF-8 byte with raw U+FFFD bytes
			// (efbfbd) rather than the literal `�` escape, so
			// encode is idempotent: a re-decoded escape produces
			// the U+FFFD codepoint, which would then re-encode as
			// raw UTF-8, different from the escape. Using raw here
			// makes both paths converge.
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
// codepoint, a 1-byte UTF-8 sequence for 0x00-0x7F and a 2-byte sequence for
// 0x80-0xFF. The Go string this returns serializes as either `\uXXXX`, for
// control chars, or the literal UTF-8 bytes, for printable codepoints. Both
// forms re-parse back to the original codepoint, which [avroJSONBytesToBytes]
// then maps back to the original byte.
//
// Inverse pair with [avroJSONBytesToBytes]: round-trip
// `avroJSONBytesToBytes(bytesToAvroJSONString(b)) == b` for every []byte.
// [jsonSerializableValue] (schema_node.go) uses it to re-emit []byte
// SchemaField.Default / SchemaNode.Props values in the Avro JSON spec's
// codepoint form rather than the default base64 a generic []byte marshal
// would produce. The Avro parser would silently re-read that base64 as the
// literal ASCII bytes of its alphabet.
func bytesToAvroJSONString(b []byte) string {
	// strings.Builder.WriteRune calls utf8.EncodeRune internally and
	// uses unsafe to return the underlying buffer as a string without
	// a final copy. Grow(len(b)) is the all-ASCII lower bound; bytes
	// ≥ 0x80 take 2 UTF-8 bytes, so the worst-case output is 2*len(b),
	// which append's geometric growth absorbs with at most one realloc.
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

// jsonCoerceToFloat64 converts a reflect.Value to float64, accepting float,
// int, uint and json.Number types. Encoding into a float or double schema is
// lossy by destination. An integer input exceeding the mantissa precision
// silently IEEE-rounds, and a float32-target overflow silently narrows to ±Inf
// at your float64-to-float32 cast. Java's GenericDatumWriter.writeFloat and
// writeDouble and fastavro's encoder do the same.
// avroType ("float" or "double") tags coercion failures as a SemanticError so
// EncodeJSON's type-mismatch errors are errors.As-able and carry a dotted field
// path through recordFieldError, matching the binary encoder.
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

// jsonCoerceInt converts a reflect.Value to an integer T, with overflow and
// whole-number checks, mirroring Encode's serInt and serLong coercion. It is
// the shared body of jsonCoerceToInt32 and jsonCoerceToInt64.
//
// hi bounds the signed int range, MaxInt32 for an int32 target and MaxInt64 for
// an int64 target; the CanInt path applies a symmetric -hi-1 lo bound
// (math.MinInt32 or math.MinInt64). floatFits and parseLenient are the matching
// int-narrowed helpers.
//
// avroType ("int" or "long") tags coercion failures as a SemanticError so
// EncodeJSON's type-mismatch errors are errors.As-able and carry a dotted field
// path through recordFieldError, matching the binary encoder.
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
