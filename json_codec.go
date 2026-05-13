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
// convention. [Schema.DecodeJSON] always accepts
// both conventions regardless of this option.
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
// null/±1e999 with [LinkedinFloats]. Standard [encoding/json.Marshal]
// cannot represent these values; use EncodeJSON instead.
//
// EncodeJSON accepts the same Go types as [Schema.Encode]. Map key order in
// the output is non-deterministic, as with [encoding/json.Marshal].
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
	return appendAvroJSON(dst, reflect.ValueOf(v), s.node, &cfg, s.customEncodes, 0)
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
	sl.put()
	return err
}

// appendAvroJSON is the single-pass Avro JSON encoder. It walks
// the Go value via reflect and the schema tree simultaneously, writing
// JSON directly without an intermediate binary encoding step. Handles
// structs, maps, all numeric coercions, time.Time, etc.
func appendAvroJSON(buf []byte, v reflect.Value, node *schemaNode, cfg *optConfig, customEncodes map[*schemaNode]func(reflect.Value) (reflect.Value, error), depth int) ([]byte, error) {
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
			return appendAvroJSON(buf, reflect.Value{}, node, cfg, customEncodes, depth+1)
		}
		v = v.Elem()
	}

	// Apply custom type encode conversion before the type switch.
	if ce := customEncodes[node]; ce != nil {
		var err error
		v, err = ce(v)
		if err != nil {
			return nil, err
		}
	}

	switch node.kind {
	case "null":
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
				ms := int32(t.Hour()*3600000 + t.Minute()*60000 + t.Second()*1000 + t.Nanosecond()/1_000_000)
				return strconv.AppendInt(buf, int64(ms), 10), nil
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
				t := v.Interface().(time.Time)
				d := time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute + time.Duration(t.Second())*time.Second + time.Duration(t.Nanosecond())
				return strconv.AppendInt(buf, d.Microseconds(), 10), nil
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
		f, err := jsonCoerceToFloat64(v, 32)
		if err != nil {
			return nil, err
		}
		return appendJSONFloat(buf, f, 32, cfg), nil

	case "double":
		f, err := jsonCoerceToFloat64(v, 64)
		if err != nil {
			return nil, err
		}
		return appendJSONFloat(buf, f, 64, cfg), nil

	case "string":
		// UUID logical type: [16]byte input canonicalizes to the RFC 4122
		// hex-dash string, matching serUUID on the binary side.
		if node.logical == "uuid" && isUUIDType(v.Type()) {
			var u [16]byte
			reflect.Copy(reflect.ValueOf(&u).Elem(), v)
			return appendJSONString(buf, uuidToString(u)), nil
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
				return nil, &SemanticError{GoType: v.Type(), AvroType: "bytes", Err: err}
			}
			if ok {
				unscaled, err := ratToUnscaled(r, node.scale)
				if err != nil {
					return nil, &SemanticError{GoType: v.Type(), AvroType: "bytes", Err: err}
				}
				if err := checkDecimalPrecision(unscaled, node.precision); err != nil {
					return nil, &SemanticError{GoType: v.Type(), AvroType: "bytes", Err: err}
				}
				return appendAvroJSONBytes(buf, bigIntToBytes(unscaled)), nil
			}
		case "big-decimal":
			r, ok, err := decimalRatFor(v)
			if err != nil {
				return nil, &SemanticError{GoType: v.Type(), AvroType: "bytes", Err: err}
			}
			if ok {
				inner, err := buildBigDecimalPayload(r)
				if err != nil {
					return nil, &SemanticError{GoType: v.Type(), AvroType: "bytes", Err: err}
				}
				return appendAvroJSONBytes(buf, inner), nil
			}
		}
		if v.Kind() == reflect.String {
			// Treat the Go string as raw UTF-8 bytes, matching serBytes
			// (ser.go's string arm appends the string bytes verbatim).
			// Pre-fix this arm parsed the string as codepoint-mapped
			// bytes (0-255 per rune), which diverged from binary: e.g.
			// "é" encoded as c3 a9 in binary but as e9 in JSON. Defaults
			// don't reach this arm — convertDefaultBytes (schema.go)
			// already turns JSON-parsed default strings into []byte, so
			// only runtime user input lands here, where the Go convention
			// is UTF-8. appendAvroJSONBytes then handles the
			// codepoint↔byte mapping on the wire form.
			return appendAvroJSONBytes(buf, []byte(v.String())), nil
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
				return nil, &SemanticError{GoType: v.Type(), AvroType: "fixed", Err: err}
			}
			if ok {
				unscaled, err := ratToUnscaled(r, node.scale)
				if err != nil {
					return nil, &SemanticError{GoType: v.Type(), AvroType: "fixed", Err: err}
				}
				if err := checkDecimalPrecision(unscaled, node.precision); err != nil {
					return nil, &SemanticError{GoType: v.Type(), AvroType: "fixed", Err: err}
				}
				b := bigIntToBytes(unscaled)
				if len(b) > node.size {
					return nil, &SemanticError{GoType: v.Type(), AvroType: "fixed", Err: fmt.Errorf("decimal value requires %d bytes, exceeds fixed size %d", len(b), node.size)}
				}
				out := make([]byte, node.size)
				if len(b) < node.size && len(b) > 0 && b[0]&0x80 != 0 {
					for i := 0; i < node.size-len(b); i++ {
						out[i] = 0xff
					}
				}
				copy(out[node.size-len(b):], b)
				return appendAvroJSONBytes(buf, out), nil
			}
		case "duration":
			if v.Type() == avroDurationType {
				raw := v.Interface().(Duration).Bytes()
				return appendAvroJSONBytes(buf, raw[:]), nil
			}
		case "uuid":
			if v.Kind() == reflect.String {
				u, err := parseUUID(v.String())
				if err != nil {
					return nil, err
				}
				return appendAvroJSONBytes(buf, u[:]), nil
			}
		}
		var raw []byte
		if v.Kind() == reflect.String {
			// Go string → raw UTF-8 bytes, matching serSize on the
			// binary side. See the bytes-string arm above for the full
			// rationale on why codepoint mapping was wrong here.
			raw = []byte(v.String())
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
		if v.Kind() == reflect.String {
			needle := v.String()
			for _, sym := range node.symbols {
				if sym == needle {
					return appendJSONString(buf, needle), nil
				}
			}
			return nil, fmt.Errorf("avro json: unknown enum symbol %q", needle)
		}
		if v.CanInt() || v.CanUint() {
			var n int
			if v.CanInt() {
				n = int(v.Int())
			} else {
				n = int(v.Uint())
			}
			if n < 0 || n >= len(node.symbols) {
				return nil, fmt.Errorf("avro json: enum index %d out of range [0, %d)", n, len(node.symbols))
			}
			return appendJSONString(buf, node.symbols[n]), nil
		}
		return nil, fmt.Errorf("avro json: expected string or integer for enum, got %s", v.Type())

	case "array":
		if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
			return nil, fmt.Errorf("avro json: expected slice/array, got %s", v.Type())
		}
		buf = append(buf, '[')
		for i := range v.Len() {
			if i > 0 {
				buf = append(buf, ',')
			}
			var err error
			buf, err = appendAvroJSON(buf, v.Index(i), node.items, cfg, customEncodes, depth+1)
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
			return nil, &SemanticError{GoType: v.Type(), AvroType: "map"}
		}
		buf = append(buf, '{')
		first := true
		iter := v.MapRange()
		for iter.Next() {
			if !first {
				buf = append(buf, ',')
			}
			first = false
			buf = appendJSONString(buf, iter.Key().String())
			buf = append(buf, ':')
			var err error
			buf, err = appendAvroJSON(buf, iter.Value(), node.values, cfg, customEncodes, depth+1)
			if err != nil {
				return nil, err
			}
		}
		return append(buf, '}'), nil

	case "record":
		return appendAvroJSONRecord(buf, v, node, cfg, customEncodes, depth+1)

	case "union":
		return appendAvroJSONUnion(buf, v, node, cfg, customEncodes, depth+1)

	default:
		return nil, fmt.Errorf("avro json: unsupported schema kind %q", node.kind)
	}
}

// appendAvroJSONRecord handles record encoding for both structs and maps.
func appendAvroJSONRecord(buf []byte, v reflect.Value, node *schemaNode, cfg *optConfig, customEncodes map[*schemaNode]func(reflect.Value) (reflect.Value, error), depth int) ([]byte, error) {
	buf = append(buf, '{')
	if v.Kind() == reflect.Map {
		if v.Type().Key().Kind() != reflect.String {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "record"}
		}
		// map[string]any fast path: MapIndex allocates via reflect.copyVal
		// for each interface{} element; direct lookup skips that.
		if v.Type() == mapStringAnyType {
			m := v.Interface().(map[string]any)
			for i, f := range node.fields {
				if i > 0 {
					buf = append(buf, ',')
				}
				buf = appendJSONString(buf, f.name)
				buf = append(buf, ':')
				val, exists := m[f.name]
				if !exists {
					if !f.hasDefault {
						return nil, fmt.Errorf("avro json: record %q missing required field %q", node.name, f.name)
					}
					// Route the default through appendAvroJSON (not a
					// pre-marshalled splice) so encoder options —
					// TaggedUnions, TagLogicalTypes, LinkedinFloats —
					// apply to defaults the same way they apply to
					// present values. A nil defaultVal is the null
					// encoding (explicit "default": null or implicit
					// ["null", T] union default).
					if f.defaultVal == nil {
						buf = append(buf, "null"...)
						continue
					}
					var err error
					buf, err = appendAvroJSON(buf, reflect.ValueOf(f.defaultVal), f.node, cfg, customEncodes, depth+1)
					if err != nil {
						return nil, err
					}
					continue
				}
				var err error
				buf, err = appendAvroJSON(buf, reflect.ValueOf(val), f.node, cfg, customEncodes, depth+1)
				if err != nil {
					return nil, err
				}
			}
			return append(buf, '}'), nil
		}
		for i, f := range node.fields {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = appendJSONString(buf, f.name)
			buf = append(buf, ':')
			val := v.MapIndex(mapKeyAs(v.Type(), f.nameVal))
			if !val.IsValid() {
				if !f.hasDefault {
					return nil, fmt.Errorf("avro json: record %q missing required field %q", node.name, f.name)
				}
				if f.defaultVal == nil {
					buf = append(buf, "null"...)
					continue
				}
				var err error
				buf, err = appendAvroJSON(buf, reflect.ValueOf(f.defaultVal), f.node, cfg, customEncodes, depth+1)
				if err != nil {
					return nil, err
				}
				continue
			}
			var err error
			buf, err = appendAvroJSON(buf, val, f.node, cfg, customEncodes, depth+1)
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
			buf, err = appendAvroJSON(buf, fv, f.node, cfg, customEncodes, depth+1)
			if err != nil {
				return nil, err
			}
		}
	} else {
		return nil, fmt.Errorf("avro json: expected struct or map for record, got %s", v.Type())
	}
	return append(buf, '}'), nil
}

// appendAvroJSONUnion handles union encoding.
func appendAvroJSONUnion(buf []byte, v reflect.Value, node *schemaNode, cfg *optConfig, customEncodes map[*schemaNode]func(reflect.Value) (reflect.Value, error), depth int) ([]byte, error) {
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
				encoded, err := appendAvroJSON(nil, inner, branch, cfg, customEncodes, depth+1)
				if err != nil {
					if errors.Is(err, errTooDeep) {
						return nil, err
					}
					// Fall through to try-each-branch loop,
					// matching Encode's serUnion behavior.
					goto tryAll
				}
				if cfg.tagged {
					return appendTaggedUnion(buf, branch, encoded, cfg.tagLogical), nil
				}
				return append(buf, encoded...), nil
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
			encoded, err := appendAvroJSON(nil, v, branch, cfg, customEncodes, depth+1)
			if err == nil {
				if cfg.tagged {
					return appendTaggedUnion(buf, branch, encoded, cfg.tagLogical), nil
				}
				return append(buf, encoded...), nil
			}
			if errors.Is(err, errTooDeep) {
				return nil, err
			}
			break // only one branch has this kind; don't waste cycles
		}
	}

tryAll:
	for _, branch := range node.branches {
		if branch.kind == "null" {
			continue
		}
		encoded, err := appendAvroJSON(nil, v, branch, cfg, customEncodes, depth+1)
		if err == nil {
			if cfg.tagged {
				buf = appendTaggedUnion(buf, branch, encoded, cfg.tagLogical)
			} else {
				buf = append(buf, encoded...)
			}
			return buf, nil
		}
		// Propagate too-deep without trying further branches; the
		// trial loop would otherwise mask the recursion limit error
		// behind a misleading "no branch matched".
		if errors.Is(err, errTooDeep) {
			return nil, err
		}
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

// parseSpecialFloat parses NaN/Infinity string representations (Java
// convention and case-insensitive variants per AVRO-4217).
func parseSpecialFloat(s string) (float64, error) {
	if strings.EqualFold(s, "nan") {
		return math.NaN(), nil
	}
	if strings.EqualFold(s, "infinity") || strings.EqualFold(s, "inf") {
		return math.Inf(1), nil
	}
	if strings.EqualFold(s, "-infinity") || strings.EqualFold(s, "-inf") {
		return math.Inf(-1), nil
	}
	return 0, fmt.Errorf("avro json: unknown float value %q", s)
}

// appendAvroJSONBytes encodes raw bytes as an Avro JSON string using
// ISO-8859-1 encoding, matching the Java canonical implementation.
// Printable ASCII bytes (0x20-0x7E, except " and \) are written as
// literal characters. All other bytes use \uXXXX escapes.
func appendAvroJSONBytes(buf []byte, b []byte) []byte {
	buf = append(buf, '"')
	for _, c := range b {
		switch c {
		case '"':
			buf = append(buf, '\\', '"')
		case '\\':
			buf = append(buf, '\\', '\\')
		case '\b':
			buf = append(buf, '\\', 'b')
		case '\t':
			buf = append(buf, '\\', 't')
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\f':
			buf = append(buf, '\\', 'f')
		case '\r':
			buf = append(buf, '\\', 'r')
		default:
			if c >= 0x20 && c <= 0x7E {
				buf = append(buf, c)
			} else {
				buf = append(buf, '\\', 'u', '0', '0', jsonHex[c>>4], jsonHex[c&0xf])
			}
		}
	}
	return append(buf, '"')
}

const jsonHex = "0123456789abcdef"

// appendJSONString appends a JSON-encoded string to buf, escaping as needed.
// This avoids the allocation that json.Marshal(s) would require. It escapes
// control characters, U+2028/U+2029 (for JavaScript safety), and replaces
// invalid UTF-8 with U+FFFD, matching encoding/json behavior.
func appendJSONString(buf []byte, s string) []byte {
	buf = append(buf, '"')
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			// ASCII fast path.
			switch c {
			case '"':
				buf = append(buf, '\\', '"')
			case '\\':
				buf = append(buf, '\\', '\\')
			case '\b':
				buf = append(buf, '\\', 'b')
			case '\t':
				buf = append(buf, '\\', 't')
			case '\n':
				buf = append(buf, '\\', 'n')
			case '\f':
				buf = append(buf, '\\', 'f')
			case '\r':
				buf = append(buf, '\\', 'r')
			default:
				if c < 0x20 {
					buf = append(buf, '\\', 'u', '0', '0', jsonHex[c>>4], jsonHex[c&0xf])
				} else {
					buf = append(buf, c)
				}
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
// to raw bytes.
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

// appendJSONFloat formats a float for JSON output, handling special values.
// With LinkedinFloats, NaN encodes as null and ±Infinity as ±1e999 (goavro
// convention). Otherwise NaN/Infinity encode as JSON strings (Java convention).
func appendJSONFloat(buf []byte, f float64, bits int, cfg *optConfig) []byte {
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
// float, int, uint, and json.Number types. bitSize is the target float
// size (32 or 64) — integer values exceeding the mantissa precision are
// rejected to avoid silent precision loss on round-trip.
func jsonCoerceToFloat64(v reflect.Value, bitSize int) (float64, error) {
	precLimit := int64(1) << 53
	if bitSize == 32 {
		precLimit = 1 << 24
	}
	var f float64
	switch {
	case v.CanFloat():
		f = v.Float()
	case v.CanInt():
		n := v.Int()
		if n < -precLimit || n > precLimit {
			return 0, fmt.Errorf("avro json: integer %d overflows float%d exact precision", n, bitSize)
		}
		f = float64(n)
	case v.CanUint():
		n := v.Uint()
		if n > uint64(precLimit) {
			return 0, fmt.Errorf("avro json: integer %d overflows float%d exact precision", n, bitSize)
		}
		f = float64(n)
	case v.Type() == jsonNumberType:
		var err error
		f, err = v.Interface().(json.Number).Float64()
		if err != nil {
			return 0, fmt.Errorf("avro json: invalid json.Number for float: %w", err)
		}
	default:
		return 0, fmt.Errorf("avro json: expected numeric, got %s", v.Type())
	}
	// Narrowing float64 → float32 must not silently clamp to ±Inf.
	// Allow ±Inf and NaN pass-through (they have dedicated JSON encodings).
	if bitSize == 32 && finiteFloat32Overflows(f) {
		return 0, fmt.Errorf("avro json: value %g overflows float32", f)
	}
	return f, nil
}

// jsonCoerceToInt32 converts a reflect.Value to int32, with overflow
// and whole-number checks. Mirrors Encode's serInt coercion.
func jsonCoerceToInt32(v reflect.Value) (int32, error) {
	if v.CanInt() {
		n := v.Int()
		if n < math.MinInt32 || n > math.MaxInt32 {
			return 0, fmt.Errorf("avro json: value %d overflows int32", n)
		}
		return int32(n), nil
	}
	if v.CanUint() {
		n := v.Uint()
		if n > math.MaxInt32 {
			return 0, fmt.Errorf("avro json: value %d overflows int32", n)
		}
		return int32(n), nil
	}
	if v.CanFloat() {
		n, err := floatFitsInt32From(v.Float(), v.Type().Bits())
		if err != nil {
			return 0, fmt.Errorf("avro json: %w", err)
		}
		return n, nil
	}
	if v.Type() == jsonNumberType {
		jn := v.Interface().(json.Number)
		if n, err := jn.Int64(); err == nil {
			if n < math.MinInt32 || n > math.MaxInt32 {
				return 0, fmt.Errorf("avro json: value %s overflows int32", jn)
			}
			return int32(n), nil
		}
		f, err := jn.Float64()
		if err != nil {
			return 0, fmt.Errorf("avro json: invalid json.Number for int: %s", jn)
		}
		n, err := floatFitsInt32(f)
		if err != nil {
			return 0, fmt.Errorf("avro json: %w", err)
		}
		return n, nil
	}
	return 0, fmt.Errorf("avro json: expected integer, got %s", v.Type())
}

// jsonCoerceToInt64 converts a reflect.Value to int64, with overflow
// and whole-number checks. Mirrors Encode's serLong coercion.
func jsonCoerceToInt64(v reflect.Value) (int64, error) {
	if v.CanInt() {
		return v.Int(), nil
	}
	if v.CanUint() {
		n := v.Uint()
		if n > math.MaxInt64 {
			return 0, fmt.Errorf("avro json: value %d overflows int64", n)
		}
		return int64(n), nil
	}
	if v.CanFloat() {
		n, err := floatFitsInt64From(v.Float(), v.Type().Bits())
		if err != nil {
			return 0, fmt.Errorf("avro json: %w", err)
		}
		return n, nil
	}
	if v.Type() == jsonNumberType {
		jn := v.Interface().(json.Number)
		if n, err := jn.Int64(); err == nil {
			return n, nil
		}
		f, err := jn.Float64()
		if err != nil {
			return 0, fmt.Errorf("avro json: invalid json.Number for long: %s", jn)
		}
		n, err := floatFitsInt64(f)
		if err != nil {
			return 0, fmt.Errorf("avro json: %w", err)
		}
		return n, nil
	}
	return 0, fmt.Errorf("avro json: expected integer, got %s", v.Type())
}
