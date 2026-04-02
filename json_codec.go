package avro

import (
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
// In [Schema.Decode] and [Schema.DecodeJSON] to *any, this wraps
// union values as map[string]any{branchName: value}.
//
// Without this option, union values are bare in all cases.
// [Schema.DecodeJSON] and [Schema.Encode] always accept both tagged
// and bare union input regardless of this option.
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
func (s *Schema) EncodeJSON(v any, opts ...Opt) ([]byte, error) {
	return s.AppendEncodeJSON(nil, v, opts...)
}

// AppendEncodeJSON is like [Schema.EncodeJSON] but appends to dst.
func (s *Schema) AppendEncodeJSON(dst []byte, v any, opts ...Opt) ([]byte, error) {
	cfg := parseOpts(opts)
	return appendAvroJSON(dst, reflect.ValueOf(v), s.node, &cfg, s.customEncodes)
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
		customDecoders: s.customDecoders,
		customSNs:      s.customSNs,
		wrapUnions:     cfg.tagged,
		qualifyLogical: cfg.tagLogical,
	}
	err := ctx.decodeValue(rv.Elem(), s.node)
	slabPool.Put(sl)
	return err
}

// appendAvroJSON is the single-pass Avro JSON encoder. It walks
// the Go value via reflect and the schema tree simultaneously, writing
// JSON directly without an intermediate binary encoding step. Handles
// structs, maps, all numeric coercions, time.Time, etc.
func appendAvroJSON(buf []byte, v reflect.Value, node *schemaNode, cfg *optConfig, customEncodes map[*schemaNode]func(reflect.Value) (reflect.Value, error)) ([]byte, error) {
	// Handle nil / invalid values.
	if !v.IsValid() {
		if node.kind == "null" || node.kind == "union" {
			return append(buf, "null"...), nil
		}
		return nil, fmt.Errorf("avro json: nil value for non-nullable type %q", node.kind)
	}
	// Dereference pointers and interfaces.
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return appendAvroJSON(buf, reflect.Value{}, node, cfg, customEncodes)
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
				return strconv.AppendInt(buf, int64(timeToDate(t)), 10), nil
			case "time-millis":
				d := time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute + time.Duration(t.Second())*time.Second + time.Duration(t.Nanosecond())
				ms, err := durationToTimeMillis(d)
				if err != nil {
					return nil, err
				}
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
		if v.CanInt() {
			return strconv.AppendInt(buf, v.Int(), 10), nil
		}
		if v.CanUint() {
			return strconv.AppendInt(buf, int64(v.Uint()), 10), nil
		}
		if v.CanFloat() {
			return strconv.AppendInt(buf, int64(v.Float()), 10), nil
		}
		return nil, fmt.Errorf("avro json: expected integer, got %s", v.Type())

	case "long":
		if v.Type() == timeType {
			t := v.Interface().(time.Time)
			switch node.logical {
			case "timestamp-millis", "local-timestamp-millis":
				return strconv.AppendInt(buf, timeToTimestampMillis(t), 10), nil
			case "timestamp-micros", "local-timestamp-micros":
				return strconv.AppendInt(buf, timeToTimestampMicros(t), 10), nil
			case "timestamp-nanos", "local-timestamp-nanos":
				return strconv.AppendInt(buf, timeToTimestampNanos(t), 10), nil
			default:
				return strconv.AppendInt(buf, timeToTimestampMillis(t), 10), nil
			}
		}
		if v.Type() == durationType {
			d := v.Interface().(time.Duration)
			switch node.logical {
			case "time-micros":
				return strconv.AppendInt(buf, durationToTimeMicros(d), 10), nil
			}
		}
		if v.CanInt() {
			return strconv.AppendInt(buf, v.Int(), 10), nil
		}
		if v.CanUint() {
			return strconv.AppendInt(buf, int64(v.Uint()), 10), nil
		}
		if v.CanFloat() {
			return strconv.AppendInt(buf, int64(v.Float()), 10), nil
		}
		return nil, fmt.Errorf("avro json: expected integer, got %s", v.Type())

	case "float":
		if v.CanFloat() {
			return appendJSONFloat(buf, v.Float(), 32, cfg), nil
		}
		return nil, fmt.Errorf("avro json: expected float, got %s", v.Type())

	case "double":
		if v.CanFloat() {
			return appendJSONFloat(buf, v.Float(), 64, cfg), nil
		}
		return nil, fmt.Errorf("avro json: expected double, got %s", v.Type())

	case "string":
		if v.Kind() == reflect.String {
			return appendJSONString(buf, v.String()), nil
		}
		return nil, fmt.Errorf("avro json: expected string, got %s", v.Type())

	case "bytes":
		// json.Number from decimal decode — write as JSON number.
		if v.Type() == jsonNumberType {
			return append(buf, v.String()...), nil
		}
		if v.Kind() == reflect.String {
			return appendAvroJSONBytes(buf, []byte(v.String())), nil
		}
		if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
			return appendAvroJSONBytes(buf, v.Bytes()), nil
		}
		return nil, fmt.Errorf("avro json: expected []byte or string, got %s", v.Type())

	case "fixed":
		if v.Type() == jsonNumberType {
			return append(buf, v.String()...), nil
		}
		if v.Type() == avroDurationType {
			raw := v.Interface().(Duration).Bytes()
			return appendAvroJSONBytes(buf, raw[:]), nil
		}
		if v.Kind() == reflect.String {
			return appendAvroJSONBytes(buf, []byte(v.String())), nil
		}
		if v.Kind() == reflect.Array && v.Type().Elem().Kind() == reflect.Uint8 {
			raw := make([]byte, v.Len())
			reflect.Copy(reflect.ValueOf(raw), v)
			return appendAvroJSONBytes(buf, raw), nil
		}
		if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
			return appendAvroJSONBytes(buf, v.Bytes()), nil
		}
		return nil, fmt.Errorf("avro json: expected []byte, [N]byte, or string, got %s", v.Type())

	case "enum":
		if v.Kind() == reflect.String {
			return appendJSONString(buf, v.String()), nil
		}
		return nil, fmt.Errorf("avro json: expected string for enum, got %s", v.Type())

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
			buf, err = appendAvroJSON(buf, v.Index(i), node.items, cfg, customEncodes)
			if err != nil {
				return nil, err
			}
		}
		return append(buf, ']'), nil

	case "map":
		if v.Kind() != reflect.Map {
			return nil, fmt.Errorf("avro json: expected map, got %s", v.Type())
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
			buf, err = appendAvroJSON(buf, iter.Value(), node.values, cfg, customEncodes)
			if err != nil {
				return nil, err
			}
		}
		return append(buf, '}'), nil

	case "record":
		return appendAvroJSONRecord(buf, v, node, cfg, customEncodes)

	case "union":
		return appendAvroJSONUnion(buf, v, node, cfg, customEncodes)

	default:
		return nil, fmt.Errorf("avro json: unsupported schema kind %q", node.kind)
	}
}

// appendAvroJSONRecord handles record encoding for both structs and maps.
func appendAvroJSONRecord(buf []byte, v reflect.Value, node *schemaNode, cfg *optConfig, customEncodes map[*schemaNode]func(reflect.Value) (reflect.Value, error)) ([]byte, error) {
	buf = append(buf, '{')
	if v.Kind() == reflect.Map {
		for i, f := range node.fields {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = appendJSONString(buf, f.name)
			buf = append(buf, ':')
			val := v.MapIndex(f.nameVal)
			if !val.IsValid() {
				if !f.hasDefault {
					return nil, fmt.Errorf("avro json: record %q missing required field %q", node.name, f.name)
				}
				buf = append(buf, f.defaultJSON...)
				continue
			}
			var err error
			buf, err = appendAvroJSON(buf, val, f.node, cfg, customEncodes)
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
			buf, err = appendAvroJSON(buf, fv, f.node, cfg, customEncodes)
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
func appendAvroJSONUnion(buf []byte, v reflect.Value, node *schemaNode, cfg *optConfig, customEncodes map[*schemaNode]func(reflect.Value) (reflect.Value, error)) ([]byte, error) {
	if !v.IsValid() || (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) && v.IsNil() {
		// unreachable: appendAvroJSON's deref loop converts nil pointers/interfaces
		// to invalid values before dispatching here, but kept as a safety net.
		return append(buf, "null"...), nil
	}
	for _, branch := range node.branches {
		if branch.kind == "null" {
			continue
		}
		encoded, err := appendAvroJSON(nil, v, branch, cfg, customEncodes)
		if err == nil {
			if cfg.tagged {
				bn, ln := unionBranchNames(branch)
				name := bn
				if cfg.tagLogical {
					name = ln
				}
				buf = append(buf, '{')
				buf = appendJSONString(buf, name)
				buf = append(buf, ':')
				buf = append(buf, encoded...)
				buf = append(buf, '}')
			} else {
				buf = append(buf, encoded...)
			}
			return buf, nil
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

// findUnionBranch finds a union branch by type name.
func findUnionBranch(union *schemaNode, name string) *schemaNode {
	for _, b := range union.branches {
		if unionBranchName(b) == name {
			return b
		}
	}
	// Fallback: goavro uses "type.logicalType" (e.g. "long.time-millis")
	// as union branch names. Try matching primitive branches by the base
	// type before the dot. Only matches primitives to avoid confusion with
	// named types that might coincidentally share a primitive type name.
	if base, _, ok := strings.Cut(name, "."); ok {
		for _, b := range union.branches {
			switch b.kind {
			case "null", "boolean", "int", "long", "float", "double", "string", "bytes":
				if b.kind == base {
					return b
				}
			}
		}
	}
	return nil
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

func parseSpecialFloat32(s string) (float32, error) {
	f, err := parseSpecialFloat(s)
	return float32(f), err
}

// appendAvroJSONBytes encodes raw bytes as an Avro JSON string using
// ISO-8859-1 encoding, matching the Java canonical implementation.
// Printable ASCII bytes (0x20-0x7E, except " and \) are written as
// literal characters. All other bytes use \uXXXX escapes.
func appendAvroJSONBytes(buf []byte, b []byte) []byte {
	buf = append(buf, '"')
	for _, c := range b {
		switch {
		case c == '"':
			buf = append(buf, '\\', '"')
		case c == '\\':
			buf = append(buf, '\\', '\\')
		case c >= 0x20 && c <= 0x7E:
			buf = append(buf, c)
		default:
			buf = append(buf, '\\', 'u', '0', '0')
			buf = append(buf, hexDigit(c>>4), hexDigit(c&0xf))
		}
	}
	return append(buf, '"')
}

func hexDigit(b byte) byte {
	if b < 10 {
		return '0' + b
	}
	return 'A' - 10 + b
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
			switch {
			case c == '"':
				buf = append(buf, '\\', '"')
			case c == '\\':
				buf = append(buf, '\\', '\\')
			case c < 0x20:
				buf = append(buf, '\\', 'u', '0', '0', jsonHex[c>>4], jsonHex[c&0xf])
			default:
				buf = append(buf, c)
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			// Invalid UTF-8 byte — replace with U+FFFD.
			buf = append(buf, `\ufffd`...)
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
