package avro

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
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
// [Schema.DecodeJSON] always accepts both tagged and bare input
// regardless of this option.
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
// output format; see [JSONOpt] for details.
//
// NaN and Infinity float values are encoded as JSON strings "NaN",
// "Infinity", and "-Infinity" by default (Java Avro convention), or as
// null/±1e999 with [LinkedinFloats]. Standard [encoding/json.Marshal]
// cannot represent these values; use EncodeJSON instead.
//
// EncodeJSON accepts the same Go types as [Schema.Encode].
func (s *Schema) EncodeJSON(v any, opts ...Opt) ([]byte, error) {
	cfg := parseOpts(opts)
	return appendAvroJSON(nil, reflect.ValueOf(v), s.node, &cfg)
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
	var raw any
	if err := json.Unmarshal(src, &raw); err != nil {
		return fmt.Errorf("avro: invalid JSON: %w", err)
	}
	native, err := fromAvroJSON(raw, s.node)
	if err != nil {
		return err
	}
	binary, err := s.Encode(native)
	if err != nil {
		return err
	}
	_, err = s.Decode(binary, v, opts...)
	return err
}

// appendAvroJSON is the single-pass Avro JSON encoder. It walks
// the Go value via reflect and the schema tree simultaneously, writing
// JSON directly without an intermediate binary encoding step. Handles
// structs, maps, all numeric coercions, time.Time, etc.
func appendAvroJSON(buf []byte, v reflect.Value, node *schemaNode, cfg *optConfig) ([]byte, error) {
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
			return appendAvroJSON(buf, reflect.Value{}, node, cfg)
		}
		v = v.Elem()
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
				return strconv.AppendInt(buf, t.Unix()/86400, 10), nil
			case "time-millis":
				d := time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute + time.Duration(t.Second())*time.Second + time.Duration(t.Nanosecond())
				return strconv.AppendInt(buf, d.Milliseconds(), 10), nil
			}
		}
		if v.Type() == durationType {
			d := v.Interface().(time.Duration)
			switch node.logical {
			case "time-millis":
				return strconv.AppendInt(buf, d.Milliseconds(), 10), nil
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
				return strconv.AppendInt(buf, t.UnixMilli(), 10), nil
			case "timestamp-micros", "local-timestamp-micros":
				return strconv.AppendInt(buf, t.UnixMicro(), 10), nil
			case "timestamp-nanos", "local-timestamp-nanos":
				return strconv.AppendInt(buf, timeToUnixNanos(t), 10), nil
			default:
				return strconv.AppendInt(buf, t.UnixMilli(), 10), nil
			}
		}
		if v.Type() == durationType {
			d := v.Interface().(time.Duration)
			switch node.logical {
			case "time-micros":
				return strconv.AppendInt(buf, int64(d/time.Microsecond), 10), nil
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
			b, _ := json.Marshal(v.String())
			return append(buf, b...), nil
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
			b, _ := json.Marshal(v.String())
			return append(buf, b...), nil
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
			buf, err = appendAvroJSON(buf, v.Index(i), node.items, cfg)
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
			key, _ := json.Marshal(iter.Key().String())
			buf = append(buf, key...)
			buf = append(buf, ':')
			var err error
			buf, err = appendAvroJSON(buf, iter.Value(), node.values, cfg)
			if err != nil {
				return nil, err
			}
		}
		return append(buf, '}'), nil

	case "record":
		return appendAvroJSONRecord(buf, v, node, cfg)

	case "union":
		return appendAvroJSONUnion(buf, v, node, cfg)

	default:
		return nil, fmt.Errorf("avro json: unsupported schema kind %q", node.kind)
	}
}

// appendAvroJSONRecord handles record encoding for both structs and maps.
func appendAvroJSONRecord(buf []byte, v reflect.Value, node *schemaNode, cfg *optConfig) ([]byte, error) {
	buf = append(buf, '{')
	if v.Kind() == reflect.Map {
		for i, f := range node.fields {
			if i > 0 {
				buf = append(buf, ',')
			}
			key, _ := json.Marshal(f.name)
			buf = append(buf, key...)
			buf = append(buf, ':')
			val := v.MapIndex(reflect.ValueOf(f.name))
			if !val.IsValid() {
				if !f.hasDefault {
					return nil, fmt.Errorf("avro json: record %q missing required field %q", node.name, f.name)
				}
				defJSON, err := json.Marshal(f.defaultVal)
				if err != nil {
					return nil, fmt.Errorf("avro json: record %q field %q: marshaling default: %w", node.name, f.name, err)
				}
				buf = append(buf, defJSON...)
				continue
			}
			var err error
			buf, err = appendAvroJSON(buf, val, f.node, cfg)
			if err != nil {
				return nil, err
			}
		}
	} else if v.Kind() == reflect.Struct {
		names := make([]string, len(node.fields))
		for i, f := range node.fields {
			names[i] = f.name
		}
		mapping, err := typeFieldMapping(names, nil, v.Type())
		if err != nil {
			return nil, err
		}
		for i, f := range node.fields {
			if i > 0 {
				buf = append(buf, ',')
			}
			key, _ := json.Marshal(f.name)
			buf = append(buf, key...)
			buf = append(buf, ':')
			fv := v.FieldByIndex(mapping.indices[i])
			buf, err = appendAvroJSON(buf, fv, f.node, cfg)
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
func appendAvroJSONUnion(buf []byte, v reflect.Value, node *schemaNode, cfg *optConfig) ([]byte, error) {
	if !v.IsValid() || (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) && v.IsNil() {
		// unreachable: appendAvroJSON's deref loop converts nil pointers/interfaces
		// to invalid values before dispatching here, but kept as a safety net.
		return append(buf, "null"...), nil
	}
	for _, branch := range node.branches {
		if branch.kind == "null" {
			continue
		}
		encoded, err := appendAvroJSON(nil, v, branch, cfg)
		if err == nil {
			if cfg.tagged {
				name := unionBranchName(branch)
				if cfg.tagLogical && branch.logical != "" {
					name = branch.kind + "." + branch.logical
				}
				buf = append(buf, '{')
				key, _ := json.Marshal(name)
				buf = append(buf, key...)
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

// fromAvroJSON converts an Avro JSON value to a standard Go value,
// guided by the schema node. This unwraps union wrappers and converts
// bytes/fixed \uXXXX strings to []byte.
func fromAvroJSON(v any, node *schemaNode) (any, error) {
	if v == nil {
		switch node.kind {
		case "float":
			return float32(math.NaN()), nil // null → NaN (goavro convention)
		case "double":
			return math.NaN(), nil // null → NaN (goavro convention)
		default:
			return nil, nil
		}
	}

	switch node.kind {
	case "null":
		return nil, nil

	case "boolean":
		if _, ok := v.(bool); !ok {
			return nil, fmt.Errorf("avro json: expected bool, got %T", v)
		}
		return v, nil

	case "string", "enum":
		if _, ok := v.(string); !ok {
			return nil, fmt.Errorf("avro json: expected string, got %T", v)
		}
		return v, nil

	case "int":
		f, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("avro json: expected number for int, got %T", v)
		}
		if f < math.MinInt32 || f > math.MaxInt32 {
			return nil, fmt.Errorf("avro json: value %v overflows int32", f)
		}
		return int32(f), nil

	case "long":
		f, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("avro json: expected number for long, got %T", v)
		}
		if f < -(1<<63) || f >= 1<<63 {
			return nil, fmt.Errorf("avro json: value %v overflows int64", f)
		}
		return int64(f), nil

	case "float":
		if s, ok := v.(string); ok {
			return parseSpecialFloat32(s)
		}
		f, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("avro json: expected number for float, got %T", v)
		}
		return float32(f), nil

	case "double":
		if s, ok := v.(string); ok {
			return parseSpecialFloat64(s)
		}
		if _, ok := v.(float64); !ok {
			return nil, fmt.Errorf("avro json: expected number for double, got %T", v)
		}
		return v, nil

	case "bytes":
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("avro json: expected string for bytes, got %T", v)
		}
		return avroJSONBytesToBytes(s)

	case "fixed":
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("avro json: expected string for fixed, got %T", v)
		}
		return avroJSONBytesToBytes(s)

	case "array":
		arr, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("avro json: expected array, got %T", v)
		}
		result := make([]any, len(arr))
		for i, item := range arr {
			var err error
			result[i], err = fromAvroJSON(item, node.items)
			if err != nil {
				return nil, err
			}
		}
		return result, nil

	case "map":
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("avro json: expected object for map, got %T", v)
		}
		result := make(map[string]any, len(m))
		for k, val := range m {
			var err error
			result[k], err = fromAvroJSON(val, node.values)
			if err != nil {
				return nil, err
			}
		}
		return result, nil

	case "record":
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("avro json: expected object for record, got %T", v)
		}
		result := make(map[string]any, len(node.fields))
		for _, f := range node.fields {
			val, exists := m[f.name]
			if !exists {
				if !f.hasDefault {
					return nil, fmt.Errorf("avro json: record %q missing required field %q", node.name, f.name)
				}
				continue
			}
			var err error
			result[f.name], err = fromAvroJSON(val, f.node)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", f.name, err)
			}
		}
		return result, nil

	case "union":
		if v == nil {
			// unreachable: the top-level nil check in fromAvroJSON returns
			// before reaching this switch case, but kept as a safety net.
			return nil, nil
		}
		// Try tagged union: {"type_name": value} with exactly one key
		// matching a union branch name.
		if m, ok := v.(map[string]any); ok && len(m) == 1 {
			for typeName, inner := range m {
				if branch := findUnionBranch(node, typeName); branch != nil {
					return fromAvroJSON(inner, branch)
				}
			}
		}
		// Bare (unwrapped) value — try each non-null branch. This handles
		// both bare primitives and bare records (from EncodeJSON without
		// TaggedUnions).
		for _, branch := range node.branches {
			if branch.kind == "null" {
				continue
			}
			if result, err := fromAvroJSON(v, branch); err == nil {
				return result, nil
			}
		}
		return v, nil

	default:
		return nil, fmt.Errorf("avro json: unsupported schema kind %q", node.kind)
	}
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

// parseSpecialFloat32 parses NaN/Infinity string representations (Java
// convention and case-insensitive variants per AVRO-4217).
func parseSpecialFloat32(s string) (float32, error) {
	switch strings.ToLower(s) {
	case "nan":
		return float32(math.NaN()), nil
	case "infinity", "inf":
		return float32(math.Inf(1)), nil
	case "-infinity", "-inf":
		return float32(math.Inf(-1)), nil
	}
	return 0, fmt.Errorf("avro json: unknown float value %q", s)
}

// parseSpecialFloat64 parses NaN/Infinity string representations (Java
// convention and case-insensitive variants per AVRO-4217).
func parseSpecialFloat64(s string) (float64, error) {
	switch strings.ToLower(s) {
	case "nan":
		return math.NaN(), nil
	case "infinity", "inf":
		return math.Inf(1), nil
	case "-infinity", "-inf":
		return math.Inf(-1), nil
	}
	return 0, fmt.Errorf("avro json: unknown double value %q", s)
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
