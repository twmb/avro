package avro

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"
)

// EncodeJSON encodes v as Avro JSON. Avro JSON differs from standard JSON
// in its handling of unions (non-null values are wrapped as {"type_name": value})
// and bytes/fixed fields (encoded as strings with \uXXXX escapes).
// EncodeJSON accepts the same Go types as [Schema.Encode].
func (s *Schema) EncodeJSON(v any) ([]byte, error) {
	return appendAvroJSON(nil, reflect.ValueOf(v), s.node)
}

// DecodeJSON decodes Avro JSON from src into v. It unwraps union wrappers,
// converts bytes/fixed strings, and coerces numeric types to match the
// schema. When v is *any, the result is returned directly. For typed
// targets (structs, etc.), the value is round-tripped through binary
// encode/decode.
//
// See [Schema.EncodeJSON] for the Avro JSON format.
func (s *Schema) DecodeJSON(src []byte, v any) error {
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
	_, err = s.Decode(binary, v)
	return err
}

// appendAvroJSON is the single-pass Avro JSON encoder. It walks
// the Go value via reflect and the schema tree simultaneously, writing
// JSON directly without an intermediate binary encoding step. Handles
// structs, maps, all numeric coercions, time.Time, etc.
func appendAvroJSON(buf []byte, v reflect.Value, node *schemaNode) ([]byte, error) {
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
			return appendAvroJSON(buf, reflect.Value{}, node)
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
			default: // unreachable: all timestamp logical types are matched above
				return strconv.AppendInt(buf, t.UnixMilli(), 10), nil
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
			return appendJSONFloat(buf, v.Float(), 32), nil
		}
		return nil, fmt.Errorf("avro json: expected float, got %s", v.Type())

	case "double":
		if v.CanFloat() {
			return appendJSONFloat(buf, v.Float(), 64), nil
		}
		return nil, fmt.Errorf("avro json: expected double, got %s", v.Type())

	case "string":
		if v.Kind() == reflect.String {
			b, _ := json.Marshal(v.String())
			return append(buf, b...), nil
		}
		return nil, fmt.Errorf("avro json: expected string, got %s", v.Type())

	case "bytes":
		if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
			return appendAvroJSONBytes(buf, v.Bytes()), nil
		}
		// json.Number from decimal decode — write as JSON number.
		if v.Type() == jsonNumberType {
			return append(buf, v.String()...), nil
		}
		return nil, fmt.Errorf("avro json: expected []byte, got %s", v.Type())

	case "fixed":
		if v.Kind() == reflect.Array && v.Type().Elem().Kind() == reflect.Uint8 {
			raw := make([]byte, v.Len())
			reflect.Copy(reflect.ValueOf(raw), v)
			return appendAvroJSONBytes(buf, raw), nil
		}
		if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
			return appendAvroJSONBytes(buf, v.Bytes()), nil
		}
		if v.Type() == jsonNumberType {
			return append(buf, v.String()...), nil
		}
		return nil, fmt.Errorf("avro json: expected []byte or [N]byte, got %s", v.Type())

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
			buf, err = appendAvroJSON(buf, v.Index(i), node.items)
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
			buf, err = appendAvroJSON(buf, iter.Value(), node.values)
			if err != nil {
				return nil, err
			}
		}
		return append(buf, '}'), nil

	case "record":
		return appendAvroJSONRecord(buf, v, node)

	case "union":
		return appendAvroJSONUnion(buf, v, node)

	default:
		return nil, fmt.Errorf("avro json: unsupported schema kind %q", node.kind)
	}
}

// appendAvroJSONRecord handles record encoding for both structs and maps.
func appendAvroJSONRecord(buf []byte, v reflect.Value, node *schemaNode) ([]byte, error) {
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
				// Missing key — write default or null.
				buf = append(buf, "null"...)
				continue
			}
			var err error
			buf, err = appendAvroJSON(buf, val, f.node)
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
			buf, err = appendAvroJSON(buf, fv, f.node)
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
func appendAvroJSONUnion(buf []byte, v reflect.Value, node *schemaNode) ([]byte, error) {
	if !v.IsValid() || (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) && v.IsNil() {
		// unreachable: appendAvroJSON's deref loop converts nil pointers/interfaces
		// to invalid values before dispatching here, but kept as a safety net.
		return append(buf, "null"...), nil
	}
	for _, branch := range node.branches {
		if branch.kind == "null" {
			continue
		}
		encoded, err := appendAvroJSON(nil, v, branch)
		if err == nil {
			name := unionBranchName(branch)
			buf = append(buf, '{')
			key, _ := json.Marshal(name)
			buf = append(buf, key...)
			buf = append(buf, ':')
			buf = append(buf, encoded...)
			buf = append(buf, '}')
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
		return nil, nil
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
		return v, nil

	case "int":
		if f, ok := v.(float64); ok {
			if f < math.MinInt32 || f > math.MaxInt32 {
				return nil, fmt.Errorf("avro json: value %v overflows int32", f)
			}
			return int32(f), nil
		}
		return v, nil

	case "long":
		if f, ok := v.(float64); ok {
			if f < -(1<<63) || f >= 1<<63 {
				return nil, fmt.Errorf("avro json: value %v overflows int64", f)
			}
			return int64(f), nil
		}
		return v, nil // unreachable: json.Unmarshal always produces float64 for numbers

	case "float":
		if f, ok := v.(float64); ok {
			return float32(f), nil
		}
		return v, nil // unreachable: json.Unmarshal always produces float64 for numbers

	case "double":
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
		// Avro JSON unions are {"type_name": value}.
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("avro json: expected object for union, got %T", v)
		}
		if len(m) != 1 {
			return nil, fmt.Errorf("avro json: union object must have exactly one key, got %d", len(m))
		}
		for typeName, inner := range m {
			branch := findUnionBranch(node, typeName)
			if branch == nil {
				return nil, fmt.Errorf("avro json: unknown union branch %q", typeName)
			}
			return fromAvroJSON(inner, branch)
		}
		return nil, nil // unreachable: for loop always has at least one iteration since unions must have branches

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
	return nil
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
func appendJSONFloat(buf []byte, f float64, bits int) []byte {
	if math.IsNaN(f) {
		return append(buf, `"NaN"`...)
	}
	if math.IsInf(f, 1) {
		return append(buf, `"Infinity"`...)
	}
	if math.IsInf(f, -1) {
		return append(buf, `"-Infinity"`...)
	}
	return strconv.AppendFloat(buf, f, 'g', -1, bits)
}
