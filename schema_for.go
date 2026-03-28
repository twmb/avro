package avro

import (
	"encoding"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"time"
)

// SchemaOpt configures [SchemaFor].
type SchemaOpt func(*schemaOpts)

type schemaOpts struct {
	namespace string
	name      string
}

// WithNamespace sets the Avro namespace for the top-level record.
func WithNamespace(ns string) SchemaOpt {
	return func(o *schemaOpts) { o.namespace = ns }
}

// WithName overrides the Avro record name. By default the Go struct name
// is used. This is useful for schema evolution when a Go struct is renamed
// but the Avro record name must stay the same.
func WithName(name string) SchemaOpt {
	return func(o *schemaOpts) { o.name = name }
}

// SchemaFor infers an Avro schema from the Go type T. T must be a struct.
//
// Field names are taken from the avro struct tag, falling back to the Go
// field name. The following tag options are supported:
//
//   - avro:"-" excludes the field
//   - avro:",inline" flattens a nested struct's fields into the parent
//   - avro:",omitzero" is recorded but does not affect the schema
//   - avro:",alias=old_name" adds a field alias (repeatable)
//   - avro:",default=value" sets the field's default value (must be last option; scalars only)
//   - avro:",timestamp-millis" overrides the logical type (also: timestamp-micros,
//     timestamp-nanos, date, time-millis, time-micros)
//   - avro:",decimal(precision,scale)" sets the decimal logical type
//   - avro:",uuid" sets the uuid logical type
//
// Type inference:
//   - bool → boolean
//   - int8, int16, int32 → int
//   - int, int64, uint32 → long
//   - uint8, uint16 → int
//   - float32 → float
//   - float64 → double
//   - string → string
//   - []byte → bytes
//   - [N]byte → fixed (size N)
//   - *T → ["null", T] union
//   - []T → array
//   - map[string]T → map
//   - struct → record (recursive)
//   - time.Time → long with timestamp-millis (override with tag)
//   - time.Duration → int with time-millis (override with tag)
//   - *big.Rat → requires explicit decimal(p,s) tag
//   - [16]byte with uuid tag → string with uuid logical type
func SchemaFor[T any](opts ...SchemaOpt) (*Schema, error) {
	var o schemaOpts
	for _, fn := range opts {
		fn(&o)
	}
	t := reflect.TypeFor[T]()
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("avro: SchemaFor requires a struct type, got %s", t)
	}
	name := o.name
	if name == "" {
		name = t.Name()
	}
	seen := make(map[reflect.Type]string)
	s, err := inferRecord(t, name, o.namespace, seen)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("avro: marshaling inferred schema: %w", err)
	}
	return Parse(string(b))
}

// MustSchemaFor is like [SchemaFor] but panics on error.
func MustSchemaFor[T any](opts ...SchemaOpt) *Schema {
	s, err := SchemaFor[T](opts...)
	if err != nil {
		panic(err)
	}
	return s
}

// inferRecord builds a schema map for a struct type. The seen map tracks
// types currently being defined (for recursion detection) and types that
// have already been fully defined (for reuse as named references).
func inferRecord(t reflect.Type, name, namespace string, seen map[reflect.Type]string) (any, error) {
	if fullName, ok := seen[t]; ok {
		if fullName == "" {
			// Currently being defined — this is a recursive type.
			n := name
			if namespace != "" {
				n = namespace + "." + name
			}
			return nil, fmt.Errorf("avro: recursive type %s not supported in SchemaFor", n)
		}
		// Already fully defined — emit a named reference.
		return fullName, nil
	}
	seen[t] = "" // mark as in-progress

	fields, err := collectFields(t, nil, make(map[reflect.Type]bool))
	if err != nil {
		return nil, err
	}

	avroFields := make([]map[string]any, 0, len(fields))
	for _, f := range fields {
		af, err := inferField(f, namespace, seen)
		if err != nil {
			return nil, fmt.Errorf("avro: field %q: %w", f.name, err)
		}
		avroFields = append(avroFields, af)
	}

	fullName := name
	if namespace != "" {
		fullName = namespace + "." + name
	}

	record := map[string]any{
		"type":   "record",
		"name":   name,
		"fields": avroFields,
	}
	if namespace != "" {
		record["namespace"] = namespace
	}
	seen[t] = fullName // mark as fully defined
	return record, nil
}

type schemaField struct {
	name    string
	index   []int
	goType  reflect.Type
	tagged  bool
	aliases []string
	dflt    *string // nil = no default; pointer to raw value string
	logical string  // e.g. "timestamp-millis", "date", "uuid"
	decimal [2]int  // [precision, scale]; zero if not decimal
}

// collectFields walks a struct type depth-first, handling embedded structs
// and inline tags. Returns deduplicated fields (tagged wins over untagged,
// shallower wins over deeper).
func collectFields(t reflect.Type, index []int, visited map[reflect.Type]bool) ([]schemaField, error) {
	if visited[t] {
		return nil, nil
	}
	visited[t] = true

	var raw []schemaField
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		idx := make([]int, len(index)+1)
		copy(idx, index)
		idx[len(index)] = i

		if sf.Anonymous {
			ft := sf.Type
			if ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				tag := sf.Tag.Get("avro")
				if tag == "-" {
					continue
				}
				parts := splitTag(tag)
				if parts[0] != "" {
					// Explicit name on embedded struct: treat as named field.
					f, err := parseSchemaTag(sf, parts, idx)
					if err != nil {
						return nil, err
					}
					raw = append(raw, f)
					continue
				}
				nested, err := collectFields(ft, idx, visited)
				if err != nil {
					return nil, err
				}
				raw = append(raw, nested...)
				continue
			}
			if !sf.IsExported() {
				continue
			}
		} else if !sf.IsExported() {
			continue
		}

		tag := sf.Tag.Get("avro")
		if tag == "-" {
			continue
		}
		parts := splitTag(tag)

		// Check for inline.
		for _, p := range parts[1:] {
			if p == "inline" {
				ft := sf.Type
				if ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				if ft.Kind() == reflect.Struct {
					nested, err := collectFields(ft, idx, visited)
					if err != nil {
						return nil, err
					}
					raw = append(raw, nested...)
				}
				goto next
			}
		}

		{
			f, err := parseSchemaTag(sf, parts, idx)
			if err != nil {
				return nil, err
			}
			raw = append(raw, f)
		}
	next:
	}

	// Deduplicate: tagged wins over untagged, shallower wins.
	type entry struct {
		idx int
		schemaField
	}
	m := make(map[string]entry, len(raw))
	for i, f := range raw {
		if existing, ok := m[f.name]; ok {
			if f.tagged && !existing.tagged {
				m[f.name] = entry{i, f}
			}
			continue
		}
		m[f.name] = entry{i, f}
	}

	// Preserve encounter order.
	result := make([]schemaField, 0, len(m))
	for _, f := range raw {
		if e, ok := m[f.name]; ok && e.idx >= 0 {
			result = append(result, e.schemaField)
			// Mark as consumed by setting idx to -1.
			e.idx = -1
			m[f.name] = e
		}
	}
	return result, nil
}

// splitTag splits a struct tag value on commas, but respects parentheses.
// For example, "name,decimal(10,2),default=0" splits into
// ["name", "decimal(10,2)", "default=0"].
func splitTag(tag string) []string {
	var parts []string
	depth := 0
	start := 0
	for i, c := range tag {
		switch c {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, tag[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, tag[start:])
	return parts
}

// parseSchemaTag parses the avro struct tag parts into a schemaField.
func parseSchemaTag(sf reflect.StructField, parts []string, index []int) (schemaField, error) {
	f := schemaField{
		name:   parts[0],
		index:  index,
		goType: sf.Type,
		tagged: parts[0] != "",
	}
	if f.name == "" {
		f.name = sf.Name
	}

	for i, opt := range parts[1:] {
		switch {
		case opt == "inline" || opt == "omitzero":
			// Already handled or recorded elsewhere.
			continue
		case strings.HasPrefix(opt, "alias="):
			f.aliases = append(f.aliases, opt[len("alias="):])
		case strings.HasPrefix(opt, "default="):
			// default= must be the last option; take the rest of the tag.
			rest := strings.Join(append([]string{opt[len("default="):]}, parts[i+2:]...), ",")
			f.dflt = &rest
			return f, nil
		case strings.HasPrefix(opt, "decimal(") && strings.HasSuffix(opt, ")"):
			inner := opt[len("decimal(") : len(opt)-1]
			var p, s int
			if _, err := fmt.Sscanf(inner, "%d,%d", &p, &s); err != nil {
				return f, fmt.Errorf("invalid decimal tag %q: %w", opt, err)
			}
			f.decimal = [2]int{p, s}
			f.logical = "decimal"
		case opt == "uuid":
			f.logical = "uuid"
		case opt == "timestamp-millis" || opt == "timestamp-micros" || opt == "timestamp-nanos" ||
			opt == "date" ||
			opt == "time-millis" || opt == "time-micros" ||
			opt == "local-timestamp-millis" || opt == "local-timestamp-micros" || opt == "local-timestamp-nanos":
			f.logical = opt
		default:
			return f, fmt.Errorf("unknown avro tag option %q", opt)
		}
	}
	return f, nil
}

var (
	timeTimeType     = reflect.TypeFor[time.Time]()
	timeDurationType = reflect.TypeFor[time.Duration]()
	bigRatPtrType    = reflect.TypeFor[*big.Rat]()
	bigRatValueType  = reflect.TypeFor[big.Rat]()
)

// inferField builds the Avro field definition for a single struct field.
func inferField(f schemaField, namespace string, seen map[reflect.Type]string) (map[string]any, error) {
	fieldDef := map[string]any{
		"name": f.name,
	}

	schema, err := inferType(f.goType, f.logical, f.decimal, namespace, seen)
	if err != nil {
		return nil, err
	}
	fieldDef["type"] = schema

	if len(f.aliases) > 0 {
		fieldDef["aliases"] = f.aliases
	}
	if f.dflt != nil {
		var v any
		raw := *f.dflt
		// Try JSON parse first; fall back to bare string.
		if err := json.Unmarshal([]byte(raw), &v); err != nil {
			v = raw
		}
		fieldDef["default"] = v
	}
	return fieldDef, nil
}

// inferType returns the Avro schema for a Go type.
func inferType(t reflect.Type, logical string, decimal [2]int, namespace string, seen map[reflect.Type]string) (any, error) {
	// Pointer → nullable union.
	if t.Kind() == reflect.Pointer {
		inner, err := inferType(t.Elem(), logical, decimal, namespace, seen)
		if err != nil {
			return nil, err
		}
		return []any{"null", inner}, nil
	}

	// Logical types for known Go types.
	switch t {
	case timeTimeType:
		lt := logical
		if lt == "" {
			lt = "timestamp-millis"
		}
		base := "long"
		if lt == "date" {
			base = "int"
		}
		return map[string]any{"type": base, "logicalType": lt}, nil

	case timeDurationType:
		lt := logical
		if lt == "" {
			lt = "time-millis"
		}
		base := "int"
		if lt == "time-micros" {
			base = "long"
		}
		return map[string]any{"type": base, "logicalType": lt}, nil

	case bigRatPtrType, bigRatValueType:
		if logical != "decimal" || decimal == [2]int{} {
			return nil, fmt.Errorf("*big.Rat requires explicit decimal(precision,scale) tag")
		}
		return map[string]any{
			"type":        "bytes",
			"logicalType": "decimal",
			"precision":   decimal[0],
			"scale":       decimal[1],
		}, nil
	}

	// UUID: string by default, but [16]byte gets fixed(16).
	if logical == "uuid" && t.Kind() != reflect.Array {
		return map[string]any{"type": "string", "logicalType": "uuid"}, nil
	}

	// Types implementing standard string interfaces are inferred as
	// string, matching the encoder's behavior.
	for _, iface := range []reflect.Type{
		reflect.TypeFor[encoding.TextMarshaler](),
		reflect.TypeFor[encoding.TextAppender](),
		reflect.TypeFor[fmt.Stringer](),
	} {
		if t.Implements(iface) || reflect.PointerTo(t).Implements(iface) {
			return "string", nil
		}
	}

	switch t.Kind() {
	case reflect.Bool:
		return "boolean", nil
	case reflect.Int8, reflect.Int16, reflect.Int32:
		return "int", nil
	case reflect.Uint8, reflect.Uint16:
		return "int", nil
	case reflect.Int, reflect.Int64, reflect.Uint32, reflect.Uint64, reflect.Uint:
		return "long", nil
	case reflect.Float32:
		return "float", nil
	case reflect.Float64:
		return "double", nil
	case reflect.String:
		return "string", nil

	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "bytes", nil
		}
		items, err := inferType(t.Elem(), "", [2]int{}, namespace, seen)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "array", "items": items}, nil

	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			if logical == "uuid" && t.Len() == 16 {
				return map[string]any{
					"type":        "fixed",
					"name":        "uuid",
					"size":        16,
					"logicalType": "uuid",
				}, nil
			}
			return map[string]any{
				"type": "fixed",
				"name": fmt.Sprintf("fixed_%d", t.Len()),
				"size": t.Len(),
			}, nil
		}
		items, err := inferType(t.Elem(), "", [2]int{}, namespace, seen)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "array", "items": items}, nil

	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			return nil, fmt.Errorf("map key must be string, got %s", t.Key())
		}
		values, err := inferType(t.Elem(), "", [2]int{}, namespace, seen)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "map", "values": values}, nil

	case reflect.Struct:
		return inferRecord(t, t.Name(), namespace, seen)

	default:
		return nil, fmt.Errorf("unsupported Go type %s", t)
	}
}
