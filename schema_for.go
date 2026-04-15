package avro

import (
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"slices"
	"strings"
)

// SchemaOpt configures schema construction via [Parse], [SchemaCache.Parse],
// or [SchemaFor]. Inapplicable options are silently ignored.
type SchemaOpt interface{ schemaOpt() }

type schemaOpts struct {
	namespace string
	name      string
}

type withNamespace string

func (withNamespace) schemaOpt() {}

// WithNamespace sets the Avro namespace for the top-level record in
// [SchemaFor]. Ignored by [Parse].
func WithNamespace(ns string) SchemaOpt { return withNamespace(ns) }

type withName string

func (withName) schemaOpt() {}

// WithName overrides the Avro record name in [SchemaFor]. By default
// the Go struct name is used. Ignored by [Parse].
func WithName(name string) SchemaOpt { return withName(name) }

// SchemaFor infers an Avro schema from the Go type T. T must be a struct.
//
// Field names are taken from the avro struct tag, falling back to the Go
// field name. The following tag options are supported:
//
//   - avro:"-" excludes the field
//   - avro:",inline" flattens a nested struct's fields into the parent
//   - avro:",omitzero" is recorded but does not affect the schema
//   - avro:",alias=old_name" adds a field alias (repeatable)
//   - avro:",type-alias=old_name" adds an alias to the field's named type (record, enum, fixed; repeatable)
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
//   - [N]byte → fixed (size N, name from Go type name or "fixed_N")
//   - *T → ["null", T] union with default null
//   - []T → array
//   - map[string]T → map
//   - struct → record (recursive)
//   - time.Time → long with timestamp-millis (override with tag)
//   - time.Duration → int with time-millis (override with tag)
//   - *big.Rat → requires explicit decimal(p,s) tag
//   - [16]byte with uuid tag → string with uuid logical type
func SchemaFor[T any](opts ...SchemaOpt) (*Schema, error) {
	var o schemaOpts
	var customTypes []CustomType
	for _, opt := range opts {
		switch v := opt.(type) {
		case withNamespace:
			o.namespace = string(v)
		case withName:
			o.name = string(v)
		case CustomType:
			customTypes = append(customTypes, v)
		}
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
	s, err := inferRecord(t, name, o.namespace, seen, customTypes)
	if err != nil {
		return nil, err
	}
	s = dedupNamedTypes(s, make(map[string]string))
	b, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("avro: marshaling inferred schema: %w", err)
	}
	return Parse(string(b), opts...)
}

// dedupNamedTypes walks a JSON-like schema tree (maps, slices, strings)
// and replaces duplicate named type definitions with name references.
// Returns the (possibly rewritten) value. This handles the case where
// SchemaFor emits the same named type (e.g. a fixed UUID) for multiple
// struct fields. Conflicting redefinitions are left intact so Parse
// reports a clear "duplicate named type" error.
func dedupNamedTypes(v any, defined map[string]string) any {
	switch v := v.(type) {
	case map[string]any:
		// Is this a named type definition?
		if name, _ := v["name"].(string); name != "" {
			switch v["type"] {
			case "record", "error", "enum", "fixed":
				if prev, exists := defined[name]; exists {
					cur, _ := json.Marshal(v)
					if string(cur) == prev {
						return name // identical — emit reference
					}
					return v // different — let Parse error
				}
				b, _ := json.Marshal(v)
				defined[name] = string(b)
			}
		}
		// Recurse into children that can hold schemas.
		if fields, ok := v["fields"].([]map[string]any); ok {
			for i, f := range fields {
				fields[i]["type"] = dedupNamedTypes(f["type"], defined)
			}
		}
		if items, ok := v["items"]; ok {
			v["items"] = dedupNamedTypes(items, defined)
		}
		if values, ok := v["values"]; ok {
			v["values"] = dedupNamedTypes(values, defined)
		}
		return v
	case []any: // union branches
		for i, elem := range v {
			v[i] = dedupNamedTypes(elem, defined)
		}
		return v
	}
	return v
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
func inferRecord(t reflect.Type, name, namespace string, seen map[reflect.Type]string, customTypes []CustomType) (any, error) {
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
	applied := make(appliedTypeAliases)
	for _, f := range fields {
		af, err := inferField(f, namespace, seen, customTypes, applied)
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
	name      string
	index     []int
	goType    reflect.Type
	tagged    bool
	aliases   []string
	typeAlias []string // aliases for the field's named type (record, enum, fixed)
	dflt      *string  // nil = no default; pointer to raw value string
	logical   string   // e.g. "timestamp-millis", "date", "uuid"
	decimal   [2]int   // [precision, scale]; zero if not decimal
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
				parts, err := splitTag(tag)
				if err != nil {
					return nil, err
				}
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
		parts, err := splitTag(tag)
		if err != nil {
			return nil, err
		}

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

// splitTag splits a struct tag value on commas, but respects parentheses
// and brackets. For example, "name,decimal(10,2),alias=[a,b]" splits into
// ["name", "decimal(10,2)", "alias=[a,b]"].
func splitTag(tag string) ([]string, error) {
	var parts []string
	var stack []rune
	start := 0
	for i, c := range tag {
		switch c {
		case '(', '[':
			stack = append(stack, c)
		case ')':
			if len(stack) == 0 || stack[len(stack)-1] != '(' {
				return nil, fmt.Errorf("unexpected %q in avro tag %q", c, tag)
			}
			stack = stack[:len(stack)-1]
		case ']':
			if len(stack) == 0 || stack[len(stack)-1] != '[' {
				return nil, fmt.Errorf("unexpected %q in avro tag %q", c, tag)
			}
			stack = stack[:len(stack)-1]
		case ',':
			if len(stack) == 0 {
				parts = append(parts, tag[start:i])
				start = i + 1
			}
		}
	}
	if len(stack) > 0 {
		return nil, fmt.Errorf("unclosed %q in avro tag %q", string(stack[len(stack)-1]), tag)
	}
	parts = append(parts, tag[start:])
	return parts, nil
}

// parseBracketedValues parses a tag value that is either a single value or
// a bracket-delimited list: "foo" returns ["foo"], "[foo,bar]" returns
// ["foo", "bar"]. Returns an error for empty values or empty brackets.
func parseBracketedValues(s string) ([]string, error) {
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		inner := s[1 : len(s)-1]
		if inner == "" {
			return nil, fmt.Errorf("empty brackets in %q", s)
		}
		// Simple comma split is safe: Avro names are [A-Za-z_][A-Za-z0-9_]*
		// and cannot contain commas or brackets.
		vals := strings.Split(inner, ",")
		for _, v := range vals {
			if v == "" {
				return nil, fmt.Errorf("empty element in %q", s)
			}
		}
		return vals, nil
	}
	if s == "" {
		return nil, fmt.Errorf("empty value")
	}
	return []string{s}, nil
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
			vals, err := parseBracketedValues(opt[len("alias="):])
			if err != nil {
				return f, fmt.Errorf("alias: %w", err)
			}
			f.aliases = append(f.aliases, vals...)
		case strings.HasPrefix(opt, "type-alias="):
			vals, err := parseBracketedValues(opt[len("type-alias="):])
			if err != nil {
				return f, fmt.Errorf("type-alias: %w", err)
			}
			f.typeAlias = append(f.typeAlias, vals...)
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
	bigRatPtrType   = reflect.TypeFor[*big.Rat]()
	bigRatValueType = reflect.TypeFor[big.Rat]()
)

// appliedTypeAliases tracks type-alias values applied to each named type,
// keyed by type name. Used to accept identical aliases on later fields
// referencing the same type, while rejecting contradictory ones.
type appliedTypeAliases map[string][]string

// inferField builds the Avro field definition for a single struct field.
func inferField(f schemaField, namespace string, seen map[reflect.Type]string, customTypes []CustomType, applied appliedTypeAliases) (map[string]any, error) {
	fieldDef := map[string]any{
		"name": f.name,
	}

	schema, err := inferType(f.goType, f.logical, f.decimal, namespace, seen, customTypes)
	if err != nil {
		return nil, err
	}
	if len(f.typeAlias) > 0 {
		r := addTypeAliases(schema, f.typeAlias)
		switch {
		case r.applied:
			applied[r.refName] = f.typeAlias
		case r.refName != "":
			if prev, ok := applied[r.refName]; ok && slices.Equal(prev, f.typeAlias) {
				// Identical aliases — accept silently.
			} else if ok {
				return nil, fmt.Errorf("type-alias on field %q conflicts with type-alias already applied to type %q on an earlier field", f.name, r.refName)
			} else {
				return nil, fmt.Errorf("type-alias on field %q has no effect: type %q was already defined on an earlier field without type-alias (move the type-alias there)", f.name, r.refName)
			}
		default:
			return nil, fmt.Errorf("type-alias on field %q: type is not a named type (record, enum, or fixed)", f.name)
		}
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
	} else if union, ok := schema.([]any); ok && len(union) > 0 && union[0] == "null" {
		// Null-first unions (from *T or CustomType) default to null so
		// the field is backward-compatible (readers can read data written
		// before this field existed). Explicit default= overrides this.
		fieldDef["default"] = nil
	}
	return fieldDef, nil
}

var avroPrimitives = map[string]bool{
	"null": true, "boolean": true, "int": true, "long": true,
	"float": true, "double": true, "string": true, "bytes": true,
}

// typeAliasResult describes what addTypeAliases found.
type typeAliasResult struct {
	applied bool   // alias was added to a type definition
	refName string // non-empty if schema was a named type reference (definition is elsewhere)
}

// addTypeAliases walks through unions, arrays, and maps to find the
// innermost named type (record, enum, fixed) and adds aliases to it.
// For unions, aliases are added to the first named-type branch found
// (typically the only one in a ["null", T] union produced by *T).
// This supports the type-alias struct tag, which sets aliases on the
// named type referenced by a field (as opposed to alias= which sets
// aliases on the field itself).
func addTypeAliases(schema any, aliases []string) typeAliasResult {
	switch s := schema.(type) {
	case map[string]any:
		switch s["type"] {
		case "record", "error", "enum", "fixed":
			// The existing aliases come from inferRecord/inferType which
			// builds the schema as map[string]any with []string values.
			// This assertion is safe for freshly-inferred schemas.
			existing, _ := s["aliases"].([]string)
			s["aliases"] = append(existing, aliases...)
			name, _ := s["name"].(string)
			return typeAliasResult{applied: true, refName: name}
		case "array":
			if items, ok := s["items"]; ok {
				return addTypeAliases(items, aliases)
			}
		case "map":
			if values, ok := s["values"]; ok {
				return addTypeAliases(values, aliases)
			}
		}
	case []any: // union
		var best typeAliasResult
		for _, branch := range s {
			r := addTypeAliases(branch, aliases)
			if r.applied {
				return r
			}
			if r.refName != "" {
				best = r
			}
		}
		return best
	case string:
		// A string is either a primitive type name ("int", "long", ...)
		// or a named type reference ("Inner"). Named references mean the
		// type was already defined on an earlier field.
		if !avroPrimitives[s] {
			return typeAliasResult{refName: s}
		}
	}
	return typeAliasResult{}
}

// inferType returns the Avro schema for a Go type.
func inferType(t reflect.Type, logical string, decimal [2]int, namespace string, seen map[reflect.Type]string, customTypes []CustomType) (any, error) {
	// Check custom types before anything else (including pointer unwrapping).
	for _, ct := range customTypes {
		if ct.GoType != nil && ct.GoType == t {
			if ct.Schema != nil {
				return ct.Schema.toJSON(), nil
			}
			if ct.AvroType == "" {
				return nil, fmt.Errorf("avro: CustomType for %s has no AvroType or Schema; cannot infer schema (set AvroType or Schema for SchemaFor)", t)
			}
			schema := map[string]any{"type": ct.AvroType}
			if ct.LogicalType != "" {
				schema["logicalType"] = ct.LogicalType
			}
			return schema, nil
		}
	}

	// Pointer → nullable union.
	if t.Kind() == reflect.Pointer {
		inner, err := inferType(t.Elem(), logical, decimal, namespace, seen, customTypes)
		if err != nil {
			return nil, err
		}
		return []any{"null", inner}, nil
	}

	// Logical types for known Go types.
	switch t {
	case timeType:
		lt := logical
		if lt == "" {
			lt = "timestamp-millis"
		}
		base := "long"
		if lt == "date" {
			base = "int"
		}
		return map[string]any{"type": base, "logicalType": lt}, nil

	case durationType:
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

	// Types implementing text interfaces are inferred as string,
	// matching the encoder (TextAppender/TextMarshaler) and decoder
	// (TextUnmarshaler) behavior.
	for _, iface := range []reflect.Type{
		textAppenderType,
		textMarshalerType,
		textUnmarshalerType,
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
		items, err := inferType(t.Elem(), "", [2]int{}, namespace, seen, customTypes)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "array", "items": items}, nil

	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			if logical == "uuid" && t.Len() == 16 {
				if _, ok := seen[t]; ok {
					return "uuid", nil
				}
				seen[t] = "uuid"
				return map[string]any{
					"type":        "fixed",
					"name":        "uuid",
					"size":        16,
					"logicalType": "uuid",
				}, nil
			}
			name := t.Name()
			if name == "" {
				name = fmt.Sprintf("fixed_%d", t.Len())
			} else if _, ok := seen[t]; ok {
				// Named fixed type already defined — emit reference.
				return name, nil
			}
			seen[t] = name
			return map[string]any{
				"type": "fixed",
				"name": name,
				"size": t.Len(),
			}, nil
		}
		items, err := inferType(t.Elem(), "", [2]int{}, namespace, seen, customTypes)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "array", "items": items}, nil

	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			return nil, fmt.Errorf("map key must be string, got %s", t.Key())
		}
		values, err := inferType(t.Elem(), "", [2]int{}, namespace, seen, customTypes)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "map", "values": values}, nil

	case reflect.Struct:
		name := t.Name()
		if name == "" {
			return nil, fmt.Errorf("anonymous struct types are not supported; use a named type")
		}
		return inferRecord(t, name, namespace, seen, customTypes)

	default:
		return nil, fmt.Errorf("unsupported Go type %s", t)
	}
}
