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
//   - *T → ["null", T] union with default null (a pointer chain of any
//     depth — **T, ***T — collapses to the same single nullable union)
//   - []T → array
//   - map[string]T → map
//   - struct → record (recursive)
//   - time.Time → long with timestamp-millis (override with tag)
//   - time.Duration → int with time-millis (override with tag; a Duration is a
//     span of time, so it is only meaningful with the time-millis/time-micros
//     logicals — overriding it onto date or a timestamp-* logical maps a
//     duration onto a point in time, and a large Duration overflows the
//     narrower wire type)
//   - *big.Rat → requires explicit decimal(p,s) tag
//   - [16]byte with uuid tag → fixed(16) with uuid logical type
//   - string (or text marshaler type) with uuid tag → string with uuid logical type
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
			if typ, _ := v["type"].(string); isNamedKind(typ) {
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

// avroFullName returns the Avro fullname for a named type: the namespace
// and name joined by a dot, or the bare name when there is no namespace.
// This is the single identity a named type is registered under (seen[t])
// and referenced by, so every site that needs a named type's identity —
// the record definition, a later name reference, and the type-alias dedup
// in inferField — derives it the same way and cannot drift. (The drift it
// prevents: registering under the bare name while referencing by fullname,
// so a same-type/identical-alias pair is wrongly seen as a fresh
// definition once a namespace is configured.)
func avroFullName(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "." + name
}

// inferRecord builds a schema map for a struct type. The seen map tracks
// types that have been visited so repeat references (both recursive and
// shared) emit a named reference instead of a duplicate definition.
func inferRecord(t reflect.Type, name, namespace string, seen map[reflect.Type]string, customTypes []CustomType) (any, error) {
	if fullName, ok := seen[t]; ok {
		return fullName, nil
	}

	fullName := avroFullName(namespace, name)
	// Register the name before processing fields so that recursive
	// references resolve to a name reference rather than re-entering here.
	seen[t] = fullName

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

	record := map[string]any{
		"type":   "record",
		"name":   name,
		"fields": avroFields,
	}
	if namespace != "" {
		record["namespace"] = namespace
	}
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
	// PER-PATH marking, in lockstep with typeFieldMapping's collect
	// (reflect.go): the on-path check terminates embed CYCLES, but a type
	// reachable through two SIBLING embed paths must be collected at each
	// occurrence — so the shallower one reaches the shallowest-wins dedup,
	// and a type genuinely inlined twice surfaces as the duplicate-field
	// collision it is (rather than being silently pruned). The two walkers
	// must agree, so this marking discipline is kept identical.
	visited[t] = true
	defer delete(visited, t)

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
					// inline is incompatible with an explicit name — the name
					// says "make this a field"; inline says "flatten, no field."
					for _, p := range parts[1:] {
						if p == "inline" {
							return nil, fmt.Errorf("avro: field %s has tag %q: inline is incompatible with an explicit field name (inline flattens the embed; there is no field at this position to name)",
								sf.Name, truncForError(tag))
						}
					}
					f, err := parseSchemaTag(sf, parts, idx)
					if err != nil {
						return nil, err
					}
					raw = append(raw, f)
					continue
				}
				// Anonymous embed with empty name flattens. The embed has
				// no Avro field of its own at this position, so options
				// that apply to a field (default=, alias=, type-alias=,
				// omitzero, logical-type tags) have no target. Reject
				// rather than silently drop.
				for _, p := range parts[1:] {
					if p != "inline" {
						return nil, fmt.Errorf("avro: field %s has tag %q: inline is incompatible with option %q (the anonymous embed flattens; there is no field at this position for the option to apply to)",
							sf.Name, truncForError(tag), truncForError(p))
					}
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
		// Reject "-,opt" / "-foo" — the "-" skip directive is exact-match
		// only. Anything else starting with "-" is a typo (user meant to
		// skip but added options, or means to name a field literally "-"
		// which Avro's naming rules reject anyway). Erroring here matches
		// the user's likely intent and avoids the silent-empty-record
		// outcome that "tag = '-,opt'" produced before this check.
		if strings.HasPrefix(tag, "-") {
			return nil, fmt.Errorf("avro: field %s has tag %q: the skip directive %q is exact-match only; remove the suffix or rename the field",
				sf.Name, truncForError(tag), "-")
		}
		parts, err := splitTag(tag)
		if err != nil {
			return nil, err
		}

		// Check for inline.
		hasInline := false
		for _, p := range parts[1:] {
			if p == "inline" {
				hasInline = true
				break
			}
		}
		if hasInline {
			// inline flattens the embedded struct into the parent — the
			// embed has no field of its own at this position. Other tag
			// options apply to a field (name, default=, alias=,
			// type-alias=, omitzero, logical-type tags) and have no
			// target with inline. Reject rather than silently drop so
			// typos surface here instead of producing a schema that
			// quietly ignores the user's tag.
			if parts[0] != "" {
				return nil, fmt.Errorf("avro: field %s has tag %q: inline is incompatible with an explicit field name (inline flattens the embed; there is no field at this position to name)",
					sf.Name, truncForError(tag))
			}
			for _, p := range parts[1:] {
				if p != "inline" {
					return nil, fmt.Errorf("avro: field %s has tag %q: inline is incompatible with option %q (inline flattens the embed; there is no field at this position for the option to apply to)",
						sf.Name, truncForError(tag), truncForError(p))
				}
			}
			ft := sf.Type
			if ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() != reflect.Struct {
				return nil, fmt.Errorf("avro: field %s has tag %q: inline requires a struct or pointer-to-struct field type; got %s (inline flattens the embed; there is no struct here to flatten)",
					sf.Name, truncForError(tag), ft)
			}
			nested, err := collectFields(ft, idx, visited)
			if err != nil {
				return nil, err
			}
			raw = append(raw, nested...)
			goto next
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

	// Deduplicate. Must agree with reflect.go's typeFieldMapping so
	// SchemaFor's inferred schema and the runtime field mapping pick the
	// same Go field for each Avro name. The precedence rules (documented on
	// the encode/decode field-mapping contract: "a tagged field wins over an
	// untagged one at any depth; among fields with the same tagged status,
	// the shallowest wins"):
	//   1. A tagged field beats an untagged one at ANY depth — a
	//      tiebreaker, so NOT an ambiguous collision. This runs first.
	//   2. Among same-tagged-status fields, the shallower (shorter index
	//      path) wins. Without this, dedup keeps first-seen — the deeper
	//      embedded field — because nested-struct fields are appended to
	//      raw BEFORE outer fields.
	//   3. Only a same-depth collision with the SAME tagged status AT THE
	//      WINNING DEPTH is genuinely ambiguous: two sibling fields disagree
	//      on who owns the name, so silently picking one would cause data
	//      loss at encode time. Java's RecordSchema.setFields rejects a true
	//      duplicate with "Duplicate field" (Schema.java:981); hamba rejects
	//      similarly. The ambiguity decision is DEFERRED, not eager: a
	//      shallower field declared LATER (the common "embeds first, own
	//      fields after" layout) resolves a same-depth deep collision, so it
	//      must be allowed to clear the ambiguity — exactly as typeFieldMapping
	//      and Go's own field promotion do. Erroring the instant two deep
	//      fields collide would reject a struct whose name a shallower field
	//      unambiguously owns.
	type entry struct {
		idx int
		schemaField
	}
	m := make(map[string]entry, len(raw))
	ambiguous := make(map[string][2]string) // name -> the two colliding Go field names
	for i, f := range raw {
		if existing, ok := m[f.name]; ok {
			// Tag tiebreaker first: tagged beats untagged regardless of
			// depth, so a tagged/untagged pair is resolved, never ambiguous.
			if f.tagged && !existing.tagged {
				m[f.name] = entry{i, f}
				delete(ambiguous, f.name)
				continue
			}
			if !f.tagged && existing.tagged {
				continue
			}
			// Same tagged status: the shallower field wins and clears any
			// ambiguity a deeper collision recorded; only a collision that
			// survives at the winning (shallowest) depth is genuinely
			// ambiguous, and that is decided after the full walk below.
			if len(f.index) < len(existing.index) {
				m[f.name] = entry{i, f}
				delete(ambiguous, f.name)
				continue
			}
			if len(f.index) == len(existing.index) {
				ambiguous[f.name] = [2]string{t.FieldByIndex(existing.index).Name, t.FieldByIndex(f.index).Name}
			}
			continue
		}
		m[f.name] = entry{i, f}
	}

	// A name still marked ambiguous after the full walk has two fields at its
	// winning depth with the same tagged status and no shallower resolver.
	// SchemaFor must emit every field, so it rejects here (the schema-driven
	// runtime mapper instead defers the error to a lookup of the specific
	// name). Report deterministically by raw encounter order.
	for _, f := range raw {
		if names, amb := ambiguous[f.name]; amb {
			return nil, fmt.Errorf("avro: duplicate field name %q in type %s (fields %q and %q both map to the same Avro name)",
				truncForError(f.name), t.String(), truncForError(names[0]), truncForError(names[1]))
		}
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
				// default= consumes the rest of the tag verbatim — its
				// value may be an arbitrary string containing unbalanced
				// brackets/parens (e.g. `default=note (a`) or commas. Stop
				// splitting and bracket-checking at this boundary so the
				// value is preserved rather than rejected as an "unclosed
				// (". parseSchemaTag's default= arm already documents that
				// it takes the rest of the tag as the last option.
				if strings.HasPrefix(tag[start:], "default=") {
					parts = append(parts, tag[start:])
					return parts, nil
				}
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
		if slices.Contains(vals, "") {
			return nil, fmt.Errorf("empty element in %q", s)
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
			// The trailing %s captures any content after the two integers so
			// decimal(9,2,3) / decimal(9,2x) / decimal(9,2e1) are rejected
			// rather than silently truncated to decimal(9,2): "%d,%d" alone
			// stops as soon as two integers match and ignores the rest.
			var extra string
			if n, _ := fmt.Sscanf(inner, "%d,%d%s", &p, &s, &extra); n < 2 || extra != "" {
				return f, fmt.Errorf("invalid decimal tag %q: want decimal(precision,scale)", opt)
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

	schema, err := inferType(f.goType, f.logical, f.decimal, namespace, seen, customTypes, 0)
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
		// Try JSON parse first; fall back to bare string. UseNumber
		// preserves long precision: a default like "9223372036854775807"
		// (MaxInt64) round-trips through float64 → "9.223372036854776e+18"
		// without it, which Parse then rejects via floatFitsInt64.
		// Mirrors unmarshalDefault (schema.go) on the Parse side; the
		// two sites must stay in lockstep on number-precision handling.
		dec := json.NewDecoder(strings.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&v); err != nil {
			v = raw
		} else if dec.More() {
			// Decode stops at the end of the first JSON value and ignores
			// the rest, so a valid JSON prefix followed by trailing content
			// (e.g. "42 oops") would silently truncate to the prefix. Treat
			// the whole tag as a verbatim string instead — matching the
			// non-JSON fallback above and decimal()'s trailing-junk guard.
			v = raw
		}
		fieldDef["default"] = v
		// A narrow Go integer kind maps to a WIDER Avro type (int8/16 and
		// uint8/16 → int; uint32 / uint → long), so a default that is a
		// valid Avro int/long but exceeds the Go field's range builds a
		// schema whose own default cannot be materialized back into the
		// field at decode-fill time. Reject it here for consistency with
		// the other Go-type/tag compatibility checks (uuid-on-wrong-kind,
		// etc.) rather than deferring to a decode-time error far from the
		// SchemaFor call.
		if err := checkIntDefaultFitsGoKind(v, f.goType); err != nil {
			return nil, fmt.Errorf("default for field %q: %w", f.name, err)
		}
	} else if union, ok := schema.([]any); ok && len(union) > 0 && union[0] == "null" {
		// Null-first unions (from *T or CustomType) default to null so
		// the field is backward-compatible (readers can read data written
		// before this field existed). Explicit default= overrides this.
		fieldDef["default"] = nil
	}
	return fieldDef, nil
}

// checkIntDefaultFitsGoKind verifies a numeric default fits the Go field's
// integer kind. It runs only when the Go field (after peeling pointers) is
// an integer kind; non-integer defaults and non-integer fields return nil
// and leave type compatibility to Parse's Avro-type validation.
//
// The default's integer value is extracted via defaultAsInt64 — the SAME
// lenient parser the wire default-fill path uses — so exponent / whole-
// number-float literals (e.g. "4e3") are caught here exactly as the wire
// path would catch them at decode time, instead of only the plain-integer
// forms. A value defaultAsInt64 can't read as an integer (a string,
// fractional, or > int64 default) is left to Parse, which validates it
// against the Avro type.
func checkIntDefaultFitsGoKind(v any, t reflect.Type) error {
	// Bound the peel so a cyclic pointer type (`type P *P`, whose Elem is
	// itself) terminates instead of looping forever. This is reached only
	// when a CustomType matched the field — so inferType returned before its
	// own (now-bounded) recursion — and a default is present. Past the cap the
	// type is still a pointer, so the switch below treats it as non-integer
	// and defers to Parse. Mirrors indirect/indirectAlloc's maxIndirectDepth.
	for i := 0; i < maxIndirectDepth && t.Kind() == reflect.Pointer; i++ {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := defaultAsInt64(v)
		if err != nil {
			return nil
		}
		if reflect.New(t).Elem().OverflowInt(n) {
			return fmt.Errorf("value %d overflows %s", n, t)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := defaultAsInt64(v)
		if err != nil {
			return nil
		}
		if n < 0 {
			return fmt.Errorf("value %d cannot be stored in unsigned %s", n, t)
		}
		if reflect.New(t).Elem().OverflowUint(uint64(n)) {
			return fmt.Errorf("value %d overflows %s", n, t)
		}
	}
	return nil
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
		typ, _ := s["type"].(string)
		switch {
		case isNamedKind(typ):
			// The existing aliases come from inferRecord/inferType which
			// builds the schema as map[string]any with []string values.
			// This assertion is safe for freshly-inferred schemas.
			existing, _ := s["aliases"].([]string)
			s["aliases"] = append(existing, aliases...)
			// refName must be the type's fullname (namespace + name) — the
			// same identity inferRecord registers in seen[t] and a later
			// field's name reference resolves to. The definition carries
			// name and namespace as separate keys; join them so the dedup
			// in inferField keys both the defining and referencing fields
			// by one identity. (A namespace-less type's fullname is its
			// bare name, so fixed types and no-namespace records are
			// unchanged.)
			name, _ := s["name"].(string)
			ns, _ := s["namespace"].(string)
			return typeAliasResult{applied: true, refName: avroFullName(ns, name)}
		case typ == "array":
			if items, ok := s["items"]; ok {
				return addTypeAliases(items, aliases)
			}
		case typ == "map":
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
// baseTypeForLogical returns the underlying Avro type required by the
// given logical type per the Avro 1.12 spec. Used by SchemaFor's
// inferType to produce schemas that validateLogical (schema.go) will
// accept regardless of the Go source type's natural Avro mapping —
// e.g. time-millis MUST annotate int even when the Go field is
// time.Time (whose default mapping is long).
func baseTypeForLogical(logical, fallback string) string {
	switch logical {
	case "date", "time-millis":
		return "int"
	case "time-micros",
		"timestamp-millis", "timestamp-micros", "timestamp-nanos",
		"local-timestamp-millis", "local-timestamp-micros", "local-timestamp-nanos":
		return "long"
	}
	return fallback
}

func inferType(t reflect.Type, logical string, decimal [2]int, namespace string, seen map[reflect.Type]string, customTypes []CustomType, depth int) (any, error) {
	// A recursive non-struct Go type — `type S []S`, `type P *P`,
	// `type M map[string]M`, or a long-enough pointer/slice/map chain — has
	// a cyclic type graph, and the pointer/slice/map arms below recurse on
	// the element type. Struct cycles terminate via seen[t] (a struct
	// registers its name before recursing into its fields), but a non-struct
	// type registers no name, so the recursion would run until the goroutine
	// stack overflows and the process dies. Bound it at maxDepth — the same
	// ceiling the wire pipeline enforces — so such a type returns a clean
	// error. depth resets to 0 at each record-field boundary (inferField),
	// so a chain of distinct nested structs stays bounded by seen, not depth,
	// and only an unbroken non-struct chain accrues depth here.
	if depth >= maxDepth {
		return nil, fmt.Errorf("avro: type %s nests too deeply or is recursive (exceeds depth %d); a recursive non-struct type such as `type T []T`, `*T`, or `map[string]T` has no Avro schema representation", t, maxDepth)
	}
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

	// Pointer → nullable union. A pointer means "nullable", and the codecs
	// treat a pointer chain as a single nullable level (indirect /
	// indirectAlloc). Recurse one level — so an intermediate pointer type
	// can still match a registered CustomType — then collapse: if the
	// inner already inferred to a null-first union (a deeper pointer like
	// **T, or a CustomType whose schema is ["null", …]), return it
	// unwrapped rather than nesting. Avro forbids a union immediately
	// inside a union, so wrapping each level would emit an unparseable
	// ["null", ["null", T]].
	if t.Kind() == reflect.Pointer {
		inner, err := inferType(t.Elem(), logical, decimal, namespace, seen, customTypes, depth+1)
		if err != nil {
			return nil, err
		}
		if u, ok := inner.([]any); ok && len(u) > 0 && u[0] == "null" {
			return u, nil
		}
		return []any{"null", inner}, nil
	}

	// Logical types for known Go types. The base type is determined by
	// the logical's spec-required underlying Avro type (NOT by the Go
	// source type), so e.g. `time.Time` tagged time-millis correctly
	// emits {int, time-millis} and `time.Duration` tagged
	// timestamp-millis correctly emits {long, timestamp-millis}.
	// Reject non-time logicals (uuid, decimal) on time types — they
	// would emit an invalid {long, uuid} or {long, decimal} schema
	// which Parse soft-drops, silently losing the user's tag.
	inferTimeLike := func(defaultLogical string) (any, error) {
		lt := logical
		if lt == "" {
			lt = defaultLogical
		}
		switch lt {
		case "date", "time-millis", "time-micros",
			"timestamp-millis", "timestamp-micros", "timestamp-nanos",
			"local-timestamp-millis", "local-timestamp-micros", "local-timestamp-nanos":
			// time-related logical, OK.
		default:
			return nil, fmt.Errorf("avro: %s does not support logical type %q; use a time or date logical type", t, lt)
		}
		return map[string]any{"type": baseTypeForLogical(lt, "long"), "logicalType": lt}, nil
	}
	switch t {
	case timeType:
		return inferTimeLike("timestamp-millis")
	case durationType:
		return inferTimeLike("time-millis")

	case jsonNumberType:
		// json.Number's Kind() is reflect.String, so the Kind switch below
		// would emit an Avro "string" — but the package's json.Number policy
		// is numeric-only: string/bytes/fixed/enum reject it on both encode
		// and decode (doc.go "Encoding from JSON input"). Emitting the one
		// Avro type the codec is guaranteed to reject for this Go type is a
		// build-accepts/encode-rejects deferred failure; reject up front,
		// matching the uuid/decimal/time strictness. (A registered CustomType
		// for json.Number still works — the loop above runs first; a NAMED
		// alias `type N json.Number` is a distinct reflect.Type that the
		// codec treats as a plain string and is unaffected.)
		return nil, fmt.Errorf("avro: json.Number has no single Avro type for SchemaFor; use a concrete Go numeric type (int32/int64/float64), string, or a CustomType")

	case bigRatPtrType, bigRatValueType:
		if logical == "" || (logical == "decimal" && decimal == [2]int{}) {
			return nil, fmt.Errorf("*big.Rat requires explicit decimal(precision,scale) tag")
		}
		if logical != "decimal" {
			return nil, fmt.Errorf("avro: *big.Rat / big.Rat does not support logical type %q; use decimal(precision,scale)", logical)
		}
		return map[string]any{
			"type":        "bytes",
			"logicalType": "decimal",
			"precision":   decimal[0],
			"scale":       decimal[1],
		}, nil
	}

	// decimal logical type requires *big.Rat or big.Rat (handled above).
	// Any other Go type carrying ",decimal(p,s)" produces a schema that
	// wouldn't reflect the user's intent — the encoder for decimal-on-bytes
	// expects *big.Rat input, not raw bytes / int / string — so reject at
	// SchemaFor time rather than silently dropping the tag.
	if logical == "decimal" {
		return nil, fmt.Errorf("avro: decimal logical type requires *big.Rat or big.Rat; got %s", t)
	}

	// UUID: spec wire form is either string or fixed(16). Accept Go
	// string kind and text-marshaler types as string; [16]byte goes
	// through the Array case below for fixed(16). Reject other Go
	// types — they would produce a schema that lies about the field's
	// Go type and cause Encode to fail at runtime far from here.
	if logical == "uuid" {
		isArr16 := t.Kind() == reflect.Array && t.Elem().Kind() == reflect.Uint8 && t.Len() == 16
		if !isArr16 {
			ok := t.Kind() == reflect.String
			if !ok {
				for _, iface := range []reflect.Type{textAppenderType, textMarshalerType, textUnmarshalerType} {
					if t.Implements(iface) || reflect.PointerTo(t).Implements(iface) {
						ok = true
						break
					}
				}
			}
			if !ok {
				return nil, fmt.Errorf("avro: uuid logical type requires Go string, [16]byte, or a text marshaler type; got %s", t)
			}
			return map[string]any{"type": "string", "logicalType": "uuid"}, nil
		}
	}

	// Integer-wire logical types (date / time-millis on int wire;
	// time-micros / timestamp-* / local-timestamp-* on long wire)
	// attached to a plain Go integer field. The user opted into the
	// logical with the tag; honor it by emitting the wire+logical
	// schema. The Go field must have a natural Avro wire type that
	// matches the logical's required wire — otherwise the encoder
	// would silently widen or narrow, hiding the user's intent and
	// producing a schema whose annotation may be soft-dropped at
	// Parse if the wire doesn't match.
	if logical != "" {
		wire := baseTypeForLogical(logical, "")
		if wire != "" {
			compat := false
			switch wire {
			case "int":
				switch t.Kind() {
				case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint8, reflect.Uint16:
					compat = true
				}
			case "long":
				switch t.Kind() {
				case reflect.Int, reflect.Int64, reflect.Uint32, reflect.Uint64, reflect.Uint:
					compat = true
				}
			}
			if !compat {
				return nil, fmt.Errorf("avro: logical type %q requires a Go integer field whose natural Avro wire type is %q; got %s", logical, wire, t)
			}
			return map[string]any{"type": wire, "logicalType": logical}, nil
		}
	}

	// Types implementing text interfaces are inferred as string,
	// matching the encoder (TextAppender/TextMarshaler) and decoder
	// (TextUnmarshaler) behavior. EXCEPTION: a ,uuid-tagged [16]byte is a
	// fixed(16) uuid handled by the Array case below — the codec trusts its
	// raw bytes and never consults the text method for a uuid-on-fixed value
	// (see [Schema.Decode]'s uuid-on-fixed contract). Letting a text method
	// downgrade it to a plain "string" here would silently drop the fixed(16)
	// shape and the uuid logical type, diverging from an identical text-less
	// [16]byte field and from what the codec actually does.
	uuidArr16 := logical == "uuid" && t.Kind() == reflect.Array &&
		t.Elem().Kind() == reflect.Uint8 && t.Len() == 16
	if !uuidArr16 {
		for _, iface := range []reflect.Type{
			textAppenderType,
			textMarshalerType,
			textUnmarshalerType,
		} {
			if t.Implements(iface) || reflect.PointerTo(t).Implements(iface) {
				return "string", nil
			}
		}
	}

	// inferArray emits the array schema for a non-byte slice/array element
	// type. Shared by the Slice and Array arms below so the items-recursion
	// + wrapping shape stays in one place.
	inferArray := func(elem reflect.Type) (any, error) {
		items, err := inferType(elem, "", [2]int{}, namespace, seen, customTypes, depth+1)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "array", "items": items}, nil
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
		return inferArray(t.Elem())

	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			// The Avro name a []byte array gets depends on the uuid tag:
			// "uuid" (with the logical type) when ,uuid-tagged, otherwise
			// t.Name() / "fixed_N". So the SAME Go type can legitimately
			// appear under two different Avro names — e.g. used once
			// ,uuid-tagged and once plain — which are distinct Avro types
			// (they differ by logicalType). seen[t] records the name an
			// earlier occurrence emitted; emit a name reference ONLY when
			// this occurrence's name matches it. A different-named form of
			// the same type emits its own full definition. The name-match
			// guard keeps the same-form dedup (so a later field can
			// reference an earlier definition that a type-alias= mutated)
			// while letting the two forms each define their own fixed.
			var name string
			var def map[string]any
			if logical == "uuid" && t.Len() == 16 {
				name = "uuid"
				def = map[string]any{
					"type":        "fixed",
					"name":        "uuid",
					"size":        16,
					"logicalType": "uuid",
				}
			} else {
				name = t.Name()
				if name == "" {
					name = fmt.Sprintf("fixed_%d", t.Len())
				}
				def = map[string]any{
					"type": "fixed",
					"name": name,
					"size": t.Len(),
				}
			}
			if prev, ok := seen[t]; ok && prev == name {
				return name, nil
			}
			seen[t] = name
			return def, nil
		}
		return inferArray(t.Elem())

	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			return nil, fmt.Errorf("map key must be string, got %s", t.Key())
		}
		values, err := inferType(t.Elem(), "", [2]int{}, namespace, seen, customTypes, depth+1)
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
