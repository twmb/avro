package avro

import (
	"errors"
	"fmt"
	"reflect"
)

// ErrSkipCustomType is returned from a [CustomType] Encode or Decode
// function to say this custom type does not handle the value. We fall
// through to the next matching custom type, or to built-in behavior.
var ErrSkipCustomType = errors.New("avro: skip custom type")

// CustomType defines a custom conversion between a Go type and an Avro type.
// [NewCustomType] is the simpler form for types backed by a primitive Avro
// type; this struct is the general form, and the only way to handle records,
// fixed types, and property-based dispatch.
//
// Pass a CustomType to [Parse] or [SchemaFor] as a [SchemaOpt].
//
// At parse time we match LogicalType and AvroType against schema nodes. All
// non-empty criteria must match:
//   - LogicalType only: matches any schema node with that logicalType
//   - LogicalType + AvroType: matches that logicalType on that Avro type
//   - AvroType only: matches all nodes of that Avro type
//   - Neither: matches every schema node (use with [ErrSkipCustomType]
//     for property-based dispatch like Kafka Connect types)
//
// At encode time we check GoType too: your Encode function only fires when
// the value's type matches GoType. This keeps the codec from intercepting
// native values (e.g. a raw int64 passes through without conversion for a
// custom-typed long field).
//
// A matching CustomType replaces our built-in logical type deserializer.
// Among your registrations, the first match wins.
//
// If the Avro type is complex, your Encode function returns map[string]any,
// []any, and so on.
type CustomType struct {
	// LogicalType narrows matching to schema nodes with this logicalType.
	LogicalType string

	// AvroType narrows matching to schema nodes of this Avro type
	// (e.g. "long", "bytes", "record"). Also used by SchemaFor to
	// infer the underlying Avro type.
	AvroType string

	// GoType adds an encode-time filter: when set, your Encode function
	// only fires when the value's concrete type matches GoType. Values
	// of other types pass through to the underlying serializer unchanged.
	// If nil, Encode fires for all values on matched schema nodes
	// (those matching LogicalType/AvroType).
	//
	// [SchemaFor] uses GoType to match struct fields: when a field's Go
	// type equals GoType, we emit AvroType + LogicalType (or Schema)
	// instead of the default type mapping. Because the custom supplies
	// the whole field schema, a logical-type tag on a matched field has
	// no effect and is rejected: set LogicalType (or Schema) here
	// instead. If nil, the custom type does not affect schema
	// generation, but we still wire it into the returned [*Schema] for
	// encode/decode.
	GoType reflect.Type

	// Schema is the full schema to emit in SchemaFor. You only need it
	// for types requiring extra metadata (fixed needs name+size, decimal
	// needs precision+scale, records need fields). If nil, we infer from
	// AvroType + LogicalType.
	//
	// We preserve every fullname the schema declares: a namespaced type
	// keeps its namespace, and a null-namespace type embedded under
	// [WithNamespace] keeps its null namespace (the emitted definition
	// carries a "namespace":"" escape). Note that a null-namespace type
	// used on two or more fields under WithNamespace is an error, because
	// Avro has no way to reference the null namespace from inside another
	// namespace.
	//
	// We work on a private copy, so SchemaFor never mutates your
	// SchemaNode or anything reachable from it. A schema over the
	// schema-tree budgets, or one holding an unnamed pointer cycle, fails
	// the build with an error.
	//
	// A union branch may be written either as a bare name ("null") or as
	// a wrapped object ({"type":"null"}); we treat the two as the same
	// type. A null branch is recognized in both spellings, whatever
	// properties or logicalType the wrapped form carries (Avro defines no
	// null logical type), so a nullable union collapses through a pointer
	// field and receives its null default the same way either way.
	Schema *SchemaNode

	// Encode converts your Go value to an Avro-native value. We call it
	// before serialization with the value as you passed it to
	// [Schema.Encode] (e.g. a custom Money type); return the
	// corresponding Avro-native value (e.g. int64 cents). Return
	// [ErrSkipCustomType] to fall through to the next matching custom
	// type or built-in behavior. Any other non-nil error is fatal.
	//
	// If nil, we use the built-in logical type encoder, which accepts
	// both enriched types ([time.Time], [time.Duration]) and raw
	// values (int64, int32, etc.).
	//
	// We build the schema argument once at Parse and share it across all
	// concurrent invocations. Treat it as read-only; in particular, do
	// not mutate schema.Props or schema.Symbols, whose slices and maps
	// alias the parser's internal state: concurrent writes from multiple
	// goroutines decoding the same [*Schema] will race.
	Encode func(v any, schema *SchemaNode) (any, error)

	// Decode converts a raw Avro-native value to your Go value. We call
	// it after deserialization with the raw Avro-native value (int32 for
	// int, int64 for long, []byte for bytes/fixed, etc.); return the
	// type you want. Return [ErrSkipCustomType] to fall through. Any
	// other non-nil error is fatal.
	//
	// When all matching decoders skip at a node, we re-decode the wire
	// into the target faithfully (identical to a no-custom decode). A
	// wildcard custom (empty LogicalType and AvroType) that matches leaf
	// nodes but skips containers therefore makes decoding into a
	// deeply-nested *typed* target (struct/slice/map) cost O(depth^2).
	// For untrusted deeply-nested data, decode into an interface /
	// map[string]any (single-pass) or register against a specific
	// LogicalType/AvroType.
	//
	// If nil, we bypass the built-in logical type handler and use the
	// base Avro type decoder directly, producing raw Avro-native values
	// (int32, int64, etc.) rather than enriched types ([time.Time],
	// [time.Duration], etc.).
	//
	// The schema argument is shared across concurrent callback invocations;
	// see [CustomType.Encode] for the read-only contract.
	//
	// Under [AliasInput], a []byte v points into the decode input, and a
	// field filled from its schema default points into the parsed
	// [Schema], which every decode of that schema shares. Read it or copy
	// from it, but do not write through it. Returning it is fine.
	Decode func(v any, schema *SchemaNode) (any, error)

	// Set by NewCustomType; if true and AvroType is "", Parse returns
	// an error ("unsupported Avro native type").
	needsAvroType bool
}

func (CustomType) schemaOpt() {}

// WithCustomType registers a custom type conversion for [Parse],
// [SchemaCache.Parse], or [SchemaFor]. [CustomType] and [NewCustomType]
// satisfy [SchemaOpt] directly, so this wrapper is optional.
func WithCustomType(ct CustomType) SchemaOpt { return ct }

func (ct CustomType) matches(node *schemaNode) bool {
	if ct.LogicalType != "" && ct.LogicalType != node.logical {
		return false
	}
	if ct.AvroType != "" && ct.AvroType != node.kind {
		return false
	}
	return true
}

// NewCustomType returns a type-safe [CustomType] for the common case of
// mapping a custom Go type to/from a primitive Avro type.
//
// G is your custom Go type (e.g. Money). A is the Avro-native Go type:
// int32 for int, int64 for long, float32 for float, float64 for double,
// string for string, []byte for bytes, bool for boolean. A may also be a
// named type whose underlying kind is one of these (e.g. type Cents int64);
// we infer the Avro type from A's kind and convert the decoded value to A.
//
// We infer GoType and AvroType from the type parameters. If A is not a
// supported Avro-native type, [Parse] or [SchemaFor] returns an error.
//
// Note that we infer AvroType from A's Go kind, which may not match the
// Avro schema's type for logical types backed by smaller types. For
// example, time-millis uses Avro "int" but time.Duration is int64, which
// infers "long". Use int32 as A, or use the [CustomType] struct directly
// with an explicit AvroType.
//
// For fixed, records, or types needing extra schema metadata, use the
// [CustomType] struct directly.
func NewCustomType[G, A any](
	logicalType string,
	encode func(G, *SchemaNode) (A, error),
	decode func(A, *SchemaNode) (G, error),
) CustomType {
	goType := reflect.TypeFor[G]()
	avroType := inferAvroType(reflect.TypeFor[A]())

	var encFn func(any, *SchemaNode) (any, error)
	if encode != nil {
		encFn = func(v any, sn *SchemaNode) (any, error) {
			return encode(v.(G), sn)
		}
	}
	var decFn func(any, *SchemaNode) (any, error)
	if decode != nil {
		aType := reflect.TypeFor[A]()
		decFn = func(v any, sn *SchemaNode) (any, error) {
			a, ok := v.(A)
			if !ok {
				// The base deserializer produces the canonical Go value for A's
				// Avro kind (int32 for int, []byte for bytes, and so on). When A
				// is a *named* type over that kind (type UnixMillis int64), the
				// value's dynamic type is the base kind, not A, so a bare v.(A)
				// panics. inferAvroType keys on A's reflect.Kind, so the canonical
				// value is always convertible to A; convert rather than assert.
				rv := reflect.ValueOf(v)
				if !rv.IsValid() || !rv.Type().ConvertibleTo(aType) {
					return nil, fmt.Errorf("avro: custom decode: cannot convert %T to %s", v, aType)
				}
				a = rv.Convert(aType).Interface().(A)
			}
			return decode(a, sn)
		}
	}

	return CustomType{
		LogicalType:   logicalType,
		AvroType:      avroType,
		GoType:        goType,
		Encode:        encFn,
		Decode:        decFn,
		needsAvroType: true,
	}
}

// inferAvroType returns "" for an unsupported type, which Parse validates.
func inferAvroType(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Bool:
		return "boolean"
	case reflect.Int32:
		return "int"
	case reflect.Int64:
		return "long"
	case reflect.Float32:
		return "float"
	case reflect.Float64:
		return "double"
	case reflect.String:
		return "string"
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "bytes"
		}
	}
	return ""
}

// setCustomResult sets a custom conversion result into the target, allocating
// pointees along the way. A result the final target cannot take is a
// SemanticError, not a reflect.Value.Set panic.
func setCustomResult(v reflect.Value, result any, avroType string) error {
	if result == nil {
		setZero(v)
		return nil
	}
	rv := reflect.ValueOf(result)
	// Walk pointers, allocating as needed, stopping early once the result is
	// directly assignable. Bounded by maxIndirectDepth, the same ceiling the
	// non-custom path applies: a cyclic target type (type P *P) would otherwise
	// allocate a level per iteration forever.
	for i := 0; v.Kind() == reflect.Pointer; i++ {
		if rv.Type().AssignableTo(v.Type()) {
			v.Set(rv)
			return nil
		}
		if i >= maxIndirectDepth {
			return &SemanticError{GoType: v.Type(), AvroType: avroType}
		}
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}
	if !rv.Type().AssignableTo(v.Type()) {
		return &SemanticError{GoType: v.Type(), AvroType: avroType}
	}
	v.Set(rv)
	return nil
}

// wrapDeserWithCustomDecoders wraps a deserfn with custom decode functions.
// When every decoder returns ErrSkipCustomType, we re-decode the wire into
// the real target through inner, the same decode a no-custom schema
// performs. A naive re-decode re-runs nested wrappers, O(depth^2), so if no
// nested custom matched during the probe, bypassCustom makes the re-decode
// one pass. A fresh interface target needs no probe: inner's output is the
// canonical value and doubles as the chain input.
func wrapDeserWithCustomDecoders(inner deserfn, decoders []func(any, *SchemaNode) (any, error), sn *SchemaNode) deserfn {
	return func(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
		// A no-match ancestor set this so the whole subtree decodes through inner
		// once: no custom matched anywhere in it, so skipping the chain is
		// identical to running it all-skip.
		if sl.bypassCustom {
			return inner(src, v, sl)
		}
		// A nil interface target decodes straight into v and reads it back
		// for the chain. A non-nil interface would reuse the held value in
		// place, so it takes the probe and re-decode path.
		if v.Kind() == reflect.Interface && v.IsNil() {
			rest, err := inner(src, v, sl)
			if err != nil {
				return rest, err
			}
			chainVal := v.Interface()
			for _, dec := range decoders {
				result, err := dec(chainVal, sn)
				if err != nil {
					if errors.Is(err, ErrSkipCustomType) {
						continue
					}
					return nil, err
				}
				sl.customMatches++
				return rest, setCustomResult(v, result, sn.Type)
			}
			return rest, nil // all-skip: v already holds the no-custom value
		}
		// Typed target: probe into a throwaway any for the chain, then re-decode
		// faithfully into v on the all-skip fall-through.
		var tmp any
		savedMatches := sl.customMatches
		rest, err := inner(src, reflect.ValueOf(&tmp).Elem(), sl)
		if err != nil {
			return rest, err
		}
		for _, dec := range decoders {
			result, err := dec(tmp, sn)
			if err != nil {
				if errors.Is(err, ErrSkipCustomType) {
					continue
				}
				return nil, err
			}
			sl.customMatches++
			return rest, setCustomResult(v, result, sn.Type)
		}
		// Every decoder skipped: re-decode the original wire into the typed
		// target. No nested custom matched, so bypass for a single pass;
		// otherwise re-decode with customs active to reproduce the nested match.
		if sl.customMatches == savedMatches {
			sl.bypassCustom = true
			_, err = inner(src, v, sl)
			sl.bypassCustom = false
			return rest, err
		}
		_, err = inner(src, v, sl)
		return rest, err
	}
}
