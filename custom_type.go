package avro

import (
	"errors"
	"reflect"
)

// ErrSkipCustomType is returned from a [CustomType] Encode or Decode
// function to indicate the value is not handled by this custom type.
// The library falls through to the next matching custom type or to
// built-in behavior.
var ErrSkipCustomType = errors.New("avro: skip custom type")

// CustomType defines a custom conversion between a Go type and an Avro
// type. Use this when you need full control over the type mapping — for
// example, to map a custom Go struct to/from an Avro fixed or record, to
// handle complex Avro types (records, arrays, maps) as backing types, or
// to dispatch on schema properties rather than logical type names. For
// simpler cases where the backing type is a primitive, prefer
// [NewCustomType] which infers the wiring from type parameters.
//
// Pass to [Parse] or [SchemaFor] as a [SchemaOpt].
//
// Matching at parse time: LogicalType and AvroType are checked against
// schema nodes. All non-empty criteria must match.
//   - LogicalType only: matches any schema node with that logicalType
//   - LogicalType + AvroType: matches that logicalType on that Avro type
//   - AvroType only: matches all nodes of that Avro type
//   - Neither: matches ALL schema nodes (use with [ErrSkipCustomType]
//     for property-based dispatch like Kafka Connect types)
//
// At encode time, GoType is also checked: the Encode function only
// fires when the value's type matches GoType. This prevents the codec
// from intercepting native values (e.g. a raw int64 passes through
// without conversion for a custom-typed long field).
//
// A matching CustomType replaces the built-in logical type
// deserializer. Among user registrations, first match wins.
//
// For custom types backed by complex Avro types (records, arrays,
// maps), use the struct form directly — the Encode function can return
// map[string]any, []any, etc. [NewCustomType] is limited to primitive
// backing types.
type CustomType struct {
	// LogicalType narrows matching to schema nodes with this logicalType.
	LogicalType string

	// AvroType narrows matching to schema nodes of this Avro type
	// (e.g. "long", "bytes", "record"). Also used by SchemaFor to
	// infer the underlying Avro type.
	AvroType string

	// GoType adds an encode-time filter: when set, the Encode function
	// only fires when the value's concrete type matches GoType. Values
	// of other types pass through to the underlying serializer unchanged.
	// If nil, Encode fires for all values on matched schema nodes
	// (those matching LogicalType/AvroType).
	//
	// [SchemaFor] uses GoType to match struct fields: when a field's Go
	// type equals GoType, SchemaFor emits AvroType + LogicalType (or
	// Schema) instead of the default type mapping. If nil, the custom
	// type does not affect schema generation, but is still wired into
	// the returned [*Schema] for encode/decode.
	GoType reflect.Type

	// Schema is the full schema to emit in SchemaFor. Only needed for
	// types requiring extra metadata (fixed needs name+size, decimal
	// needs precision+scale, records need fields). If nil, SchemaFor
	// infers from AvroType + LogicalType.
	Schema *SchemaNode

	// Encode converts a caller-provided Go value to an Avro-native
	// value, called before serialization. The callback receives the
	// value as passed to [Schema.Encode] (e.g. a custom Money type),
	// and should return the corresponding Avro-native value (e.g.
	// int64 cents). Return [ErrSkipCustomType] to fall through to the
	// next matching custom type or built-in behavior. Any other
	// non-nil error is fatal.
	//
	// If nil, the built-in logical type encoder is used, which accepts
	// both enriched types ([time.Time], [time.Duration]) and raw
	// values (int64, int32, etc.).
	Encode func(v any, schema *SchemaNode) (any, error)

	// Decode converts a raw Avro-native value to a custom Go value,
	// called after deserialization. The callback receives the raw
	// Avro-native value (int32 for int, int64 for long, []byte for
	// bytes/fixed, etc.) and should return the desired Go type.
	// Return [ErrSkipCustomType] to fall through. Any other non-nil
	// error is fatal.
	//
	// If nil, the built-in logical type handler is bypassed and the
	// base Avro type decoder is used directly, producing raw
	// Avro-native values (int32, int64, etc.) rather than enriched
	// types ([time.Time], [time.Duration], etc.).
	Decode func(v any, schema *SchemaNode) (any, error)

	// Set by NewCustomType; if true and AvroType is "", Parse returns
	// an error ("unsupported Avro native type").
	needsAvroType bool
}

func (CustomType) schemaOpt() {}

// WithCustomType registers a custom type conversion for use with
// [Parse], [SchemaCache.Parse], or [SchemaFor]. [CustomType] and
// [NewCustomType] both satisfy [SchemaOpt] directly, so this wrapper
// is optional — it exists for discoverability.
func WithCustomType(ct CustomType) SchemaOpt { return ct }

// matches returns true if ct's criteria match the given schema node.
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
// mapping a custom Go type to/from a primitive Avro type. For example,
// use this to decode Avro longs into a domain-specific ID type, or to
// encode a Money type as Avro bytes with a "decimal" logical type.
//
// G is the custom Go type (e.g. Money). A is the Avro-native Go type:
// int32 for int, int64 for long, float32 for float, float64 for double,
// string for string, []byte for bytes, bool for boolean.
//
// GoType and AvroType are inferred from the type parameters. If A is
// not a supported Avro-native type, [Parse] or [SchemaFor] returns an
// error.
//
// Note: AvroType is inferred from A's Go kind, which may not match
// the Avro schema's type for logical types backed by smaller types.
// For example, time-millis uses Avro "int" but time.Duration is int64
// (which infers "long"). Use int32 as A, or use the [CustomType]
// struct directly with an explicit AvroType.
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
		decFn = func(v any, sn *SchemaNode) (any, error) {
			return decode(v.(A), sn)
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

// inferAvroType maps a Go reflect.Type to an Avro type name.
// Returns "" for unsupported types (validated at Parse time).
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

// setCustomResult sets a custom type conversion result into the target
// reflect.Value. For interface targets (e.g. *any), we set the interface
// directly. For pointer targets, we allocate and set the pointee. For
// concrete struct targets, we set the value directly.
func setCustomResult(v reflect.Value, result any) {
	if result == nil {
		// Nil result — set zero value for nullable types.
		switch v.Kind() {
		case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice:
			v.Set(reflect.Zero(v.Type()))
		}
		return
	}
	rv := reflect.ValueOf(result)
	// Walk through pointers, allocating as needed. Stop early if
	// the result is directly assignable (e.g. pointer-valued custom
	// decoder returning *T into a *T target).
	for v.Kind() == reflect.Pointer {
		if rv.Type().AssignableTo(v.Type()) {
			v.Set(rv)
			return
		}
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}
	v.Set(rv)
}

// wrapDeserWithCustomDecoders wraps a deserfn with custom decode functions.
// Used both at parse time and during schema resolution to re-apply
// custom decoders to promoted/resolved nodes.
func wrapDeserWithCustomDecoders(inner deserfn, decoders []func(any, *SchemaNode) (any, error), sn *SchemaNode) deserfn {
	return func(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
		var tmp any
		src, err := inner(src, reflect.ValueOf(&tmp).Elem(), sl)
		if err != nil {
			return src, err
		}
		for _, dec := range decoders {
			result, err := dec(tmp, sn)
			if err != nil {
				if errors.Is(err, ErrSkipCustomType) {
					continue
				}
				return nil, err
			}
			setCustomResult(v, result)
			return src, nil
		}
		// No decoder matched — set the raw Avro-native value.
		setCustomResult(v, tmp)
		return src, nil
	}
}
