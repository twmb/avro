package avro

import (
	"encoding"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

var (
	textAppenderType    = reflect.TypeFor[encoding.TextAppender]()
	textMarshalerType   = reflect.TypeFor[encoding.TextMarshaler]()
	textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()
)

// reuseOrMakeStringAnyMap reuses v's existing map[string]any backing
// when v is an interface wrapping one (the streaming-decode pattern
// pinned by TestDecodeReuseAnyTargetStaleKeys); otherwise allocates a
// fresh map sized to hint. Shared by deserRecord's interface arm and
// decodeRecordAny so the two record-into-*any paths agree on reuse.
//
// Reuse retains keys not present in the schema (matches encoding/json
// into a non-empty map). Callers that need a fresh decode should clear
// or replace the map before each call.
func reuseOrMakeStringAnyMap(v reflect.Value, hint int) map[string]any {
	if inner := v.Elem(); inner.IsValid() && inner.Type() == mapStringAnyType {
		return inner.Interface().(map[string]any)
	}
	return make(map[string]any, hint)
}

// tryTextUnmarshal calls (*v).UnmarshalText(b) when v is addressable
// and its address implements [encoding.TextUnmarshaler]. Returns
// (true, err) when invoked; (false, nil) when v can't accept the text.
// Caller owns b — the helper does not copy. Used at every text-shaped
// decode site (Avro string, string+uuid, fixed+uuid, enum symbol;
// binary and JSON).
//
// TextUnmarshaler stands on its own — the decoder does not require
// the type to also implement TextMarshaler. The one-way Go idiom
// (parse-only types: config values, enum keys, lookup tables) is
// supported. A user can pair MarshalText on type A with
// UnmarshalText on type B without either type implementing both.
func tryTextUnmarshal(v reflect.Value, b []byte) (bool, error) {
	if !v.CanAddr() || !v.Addr().Type().Implements(textUnmarshalerType) {
		return false, nil
	}
	return true, v.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText(b)
}

// textOutFor returns the strongest text-out method on v: TextAppender
// is preferred (alloc-free), TextMarshaler is the fallback. Checks
// both v.Interface() (value method set) and v.Addr().Interface()
// (pointer method set on addressable values) so pointer-receiver
// MarshalText/AppendText on an addressable struct field is reachable
// — mirroring tryTextUnmarshal's TextUnmarshaler discovery via v.Addr().
//
// TextMarshaler / TextAppender stand on their own — the encoder does
// not require the type to also implement TextUnmarshaler.
func textOutFor(v reflect.Value) (encoding.TextAppender, encoding.TextMarshaler) {
	var appender encoding.TextAppender
	var marshaler encoding.TextMarshaler
	if v.CanInterface() {
		i := v.Interface()
		if a, ok := i.(encoding.TextAppender); ok {
			appender = a
		}
		if m, ok := i.(encoding.TextMarshaler); ok {
			marshaler = m
		}
	}
	if (appender == nil || marshaler == nil) && v.CanAddr() {
		i := v.Addr().Interface()
		if appender == nil {
			if a, ok := i.(encoding.TextAppender); ok {
				appender = a
			}
		}
		if marshaler == nil {
			if m, ok := i.(encoding.TextMarshaler); ok {
				marshaler = m
			}
		}
	}
	return appender, marshaler
}

// textValue materializes v's TextAppender or TextMarshaler output as
// a Go string. Returns (text, true, nil) on success; ("", false, nil)
// when v has no text-out method (caller falls through); ("", false,
// SemanticError) when the text-out method itself errored. avroType
// labels the SemanticError. Used at every text-shaped encode site
// (string+uuid, fixed+uuid, enum symbol; binary and JSON) so all
// callers share the wrap shape and the AppendText vs MarshalText
// preference.
func textValue(v reflect.Value, avroType string) (string, bool, error) {
	a, m := textOutFor(v)
	if a == nil && m == nil {
		return "", false, nil
	}
	var text []byte
	var err error
	if a != nil {
		text, err = a.AppendText(nil)
	} else {
		text, err = m.MarshalText()
	}
	if err != nil {
		return "", false, &SemanticError{GoType: v.Type(), AvroType: avroType, Err: err}
	}
	return string(text), true, nil
}

var (
	errIndirectNil  = errors.New("invalid nil in non-union, non-null")
	errIndirectDeep = errors.New("avro: pointer/interface chain on input is cyclic or nests deeper than supported")
)

// maxIndirectDepth bounds indirect/indirectAlloc unwrap loops. A self-
// referential interface (e.g. `var p any; p = &p`) creates a real cycle in
// Go that would otherwise spin forever in reflect.Value.Elem(). Five levels
// of pointer/interface wrapping is more than any realistic user value.
const maxIndirectDepth = 5

func indirect(v reflect.Value) (reflect.Value, error) {
	for range maxIndirectDepth {
		switch v.Kind() {
		case reflect.Invalid:
			// Defensive: an invalid Value (e.g. reflect.ValueOf(nil)
			// somewhere internally) reaches this guard rather than
			// panicking on a subsequent v.Type() call. Treat as nil.
			return v, errIndirectNil
		case reflect.Pointer, reflect.Interface:
			if v.IsNil() {
				return v, errIndirectNil
			}
			v = v.Elem()
		default:
			return v, nil
		}
	}
	return v, errIndirectDeep
}

func indirectAlloc(v reflect.Value) reflect.Value {
	for range maxIndirectDepth {
		switch v.Kind() {
		case reflect.Pointer:
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		case reflect.Interface:
			if v.IsNil() {
				return v
			}
			// Non-nil interface: unwrap only if the inner is a
			// non-nil pointer (write through the pointer is
			// addressable). For ANY other concrete — primitives,
			// structs, slices, maps, nil pointers — v.Elem() is
			// not addressable. Some decoders reach for v.Set(...)
			// on the unwrapped value (e.g. decodeNull zeros it,
			// decodeArray replaces the slice), which panics. Keep
			// the interface itself as the destination so those
			// decoders write via Set on the settable interface
			// Value.
			inner := v.Elem()
			if inner.Kind() != reflect.Pointer || inner.IsNil() {
				return v
			}
			v = inner
		default:
			return v
		}
	}
	return v
}

// setIface assigns rv to an interface-kind v with an assignability
// check. Returns a SemanticError if rv's type isn't assignable to v's
// interface type — the common case being a user passing
// *interface{Foo()} as a decode target, where the decoder produces a
// value that doesn't implement Foo. Without the check, reflect.Value.Set
// panics with "value of type X is not assignable to type Y".
//
// Caller contract: v.Kind() must be reflect.Interface. Concrete-kind v
// is rejected with a SemanticError rather than silently calling Set.
// Concrete-target paths must split the dispatch at the call site — see
// deserFixedUUIDReflect (Interface vs isUUIDType arms), deserTimeMillis
// (Interface vs durationType), and deserDuration (Interface vs
// avroDurationType) for the pattern.
//
// Use this on the cold paths (logical types, promoted decoders, resolved
// records, etc.) where the per-call function-boundary cost doesn't matter.
//
// On the HOT primitive paths (deserBoolean / setIntValue / deserString /
// the toAny=true branches in json_decode), do NOT use setIface. Pass rv
// across a function boundary and escape analysis loses sight of it,
// forcing every reflect.ValueOf(primitive) call to heap-allocate per
// decode (~+2 allocs / +330 B per record decode in the bench). Inline the
// check at the callsite instead, with the fast path written first so
// rv only exists on the slow branch:
//
//	if v.Type().NumMethod() == 0 {        // empty interface (any) — common
//	    v.Set(reflect.ValueOf(b))
//	    return nil
//	}
//	rv := reflect.ValueOf(b)              // slow path: typed interface
//	if !rv.Type().AssignableTo(v.Type()) {
//	    return &SemanticError{GoType: v.Type(), AvroType: "boolean"}
//	}
//	v.Set(rv)
//	return nil
func setIface(v, rv reflect.Value, avroType string) error {
	if v.Kind() != reflect.Interface {
		return &SemanticError{GoType: v.Type(), AvroType: avroType}
	}
	if v.Type().NumMethod() == 0 || rv.Type().AssignableTo(v.Type()) {
		v.Set(rv)
		return nil
	}
	return &SemanticError{GoType: v.Type(), AvroType: avroType}
}

// mapKeyAs returns key as a reflect.Value typed to match mapType.Key().
// Fast-path identity when the types already match (the common
// map[string]V case); slow-path Convert() for named-string-key maps
// (e.g. `type UserID string; map[UserID]V`). Without this conversion,
// reflect.MapIndex / SetMapIndex panics with "value of type string is
// not assignable to type X". Used by every record-as-map encode and
// every map-decode site that builds an Avro map with string keys.
func mapKeyAs(mapType reflect.Type, key reflect.Value) reflect.Value {
	if mapType.Key() == key.Type() {
		return key
	}
	return key.Convert(mapType.Key())
}

// fieldByIndex is like reflect.Value.FieldByIndex but allocates nil embedded
// pointer structs along the path, which is needed during deserialization.
func fieldByIndex(v reflect.Value, index []int) reflect.Value {
	for _, i := range index {
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v
}

// cachedMapping holds the results of typeFieldMapping, cached per Go type.
type cachedMapping struct {
	indices  [][]int
	omitzero []bool
}

// typeFieldMapping returns the field index paths for each schema field in the
// given Go type. It handles embedded (anonymous) structs and inline-tagged
// fields by recursing into them. Avro-tagged fields take priority over
// name-matched fields, and shallower fields take priority over deeper ones.
//
// The result is cached in the provided sync.Map for subsequent calls with the
// same type.
func typeFieldMapping(fieldNames []string, cache *sync.Map, t reflect.Type) (*cachedMapping, error) {
	if cache != nil {
		if v, ok := cache.Load(t); ok {
			return v.(*cachedMapping), nil
		}
	}

	type fieldInfo struct {
		name     string
		index    []int
		tagged   bool
		omitzero bool
	}

	// collect walks the struct tree depth-first, recording fields in
	// encounter order. Shallower fields are seen first, which matters
	// for the priority logic below.
	var fields []fieldInfo
	var collect func(t reflect.Type, index []int, visited map[reflect.Type]bool)
	collect = func(t reflect.Type, index []int, visited map[reflect.Type]bool) {
		if visited[t] {
			return // prevent infinite recursion on embedded struct cycles
		}
		visited[t] = true
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
				// Recurse into embedded structs (even unexported
				// ones, since they can have exported fields).
				if ft.Kind() == reflect.Struct {
					tag := sf.Tag.Get("avro")
					if tag == "-" {
						continue
					}
					// If the embedded struct has an explicit avro
					// tag, treat it as a named field rather than
					// inlining its fields.
					parts := strings.Split(tag, ",")
					name := parts[0]
					if name != "" {
						_, oz := parseTagOptions(parts[1:])
						fields = append(fields, fieldInfo{
							name:     name,
							index:    idx,
							tagged:   true,
							omitzero: oz,
						})
						continue
					}
					collect(ft, idx, visited)
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
			parts := strings.Split(tag, ",")
			name := parts[0]
			tagged := name != ""
			inline, oz := parseTagOptions(parts[1:])

			// inline: recurse into the struct's fields like an
			// anonymous embed.
			if inline {
				ft := sf.Type
				if ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				if ft.Kind() == reflect.Struct {
					collect(ft, idx, visited)
					continue
				}
			}

			if name == "" {
				name = sf.Name
			}
			fields = append(fields, fieldInfo{
				name:     name,
				index:    idx,
				tagged:   tagged,
				omitzero: oz,
			})
		}
	}
	collect(t, nil, make(map[reflect.Type]bool))

	// Build name -> index map. Tagged fields win over untagged, and
	// shallower fields win over deeper ones.
	type entry struct {
		index    []int
		tagged   bool
		omitzero bool
	}
	m := make(map[string]entry, len(fields))
	for _, f := range fields {
		if existing, ok := m[f.name]; ok {
			// Tagged always beats untagged.
			if f.tagged && !existing.tagged {
				m[f.name] = entry{f.index, f.tagged, f.omitzero}
				continue
			}
			if !f.tagged && existing.tagged {
				continue
			}
			// Same tagged status: shallower (shorter index) wins.
			if len(f.index) < len(existing.index) {
				m[f.name] = entry{f.index, f.tagged, f.omitzero}
			}
			continue
		}
		m[f.name] = entry{f.index, f.tagged, f.omitzero}
	}

	ats := make([][]int, 0, len(fieldNames))
	ozs := make([]bool, 0, len(fieldNames))
	for _, name := range fieldNames {
		e, exists := m[name]
		if !exists {
			return nil, &SemanticError{GoType: t, AvroType: "record", Err: fmt.Errorf("missing field %s", name)}
		}
		ats = append(ats, e.index)
		ozs = append(ozs, e.omitzero)
	}

	result := &cachedMapping{indices: ats, omitzero: ozs}
	if cache != nil {
		cache.Store(t, result)
	}
	return result, nil
}

// parseTagOptions parses tag options after the field name. It returns whether
// "inline" and "omitzero" were found.
func parseTagOptions(opts []string) (inline, omitzero bool) {
	for _, o := range opts {
		switch o {
		case "inline":
			inline = true
		case "omitzero":
			omitzero = true
		}
	}
	return
}

// valueIsZero reports whether v is the zero value for its type, or implements
// an IsZero() bool method that returns true.
func valueIsZero(v reflect.Value) bool {
	if v.CanInterface() {
		if z, ok := v.Interface().(interface{ IsZero() bool }); ok {
			return z.IsZero()
		}
	}
	return v.IsZero()
}
