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
	isZeroerType        = reflect.TypeFor[interface{ IsZero() bool }]()
	byteType            = reflect.TypeFor[byte]()
)

// copyBytesToArray writes b into the byte-array v: reflect.Copy's memmove for
// an exact-uint8 element, element-wise SetUint for a *named* byte type. You
// have already established that v is an Array of Uint8-kind elements with
// v.Len() == len(b).
//
// reflect.Copy panics on [N]B where type B byte. That panic reaches the public
// Decode on a value we happily encode, since the byte encoders iterate via
// Uint. SetUint writes through the Kind and restores round-trip parity.
func copyBytesToArray(v reflect.Value, b []byte) {
	if v.Type().Elem() == byteType {
		reflect.Copy(v, reflect.ValueOf(b))
		return
	}
	for i := range b {
		v.Index(i).SetUint(uint64(b[i]))
	}
}

// byteArrayToSlice reads the byte-array v into a fresh []byte, the encode-side
// counterpart of [copyBytesToArray]: reflect.Copy's memmove for an exact-uint8
// element, element-wise reads for a named byte element ([N]B, type B byte),
// where reflect.Copy would panic on the exact-type mismatch. You have already
// established that v is an Array whose element Kind is Uint8.
func byteArrayToSlice(v reflect.Value) []byte {
	b := make([]byte, v.Len())
	if v.Type().Elem() == byteType {
		reflect.Copy(reflect.ValueOf(b), v)
		return b
	}
	for i := range b {
		b[i] = byte(v.Index(i).Uint())
	}
	return b
}

// reuseOrMakeStringAnyMap reuses v's existing map[string]any when v is an
// interface wrapping one, the streaming-decode pattern; otherwise we allocate a
// fresh map sized to hint. deserRecord's interface arm and decodeRecordAny both
// use it, so the two record-into-*any paths agree on reuse.
//
// Reuse keeps keys the schema does not name. If you want a fresh decode, clear
// or replace the map before each call.
func reuseOrMakeStringAnyMap(v reflect.Value, hint int) map[string]any {
	if inner := v.Elem(); inner.IsValid() && inner.Type() == mapStringAnyType {
		return inner.Interface().(map[string]any)
	}
	return make(map[string]any, hint)
}

// tryTextUnmarshal calls (*v).UnmarshalText(b) when v is addressable and its
// address implements [encoding.TextUnmarshaler]. Returns (true, err) when we
// called it, (false, nil) when v cannot accept the text. You own b; we do not
// copy it. Every text-shaped decode site uses it, binary and JSON.
//
// TextUnmarshaler stands alone: we do not also require TextMarshaler, so your
// one-way parse-only type works.
func tryTextUnmarshal(v reflect.Value, b []byte) (bool, error) {
	if !v.CanAddr() || !v.Addr().Type().Implements(textUnmarshalerType) {
		return false, nil
	}
	return true, v.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText(b)
}

// textOutFor returns v's text-out methods, preferring TextAppender
// (alloc-free) over TextMarshaler. We check both the value method set and,
// on an addressable value, the pointer method set, so a pointer-receiver
// method on an addressable struct field is reachable, as tryTextUnmarshal
// does for the other direction.
//
// Every encode site tries text-out before the reflect.String and enum arms,
// so this runs on every plain string and enum encode. The type check up front
// short-circuits before the v.Interface() boxing for types with no text-out
// method, which keeps that common path allocation-free.
func textOutFor(v reflect.Value) (encoding.TextAppender, encoding.TextMarshaler) {
	if !implementsTextMarshaler(v.Type()) {
		return nil, nil
	}
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

// textValue materializes v's TextAppender or TextMarshaler output as a string:
// (text, true, nil) on success, ("", false, nil) when v has no text-out method
// and you should fall through, ("", false, SemanticError) when the method
// itself errored. Every text-shaped encode site uses it, so they share one wrap
// shape and one AppendText-over-MarshalText preference.
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

// implementsTextMarshaler reports whether t's value or pointer method set
// implements TextMarshaler or TextAppender. The fast string-encode paths read
// the underlying string directly and bypass appendAvroString's text-out arm.
// Keeping text-method types off them is what makes your type encode its
// marshaled form in a struct field as it does as a scalar. We evaluate
// this once per type at compile time, never per value.
func implementsTextMarshaler(t reflect.Type) bool {
	// The pointer method set is a superset of the value one, so an empty
	// pointer set means no methods at all. One NumMethod read beats four
	// Implements scans on the per-element encode path, where zero-method
	// string and enum types dominate.
	pt := reflect.PointerTo(t)
	if pt.NumMethod() == 0 {
		return false
	}
	return t.Implements(textMarshalerType) || t.Implements(textAppenderType) ||
		pt.Implements(textMarshalerType) || pt.Implements(textAppenderType)
}

// implementsTextUnmarshaler reports whether *t implements TextUnmarshaler. The
// fast string-decode paths and the array/map loops write the wire string
// directly and bypass setStringValue's UnmarshalText arm, so keeping such types
// off them makes a struct field or container decode as a scalar does.
func implementsTextUnmarshaler(t reflect.Type) bool {
	return reflect.PointerTo(t).Implements(textUnmarshalerType)
}

// stringFastPathEligibleEncode reports whether a reflect.String-kind Go type
// may take a fast string-encode path. It must take reflect when it is
// json.Number (appendAvroString's RFC 8259 reject) or implements a text-out
// method (appendAvroString's text-out arm), since the fast paths bypass both.
// Every encode gate asks this one function.
func stringFastPathEligibleEncode(t reflect.Type) bool {
	return t != jsonNumberType && !implementsTextMarshaler(t)
}

// stringFastPathEligibleDecode is the decode-side counterpart: a
// reflect.String-kind target is fast-path-ineligible when it is json.Number
// (setStringValue's RFC 8259 guard) or implements TextUnmarshaler
// (setStringValue's UnmarshalText arm).
func stringFastPathEligibleDecode(t reflect.Type) bool {
	return t != jsonNumberType && !implementsTextUnmarshaler(t)
}

var (
	errIndirectNil  = errors.New("invalid nil in non-union, non-null")
	errIndirectDeep = errors.New("avro: pointer/interface chain on input is cyclic or nests deeper than supported")
)

// maxIndirectDepth bounds the indirect/indirectAlloc unwrap loops. A
// self-referential interface (`var p any; p = &p`) is a real cycle in Go that
// would otherwise spin forever in reflect.Value.Elem(). Five levels of
// pointer/interface wrapping is a generous cap; deeper chains error.
const maxIndirectDepth = 5

func indirect(v reflect.Value) (reflect.Value, error) {
	for range maxIndirectDepth {
		switch v.Kind() {
		case reflect.Invalid:
			// Defensive: an invalid Value (reflect.ValueOf(nil) somewhere
			// internally) ends up here instead of panicking on a later
			// v.Type() call. We treat it as nil.
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
	// A base reached at exactly the cap is accepted, matching indirectAlloc,
	// isNilValue, and serNull; otherwise a maxIndirectDepth-deep pointer
	// value would decode but fail to encode. Only a still-indirect value is
	// too deep.
	switch v.Kind() {
	case reflect.Invalid:
		return v, errIndirectNil
	case reflect.Pointer, reflect.Interface:
		return v, errIndirectDeep
	default:
		return v, nil
	}
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
			// Non-nil interface: we unwrap only if the inner is a
			// non-nil pointer, since writing through the pointer
			// is addressable. For any other concrete value, be it
			// a primitive, struct, slice, map, or nil pointer,
			// v.Elem() is not addressable, and some decoders reach
			// for v.Set(...) on the unwrapped value (decodeNull
			// zeros it, decodeArray replaces the slice), which
			// panics. So we keep the interface itself as the
			// destination and let those decoders Set through the
			// settable interface Value.
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

// setIface assigns rv to an interface-kind v, checking assignability first.
// Without the check reflect.Value.Set panics, which is what you get when you
// pass *interface{Foo()} as a decode target.
//
// v.Kind() must be reflect.Interface. A concrete v gets a SemanticError, so
// concrete-target paths split the dispatch at the call site; see
// deserFixedUUIDReflect for the pattern.
//
// Cold paths only. Do *not* use setIface on the hot primitive paths: passing rv
// across a function boundary loses escape analysis and heap-allocates every
// reflect.ValueOf(primitive), ~+2 allocs / +330 B per record decode in the
// bench. Inline the check instead, fast path first so rv exists only on the
// slow branch:
//
//	if v.Type().NumMethod() == 0 {        // empty interface (any), common
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

// mapKeyAs returns key typed to match mapType.Key(): key itself when the types
// already match (the common map[string]V), Convert() for a named-string-key map
// (`type UserID string; map[UserID]V`). Without the conversion, reflect.MapIndex
// and SetMapIndex panic with "value of type string is not assignable to type X".
// Every record-as-map encode and every string-keyed map decode goes through it.
func mapKeyAs(mapType reflect.Type, key reflect.Value) reflect.Value {
	if mapType.Key() == key.Type() {
		return key
	}
	return key.Convert(mapType.Key())
}

// fieldByIndex is reflect.Value.FieldByIndex, except we allocate nil embedded
// pointer structs along the path. Decoding needs that.
func fieldByIndex(v reflect.Value, index []int) (reflect.Value, error) {
	for _, i := range index {
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				// A nil embedded pointer reached through an *unexported*
				// embedded field name is unsettable: Go reflection cannot
				// allocate it. We error cleanly rather than panic in Set.
				// encoding/json cannot write through one either.
				if !v.CanSet() {
					return reflect.Value{}, fmt.Errorf("avro: cannot decode into a field promoted through a nil unexported embedded pointer")
				}
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v, nil
}

// fieldByIndexZero is the read-only (encode) counterpart of fieldByIndex. We
// walk index from v and return the target field's zero value when the path
// crosses a nil embedded pointer, rather than panicking. So a nil embedded
// *struct encodes its promoted fields as zero, symmetric with fieldByIndex
// allocating them on decode.
func fieldByIndexZero(v reflect.Value, index []int) reflect.Value {
	for n, i := range index {
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return reflect.Zero(fieldTypeByIndex(v.Type(), index[n:]))
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v
}

func fieldTypeByIndex(t reflect.Type, index []int) reflect.Type {
	for _, i := range index {
		if t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		t = t.Field(i).Type
	}
	return t
}

type cachedMapping struct {
	indices  [][]int
	omitzero []bool
}

// unmapped reports whether schema field i has no Go field, the sentinel
// [typeFieldMappingSkip] leaves behind for [SkipUnknown]. A mapped field's path
// always has at least one element, so an empty one cannot collide.
func (m *cachedMapping) unmapped(i int) bool { return len(m.indices[i]) == 0 }

// mappingKey keys the field-map cache. skipUnknown belongs in the key because
// it changes what we compile: leave it out and one call site's mapping answers
// another site's question in the opposite mode.
type mappingKey struct {
	t           reflect.Type
	skipUnknown bool
}

// typeFieldMapping maps every schema field to a Go field, erroring on the first
// with no match. This is the encode spelling and the strict default: a struct
// that does not cover the schema must not encode, or the fields it lacks go
// out as zero values.
func typeFieldMapping(fieldNames []string, cache *sync.Map, t reflect.Type) (*cachedMapping, error) {
	return typeFieldMappingSkip(fieldNames, cache, t, false)
}

// typeFieldMappingSkip returns the field index path for each schema field in
// the given Go type. We recurse into embedded (anonymous) structs and
// inline-tagged fields. Avro-tagged fields beat name-matched ones, and
// shallower fields beat deeper ones.
//
// With skipUnknown, a schema field your type has no home for yields a nil index
// path instead of an error; see [cachedMapping.unmapped]. An ambiguous name
// still errors either way: your type does have fields for it, and picking one
// arbitrarily is not skipping.
//
// We cache the result in the given sync.Map, keyed by type and mode.
func typeFieldMappingSkip(fieldNames []string, cache *sync.Map, t reflect.Type, skipUnknown bool) (*cachedMapping, error) {
	ckey := mappingKey{t, skipUnknown}
	if cache != nil {
		if v, ok := cache.Load(ckey); ok {
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
	// encounter order. We see shallower fields first, which is what the
	// priority logic below rests on.
	var fields []fieldInfo
	var collect func(t reflect.Type, index []int, visited map[reflect.Type]bool)
	collect = func(t reflect.Type, index []int, visited map[reflect.Type]bool) {
		if visited[t] {
			return // prevent infinite recursion on embedded struct cycles
		}
		// Per-path marking: the same type reached through two sibling embed
		// paths is not a cycle, and each occurrence must be collected so the
		// shallower one reaches the shallowest-wins dedup below. Marking
		// forever pruned the sibling occurrence and picked the deeper field.
		visited[t] = true
		defer delete(visited, t)
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
					// An embedded struct with an explicit avro tag
					// is a named field, not an inline of its own
					// fields.
					parts := splitFieldTag(tag)
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
			parts := splitFieldTag(tag)
			name := parts[0]
			tagged := name != ""
			inline, oz := parseTagOptions(parts[1:])

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

	type entry struct {
		index    []int
		tagged   bool
		omitzero bool
	}
	m := make(map[string]entry, len(fields))
	ambiguous := make(map[string][2]string) // name -> the two colliding Go field names
	for _, f := range fields {
		if existing, ok := m[f.name]; ok {
			if f.tagged && !existing.tagged {
				m[f.name] = entry{f.index, f.tagged, f.omitzero}
				delete(ambiguous, f.name)
				continue
			}
			if !f.tagged && existing.tagged {
				continue
			}
			if len(f.index) < len(existing.index) {
				m[f.name] = entry{f.index, f.tagged, f.omitzero}
				delete(ambiguous, f.name)
				continue
			}
			if len(f.index) == len(existing.index) {
				// Equal depth, same tagged status: ambiguous. encoding/json
				// drops such a field; we defer the error to lookup, so a
				// collision on a name the schema never references does not
				// break the whole struct. SchemaFor's collectFields rejects
				// eagerly because it must emit every field.
				ambiguous[f.name] = [2]string{t.FieldByIndex(existing.index).Name, t.FieldByIndex(f.index).Name}
			}
			continue
		}
		m[f.name] = entry{f.index, f.tagged, f.omitzero}
	}

	ats := make([][]int, 0, len(fieldNames))
	ozs := make([]bool, 0, len(fieldNames))
	for _, name := range fieldNames {
		if names, amb := ambiguous[name]; amb {
			return nil, &SemanticError{GoType: t, AvroType: "record", Err: fmt.Errorf(
				"duplicate field name %q in type %s (fields %q and %q both map to the same Avro name)",
				truncForError(name), t.String(), truncForError(names[0]), truncForError(names[1]))}
		}
		e, exists := m[name]
		if !exists {
			if skipUnknown {
				ats = append(ats, nil)
				ozs = append(ozs, false)
				continue
			}
			// name has no length cap and rides in .Err, which Error() does
			// not truncate, so we bound it here.
			return nil, &SemanticError{GoType: t, AvroType: "record", Err: fmt.Errorf("missing field %s", truncForError(name))}
		}
		ats = append(ats, e.index)
		ozs = append(ozs, e.omitzero)
	}

	result := &cachedMapping{indices: ats, omitzero: ozs}
	if cache != nil {
		cache.Store(ckey, result)
	}
	return result, nil
}

// splitFieldTag tokenizes an avro struct tag with the grammar SchemaFor uses
// (splitTag): top-level commas separate options, default= takes the rest
// verbatim, and bracketed alias=[...] and decimal(...) values do not split on
// their internal commas, so `alias=[x,inline,y]` cannot fire inline. A
// malformed tag (unbalanced brackets) does not error, since encode and decode
// never did, but fires no options at all: we map the field under the tag text
// up to the first comma and drop the rest, rather than let a bracket typo
// flip a field between nested and inlined.
func splitFieldTag(tag string) []string {
	parts, err := splitTag(tag)
	if err != nil {
		name, _, _ := strings.Cut(tag, ",")
		return []string{name}
	}
	return parts
}

// parseTagOptions reports whether "inline" and "omitzero" appear in the tag
// options after the field name. Input comes from [splitFieldTag], so an option
// carrying a value (default=..., alias=...) arrives as one part that can never
// equal a bare keyword.
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
	// A nil pointer or interface *is* the zero value. We short-circuit before
	// the IsZero assertion: a promoted value-receiver IsZero on a nil
	// *time.Time dereferences the nil and panics.
	if k := v.Kind(); (k == reflect.Pointer || k == reflect.Interface) && v.IsNil() {
		return true
	}
	if v.CanInterface() {
		if z, ok := v.Interface().(interface{ IsZero() bool }); ok {
			return z.IsZero()
		}
		// Pointer-receiver IsZero lives in *T's method set, which the assertion
		// above misses. We box a non-addressable value into an addressable temp
		// so Encode(v) and Encode(&v) agree, as they already do for a
		// value-receiver IsZero. The nil short-circuit above ran, so v is
		// non-nil and *T IsZero is safe to call.
		if reflect.PointerTo(v.Type()).Implements(isZeroerType) {
			if !v.CanAddr() {
				box := reflect.New(v.Type())
				box.Elem().Set(v)
				v = box.Elem()
			}
			return v.Addr().Interface().(interface{ IsZero() bool }).IsZero()
		}
	}
	return v.IsZero()
}

// setZero sets v to its type's zero value. A null must clear the target,
// concrete primitives included, or a pre-populated field keeps stale data
// across a reused decode and breaks our documented promise that null clears.
func setZero(v reflect.Value) {
	v.Set(reflect.Zero(v.Type()))
}
