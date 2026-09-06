package avro

import (
	"encoding"
	"errors"
	"fmt"
	"reflect"
	"slices"
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

	// The walk is lenient: a tag SchemaFor refuses never stopped an encode
	// or decode, and a name the schema never asks for costs nothing.
	var fields []promotedField
	walkPromotedFields(t, nil, make(map[reflect.Type]bool), false, func(f promotedField) error {
		fields = append(fields, f)
		return nil
	})
	winners, ambiguous := resolvePromotedFields(t, fields)
	byName := make(map[string]int, len(winners))
	for _, w := range winners {
		byName[fields[w].name()] = w
	}

	ats := make([][]int, 0, len(fieldNames))
	ozs := make([]bool, 0, len(fieldNames))
	for _, name := range fieldNames {
		if names, amb := ambiguous[name]; amb {
			return nil, &SemanticError{GoType: t, AvroType: "record", Err: ambiguousFieldError(t, name, names)}
		}
		w, exists := byName[name]
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
		ats = append(ats, fields[w].index)
		ozs = append(ozs, fields[w].hasOption("omitzero"))
	}

	result := &cachedMapping{indices: ats, omitzero: ozs}
	if cache != nil {
		cache.Store(ckey, result)
	}
	return result, nil
}

// promotedField is one field a struct type promotes to the Avro record: the
// Go field, its index path from the root type, and its avro tag split into
// the name and the options.
type promotedField struct {
	sf    reflect.StructField
	index []int
	parts []string // parts[0] is the tag name, "" when untagged
}

func (f *promotedField) name() string {
	if f.parts[0] != "" {
		return f.parts[0]
	}
	return f.sf.Name
}

func (f *promotedField) tagged() bool { return f.parts[0] != "" }

func (f *promotedField) hasOption(opt string) bool { return slices.Contains(f.parts[1:], opt) }

// structBehind returns t through one pointer and whether that is a struct.
func structBehind(t reflect.Type) (reflect.Type, bool) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t, t.Kind() == reflect.Struct
}

// walkPromotedFields walks t depth-first and calls visit for every field t
// promotes to the Avro record: exported fields, and the fields of embedded
// and inline-tagged structs, flattened. Fields arrive in encounter order,
// shallower first, which is what resolvePromotedFields rests on. SchemaFor
// and the runtime field mapper share the walk, so the schema SchemaFor
// infers names exactly the fields Encode and Decode reach.
//
// strict refuses the tags SchemaFor rejects: a skip directive with a suffix,
// unbalanced brackets, inline beside a name or another option, and inline on
// a non-struct. The mapper walks lenient, since encode and decode never
// rejected them: a malformed tag keeps its name up to the first comma and no
// options, and inline flattens a struct field whatever else the tag says.
//
// visited marks the type on the current path only. The same type reached
// through two sibling embed paths is collected at each occurrence, so the
// shallower one reaches the shallowest-wins resolution and a type inlined
// twice surfaces as the collision it is. Marking forever pruned the sibling
// occurrence and picked the deeper field.
func walkPromotedFields(t reflect.Type, index []int, visited map[reflect.Type]bool, strict bool, visit func(promotedField) error) error {
	if visited[t] {
		return nil
	}
	visited[t] = true
	defer delete(visited, t)
	for i := range t.NumField() {
		sf := t.Field(i)
		ft, isStruct := structBehind(sf.Type)
		// We recurse into an unexported embedded struct too, since it can
		// carry exported fields.
		embedded := sf.Anonymous && isStruct
		if !embedded && !sf.IsExported() {
			continue
		}
		tag := sf.Tag.Get("avro")
		if tag == "-" {
			continue
		}
		if err := checkSkipDirectiveExact(sf.Name, tag); err != nil && strict {
			return err
		}
		parts, err := splitTag(tag)
		if err != nil {
			if strict {
				return err
			}
			name, _, _ := strings.Cut(tag, ",")
			parts = []string{name}
		}
		idx := make([]int, len(index)+1)
		copy(idx, index)
		idx[len(index)] = i
		f := promotedField{sf: sf, index: idx, parts: parts}
		inline := f.hasOption("inline")
		switch {
		case embedded && f.tagged():
			// An embedded struct with an explicit name is a field, not an
			// inline of its own fields; inline says the opposite.
			if strict && inline {
				return inlineNameError(sf, tag)
			}
		case embedded:
			// An anonymous embed flattens. It has no Avro field of its own
			// at this position, so an option that applies to a field has no
			// target; we reject rather than drop.
			if strict {
				for _, p := range parts[1:] {
					if p != "inline" {
						return inlineOptionError(sf, tag, p, "the anonymous embed flattens")
					}
				}
			}
			if err := walkPromotedFields(ft, idx, visited, strict, visit); err != nil {
				return err
			}
			continue
		case inline:
			if strict {
				if f.tagged() {
					return inlineNameError(sf, tag)
				}
				for _, p := range parts[1:] {
					if p != "inline" {
						return inlineOptionError(sf, tag, p, "inline flattens the embed")
					}
				}
				if !isStruct {
					return fmt.Errorf("avro: field %s has tag %q: inline requires a struct or pointer-to-struct field type; got %s (inline flattens the embed; there is no struct here to flatten)",
						sf.Name, truncForError(tag), ft)
				}
			}
			if isStruct {
				if err := walkPromotedFields(ft, idx, visited, strict, visit); err != nil {
					return err
				}
				continue
			}
		}
		if err := visit(f); err != nil {
			return err
		}
	}
	return nil
}

func inlineNameError(sf reflect.StructField, tag string) error {
	return fmt.Errorf("avro: field %s has tag %q: inline is incompatible with an explicit field name (inline flattens the embed; there is no field at this position to name)",
		sf.Name, truncForError(tag))
}

func inlineOptionError(sf reflect.StructField, tag, opt, why string) error {
	return fmt.Errorf("avro: field %s has tag %q: inline is incompatible with option %q (%s; there is no field at this position for the option to apply to)",
		sf.Name, truncForError(tag), truncForError(opt), why)
}

// resolvePromotedFields decides which promoted field owns each Avro name over
// the complete set walkPromotedFields collected from t, so the inferred
// schema and the runtime mapping pick the same Go field for every name.
// Tagged beats untagged at any depth; among same-tagged-status fields the
// shallower wins; and only a collision that survives at the winning depth is
// ambiguous, as Java's setFields and hamba treat it. The rule ranges over the
// whole set rather than per recursion level, because a shallower field
// declared later resolves a same-depth deep collision, as Go's own promotion
// does.
//
// winners holds the owning fields' positions, one per name in the order the
// names were first encountered; ambiguous maps each unresolved name to the
// two colliding Go field names.
func resolvePromotedFields(t reflect.Type, fields []promotedField) (winners []int, ambiguous map[string][2]string) {
	owner := make(map[string]int, len(fields))
	ambiguous = make(map[string][2]string)
	for i := range fields {
		f := &fields[i]
		name := f.name()
		cur, ok := owner[name]
		if !ok {
			owner[name] = i
			continue
		}
		existing := &fields[cur]
		switch {
		case f.tagged() && !existing.tagged():
			owner[name] = i
			delete(ambiguous, name)
		case !f.tagged() && existing.tagged():
		case len(f.index) < len(existing.index):
			owner[name] = i
			delete(ambiguous, name)
		case len(f.index) == len(existing.index):
			ambiguous[name] = [2]string{t.FieldByIndex(existing.index).Name, t.FieldByIndex(f.index).Name}
		}
	}
	seen := make(map[string]bool, len(owner))
	for i := range fields {
		name := fields[i].name()
		if seen[name] {
			continue
		}
		seen[name] = true
		winners = append(winners, owner[name])
	}
	return winners, ambiguous
}

// ambiguousFieldError reports an Avro name that two promoted fields of t
// claim at the winning depth. SchemaFor prefixes it and the mapper wraps it
// in a SemanticError, so the two report one message.
func ambiguousFieldError(t reflect.Type, name string, fields [2]string) error {
	return fmt.Errorf("duplicate field name %q in type %s (fields %q and %q both map to the same Avro name)",
		truncForError(name), t.String(), truncForError(fields[0]), truncForError(fields[1]))
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
