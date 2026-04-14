package avro

import (
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type serfn func([]byte, reflect.Value) ([]byte, error)

// AppendEncode appends the Avro binary encoding of v to dst. See
// [Schema.Decode] for the Go-to-Avro type mapping. In addition to the types
// listed there, encoding also accepts:
//   - [encoding/json.Number] for any numeric Avro type (int, long, float, double)
//   - RFC 3339 strings for timestamp and date logical types
//   - [*big.Rat], [big.Rat], float64, [encoding/json.Number], and numeric strings for decimal logical types
//   - [encoding.TextAppender], [encoding.TextMarshaler], and []byte for string types (and vice versa for [encoding.TextUnmarshaler])
//   - string (hex-dash UUID format) for fixed(16) UUID logical types
//   - Tagged union maps (map[string]any{"typeName": value}) for union types,
//     as produced by [Schema.Decode] with [TaggedUnions]
func (s *Schema) AppendEncode(dst []byte, v any, opts ...Opt) ([]byte, error) {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		// nil is valid for null schemas and unions (null branch).
		switch s.node.kind {
		case "null":
			return dst, nil
		case "union":
			return s.ser(dst, rv)
		default:
			return nil, &SemanticError{AvroType: s.node.kind, Err: errors.New("cannot encode nil")}
		}
	}
	return s.ser(dst, rv)
}

// Encode encodes v as Avro binary. It is shorthand for AppendEncode(nil, v).
func (s *Schema) Encode(v any, opts ...Opt) ([]byte, error) {
	return s.AppendEncode(nil, v, opts...)
}

///////////
// UNION //
///////////

type serUnion struct {
	fns         []serfn
	branchNames map[string]int // branch name → index for tagged union map unwrapping
}

// tryUnwrapTagged checks if v is a single-key map whose key matches a
// branch name. Returns the branch index and unwrapped value on match.
func (s *serUnion) tryUnwrapTagged(v reflect.Value) (int, reflect.Value, bool) {
	if v.Kind() == reflect.Interface && !v.IsNil() {
		v = v.Elem()
	}
	if !v.IsValid() || v.Kind() != reflect.Map || v.Type().Key().Kind() != reflect.String || v.Len() != 1 {
		return 0, v, false
	}
	iter := v.MapRange()
	iter.Next()
	if idx, ok := s.branchNames[iter.Key().String()]; ok {
		return idx, iter.Value(), true
	}
	return 0, v, false
}

// ser encodes a union value. Tagged union maps are tried first; if
// that fails or v is not a tagged map, each branch is tried in order.
func (s *serUnion) ser(dst []byte, v reflect.Value) ([]byte, error) {
	if idx, inner, ok := s.tryUnwrapTagged(v); ok {
		attempt := appendVarint(dst, int32(idx))
		if result, err := s.fns[idx](attempt, inner); err == nil {
			return result, nil
		}
	}

	base := dst
	var err error
	for i, fn := range s.fns {
		attempt := appendVarint(base, int32(i))
		if attempt, err = fn(attempt, v); err == nil {
			return attempt, nil
		}
	}
	e := &SemanticError{AvroType: "union", Err: errors.New("no matching branch")}
	if v.IsValid() {
		e.GoType = v.Type()
	}
	return nil, e
}

// Avro encodes the union branch index as a varint before the value.
// Varint 0 encodes to byte 0x00, varint 1 encodes to byte 0x02
// (zigzag: 1 << 1 = 2). These two-branch null-union helpers inline
// the single-byte varints directly.

// serNullUnion handles ["null", T] unions: null is index 0 (byte 0),
// T is index 1 (byte 2).
func serNullUnion(u *serUnion) serfn {
	return func(dst []byte, v reflect.Value) ([]byte, error) {
		if isNilValue(v) {
			return append(dst, 0), nil
		}
		if idx, inner, ok := u.tryUnwrapTagged(v); ok {
			if idx == 0 && isNilValue(inner) {
				return append(dst, 0), nil
			}
			if idx == 1 {
				if result, err := u.fns[1](append(dst, 2), inner); err == nil {
					return result, nil
				}
			}
		}
		return u.fns[1](append(dst, 2), v)
	}
}

// serNullSecondUnion handles ["T", "null"] unions: T is index 0 (byte 0),
// null is index 1 (byte 2).
func serNullSecondUnion(u *serUnion) serfn {
	return func(dst []byte, v reflect.Value) ([]byte, error) {
		if isNilValue(v) {
			return append(dst, 2), nil
		}
		if idx, inner, ok := u.tryUnwrapTagged(v); ok {
			if idx == 1 && isNilValue(inner) {
				return append(dst, 2), nil
			}
			if idx == 0 {
				if result, err := u.fns[0](append(dst, 0), inner); err == nil {
					return result, nil
				}
			}
		}
		return u.fns[0](append(dst, 0), v)
	}
}

// isNilValue reports whether v is nil, peeling through pointer and
// interface layers. This handles the case where AppendEncode receives
// &nilPtr (a **T with non-nil outer pointer) for a nullable union.
func isNilValue(v reflect.Value) bool {
	if !v.IsValid() {
		return true
	}
	for {
		switch v.Kind() {
		case reflect.Pointer, reflect.Interface:
			if v.IsNil() {
				return true
			}
			v = v.Elem()
		case reflect.Map, reflect.Slice:
			return v.IsNil()
		default:
			return false
		}
	}
}

////////////////
// PRIMITIVES //
////////////////

var serPrimitive = map[string]serfn{
	"null":    serNull,
	"boolean": serBoolean,
	"int":     serInt,
	"long":    serLong,
	"float":   serFloat,
	"double":  serDouble,
	"bytes":   serBytes,
	"string":  serString,
}

// For unions, we try encoding across all values until one works, and often we
// hit "null" at the start with an error. This error is saved to avoid allocs.
var errNonNil = errors.New("cannot encode non-nil value as null")

func serNull(dst []byte, v reflect.Value) ([]byte, error) {
	if !v.IsValid() {
		return dst, nil
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
		if v.IsNil() {
			return dst, nil
		}
	}
	return dst, errNonNil
}

func serBoolean(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.Kind() != reflect.Bool {
		return nil, &SemanticError{GoType: v.Type(), AvroType: "boolean"}
	}
	if v.Bool() {
		return append(dst, 1), nil
	}
	return append(dst, 0), nil
}

var jsonNumberType = reflect.TypeFor[json.Number]()

// jsonNumberToFloat converts a json.Number to a float64 reflect.Value.
func jsonNumberToFloat(v reflect.Value) (reflect.Value, bool) {
	if v.Type() != jsonNumberType {
		return v, false
	}
	f, err := v.Interface().(json.Number).Float64()
	if err != nil {
		return v, false
	}
	return reflect.ValueOf(f), true
}

// jsonNumberToInt64 converts a json.Number reflect.Value to a validated int64,
// checking that the value is a whole number within int64 range. It tries
// Int64() first for full precision, falling back to Float64() for
// fractional number detection.
func jsonNumberToInt64(v reflect.Value) (int64, bool, error) {
	if v.Type() != jsonNumberType {
		return 0, false, nil
	}
	jn := v.Interface().(json.Number)
	// Try exact integer parse first — handles the full int64 range
	// without float64 precision loss.
	if n, err := jn.Int64(); err == nil {
		return n, true, nil
	}
	// Fall back to float64 to detect fractional values and produce
	// a clear error.
	f, err := jn.Float64()
	if err != nil {
		return 0, true, &SemanticError{GoType: v.Type(), AvroType: "long", Err: fmt.Errorf("value %s is not a valid number", jn)}
	}
	if f != math.Trunc(f) {
		return 0, true, &SemanticError{GoType: v.Type(), AvroType: "long", Err: fmt.Errorf("value %v is not a whole number", f)}
	}
	return 0, true, &SemanticError{GoType: v.Type(), AvroType: "long", Err: fmt.Errorf("value %s overflows int64", jn)}
}

func serInt(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.CanInt() {
		n := v.Int()
		if n < math.MinInt32 || n > math.MaxInt32 {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "int", Err: fmt.Errorf("value %d overflows int32", n)}
		}
		return appendVarint(dst, int32(n)), nil
	} else if v.CanUint() {
		n := v.Uint()
		if n > math.MaxInt32 {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "int", Err: fmt.Errorf("value %d overflows int32", n)}
		}
		return appendVarint(dst, int32(n)), nil
	} else if v.CanFloat() {
		f := v.Float()
		n := math.Trunc(f)
		if f != n {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "int", Err: fmt.Errorf("value %v is not a whole number", f)}
		}
		if n < math.MinInt32 || n > math.MaxInt32 {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "int", Err: fmt.Errorf("value %v overflows int32", f)}
		}
		return appendVarint(dst, int32(n)), nil
	} else if n, ok, err := jsonNumberToInt64(v); ok {
		if err != nil {
			return nil, err
		}
		if n < math.MinInt32 || n > math.MaxInt32 {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "int", Err: fmt.Errorf("value %d overflows int32", n)}
		}
		return appendVarint(dst, int32(n)), nil
	} else if fv, ok := jsonNumberToFloat(v); ok {
		return serInt(dst, fv)
	}
	return nil, &SemanticError{GoType: v.Type(), AvroType: "int"}
}

func serLong(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.CanInt() {
		return appendVarlong(dst, int64(v.Int())), nil
	} else if v.CanUint() {
		n := v.Uint()
		if n > math.MaxInt64 {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "long", Err: fmt.Errorf("value %d overflows int64", n)}
		}
		return appendVarlong(dst, int64(n)), nil
	} else if v.CanFloat() {
		f := v.Float()
		n := math.Trunc(f)
		if f != n {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "long", Err: fmt.Errorf("value %v is not a whole number", f)}
		}
		if n < -(1<<63) || n >= 1<<63 {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "long", Err: fmt.Errorf("value %v overflows int64", f)}
		}
		return appendVarlong(dst, int64(n)), nil
	} else if n, ok, err := jsonNumberToInt64(v); ok {
		if err != nil {
			return nil, err
		}
		return appendVarlong(dst, n), nil
	} else if fv, ok := jsonNumberToFloat(v); ok {
		return serLong(dst, fv)
	}
	return nil, &SemanticError{GoType: v.Type(), AvroType: "long"}
}

func serFloat(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.CanFloat() {
		f := v.Float()
		// Narrowing float64 → float32 must not silently clamp to ±Inf.
		// Allow ±Inf and NaN pass-through.
		if v.Kind() == reflect.Float64 && !math.IsInf(f, 0) && !math.IsNaN(f) && math.IsInf(float64(float32(f)), 0) {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "float", Err: fmt.Errorf("value %g overflows float32", f)}
		}
		return appendUint32(dst, math.Float32bits(float32(f))), nil
	} else if v.CanInt() {
		n := v.Int()
		if n < -1<<24 || n > 1<<24 {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "float", Err: errors.New("integer overflows float32 exact precision")}
		}
		return appendUint32(dst, math.Float32bits(float32(n))), nil
	} else if v.CanUint() {
		n := v.Uint()
		if n > 1<<24 {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "float", Err: errors.New("integer overflows float32 exact precision")}
		}
		return appendUint32(dst, math.Float32bits(float32(n))), nil
	} else if fv, ok := jsonNumberToFloat(v); ok {
		return serFloat(dst, fv)
	}
	return nil, &SemanticError{GoType: v.Type(), AvroType: "float"}
}

func serDouble(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.CanFloat() {
		return appendUint64(dst, math.Float64bits(v.Float())), nil
	} else if v.CanInt() {
		n := v.Int()
		if n < -1<<53 || n > 1<<53 {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "double", Err: errors.New("integer overflows float64 exact precision")}
		}
		return appendUint64(dst, math.Float64bits(float64(n))), nil
	} else if v.CanUint() {
		n := v.Uint()
		if n > 1<<53 {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "double", Err: errors.New("integer overflows float64 exact precision")}
		}
		return appendUint64(dst, math.Float64bits(float64(n))), nil
	} else if fv, ok := jsonNumberToFloat(v); ok {
		return serDouble(dst, fv)
	}
	return nil, &SemanticError{GoType: v.Type(), AvroType: "double"}
}

func serBytes(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	// Accept string for json.Unmarshal pipelines where JSON strings
	// may represent Avro bytes fields.
	if v.Kind() == reflect.String {
		return doSerString(dst, v.String()), nil
	}
	if (v.Kind() != reflect.Array && v.Kind() != reflect.Slice) || v.Type().Elem().Kind() != reflect.Uint8 {
		return nil, &SemanticError{GoType: v.Type(), AvroType: "bytes"}
	}
	return doSerBytes(dst, v), nil
}

func serString(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	// json.Number has Kind==String but represents a numeric value.
	// Reject it here so that union dispatch routes it to numeric
	// branches instead of silently encoding the text as a string.
	if v.Type() == jsonNumberType {
		return nil, &SemanticError{GoType: v.Type(), AvroType: "string"}
	}
	if v.Kind() == reflect.String {
		return doSerString(dst, v.String()), nil
	}
	// Text interfaces before []byte: named []byte subtypes like net.IP
	// should use their text representation, not raw bytes.
	if v.CanInterface() {
		i := v.Interface()
		if a, ok := i.(encoding.TextAppender); ok {
			mark := len(dst)
			dst = appendVarlong(dst, 0) // placeholder for length
			hdrLen := len(dst) - mark
			dst, err = a.AppendText(dst)
			if err != nil {
				return nil, err
			}
			textLen := len(dst) - mark - hdrLen
			var buf [10]byte
			hdr := appendVarlong(buf[:0], int64(textLen))
			if len(hdr) == hdrLen {
				copy(dst[mark:], hdr)
			} else {
				// Header grew; shift text to make room.
				dst = append(dst, make([]byte, len(hdr)-hdrLen)...)
				copy(dst[mark+len(hdr):], dst[mark+hdrLen:mark+hdrLen+textLen])
				copy(dst[mark:], hdr)
			}
			return dst, nil
		}
		if m, ok := i.(encoding.TextMarshaler); ok {
			text, err := m.MarshalText()
			if err != nil {
				return nil, err
			}
			return doSerString(dst, string(text)), nil
		}
	}
	// Accept []byte for symmetry with bytes accepting string.
	if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		return doSerString(dst, string(v.Bytes())), nil
	}
	return nil, &SemanticError{GoType: v.Type(), AvroType: "string"}
}

////////////////////
// STRING & BYTES //
////////////////////

func doSerBytes(dst []byte, v reflect.Value) []byte {
	l := v.Len()
	dst = appendVarlong(dst, int64(l))
	if l == 0 {
		return dst
	}
	if v.CanAddr() {
		return append(dst, v.Slice(0, l).Bytes()...)
	}
	for i := range l {
		dst = append(dst, byte(v.Index(i).Uint()))
	}
	return dst
}

func doSerString(dst []byte, s string) []byte {
	dst = appendVarlong(dst, int64(len(s)))
	return append(dst, s...)
}

/////////////
// COMPLEX //
/////////////

type serRecordField struct {
	name         string
	nameVal      reflect.Value // pre-computed reflect.ValueOf(name); avoids alloc per map lookup
	fn           serfn
	avroType     string
	meta         *fieldMeta
	defaultBytes []byte // pre-encoded Avro binary for the field's default value
	hasDefault   bool
}

type serRecord struct {
	fields []serRecordField
	names  []string
	cache  sync.Map                      // map[reflect.Type]*cachedMapping
	fast   atomic.Pointer[fastRecordSer] // lazily compiled unsafe fast path, atomic for concurrent encode
}

func (s *serRecord) ser(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	k := v.Kind()
	t := v.Type()
	if k != reflect.Struct && (k != reflect.Map || t.Key().Kind() != reflect.String) {
		return nil, &SemanticError{GoType: t, AvroType: "record"}
	}
	if k == reflect.Map {
		for _, f := range s.fields {
			value := v.MapIndex(f.nameVal)
			if !value.IsValid() {
				if !f.hasDefault {
					return nil, &SemanticError{GoType: t, AvroType: "record", Field: f.name, Err: errors.New("missing key")}
				}
				dst = append(dst, f.defaultBytes...)
				continue
			}
			if dst, err = f.fn(dst, value); err != nil {
				return nil, recordFieldError(t, f.name, err)
			}
		}
		return dst, nil
	}
	// Struct: try precompiled unsafe fast path. Requires addressable
	// value so we can take a pointer for unsafe field access.
	if v.CanAddr() {
		if fast := s.fast.Load(); fast != nil && fast.typ == t {
			return serRecordFast(dst, fast, v)
		}
		if fast := compileFastSer(s.fields, s.names, &s.cache, t); fast != nil {
			s.fast.Store(fast)
			return serRecordFast(dst, fast, v)
		}
	}
	// Slow path: reflect-based field access.
	mapping, err := typeFieldMapping(s.names, &s.cache, t)
	if err != nil {
		return nil, err
	}
	for i, f := range s.fields {
		fv := v.FieldByIndex(mapping.indices[i])
		// omitzero + nullunion: if the Go field is zero, encode as
		// the null branch (index 0) instead of trying the real encoder.
		if mapping.omitzero[i] && f.avroType == "nullunion" && valueIsZero(fv) {
			dst = append(dst, 0)
			continue
		}
		if dst, err = f.fn(dst, fv); err != nil {
			return nil, recordFieldError(t, f.name, err)
		}
	}
	return dst, nil
}

type serEnum struct {
	symbols []string
}

func (s *serEnum) ser(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	switch {
	case v.Kind() == reflect.String:
		needle := v.String()
		for i, symbol := range s.symbols {
			if symbol == needle {
				return appendVarint(dst, int32(i)), nil
			}
		}
		return nil, &SemanticError{GoType: v.Type(), AvroType: "enum", Err: fmt.Errorf("unknown symbol %q", needle)}

	case v.CanInt() || v.CanUint():
		var n int
		if v.CanInt() {
			n = int(v.Int())
		} else {
			n = int(v.Uint())
		}
		if n < 0 || n >= len(s.symbols) {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "enum", Err: fmt.Errorf("index %d out of range [0, %d)", n, len(s.symbols))}
		}
		return appendVarint(dst, int32(n)), nil

	default:
		return nil, &SemanticError{GoType: v.Type(), AvroType: "enum"}
	}
}

type serArray struct {
	serItem serfn
}

func (s *serArray) ser(dst []byte, v reflect.Value) ([]byte, error) {
	dst, v, l, err := serArrayPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	for i := range l {
		if dst, err = s.serItem(dst, v.Index(i)); err != nil {
			return nil, err
		}
	}
	return append(dst, 0), nil
}

// serArrayPreamble handles the shared preamble for all serArray methods:
// indirect, kind check, length encoding, and empty-return. Called once
// per encode — no performance impact.
func serArrayPreamble(dst []byte, v reflect.Value) ([]byte, reflect.Value, int, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, v, 0, err
	}
	if v.Kind() != reflect.Array && v.Kind() != reflect.Slice {
		return nil, v, 0, &SemanticError{GoType: v.Type(), AvroType: "array"}
	}
	l := v.Len()
	dst = appendVarlong(dst, int64(l))
	return dst, v, l, nil
}

// The following serArray methods serialize array items by encoding
// primitive values directly from v.Index(i), avoiding reflect.Value
// escapes through serfn function pointers. Each is selected at schema
// build time based on the array's item type.

func (s *serArray) serString(dst []byte, v reflect.Value) ([]byte, error) {
	dst, v, l, err := serArrayPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	for i := range l {
		elem := v.Index(i)
		if elem.Kind() == reflect.Interface {
			elem = elem.Elem()
		}
		if elem.Type() == jsonNumberType {
			return nil, &SemanticError{GoType: elem.Type(), AvroType: "string"}
		}
		if elem.Kind() == reflect.String {
			dst = doSerString(dst, elem.String())
		} else if elem.Kind() == reflect.Slice && elem.Type().Elem().Kind() == reflect.Uint8 {
			dst = doSerString(dst, string(elem.Bytes()))
		} else {
			return nil, &SemanticError{GoType: elem.Type(), AvroType: "string"}
		}
	}
	return append(dst, 0), nil
}

func (s *serArray) serBoolean(dst []byte, v reflect.Value) ([]byte, error) {
	dst, v, l, err := serArrayPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	for i := range l {
		elem := v.Index(i)
		if elem.Kind() == reflect.Interface {
			elem = elem.Elem()
		}
		if elem.Kind() != reflect.Bool {
			return nil, &SemanticError{GoType: elem.Type(), AvroType: "boolean"}
		}
		if elem.Bool() {
			dst = append(dst, 1)
		} else {
			dst = append(dst, 0)
		}
	}
	return append(dst, 0), nil
}

func (s *serArray) serInt(dst []byte, v reflect.Value) ([]byte, error) {
	dst, v, l, err := serArrayPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	for i := range l {
		elem := v.Index(i)
		if elem.Kind() == reflect.Interface {
			elem = elem.Elem()
		}
		if elem.CanInt() {
			n := elem.Int()
			if n < math.MinInt32 || n > math.MaxInt32 {
				return nil, &SemanticError{GoType: elem.Type(), AvroType: "int", Err: fmt.Errorf("value %d overflows int32", n)}
			}
			dst = appendVarint(dst, int32(n))
		} else if elem.CanUint() {
			n := elem.Uint()
			if n > math.MaxInt32 {
				return nil, &SemanticError{GoType: elem.Type(), AvroType: "int", Err: fmt.Errorf("value %d overflows int32", n)}
			}
			dst = appendVarint(dst, int32(n))
		} else if elem.CanFloat() {
			f := elem.Float()
			n := math.Trunc(f)
			if f != n {
				return nil, &SemanticError{GoType: elem.Type(), AvroType: "int", Err: fmt.Errorf("value %v is not a whole number", f)}
			}
			if n < math.MinInt32 || n > math.MaxInt32 {
				return nil, &SemanticError{GoType: elem.Type(), AvroType: "int", Err: fmt.Errorf("value %v overflows int32", f)}
			}
			dst = appendVarint(dst, int32(n))
		} else if fv, ok := jsonNumberToFloat(elem); ok {
			f := fv.Float()
			n := math.Trunc(f)
			if f != n || n < math.MinInt32 || n > math.MaxInt32 {
				return nil, &SemanticError{GoType: elem.Type(), AvroType: "int", Err: fmt.Errorf("value %v invalid for int32", f)}
			}
			dst = appendVarint(dst, int32(n))
		} else {
			return nil, &SemanticError{GoType: elem.Type(), AvroType: "int"}
		}
	}
	return append(dst, 0), nil
}

func (s *serArray) serLong(dst []byte, v reflect.Value) ([]byte, error) {
	dst, v, l, err := serArrayPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	for i := range l {
		elem := v.Index(i)
		if elem.Kind() == reflect.Interface {
			elem = elem.Elem()
		}
		if elem.CanInt() {
			dst = appendVarlong(dst, elem.Int())
		} else if elem.CanUint() {
			n := elem.Uint()
			if n > math.MaxInt64 {
				return nil, &SemanticError{GoType: elem.Type(), AvroType: "long", Err: fmt.Errorf("value %d overflows int64", n)}
			}
			dst = appendVarlong(dst, int64(n))
		} else if elem.CanFloat() {
			f := elem.Float()
			n := math.Trunc(f)
			if f != n {
				return nil, &SemanticError{GoType: elem.Type(), AvroType: "long", Err: fmt.Errorf("value %v is not a whole number", f)}
			}
			if n < -(1<<63) || n >= 1<<63 {
				return nil, &SemanticError{GoType: elem.Type(), AvroType: "long", Err: fmt.Errorf("value %v overflows int64", f)}
			}
			dst = appendVarlong(dst, int64(n))
		} else if n, ok, err := jsonNumberToInt64(elem); ok {
			if err != nil {
				return nil, err
			}
			dst = appendVarlong(dst, n)
		} else {
			return nil, &SemanticError{GoType: elem.Type(), AvroType: "long"}
		}
	}
	return append(dst, 0), nil
}

func (s *serArray) serFloat(dst []byte, v reflect.Value) ([]byte, error) {
	dst, v, l, err := serArrayPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	for i := range l {
		elem := v.Index(i)
		if elem.Kind() == reflect.Interface {
			elem = elem.Elem()
		}
		if !elem.CanFloat() {
			if fv, ok := jsonNumberToFloat(elem); ok {
				elem = fv
			} else {
				return nil, &SemanticError{GoType: elem.Type(), AvroType: "float"}
			}
		}
		dst = appendUint32(dst, math.Float32bits(float32(elem.Float())))
	}
	return append(dst, 0), nil
}

func (s *serArray) serDouble(dst []byte, v reflect.Value) ([]byte, error) {
	dst, v, l, err := serArrayPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	for i := range l {
		elem := v.Index(i)
		if elem.Kind() == reflect.Interface {
			elem = elem.Elem()
		}
		if !elem.CanFloat() {
			if fv, ok := jsonNumberToFloat(elem); ok {
				elem = fv
			} else {
				return nil, &SemanticError{GoType: elem.Type(), AvroType: "double"}
			}
		}
		dst = appendUint64(dst, math.Float64bits(elem.Float()))
	}
	return append(dst, 0), nil
}

type serMap struct {
	serItem serfn
}

func (s *serMap) ser(dst []byte, v reflect.Value) ([]byte, error) {
	dst, v, l, err := serMapPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	iter := v.MapRange()
	for iter.Next() {
		dst = doSerString(dst, iter.Key().String())
		if dst, err = s.serItem(dst, iter.Value()); err != nil {
			return nil, err
		}
	}
	return append(dst, 0), nil
}

// serMapPreamble handles the shared preamble for all serMap methods:
// indirect, map+key check, length encoding, and empty-return. Called
// once per encode — no performance impact.
func serMapPreamble(dst []byte, v reflect.Value) ([]byte, reflect.Value, int, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, v, 0, err
	}
	t := v.Type()
	if t.Kind() != reflect.Map || t.Key().Kind() != reflect.String {
		return nil, v, 0, &SemanticError{GoType: t, AvroType: "map"}
	}
	l := v.Len()
	dst = appendVarlong(dst, int64(l))
	return dst, v, l, nil
}

// The following serMap methods serialize map values by extracting
// primitive values directly from iter.Value(), avoiding reflect.Value
// escapes through serfn function pointers. Each is selected at schema
// build time based on the map's value type.

func (s *serMap) serString(dst []byte, v reflect.Value) ([]byte, error) {
	dst, v, l, err := serMapPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	iter := v.MapRange()
	for iter.Next() {
		dst = doSerString(dst, iter.Key().String())
		val := iter.Value()
		if val.Kind() == reflect.Interface {
			val = val.Elem()
		}
		if val.Type() == jsonNumberType {
			return nil, &SemanticError{GoType: val.Type(), AvroType: "string"}
		}
		if val.Kind() == reflect.String {
			dst = doSerString(dst, val.String())
		} else if val.Kind() == reflect.Slice && val.Type().Elem().Kind() == reflect.Uint8 {
			dst = doSerString(dst, string(val.Bytes()))
		} else {
			return nil, &SemanticError{GoType: val.Type(), AvroType: "string"}
		}
	}
	return append(dst, 0), nil
}

func (s *serMap) serBoolean(dst []byte, v reflect.Value) ([]byte, error) {
	dst, v, l, err := serMapPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	iter := v.MapRange()
	for iter.Next() {
		dst = doSerString(dst, iter.Key().String())
		val := iter.Value()
		if val.Kind() == reflect.Interface {
			val = val.Elem()
		}
		if val.Kind() != reflect.Bool {
			return nil, &SemanticError{GoType: val.Type(), AvroType: "boolean"}
		}
		if val.Bool() {
			dst = append(dst, 1)
		} else {
			dst = append(dst, 0)
		}
	}
	return append(dst, 0), nil
}

func (s *serMap) serInt(dst []byte, v reflect.Value) ([]byte, error) {
	dst, v, l, err := serMapPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	iter := v.MapRange()
	for iter.Next() {
		dst = doSerString(dst, iter.Key().String())
		val := iter.Value()
		if val.Kind() == reflect.Interface {
			val = val.Elem()
		}
		if val.CanInt() {
			n := val.Int()
			if n < math.MinInt32 || n > math.MaxInt32 {
				return nil, &SemanticError{GoType: val.Type(), AvroType: "int", Err: fmt.Errorf("value %d overflows int32", n)}
			}
			dst = appendVarint(dst, int32(n))
		} else if val.CanUint() {
			n := val.Uint()
			if n > math.MaxInt32 {
				return nil, &SemanticError{GoType: val.Type(), AvroType: "int", Err: fmt.Errorf("value %d overflows int32", n)}
			}
			dst = appendVarint(dst, int32(n))
		} else if val.CanFloat() {
			f := val.Float()
			n := math.Trunc(f)
			if f != n {
				return nil, &SemanticError{GoType: val.Type(), AvroType: "int", Err: fmt.Errorf("value %v is not a whole number", f)}
			}
			if n < math.MinInt32 || n > math.MaxInt32 {
				return nil, &SemanticError{GoType: val.Type(), AvroType: "int", Err: fmt.Errorf("value %v overflows int32", f)}
			}
			dst = appendVarint(dst, int32(n))
		} else if fv, ok := jsonNumberToFloat(val); ok {
			val = fv
			f := val.Float()
			n := math.Trunc(f)
			if f != n || n < math.MinInt32 || n > math.MaxInt32 {
				return nil, &SemanticError{GoType: val.Type(), AvroType: "int", Err: fmt.Errorf("value %v invalid for int32", f)}
			}
			dst = appendVarint(dst, int32(n))
		} else {
			return nil, &SemanticError{GoType: val.Type(), AvroType: "int"}
		}
	}
	return append(dst, 0), nil
}

func (s *serMap) serLong(dst []byte, v reflect.Value) ([]byte, error) {
	dst, v, l, err := serMapPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	iter := v.MapRange()
	for iter.Next() {
		dst = doSerString(dst, iter.Key().String())
		val := iter.Value()
		if val.Kind() == reflect.Interface {
			val = val.Elem()
		}
		if val.CanInt() {
			dst = appendVarlong(dst, val.Int())
		} else if val.CanUint() {
			n := val.Uint()
			if n > math.MaxInt64 {
				return nil, &SemanticError{GoType: val.Type(), AvroType: "long", Err: fmt.Errorf("value %d overflows int64", n)}
			}
			dst = appendVarlong(dst, int64(n))
		} else if val.CanFloat() {
			f := val.Float()
			n := math.Trunc(f)
			if f != n {
				return nil, &SemanticError{GoType: val.Type(), AvroType: "long", Err: fmt.Errorf("value %v is not a whole number", f)}
			}
			if n < -(1<<63) || n >= 1<<63 {
				return nil, &SemanticError{GoType: val.Type(), AvroType: "long", Err: fmt.Errorf("value %v overflows int64", f)}
			}
			dst = appendVarlong(dst, int64(n))
		} else if n, ok, err := jsonNumberToInt64(val); ok {
			if err != nil {
				return nil, err
			}
			dst = appendVarlong(dst, n)
		} else {
			return nil, &SemanticError{GoType: val.Type(), AvroType: "long"}
		}
	}
	return append(dst, 0), nil
}

func (s *serMap) serFloat(dst []byte, v reflect.Value) ([]byte, error) {
	dst, v, l, err := serMapPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	iter := v.MapRange()
	for iter.Next() {
		dst = doSerString(dst, iter.Key().String())
		val := iter.Value()
		if val.Kind() == reflect.Interface {
			val = val.Elem()
		}
		if !val.CanFloat() {
			if fv, ok := jsonNumberToFloat(val); ok {
				val = fv
			} else {
				return nil, &SemanticError{GoType: val.Type(), AvroType: "float"}
			}
		}
		dst = appendUint32(dst, math.Float32bits(float32(val.Float())))
	}
	return append(dst, 0), nil
}

func (s *serMap) serDouble(dst []byte, v reflect.Value) ([]byte, error) {
	dst, v, l, err := serMapPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	iter := v.MapRange()
	for iter.Next() {
		dst = doSerString(dst, iter.Key().String())
		val := iter.Value()
		if val.Kind() == reflect.Interface {
			val = val.Elem()
		}
		if !val.CanFloat() {
			if fv, ok := jsonNumberToFloat(val); ok {
				val = fv
			} else {
				return nil, &SemanticError{GoType: val.Type(), AvroType: "double"}
			}
		}
		dst = appendUint64(dst, math.Float64bits(val.Float()))
	}
	return append(dst, 0), nil
}

type serSize struct {
	n int
}

func (s *serSize) ser(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	t := v.Type()
	// Accept [N]byte arrays, []byte slices, and strings of the correct length.
	switch t.Kind() {
	case reflect.Array:
		if t.Elem().Kind() != reflect.Uint8 || t.Len() != s.n {
			return nil, &SemanticError{GoType: t, AvroType: "fixed"}
		}
	case reflect.Slice:
		if t.Elem().Kind() != reflect.Uint8 || v.Len() != s.n {
			return nil, &SemanticError{GoType: t, AvroType: "fixed"}
		}
		return append(dst, v.Bytes()...), nil
	case reflect.String:
		str := v.String()
		if len(str) != s.n {
			return nil, &SemanticError{GoType: t, AvroType: "fixed"}
		}
		return append(dst, str...), nil
	default:
		return nil, &SemanticError{GoType: t, AvroType: "fixed"}
	}
	// Fixed is written as raw bytes with no length prefix.
	if v.CanAddr() {
		return append(dst, v.Slice(0, s.n).Bytes()...), nil
	}
	for i := 0; i < s.n; i++ {
		dst = append(dst, byte(v.Index(i).Uint()))
	}
	return dst, nil
}

/////////////////////////////
// LOGICAL TYPE SERIALIZERS //
/////////////////////////////

var (
	timeType         = reflect.TypeFor[time.Time]()
	durationType     = reflect.TypeFor[time.Duration]()
	avroDurationType = reflect.TypeFor[Duration]()
	bigRatType       = reflect.TypeFor[big.Rat]()
)

// Duration represents the Avro duration logical type: a 12-byte fixed
// value containing three little-endian unsigned 32-bit integers
// representing months, days, and milliseconds.
type Duration struct {
	Months       uint32
	Days         uint32
	Milliseconds uint32
}

// Bytes encodes the Duration as a 12-byte little-endian fixed value,
// matching the Avro duration wire format.
func (d Duration) Bytes() [12]byte {
	var b [12]byte
	b[0] = byte(d.Months)
	b[1] = byte(d.Months >> 8)
	b[2] = byte(d.Months >> 16)
	b[3] = byte(d.Months >> 24)
	b[4] = byte(d.Days)
	b[5] = byte(d.Days >> 8)
	b[6] = byte(d.Days >> 16)
	b[7] = byte(d.Days >> 24)
	b[8] = byte(d.Milliseconds)
	b[9] = byte(d.Milliseconds >> 8)
	b[10] = byte(d.Milliseconds >> 16)
	b[11] = byte(d.Milliseconds >> 24)
	return b
}

// DurationFromBytes decodes a 12-byte little-endian fixed value into a
// Duration. Returns zero Duration if b is shorter than 12 bytes. This is
// useful in [CustomType] Decode callbacks that override the default duration
// handling: the callback receives raw []byte and can use this function to
// interpret the value before converting to a custom Go type.
func DurationFromBytes(b []byte) Duration {
	if len(b) < 12 {
		return Duration{}
	}
	return Duration{
		Months:       uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24,
		Days:         uint32(b[4]) | uint32(b[5])<<8 | uint32(b[6])<<16 | uint32(b[7])<<24,
		Milliseconds: uint32(b[8]) | uint32(b[9])<<8 | uint32(b[10])<<16 | uint32(b[11])<<24,
	}
}

// String returns an ISO 8601 duration string. Zero components are omitted
// for readability. Examples: "P1Y3M15DT1H30M0.500S", "P30D", "PT1H".
func (d Duration) String() string {
	if d.Months == 0 && d.Days == 0 && d.Milliseconds == 0 {
		return "P0D"
	}
	buf := []byte{'P'}
	if y := d.Months / 12; y > 0 {
		buf = append(buf, fmt.Sprintf("%dY", y)...)
	}
	if m := d.Months % 12; m > 0 {
		buf = append(buf, fmt.Sprintf("%dM", m)...)
	}
	if d.Days > 0 {
		buf = append(buf, fmt.Sprintf("%dD", d.Days)...)
	}
	if d.Milliseconds > 0 {
		ms := d.Milliseconds
		h := ms / 3600000
		ms %= 3600000
		m := ms / 60000
		ms %= 60000
		s := ms / 1000
		frac := ms % 1000
		buf = append(buf, 'T')
		if h > 0 {
			buf = append(buf, fmt.Sprintf("%dH", h)...)
		}
		if m > 0 {
			buf = append(buf, fmt.Sprintf("%dM", m)...)
		}
		if frac > 0 {
			buf = append(buf, fmt.Sprintf("%d.%03dS", s, frac)...)
		} else if s > 0 {
			buf = append(buf, fmt.Sprintf("%dS", s)...)
		}
	}
	return string(buf)
}

// tryParseTimeString attempts to parse a string value as RFC 3339.
func tryParseTimeString(v reflect.Value) (time.Time, bool) {
	if v.Kind() != reflect.String {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, v.String())
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// tryParseDateString attempts to parse a string value as either RFC 3339 or
// ISO 8601 date-only ("2006-01-02").
func tryParseDateString(v reflect.Value) (time.Time, bool) {
	if v.Kind() != reflect.String {
		return time.Time{}, false
	}
	s := v.String()
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, err = time.Parse(time.DateOnly, s)
	}
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func serTimestampMillis(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.Type() == timeType {
		return appendVarlong(dst, timeToTimestampMillis(v.Interface().(time.Time))), nil
	}
	if t, ok := tryParseTimeString(v); ok {
		return appendVarlong(dst, timeToTimestampMillis(t)), nil
	}
	return serLong(dst, v)
}

func serTimestampMicros(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.Type() == timeType {
		return appendVarlong(dst, timeToTimestampMicros(v.Interface().(time.Time))), nil
	}
	if t, ok := tryParseTimeString(v); ok {
		return appendVarlong(dst, timeToTimestampMicros(t)), nil
	}
	return serLong(dst, v)
}

func serTimestampNanos(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.Type() == timeType {
		return appendVarlong(dst, timeToTimestampNanos(v.Interface().(time.Time))), nil
	}
	if t, ok := tryParseTimeString(v); ok {
		return appendVarlong(dst, timeToTimestampNanos(t)), nil
	}
	return serLong(dst, v)
}

func serDate(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.Type() == timeType {
		return appendVarint(dst, timeToDate(v.Interface().(time.Time))), nil
	}
	if t, ok := tryParseDateString(v); ok {
		return appendVarint(dst, timeToDate(t)), nil
	}
	return serInt(dst, v)
}

func serTimeMillis(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.Type() == durationType {
		ms, err := durationToTimeMillis(time.Duration(v.Int()))
		if err != nil {
			return nil, &SemanticError{GoType: durationType, AvroType: "time-millis", Err: err}
		}
		return appendVarint(dst, ms), nil
	}
	if v.Type() == timeType {
		t := v.Interface().(time.Time)
		d := time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute + time.Duration(t.Second())*time.Second + time.Duration(t.Nanosecond())
		ms, err := durationToTimeMillis(d)
		if err != nil {
			return nil, &SemanticError{GoType: timeType, AvroType: "time-millis", Err: err}
		}
		return appendVarint(dst, ms), nil
	}
	return serInt(dst, v)
}

func serTimeMicros(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.Type() == durationType {
		return appendVarlong(dst, durationToTimeMicros(time.Duration(v.Int()))), nil
	}
	return serLong(dst, v)
}

func serDuration(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.Type() == avroDurationType {
		b := v.Interface().(Duration).Bytes()
		return append(dst, b[:]...), nil
	}
	return (&serSize{12}).ser(dst, v)
}

// tryCoerceToRat attempts to convert a value to *big.Rat for decimal logical
// types. Accepts float64, json.Number, and numeric strings (e.g. "3.14").
func tryCoerceToRat(v reflect.Value) (*big.Rat, bool) {
	if v.CanFloat() {
		return new(big.Rat).SetFloat64(v.Float()), true
	}
	if v.Type() == jsonNumberType {
		if r, ok := new(big.Rat).SetString(v.Interface().(json.Number).String()); ok {
			return r, true
		}
	}
	if v.Kind() == reflect.String {
		if r, ok := new(big.Rat).SetString(v.String()); ok {
			return r, true
		}
	}
	return nil, false
}

type serBytesDecimal struct {
	scale int
}

func (s *serBytesDecimal) ser(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.Type() == bigRatType {
		tmp := v.Interface().(big.Rat)
		b := ratToBytes(&tmp, s.scale)
		dst = appendVarlong(dst, int64(len(b)))
		return append(dst, b...), nil
	}
	if r, ok := tryCoerceToRat(v); ok {
		b := ratToBytes(r, s.scale)
		dst = appendVarlong(dst, int64(len(b)))
		return append(dst, b...), nil
	}
	return serBytes(dst, v)
}

type serFixedDecimal struct {
	size  int
	scale int
}

func (s *serFixedDecimal) serRat(dst []byte, r *big.Rat) ([]byte, error) {
	b := ratToBytes(r, s.scale)
	if len(b) > s.size {
		return nil, &SemanticError{GoType: bigRatType, AvroType: "fixed", Err: fmt.Errorf("decimal value requires %d bytes, exceeds fixed size %d", len(b), s.size)}
	}
	// Pad to fixed size with sign extension.
	pad := byte(0)
	if len(b) > 0 && b[0]&0x80 != 0 {
		pad = 0xff
	}
	for i := len(b); i < s.size; i++ {
		dst = append(dst, pad)
	}
	return append(dst, b...), nil
}

func (s *serFixedDecimal) ser(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.Type() == bigRatType {
		tmp := v.Interface().(big.Rat)
		return s.serRat(dst, &tmp)
	}
	if r, ok := tryCoerceToRat(v); ok {
		return s.serRat(dst, r)
	}
	return (&serSize{s.size}).ser(dst, v)
}

// ratToBytes converts a *big.Rat to big-endian two's complement bytes
// using the given scale: unscaled = rat * 10^scale.
func ratToBytes(r *big.Rat, scale int) []byte {
	// unscaled = num * 10^scale / denom
	s := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	unscaled := new(big.Int).Mul(r.Num(), s)
	unscaled.Div(unscaled, r.Denom())
	return bigIntToBytes(unscaled)
}

// bigIntToBytes encodes i as big-endian two's complement, using the
// minimum number of bytes needed to represent the value with correct sign.
func bigIntToBytes(i *big.Int) []byte {
	switch i.Sign() {
	case 0:
		return []byte{0}
	case 1:
		b := i.Bytes() // big-endian unsigned
		if b[0]&0x80 != 0 {
			// High bit set would look negative in two's complement;
			// prepend a zero byte to keep it positive.
			b = append([]byte{0}, b...)
		}
		return b
	default:
		// Two's complement for negative: flip bits of (|i| - 1).
		// This works because -n in two's complement is ^(n-1).
		abs := new(big.Int).Neg(i)
		abs.Sub(abs, big.NewInt(1))
		b := abs.Bytes()
		if len(b) == 0 {
			return []byte{0xff} // -1
		}
		for j := range b {
			b[j] = ^b[j]
		}
		if b[0]&0x80 == 0 {
			// High bit clear would look positive; prepend 0xff
			// to preserve the negative sign.
			b = append([]byte{0xff}, b...)
		}
		return b
	}
}

// serFixedUUIDReflect serializes a fixed(16) UUID. Accepts [16]byte (raw),
// string (hex-dash UUID parsed to bytes), or []byte of length 16.
func serFixedUUIDReflect(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if isUUIDType(v.Type()) {
		var u [16]byte
		reflect.Copy(reflect.ValueOf(&u).Elem(), v)
		return append(dst, u[:]...), nil
	}
	if v.Kind() == reflect.String {
		u, err := parseUUID(v.String())
		if err != nil {
			return nil, err
		}
		return append(dst, u[:]...), nil
	}
	return (&serSize{16}).ser(dst, v)
}

// isUUIDType returns true when t is an array of 16 uint8 bytes (e.g. [16]byte
// or any type whose underlying type is [16]byte).
func isUUIDType(t reflect.Type) bool {
	return t.Kind() == reflect.Array && t.Len() == 16 && t.Elem().Kind() == reflect.Uint8
}

// uuidToString formats a [16]byte as the RFC 4122 hex-dash string
// xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx.
func uuidToString(u [16]byte) string {
	var buf [36]byte
	hex.Encode(buf[0:8], u[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], u[10:16])
	return string(buf[:])
}

func serUUID(dst []byte, v reflect.Value) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if isUUIDType(v.Type()) {
		var u [16]byte
		reflect.Copy(reflect.ValueOf(&u).Elem(), v)
		return doSerString(dst, uuidToString(u)), nil
	}
	return serString(dst, v)
}
