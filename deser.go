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
	"unsafe"
)

type deserfn func(src []byte, v reflect.Value, sl *slab) ([]byte, error)

var anyType = reflect.TypeFor[any]()

// slab batches small string allocations into a single backing buffer.
// Strings are immutable so sharing backing memory is safe.
type slab struct {
	buf             []byte
	taggedUnions    bool
	tagLogicalTypes bool
}

const slabSize = 1024

func (s *slab) string(src []byte, n int) string {
	if len(s.buf) < n {
		s.buf = make([]byte, max(slabSize, n))
	}
	b := s.buf[:n:n]
	copy(b, src[:n])
	s.buf = s.buf[n:]
	return unsafe.String(unsafe.SliceData(b), n)
}

var slabPool = sync.Pool{New: func() any { return &slab{} }}

// Decode reads Avro binary from src into v and returns the remaining bytes.
// v must be a non-nil pointer to a type compatible with the schema:
//
//   - null: any (always decodes to nil)
//   - boolean: bool, any
//   - int, long: int, int8–int64, uint8–uint64, any
//   - float: float32, float64, any
//   - double: float64, float32, any
//   - string: string, []byte, any; also [encoding.TextUnmarshaler]
//   - bytes: []byte, string, any
//   - enum: string, int/uint (ordinal), any
//   - fixed: [N]byte, []byte, any
//   - array: slice, any
//   - map: map[string]T, any
//   - union: any, *T (for ["null", T] unions), or the matched branch type
//   - record: struct (matched by field name or `avro` tag), map[string]any, any
//
// When decoding into *any, primitive types become nil, bool, int32, int64,
// float32, float64, string, []byte, []any, or map[string]any (for records).
// Logical types decode to their natural Go equivalents:
//
//   - date, timestamp-millis/micros/nanos, local-timestamp-*: [time.Time] (UTC)
//   - time-millis, time-micros: [time.Duration]
//   - decimal: [*math/big.Rat]
//   - uuid on string: string
//   - uuid on fixed(16): [16]byte
//   - duration: [Duration]
//
// To produce JSON from decoded *any data, use [Schema.EncodeJSON] rather
// than [encoding/json.Marshal]. EncodeJSON is schema-aware and converts
// these types back to their Avro representations (e.g. time.Time to
// epoch integers, []byte to \uXXXX strings).
func (s *Schema) Decode(src []byte, v any, opts ...Opt) ([]byte, error) {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return nil, errors.New("decode requires a non-nil pointer")
	}
	sl := slabPool.Get().(*slab)
	if len(opts) > 0 {
		cfg := parseOpts(opts)
		sl.taggedUnions = cfg.tagged
		sl.tagLogicalTypes = cfg.tagLogical
	}
	rest, err := s.deser(src, rv.Elem(), sl)
	sl.taggedUnions = false
	sl.tagLogicalTypes = false
	slabPool.Put(sl)
	return rest, err
}

///////////
// UNION //
///////////

type deserUnion struct {
	fns          []deserfn
	branchNames  []string // standard names: "null", "string", "com.example.Foo"
	logicalNames []string // with logical type: "long.timestamp-millis"; empty if same as branchNames
}

func (s *deserUnion) deser(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	idx, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	if idx < 0 || int(idx) >= len(s.fns) {
		return nil, fmt.Errorf("union index %d out of range [0, %d)", idx, len(s.fns))
	}
	src, err = s.fns[idx](src, v, sl)
	if err == nil {
		s.maybeWrap(v, sl, idx)
	}
	return src, err
}

// maybeWrap wraps a decoded union value with its branch name when
// TaggedUnions is enabled and the target is *any.
func (s *deserUnion) maybeWrap(v reflect.Value, sl *slab, idx int32) {
	if !sl.taggedUnions || v.Kind() != reflect.Interface || !v.Elem().IsValid() {
		return
	}
	name := s.branchNames[idx]
	if sl.tagLogicalTypes {
		name = s.logicalNames[idx]
	}
	v.Set(reflect.ValueOf(map[string]any{name: v.Elem().Interface()}))
}

// deserNullUnion handles ["null", T] unions. The branch index is a varint:
// 0x00 = index 0 (null), 0x02 = index 1 (T). Since the only valid indices
// are 0 and 1, the varint is always a single byte.
func deserNullUnion(u *deserUnion) deserfn {
	return func(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
		if len(src) < 1 {
			return nil, &ShortBufferError{Type: "union index"}
		}
		switch src[0] {
		case 0: // null
			v.Set(reflect.Zero(v.Type()))
			return src[1:], nil
		case 2: // T
			if v.Kind() == reflect.Pointer {
				if v.IsNil() {
					v.Set(reflect.New(v.Type().Elem()))
				}
				return u.fns[1](src[1:], v.Elem(), sl)
			}
			src, err := u.fns[1](src[1:], v, sl)
			if err == nil {
				u.maybeWrap(v, sl, 1)
			}
			return src, err
		default:
			return nil, fmt.Errorf("invalid null-union index byte 0x%02x", src[0])
		}
	}
}

// deserNullSecondUnion handles ["T", "null"] unions: 0x00 = index 0 (T),
// 0x02 = index 1 (null).
func deserNullSecondUnion(u *deserUnion) deserfn {
	return func(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
		if len(src) < 1 {
			return nil, &ShortBufferError{Type: "union index"}
		}
		switch src[0] {
		case 0: // index 0: the T branch
			if v.Kind() == reflect.Pointer {
				if v.IsNil() {
					v.Set(reflect.New(v.Type().Elem()))
				}
				return u.fns[0](src[1:], v.Elem(), sl)
			}
			src, err := u.fns[0](src[1:], v, sl)
			if err == nil {
				u.maybeWrap(v, sl, 0)
			}
			return src, err
		case 2: // index 1: null
			v.Set(reflect.Zero(v.Type()))
			return src[1:], nil
		default:
			return nil, fmt.Errorf("invalid null-union index byte 0x%02x", src[0])
		}
	}
}

////////////////
// PRIMITIVES //
////////////////

var deserPrimitive = map[string]deserfn{
	"null":    deserNull,
	"boolean": deserBoolean,
	"int":     deserInt,
	"long":    deserLong,
	"float":   deserFloat,
	"double":  deserDouble,
	"bytes":   deserBytes,
	"string":  deserString,
}

func deserNull(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
	v.Set(reflect.Zero(v.Type()))
	return src, nil
}

func deserBoolean(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	if len(src) < 1 {
		return nil, &ShortBufferError{Type: "boolean"}
	}
	b := src[0] != 0
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(b))
		return src[1:], nil
	}
	if v.Kind() != reflect.Bool {
		return nil, &SemanticError{GoType: v.Type(), AvroType: "boolean"}
	}
	v.SetBool(b)
	return src[1:], nil
}

func deserInt(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	val, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	return src, setIntValue(indirectAlloc(v), val)
}

func deserLong(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	return src, setLongValue(indirectAlloc(v), val)
}

func deserFloat(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	u, src, err := readUint32(src)
	if err != nil {
		return nil, err
	}
	f := math.Float32frombits(u)
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(f))
		return src, nil
	}
	if !v.CanFloat() {
		return nil, &SemanticError{GoType: v.Type(), AvroType: "float"}
	}
	v.SetFloat(float64(f))
	return src, nil
}

func deserDouble(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	u, src, err := readUint64(src)
	if err != nil {
		return nil, err
	}
	f := math.Float64frombits(u)
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(f))
		return src, nil
	}
	if !v.CanFloat() {
		return nil, &SemanticError{GoType: v.Type(), AvroType: "double"}
	}
	// Overflow check: narrowing a finite float64 into float32 must not
	// silently clamp to ±Inf. Allow ±Inf and NaN pass-through.
	if v.Kind() == reflect.Float32 && !math.IsInf(f, 0) && !math.IsNaN(f) && math.IsInf(float64(float32(f)), 0) {
		return nil, &SemanticError{GoType: v.Type(), AvroType: "double", Err: fmt.Errorf("value %g overflows float32", f)}
	}
	v.SetFloat(f)
	return src, nil
}

func deserBytes(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	length, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, fmt.Errorf("invalid negative bytes length %d", length)
	}
	n := int(length)
	if len(src) < n {
		return nil, &ShortBufferError{Type: "bytes", Need: n, Have: len(src)}
	}
	b := make([]byte, n)
	copy(b, src[:n])
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(b))
		return src[n:], nil
	}
	switch v.Kind() {
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.Uint8 {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "bytes"}
		}
		v.SetBytes(b)
	case reflect.Array:
		if v.Type().Elem().Kind() != reflect.Uint8 {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "bytes"}
		}
		if v.Len() != n {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "bytes", Err: fmt.Errorf("cannot decode %d bytes into array of length %d", n, v.Len())}
		}
		reflect.Copy(v, reflect.ValueOf(src[:n]))
	case reflect.String:
		v.SetString(string(b))
	default:
		return nil, &SemanticError{GoType: v.Type(), AvroType: "bytes"}
	}
	return src[n:], nil
}

func deserString(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	length, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, fmt.Errorf("invalid negative string length %d", length)
	}
	n := int(length)
	if len(src) < n {
		return nil, &ShortBufferError{Type: "string", Need: n, Have: len(src)}
	}
	str := sl.string(src, n)
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(str))
		return src[n:], nil
	}
	if v.Kind() == reflect.String {
		v.SetString(str)
		return src[n:], nil
	}
	// TextUnmarshaler before []byte: named []byte subtypes like net.IP
	// should use their text parsing, not raw byte assignment.
	if v.CanAddr() && v.Addr().Type().Implements(textUnmarshalerType) {
		if err := v.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText([]byte(str)); err != nil {
			return nil, err
		}
		return src[n:], nil
	}
	if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		v.SetBytes([]byte(str))
		return src[n:], nil
	}
	return nil, &SemanticError{GoType: v.Type(), AvroType: "string"}
}

/////////////
// COMPLEX //
/////////////

type deserRecordField struct {
	name       string
	nameVal    reflect.Value // pre-computed reflect.ValueOf(name); avoids alloc per map lookup
	fn         deserfn
	avroType   string
	meta       *fieldMeta
	defaultVal any
	hasDefault bool
}

type deserRecord struct {
	fields []deserRecordField
	names  []string
	cache  sync.Map                        // map[reflect.Type]*cachedMapping
	fast   atomic.Pointer[fastRecordDeser] // lazily compiled unsafe fast path, atomic for concurrent decode
}

func (s *deserRecord) deser(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	v = indirectAlloc(v)
	k := v.Kind()
	if k == reflect.Interface {
		// Generic decode: create map[string]any.
		m := make(map[string]any, len(s.fields))
		elem := reflect.New(anyType).Elem()
		var err error
		for _, f := range s.fields {
			if src, err = f.fn(src, elem, sl); err != nil {
				return nil, recordFieldError(nil, f.name, err)
			}
			m[f.name] = elem.Interface()
			elem.SetZero()
		}
		v.Set(reflect.ValueOf(m))
		return src, nil
	}
	t := v.Type()
	if k != reflect.Struct && (k != reflect.Map || t.Key().Kind() != reflect.String) {
		return nil, &SemanticError{GoType: t, AvroType: "record"}
	}
	var err error
	if k == reflect.Map {
		if v.IsNil() {
			v.Set(reflect.MakeMap(t))
		}
		elem := reflect.New(t.Elem()).Elem()
		for _, f := range s.fields {
			if src, err = f.fn(src, elem, sl); err != nil {
				return nil, recordFieldError(nil, f.name, err)
			}
			v.SetMapIndex(f.nameVal, elem)
			elem.SetZero()
		}
		return src, nil
	}
	// Struct: try precompiled unsafe fast path.
	if v.CanAddr() {
		if fast := s.fast.Load(); fast != nil && fast.typ == t {
			return deserRecordFast(src, fast, v, sl)
		}
		if fast := compileFastDeser(s.fields, s.names, &s.cache, t); fast != nil {
			s.fast.Store(fast)
			return deserRecordFast(src, fast, v, sl)
		}
	}
	// compileFastDeser returned nil because typeFieldMapping failed;
	// re-call to surface the error.
	_, err = typeFieldMapping(s.names, &s.cache, t)
	return nil, err
}

type deserEnum struct {
	symbols []string
}

func (s *deserEnum) deser(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	idx, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	if idx < 0 || int(idx) >= len(s.symbols) {
		return nil, fmt.Errorf("enum index %d out of range [0, %d)", idx, len(s.symbols))
	}
	v = indirectAlloc(v)
	switch {
	case v.Kind() == reflect.Interface:
		v.Set(reflect.ValueOf(s.symbols[idx]))
	case v.Kind() == reflect.String:
		v.SetString(s.symbols[idx])
	case v.CanInt():
		if v.OverflowInt(int64(idx)) {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "enum", Err: fmt.Errorf("ordinal %d overflows %s", idx, v.Type())}
		}
		v.SetInt(int64(idx))
	case v.CanUint():
		if v.OverflowUint(uint64(idx)) {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "enum", Err: fmt.Errorf("ordinal %d overflows %s", idx, v.Type())}
		}
		v.SetUint(uint64(idx))
	default:
		return nil, &SemanticError{GoType: v.Type(), AvroType: "enum"}
	}
	return src, nil
}

type deserArray struct {
	deserItem    deserfn
	fastLoop     func(src []byte, sliceVal reflect.Value, start, count int, sl *slab) ([]byte, error)
	fastElemKind reflect.Kind
}

func (s *deserArray) deser(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	v = indirectAlloc(v)
	iface := v.Kind() == reflect.Interface
	fixedArray := v.Kind() == reflect.Array
	if !iface && !fixedArray && v.Kind() != reflect.Slice {
		return nil, &SemanticError{GoType: v.Type(), AvroType: "array"}
	}
	// Fixed-size Go arrays: decode directly into array elements and
	// verify the Avro data has exactly the right number of elements.
	if fixedArray {
		return s.deserFixedArray(src, v, sl)
	}
	// For interface targets, build a []any.
	var sliceVal reflect.Value
	if iface {
		sliceVal = reflect.MakeSlice(reflect.SliceOf(anyType), 0, 0)
	} else {
		v.SetLen(0)
		sliceVal = v
	}
	// For primitive item types with matching Go element types, use
	// a specialized loop that avoids per-element function pointer calls.
	useFast := !iface && s.fastLoop != nil && sliceVal.Type().Elem().Kind() == s.fastElemKind
	// Avro arrays are encoded as a series of blocks. Each block starts
	// with a count: positive means N elements follow, zero means end of
	// array, negative means |N| elements follow and the next varint is
	// the block's byte size (for skipping without decoding).
	var err error
	for {
		var count int64
		count, src, err = readVarlong(src)
		if err != nil {
			return nil, err
		}
		if count == 0 {
			if iface {
				v.Set(sliceVal)
			}
			return src, nil
		}
		if count < 0 {
			count = -count
			if count < 0 {
				return nil, errors.New("invalid array block count")
			}
			_, src, err = readVarlong(src) // skip block byte size
			if err != nil {
				return nil, err
			}
		}
		if count > int64(len(src)) {
			return nil, fmt.Errorf("array block count %d exceeds remaining buffer length %d", count, len(src))
		}
		n := int(count)
		start := sliceVal.Len()
		newLen := start + n
		if sliceVal.Cap() < newLen {
			ns := reflect.MakeSlice(sliceVal.Type(), newLen, newLen)
			reflect.Copy(ns, sliceVal)
			sliceVal = ns
			if !iface {
				v.Set(sliceVal)
			}
		} else {
			sliceVal.SetLen(newLen)
		}
		if useFast {
			src, err = s.fastLoop(src, sliceVal, start, n, sl)
			if err != nil {
				return nil, err
			}
			continue
		}
		elemType := sliceVal.Type().Elem()
		if elemType.Kind() == reflect.Pointer {
			innerType := elemType.Elem()
			backing := reflect.MakeSlice(reflect.SliceOf(innerType), n, n)
			for i := range n {
				sliceVal.Index(start + i).Set(backing.Index(i).Addr())
			}
		}
		for i := start; i < newLen; i++ {
			src, err = s.deserItem(src, sliceVal.Index(i), sl)
			if err != nil {
				return nil, err
			}
		}
	}
}

// deserFixedArray decodes an Avro array into a fixed-size Go array.
// Returns an error if the Avro data does not contain exactly len(v) elements.
func (s *deserArray) deserFixedArray(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	arrLen := v.Len()
	idx := 0
	var err error
	for {
		var count int64
		count, src, err = readVarlong(src)
		if err != nil {
			return nil, err
		}
		if count == 0 {
			if idx != arrLen {
				return nil, &SemanticError{GoType: v.Type(), AvroType: "array", Err: fmt.Errorf("expected %d elements, got %d", arrLen, idx)}
			}
			return src, nil
		}
		if count < 0 {
			count = -count
			if count < 0 {
				return nil, errors.New("invalid array block count")
			}
			_, src, err = readVarlong(src)
			if err != nil {
				return nil, err
			}
		}
		n := int(count)
		if idx+n > arrLen {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "array", Err: fmt.Errorf("expected %d elements, got more", arrLen)}
		}
		for range n {
			src, err = s.deserItem(src, v.Index(idx), sl)
			if err != nil {
				return nil, err
			}
			idx++
		}
	}
}

// The following loop functions decode array items for primitive types,
// avoiding per-element function pointer calls and type checks. Each is
// selected at schema build time based on the array's item type.

func deserArrayStringLoop(src []byte, sliceVal reflect.Value, start, count int, sl *slab) ([]byte, error) {
	var err error
	for i := start; i < start+count; i++ {
		var length int64
		length, src, err = readVarlong(src)
		if err != nil {
			return nil, err
		}
		if length < 0 {
			return nil, fmt.Errorf("invalid negative string length %d", length)
		}
		n := int(length)
		if len(src) < n {
			return nil, &ShortBufferError{Type: "string", Need: n, Have: len(src)}
		}
		sliceVal.Index(i).SetString(sl.string(src, n))
		src = src[n:]
	}
	return src, nil
}

func deserArrayBooleanLoop(src []byte, sliceVal reflect.Value, start, count int, sl *slab) ([]byte, error) {
	// The caller guarantees len(src) >= count (via the block count check),
	// and each boolean consumes exactly 1 byte, so bounds are always safe.
	for i := start; i < start+count; i++ {
		sliceVal.Index(i).SetBool(src[0] != 0)
		src = src[1:]
	}
	return src, nil
}

func deserArrayIntLoop(src []byte, sliceVal reflect.Value, start, count int, sl *slab) ([]byte, error) {
	var err error
	for i := start; i < start+count; i++ {
		var val int32
		val, src, err = readVarint(src)
		if err != nil {
			return nil, err
		}
		sliceVal.Index(i).SetInt(int64(val))
	}
	return src, nil
}

func deserArrayLongLoop(src []byte, sliceVal reflect.Value, start, count int, sl *slab) ([]byte, error) {
	var err error
	for i := start; i < start+count; i++ {
		var val int64
		val, src, err = readVarlong(src)
		if err != nil {
			return nil, err
		}
		sliceVal.Index(i).SetInt(val)
	}
	return src, nil
}

func deserArrayFloatLoop(src []byte, sliceVal reflect.Value, start, count int, sl *slab) ([]byte, error) {
	var err error
	for i := start; i < start+count; i++ {
		var u uint32
		u, src, err = readUint32(src)
		if err != nil {
			return nil, err
		}
		sliceVal.Index(i).SetFloat(float64(math.Float32frombits(u)))
	}
	return src, nil
}

func deserArrayDoubleLoop(src []byte, sliceVal reflect.Value, start, count int, sl *slab) ([]byte, error) {
	var err error
	for i := start; i < start+count; i++ {
		var u uint64
		u, src, err = readUint64(src)
		if err != nil {
			return nil, err
		}
		sliceVal.Index(i).SetFloat(math.Float64frombits(u))
	}
	return src, nil
}

type deserMap struct {
	deserItem    deserfn
	fastBlock    func(src []byte, mapVal, keyVal, elemVal reflect.Value, count int, sl *slab) ([]byte, error)
	fastElemKind reflect.Kind
}

func (s *deserMap) deser(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	v = indirectAlloc(v)
	iface := v.Kind() == reflect.Interface
	var (
		mapVal  reflect.Value
		elemTyp reflect.Type
	)
	if iface {
		mapVal = reflect.MakeMap(reflect.MapOf(reflect.TypeFor[string](), anyType))
		elemTyp = anyType
	} else {
		t := v.Type()
		if t.Kind() != reflect.Map || t.Key().Kind() != reflect.String {
			return nil, &SemanticError{GoType: t, AvroType: "map"}
		}
		if v.IsNil() {
			v.Set(reflect.MakeMap(t))
		}
		mapVal = v
		elemTyp = t.Elem()
	}
	// For primitive value types with matching Go element types, use
	// reusable reflect.Value containers to avoid per-entry allocations.
	useFast := !iface && s.fastBlock != nil && elemTyp.Kind() == s.fastElemKind
	// Pre-allocate reusable key and elem containers to avoid
	// per-entry reflect.ValueOf / reflect.New allocations.
	keyVal := reflect.New(reflect.TypeFor[string]()).Elem()
	elemVal := reflect.New(elemTyp).Elem()
	var err error
	for {
		var count int64
		count, src, err = readVarlong(src)
		if err != nil {
			return nil, err
		}
		if count == 0 {
			if iface {
				v.Set(mapVal)
			}
			return src, nil
		}
		if count < 0 {
			count = -count
			if count < 0 {
				return nil, errors.New("invalid map block count")
			}
			_, src, err = readVarlong(src) // skip block size
			if err != nil {
				return nil, err
			}
		}
		if count > int64(len(src)) {
			return nil, fmt.Errorf("map block count %d exceeds remaining buffer length %d", count, len(src))
		}
		if useFast {
			src, err = s.fastBlock(src, mapVal, keyVal, elemVal, int(count), sl)
			if err != nil {
				return nil, err
			}
			continue
		}
		for range int(count) {
			src, err = readMapKey(src, keyVal, sl)
			if err != nil {
				return nil, err
			}
			src, err = s.deserItem(src, elemVal, sl)
			if err != nil {
				return nil, err
			}
			mapVal.SetMapIndex(keyVal, elemVal)
			elemVal.SetZero()
		}
	}
}

// readMapKey reads an Avro map key from src into keyVal and returns the
// remaining bytes. It is called once per map entry; the work inside
// (readVarlong, slab string copy) dominates the call overhead.
func readMapKey(src []byte, keyVal reflect.Value, sl *slab) ([]byte, error) {
	keyLen, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	if keyLen < 0 || int(keyLen) > len(src) {
		return nil, fmt.Errorf("invalid map key length %d", keyLen)
	}
	keyVal.SetString(sl.string(src, int(keyLen)))
	return src[int(keyLen):], nil
}

// The following block functions decode map entries for primitive value
// types using reusable reflect.Value containers. Each is selected at
// schema build time based on the map's value type.

func deserMapStringBlock(src []byte, mapVal, keyVal, elemVal reflect.Value, count int, sl *slab) ([]byte, error) {
	var err error
	for range count {
		src, err = readMapKey(src, keyVal, sl)
		if err != nil {
			return nil, err
		}

		var valLen int64
		valLen, src, err = readVarlong(src)
		if err != nil {
			return nil, err
		}
		if valLen < 0 || int(valLen) > len(src) {
			return nil, &ShortBufferError{Type: "string", Need: int(valLen), Have: len(src)}
		}
		elemVal.SetString(sl.string(src, int(valLen)))
		src = src[int(valLen):]

		mapVal.SetMapIndex(keyVal, elemVal)
	}
	return src, nil
}

func deserMapBooleanBlock(src []byte, mapVal, keyVal, elemVal reflect.Value, count int, sl *slab) ([]byte, error) {
	var err error
	for range count {
		src, err = readMapKey(src, keyVal, sl)
		if err != nil {
			return nil, err
		}

		if len(src) < 1 {
			return nil, &ShortBufferError{Type: "boolean"}
		}
		elemVal.SetBool(src[0] != 0)
		src = src[1:]

		mapVal.SetMapIndex(keyVal, elemVal)
	}
	return src, nil
}

func deserMapIntBlock(src []byte, mapVal, keyVal, elemVal reflect.Value, count int, sl *slab) ([]byte, error) {
	var err error
	for range count {
		src, err = readMapKey(src, keyVal, sl)
		if err != nil {
			return nil, err
		}

		var val int32
		val, src, err = readVarint(src)
		if err != nil {
			return nil, err
		}
		elemVal.SetInt(int64(val))

		mapVal.SetMapIndex(keyVal, elemVal)
	}
	return src, nil
}

func deserMapLongBlock(src []byte, mapVal, keyVal, elemVal reflect.Value, count int, sl *slab) ([]byte, error) {
	var err error
	for range count {
		src, err = readMapKey(src, keyVal, sl)
		if err != nil {
			return nil, err
		}

		var val int64
		val, src, err = readVarlong(src)
		if err != nil {
			return nil, err
		}
		elemVal.SetInt(val)

		mapVal.SetMapIndex(keyVal, elemVal)
	}
	return src, nil
}

func deserMapFloatBlock(src []byte, mapVal, keyVal, elemVal reflect.Value, count int, sl *slab) ([]byte, error) {
	var err error
	for range count {
		src, err = readMapKey(src, keyVal, sl)
		if err != nil {
			return nil, err
		}

		var u uint32
		u, src, err = readUint32(src)
		if err != nil {
			return nil, err
		}
		elemVal.SetFloat(float64(math.Float32frombits(u)))

		mapVal.SetMapIndex(keyVal, elemVal)
	}
	return src, nil
}

func deserMapDoubleBlock(src []byte, mapVal, keyVal, elemVal reflect.Value, count int, sl *slab) ([]byte, error) {
	var err error
	for range count {
		src, err = readMapKey(src, keyVal, sl)
		if err != nil {
			return nil, err
		}

		var u uint64
		u, src, err = readUint64(src)
		if err != nil {
			return nil, err
		}
		elemVal.SetFloat(math.Float64frombits(u))

		mapVal.SetMapIndex(keyVal, elemVal)
	}
	return src, nil
}

// deserFixedUUIDReflect decodes a fixed(16) UUID. Into any it returns
// [16]byte; into [16]byte it copies the raw bytes; into string it
// formats a hex-dash UUID string; into []byte it falls back to raw bytes.
func deserFixedUUIDReflect(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	if len(src) < 16 {
		return nil, &ShortBufferError{Type: "uuid", Need: 16, Have: len(src)}
	}
	b := [16]byte(src[:16])
	v = indirectAlloc(v)
	switch {
	case v.Kind() == reflect.Interface, isUUIDType(v.Type()):
		v.Set(reflect.ValueOf(b))
	case v.Kind() == reflect.String:
		v.SetString(uuidToString(b))
	case v.Type().Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8:
		v.SetBytes(src[:16])
	default:
		return nil, &SemanticError{GoType: v.Type(), AvroType: "fixed", Err: errors.New("uuid")}
	}
	return src[16:], nil
}

type deserFixed struct {
	n int
}

func (s *deserFixed) deser(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	if len(src) < s.n {
		return nil, &ShortBufferError{Type: "fixed", Need: s.n, Have: len(src)}
	}
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		b := make([]byte, s.n)
		copy(b, src[:s.n])
		v.Set(reflect.ValueOf(b))
		return src[s.n:], nil
	}
	t := v.Type()
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		b := make([]byte, s.n)
		copy(b, src[:s.n])
		v.Set(reflect.ValueOf(b))
		return src[s.n:], nil
	}
	if t.Kind() != reflect.Array || t.Elem().Kind() != reflect.Uint8 {
		return nil, &SemanticError{GoType: t, AvroType: "fixed"}
	}
	if t.Len() != s.n {
		return nil, &SemanticError{GoType: t, AvroType: "fixed"}
	}
	reflect.Copy(v, reflect.ValueOf(src[:s.n]))
	return src[s.n:], nil
}

///////////////////////////////
// LOGICAL TYPE DESERIALIZERS //
///////////////////////////////

// setLongValue sets v to val, handling interface, int, and uint targets.
// Returns an error if val does not fit in v's Go type.
func setLongValue(v reflect.Value, val int64) error {
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(val))
		return nil
	}
	if v.CanInt() {
		if v.OverflowInt(val) {
			return &SemanticError{GoType: v.Type(), AvroType: "long", Err: fmt.Errorf("value %d overflows %s", val, v.Type())}
		}
		v.SetInt(val)
		return nil
	}
	if v.CanUint() {
		if val < 0 || v.OverflowUint(uint64(val)) {
			return &SemanticError{GoType: v.Type(), AvroType: "long", Err: fmt.Errorf("value %d overflows %s", val, v.Type())}
		}
		v.SetUint(uint64(val))
		return nil
	}
	return &SemanticError{GoType: v.Type(), AvroType: "long"}
}

// setIntValue sets v to val, handling interface, int, and uint targets.
// Returns an error if val does not fit in v's Go type.
func setIntValue(v reflect.Value, val int32) error {
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(val))
		return nil
	}
	if v.CanInt() {
		if v.OverflowInt(int64(val)) {
			return &SemanticError{GoType: v.Type(), AvroType: "int", Err: fmt.Errorf("value %d overflows %s", val, v.Type())}
		}
		v.SetInt(int64(val))
		return nil
	}
	if v.CanUint() {
		if val < 0 || v.OverflowUint(uint64(val)) {
			return &SemanticError{GoType: v.Type(), AvroType: "int", Err: fmt.Errorf("value %d overflows %s", val, v.Type())}
		}
		v.SetUint(uint64(val))
		return nil
	}
	return &SemanticError{GoType: v.Type(), AvroType: "int"}
}

func deserTimestampMillis(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface || v.Type() == timeType {
		v.Set(reflect.ValueOf(timestampMillisToTime(val)))
		return src, nil
	}
	return src, setLongValue(v, val)
}

func deserTimestampMicros(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface || v.Type() == timeType {
		v.Set(reflect.ValueOf(timestampMicrosToTime(val)))
		return src, nil
	}
	return src, setLongValue(v, val)
}

func deserTimestampNanos(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface || v.Type() == timeType {
		v.Set(reflect.ValueOf(timestampNanosToTime(val)))
		return src, nil
	}
	return src, setLongValue(v, val)
}

func deserDate(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	val, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface || v.Type() == timeType {
		v.Set(reflect.ValueOf(dateToTime(val)))
		return src, nil
	}
	return src, setIntValue(v, val)
}

func deserTimeMillis(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	val, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface || v.Type() == durationType {
		v.Set(reflect.ValueOf(timeMillisToDuration(val)))
		return src, nil
	}
	return src, setIntValue(v, val)
}

func deserTimeMicros(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	v = indirectAlloc(v)
	if v.Type() == durationType {
		if val > math.MaxInt64/int64(time.Microsecond) || val < math.MinInt64/int64(time.Microsecond) {
			return nil, fmt.Errorf("time-micros value %d overflows time.Duration", val)
		}
		v.Set(reflect.ValueOf(timeMicrosToDuration(val)))
		return src, nil
	}
	if v.Kind() == reflect.Interface {
		// No overflow check for *any targets: spec-violating values
		// silently wrap. Valid time-of-day values (< 86400000000 micros)
		// never overflow. The value may be transient — passed to a
		// CustomType decoder or EncodeJSON.
		v.Set(reflect.ValueOf(timeMicrosToDuration(val)))
		return src, nil
	}
	return src, setLongValue(v, val)
}

func deserDuration(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	if len(src) < 12 {
		return nil, &ShortBufferError{Type: "duration", Need: 12, Have: len(src)}
	}
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface || v.Type() == avroDurationType {
		v.Set(reflect.ValueOf(DurationFromBytes(src[:12])))
		return src[12:], nil
	}
	// Fall back to [12]byte fixed.
	return (&deserFixed{12}).deser(src, v, sl)
}

// setDecimalValue sets v from decimal bytes. Returns true if v was set,
// false if v's Go type is not supported by the decimal decoder (caller
// may fall back to the underlying bytes/fixed handler).
func setDecimalValue(v reflect.Value, b []byte, scale int) (bool, error) {
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(bytesToRat(b, scale)))
		return true, nil
	}
	if v.Type() == bigRatType {
		v.Set(reflect.ValueOf(*bytesToRat(b, scale)))
		return true, nil
	}
	if v.Type() == jsonNumberType {
		r := bytesToRat(b, scale)
		v.Set(reflect.ValueOf(json.Number(r.FloatString(scale))))
		return true, nil
	}
	if v.CanFloat() {
		r := bytesToRat(b, scale)
		f, _ := r.Float64()
		// big.Rat.Float64 returns ±Inf when the rational is too large
		// for float64; reject rather than silently writing Inf.
		if math.IsInf(f, 0) {
			return true, &SemanticError{GoType: v.Type(), AvroType: "decimal", Err: fmt.Errorf("decimal value %s overflows %s", r.RatString(), v.Kind())}
		}
		if v.Kind() == reflect.Float32 && math.IsInf(float64(float32(f)), 0) {
			return true, &SemanticError{GoType: v.Type(), AvroType: "decimal", Err: fmt.Errorf("value %g overflows float32", f)}
		}
		v.SetFloat(f)
		return true, nil
	}
	if v.Kind() == reflect.String {
		r := bytesToRat(b, scale)
		v.SetString(r.FloatString(scale))
		return true, nil
	}
	return false, nil
}

type deserBytesDecimal struct {
	scale int
}

func (s *deserBytesDecimal) deser(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	length, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, fmt.Errorf("invalid negative bytes length %d", length)
	}
	n := int(length)
	if len(src) < n {
		return nil, &ShortBufferError{Type: "decimal", Need: n, Have: len(src)}
	}
	b := src[:n]
	src = src[n:]
	v = indirectAlloc(v)
	if ok, err := setDecimalValue(v, b, s.scale); ok {
		return src, err
	}
	return nil, &SemanticError{GoType: v.Type(), AvroType: "decimal"}
}

type deserFixedDecimal struct {
	size  int
	scale int
}

func (s *deserFixedDecimal) deser(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	if len(src) < s.size {
		return nil, &ShortBufferError{Type: "decimal", Need: s.size, Have: len(src)}
	}
	b := src[:s.size]
	src = src[s.size:]
	v = indirectAlloc(v)
	if ok, err := setDecimalValue(v, b, s.scale); ok {
		return src, err
	}
	// Fall back to [N]byte fixed.
	return (&deserFixed{s.size}).deser(append(b[:0:0], b...), v, sl)
}

// RatFromBytes converts Avro decimal bytes (big-endian two's complement)
// to *big.Rat with the given scale. This is useful in [CustomType] Decode
// callbacks that override the default decimal handling: the callback
// receives raw []byte and can use this function to interpret the value
// before converting to a custom Go type.
func RatFromBytes(b []byte, scale int) *big.Rat {
	return bytesToRat(b, scale)
}

func bytesToRat(b []byte, scale int) *big.Rat {
	unscaled := bytesToBigInt(b)
	s := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	return new(big.Rat).SetFrac(unscaled, s)
}

// bytesToBigInt decodes big-endian two's complement bytes into a *big.Int.
func bytesToBigInt(b []byte) *big.Int {
	if len(b) == 0 {
		return new(big.Int)
	}
	i := new(big.Int).SetBytes(b) // unsigned big-endian
	if b[0]&0x80 != 0 {
		// High bit set means negative in two's complement.
		// SetBytes treated it as unsigned, so subtract 2^(8*len)
		// to recover the signed value.
		modulus := new(big.Int).Lsh(big.NewInt(1), uint(8*len(b)))
		i.Sub(i, modulus)
	}
	return i
}

// parseUUID parses an RFC 4122 hex-dash UUID string into a [16]byte.
func parseUUID(s string) ([16]byte, error) {
	var u [16]byte
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return u, fmt.Errorf("invalid UUID %q", s)
	}
	_, err := hex.Decode(u[0:4], []byte(s[0:8]))
	if err != nil {
		return u, fmt.Errorf("invalid UUID %q: %w", s, err)
	}
	_, err = hex.Decode(u[4:6], []byte(s[9:13]))
	if err != nil {
		return u, fmt.Errorf("invalid UUID %q: %w", s, err)
	}
	_, err = hex.Decode(u[6:8], []byte(s[14:18]))
	if err != nil {
		return u, fmt.Errorf("invalid UUID %q: %w", s, err)
	}
	_, err = hex.Decode(u[8:10], []byte(s[19:23]))
	if err != nil {
		return u, fmt.Errorf("invalid UUID %q: %w", s, err)
	}
	_, err = hex.Decode(u[10:16], []byte(s[24:36]))
	if err != nil {
		return u, fmt.Errorf("invalid UUID %q: %w", s, err)
	}
	return u, nil
}

func deserUUID(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	length, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, fmt.Errorf("invalid negative string length %d", length)
	}
	n := int(length)
	if len(src) < n {
		return nil, &ShortBufferError{Type: "string", Need: n, Have: len(src)}
	}
	s := string(src[:n])
	v = indirectAlloc(v)
	if isUUIDType(v.Type()) {
		u, err := parseUUID(s)
		if err != nil {
			return nil, err
		}
		reflect.Copy(v, reflect.ValueOf(u))
		return src[n:], nil
	}
	if v.Kind() == reflect.Interface {
		v.Set(reflect.ValueOf(s))
		return src[n:], nil
	}
	if v.Kind() == reflect.String {
		v.SetString(s)
		return src[n:], nil
	}
	if v.CanAddr() && v.Addr().Type().Implements(textUnmarshalerType) {
		if err := v.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText([]byte(s)); err != nil {
			return nil, err
		}
		return src[n:], nil
	}
	return nil, &SemanticError{GoType: v.Type(), AvroType: "string"}
}
