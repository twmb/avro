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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type deserfn func(src []byte, v reflect.Value, sl *slab) ([]byte, error)

// readLength reads a length-prefixed varlong, validates that it is
// non-negative and fits in the remaining buffer, then returns the
// narrowed length, the advanced buffer, and nil. typeName populates
// ShortBufferError.Type and the negative-length message — keep it short
// (e.g. "bytes", "string", "decimal"). Used by all length-prefixed
// reads (bytes/string/decimal/UUID); the comparison happens in int64
// to keep the bound correct on 32-bit. The function is small and
// callee-cheap; mid-stack inlining keeps perf parity with the inlined
// version.
func readLength(src []byte, typeName string) (int, []byte, error) {
	length, src, err := readVarlong(src)
	if err != nil {
		return 0, nil, err
	}
	if length < 0 {
		return 0, nil, fmt.Errorf("invalid negative %s length %d", typeName, length)
	}
	if length > int64(len(src)) {
		return 0, nil, &ShortBufferError{Type: typeName, Need: int(length), Have: len(src)}
	}
	return int(length), src, nil
}

var anyType = reflect.TypeFor[any]()
var sliceAnyType = reflect.SliceOf(anyType)

// slab batches small string allocations into a single backing buffer.
// Strings are immutable so sharing backing memory is safe.
type slab struct {
	buf             []byte
	depth           int // recursion depth; bumped at recursive dispatch sites
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

// put resets sl's per-call state and returns it to the pool. The buf field
// is intentionally retained so subsequent callers reuse its backing memory.
func (sl *slab) put() {
	sl.depth = 0
	sl.taggedUnions = false
	sl.tagLogicalTypes = false
	slabPool.Put(sl)
}

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
//   - date, timestamp-millis/micros/nanos: [time.Time] (UTC)
//   - local-timestamp-millis/micros/nanos: [time.Time] (UTC; wall-clock
//     fields encode/decode as if UTC, matching Java's reference impl)
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
//
// Decode is liberal in what it accepts: non-canonical input (e.g. a
// non-0/1 boolean byte; the Java reference treats it as false, and so
// does Decode) is tolerated rather than rejected. Encode is canonical,
// so a non-canonical input round-trips to the canonical form on encode.
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
	sl.put()
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
	if sl.depth >= maxDepth {
		return nil, errTooDeep
	}
	sl.depth++
	defer func() { sl.depth-- }()
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
// TaggedUnions is enabled and the target is an interface type that
// map[string]any can be assigned to (in practice: *any, since any
// non-empty interface's method set wouldn't be satisfied by a plain
// map). Non-interface targets and interfaces with methods are
// skipped silently.
func (s *deserUnion) maybeWrap(v reflect.Value, sl *slab, idx int32) {
	if !sl.taggedUnions || v.Kind() != reflect.Interface || !v.Elem().IsValid() {
		return
	}
	// Skip silently if the wrapping map[string]any can't be assigned
	// to v's interface type. Use the cached type rather than building
	// a throwaway reflect.Value(map[string]any{}) which allocates per
	// call.
	if !mapStringAnyType.AssignableTo(v.Type()) {
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
func deserNullUnion(u *deserUnion) deserfn { return deserNullUnionAt(u, 1, 0, 2) }

// deserNullSecondUnion handles ["T", "null"] unions: 0x00 = index 0 (T),
// 0x02 = index 1 (null).
func deserNullSecondUnion(u *deserUnion) deserfn { return deserNullUnionAt(u, 0, 2, 0) }

// readNullUnionIndex decodes the wire-encoded branch index in a 2-branch
// null-union and returns (isValBranch, advancedSrc, err).
//
// Avro encodes union indices as a generic zigzag varint, not a single
// byte. The canonical 1-byte encoding for indices 0 and 1 is 0x00 /
// 0x02, but a producer is allowed to emit a non-canonical multi-byte
// form like 0x80 0x00 (= 0). Java's BinaryDecoder.readIndex calls
// readInt() unconditionally and accepts both; we fast-path the
// canonical case and fall through to readVarint for the rest.
//
// Used by deserNullUnionAt and the unsafe udNullUnion* paths to share
// one varint-tolerant index-decode rather than three byte-switches that
// could drift apart.
func readNullUnionIndex(src []byte, valIdx int, nullByte, valByte byte) (bool, []byte, error) {
	if len(src) < 1 {
		return false, nil, &ShortBufferError{Type: "union index"}
	}
	if src[0]&0x80 == 0 {
		switch src[0] {
		case nullByte:
			return false, src[1:], nil
		case valByte:
			return true, src[1:], nil
		default:
			return false, nil, fmt.Errorf("invalid null-union index byte 0x%02x", src[0])
		}
	}
	i, rest, err := readVarint(src)
	if err != nil {
		return false, nil, err
	}
	switch i {
	case int32(valIdx):
		return true, rest, nil
	case int32(1 - valIdx):
		return false, rest, nil
	default:
		return false, nil, fmt.Errorf("union index %d out of range [0,2)", i)
	}
}

// deserNullUnionAt is the shared implementation. valIdx is the index of T
// in the union; nullByte and valByte are the canonical (single-byte)
// wire-format bytes for the null and value branches.
func deserNullUnionAt(u *deserUnion, valIdx int, nullByte, valByte byte) deserfn {
	return func(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
		isVal, src, err := readNullUnionIndex(src, valIdx, nullByte, valByte)
		if err != nil {
			return nil, err
		}
		if !isVal {
			v.Set(reflect.Zero(v.Type()))
			return src, nil
		}
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			return u.fns[valIdx](src, v.Elem(), sl)
		}
		out, err := u.fns[valIdx](src, v, sl)
		if err == nil {
			u.maybeWrap(v, sl, int32(valIdx))
		}
		return out, err
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
	// Not spec-exact (spec says 0 or 1), but matches Java reference:
	// only 0x01 is true. Encoder is canonical 0/1.
	b := src[0] == 1
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		// Fast path: empty interface (any) — most decode targets.
		if v.Type().NumMethod() == 0 {
			v.Set(reflect.ValueOf(b))
			return src[1:], nil
		}
		// Slow path: non-empty interface — guard against panic in
		// reflect.Value.Set when bool isn't assignable.
		rv := reflect.ValueOf(b)
		if !rv.Type().AssignableTo(v.Type()) {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "boolean"}
		}
		v.Set(rv)
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
	return src, setFloatValue(indirectAlloc(v), float64(math.Float32frombits(u)), "float", 32)
}

func deserDouble(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	u, src, err := readUint64(src)
	if err != nil {
		return nil, err
	}
	return src, setFloatValue(indirectAlloc(v), math.Float64frombits(u), "double", 64)
}

func deserBytes(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	n, src, err := readLength(src, "bytes")
	if err != nil {
		return nil, err
	}
	b := make([]byte, n)
	copy(b, src[:n])
	if err := setBytesValue(indirectAlloc(v), b, "bytes"); err != nil {
		return nil, err
	}
	return src[n:], nil
}

func deserString(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	n, src, err := readLength(src, "string")
	if err != nil {
		return nil, err
	}
	if err := setStringValue(indirectAlloc(v), src, n, sl); err != nil {
		return nil, err
	}
	return src[n:], nil
}

/////////////
// COMPLEX //
/////////////

// deserIfaceFn decodes a primitive Avro value and returns it as a Go
// `any`. Used by record/map/array decode paths into interface targets
// to bypass the reflect.ValueOf alloc that the generic deserfn would
// pay when boxing a primitive into a reflect.Value of interface kind.
// nil for complex types (record/array/map/union/logical-no-fast); the
// caller falls back to deserfn for those.
type deserIfaceFn func(src []byte, sl *slab) (any, []byte, error)

type deserRecordField struct {
	name       string
	nameVal    reflect.Value // pre-computed reflect.ValueOf(name); avoids alloc per map lookup
	fn         deserfn
	fnIface    deserIfaceFn // non-nil iff f.fn handles a primitive that benefits from iface-direct decode
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
	if sl.depth >= maxDepth {
		return nil, errTooDeep
	}
	sl.depth++
	defer func() { sl.depth-- }()
	v = indirectAlloc(v)
	k := v.Kind()
	if k == reflect.Interface {
		if v.Type().NumMethod() != 0 && !mapStringAnyType.AssignableTo(v.Type()) {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "record"}
		}
		// Reuse the existing map[string]any if v already wraps one.
		// This is the streaming-decode pattern (OCF reader, batch
		// consumer reusing &out across many records). We do this
		// explicitly here rather than in indirectAlloc — unwrapping a
		// non-nil interface there would break decoders that
		// v.Set(...) on the result (decodeNull, decodeArray's typed
		// branch, etc.) since the unwrapped Value isn't addressable.
		// Here we only need SetMapIndex, which works on the
		// non-addressable Map.
		//
		// Reuse retains keys not present in the schema (matches
		// encoding/json into a non-empty map). Callers that need a
		// fresh decode should clear or replace the map before each
		// call. Pinned by TestDecodeReuseAnyTargetStaleKeys.
		var m map[string]any
		if inner := v.Elem(); inner.IsValid() && inner.Type() == mapStringAnyType {
			m = inner.Interface().(map[string]any)
		} else {
			m = make(map[string]any, len(s.fields))
		}
		var elem reflect.Value
		var err error
		for _, f := range s.fields {
			if f.fnIface != nil {
				var val any
				val, src, err = f.fnIface(src, sl)
				if err != nil {
					return nil, recordFieldError(nil, f.name, err)
				}
				m[f.name] = val
				continue
			}
			if !elem.IsValid() {
				elem = reflect.New(anyType).Elem()
			}
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
			v.Set(reflect.MakeMapWithSize(t, len(s.fields)))
		}
		elem := reflect.New(t.Elem()).Elem()
		for _, f := range s.fields {
			if src, err = f.fn(src, elem, sl); err != nil {
				return nil, recordFieldError(nil, f.name, err)
			}
			v.SetMapIndex(mapKeyAs(t, f.nameVal), elem)
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
		if v.Type().NumMethod() == 0 {
			v.Set(reflect.ValueOf(s.symbols[idx]))
			return src, nil
		}
		rv := reflect.ValueOf(s.symbols[idx])
		if !rv.Type().AssignableTo(v.Type()) {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "enum"}
		}
		v.Set(rv)
		return src, nil
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
	// fastIfaceLoop decodes one block of items directly into a []any,
	// bypassing reflect for primitive items. Selected at schema-build
	// time based on the avro item type. nil for non-primitive items.
	fastIfaceLoop func(src []byte, slice []any, start, count int, sl *slab) ([]byte, error)
	// minItemBytes is the minimum wire bytes for one encoded item.
	// Used to tightly bound block counts: count > len(src)/minItemBytes
	// rejects DoS-sized counts. 0 means items can take 0 wire bytes
	// (e.g. array<null>, array<EmptyRecord>); the maxZeroByteItems
	// absolute cap applies in that case.
	minItemBytes int
}

// maxZeroByteItems caps the cumulative element count for arrays whose
// items take 0 wire bytes (array<null>, array<EmptyRecord>, etc.). Without
// this cap a 10-byte input claiming 10 billion nulls would allocate a
// 160 GiB []any. Hardcoded rather than configurable: legitimate use of
// zero-byte arrays with more than a few thousand elements is essentially
// always a schema-design problem.
const maxZeroByteItems = 4 << 10

// checkArrayBlockBounds validates an array/map block's count against
// (a) the buffer-relative cap for items with non-zero minimum wire
// size, and (b) the cumulative zero-byte-element cap. The zero-byte
// branch uses the pre-add form `count > maxZeroByteItems-totalItems`
// so a hostile count near MaxInt64 cannot wrap totalItems past the cap
// (the post-add form `totalItems += count; if totalItems > cap` wraps
// to a negative int64 and bypasses the check). Caller updates
// totalItems after a non-error return. Used by skipArray, deserArray,
// udArrayPtrRecord, and udArrayDirect to keep the four sites from
// drifting on this rule.
func checkArrayBlockBounds(count int64, totalItems int64, srcLen int, minItemBytes int) error {
	if minItemBytes > 0 {
		if count > int64(srcLen)/int64(minItemBytes) {
			return fmt.Errorf("array block count %d exceeds remaining buffer length %d (min %d byte/item)", count, srcLen, minItemBytes)
		}
		return nil
	}
	if count > int64(maxZeroByteItems)-totalItems {
		return fmt.Errorf("array of zero-byte items exceeds %d-element limit", maxZeroByteItems)
	}
	return nil
}

// decimalScaleLimit caps decimal/big-decimal scale and precision to
// prevent attacker-controlled 10^scale allocations during Exp.
// Applied at schema parse (regular decimal precision/scale), wire
// decode (big-decimal scale read from the producer), and encode
// (finiteScale-derived scale for big-decimal). 10^(1<<16) is a
// ~27 KB big.Int — generous for cryptography and scientific
// precision while bounding hostile-input allocation. Java caps at
// int32 implicitly and never eagerly evaluates 10^scale; avro-rs
// the same. twmb is the only impl that materializes the magnitude
// at decode time, so the cap has to live here.
const decimalScaleLimit = 1 << 16

// maxRatInputLen caps the byte length boundedRatFromString routes
// through big.Rat.SetString. SetString is O(n²) over input length, so
// 1 MiB of digits burns ~2 sec CPU; 128 KiB rejects in ~25 ms worst
// case. Legitimate decimal use is bounded by decimalScaleLimit
// (mantissa+exponent ≤ 65536), so a cap matched to twice that magnitude
// covers every schema-conforming input with headroom while still
// bounding the new helper's parse cost. Java/avro-rs don't materialize
// 10^N during parsing so don't need this cap; twmb does, so the cap
// has to live at the boundary the parsing actually crosses.
const maxRatInputLen = 1 << 17 // 128 KiB

// isJSONNumber reports whether s is a JSON number per RFC 8259.
// json.Valid validates the grammar; the boundary-whitespace and
// first-char checks reject (a) whitespace-padded numbers (JSON's
// "ws value ws" production accepts them as JSON-text but not as a
// standalone number), and (b) other JSON values that are valid but
// non-numeric (strings, booleans, null, arrays, objects).
//
// boundedRatFromString uses this gate because big.Rat.SetString's
// accepted-input set is strictly broader than JSON: it accepts
// hex ("0x10"), binary ("0b10"), octal ("0o10"), underscore-separated
// ("1_000"), rational ("5/1"), and hex-float-with-binary-exponent
// ("0x1p4") forms. None of these are valid JSON numbers, and all of
// them silently produced an integer value when they leaked into the
// integer / decimal / big-decimal encode paths via parseInt64Lenient,
// jsonNumberToInt64, jsonCoerceToInt32/64, and tryCoerceToRat.
func isJSONNumber(s string) bool {
	if len(s) == 0 {
		return false
	}
	first := s[0]
	if first == ' ' || first == '\t' || first == '\n' || first == '\r' {
		return false
	}
	last := s[len(s)-1]
	if last == ' ' || last == '\t' || last == '\n' || last == '\r' {
		return false
	}
	if first != '-' && (first < '0' || first > '9') {
		return false
	}
	// json.Valid is read-only; alias s's bytes to avoid the []byte(s) copy.
	// Mirrors the parseUUID/parseUUIDBytes unsafe-slice pattern.
	return json.Valid(unsafe.Slice(unsafe.StringData(s), len(s)))
}

// boundedRatFromString parses s into a *big.Rat, validating that s is
// a JSON-spec number (via isJSONNumber) before reaching big.Rat.SetString
// and rejecting decimal-form inputs whose net 10^exponent magnitude
// exceeds decimalScaleLimit. SetString materializes 10^|net-exp| eagerly
// during parsing — a 9-byte "1e1000000" allocates ~3 MB without the
// magnitude guard, and 1 MiB of digits costs ~2 sec without the length
// cap. Mirrors the wire-side bound in parseBigDecimalPayload so every
// external decimal path (JSON decode bytes/fixed decimal, encode of
// json.Number and string-typed values) shares the same caps.
//
// Three-valued return: (rat, true, nil) on success; (nil, false, nil)
// when s is not a number form at all (e.g. "abc", empty, or any input
// that doesn't start with '-' or a digit — caller may fall back to raw
// bytes for legitimate non-numeric inputs); (nil, false, err) when s
// IS number-shaped (leading '-' or digit) but rejected — JSON-invalid
// grammar, length cap, or magnitude cap. Callers must propagate err
// so hostile numeric-looking input cannot silently re-encode as raw
// bytes via the fall-through path.
func boundedRatFromString(s string) (*big.Rat, bool, error) {
	if len(s) > maxRatInputLen {
		return nil, false, fmt.Errorf("decimal value exceeds %d byte length cap", maxRatInputLen)
	}
	if !isJSONNumber(s) {
		// Numeric-looking but JSON-invalid (e.g. "0x10", "1_000",
		// "5/1", "+5", ".5") surfaces as an error so the caller's
		// typed-numeric path doesn't silently drop into the raw-bytes
		// fallback. The "numeric-looking" predicate is broader than
		// JSON-spec's number-start (which is just '-' or digit): it
		// also includes '+' (Go/C-style sign that strconv accepts)
		// and '.' (Python/JS-style leading dot that the user likely
		// intended as a fractional). Genuinely non-numeric inputs
		// (first char something like 'a', 'N', '{') stay in the
		// (nil, false, nil) lane for the reflect.String → opaque
		// raw-bytes fall-through.
		if len(s) > 0 {
			c := s[0]
			if c == '-' || c == '+' || c == '.' || (c >= '0' && c <= '9') {
				return nil, false, fmt.Errorf("invalid JSON number %q", s)
			}
		}
		return nil, false, nil
	}
	netExp := int64(0)
	body := s
	if i := strings.IndexAny(body, "eE"); i >= 0 {
		exp, err := strconv.ParseInt(body[i+1:], 10, 64)
		if err != nil {
			return nil, false, nil
		}
		netExp = exp
		body = body[:i]
	}
	if i := strings.IndexByte(body, '.'); i >= 0 {
		fracLen := int64(len(body) - i - 1)
		if netExp < math.MinInt64+fracLen {
			return nil, false, fmt.Errorf("decimal exponent overflow")
		}
		netExp -= fracLen
	}
	if netExp > decimalScaleLimit || netExp < -decimalScaleLimit {
		return nil, false, fmt.Errorf("decimal value 10^%d magnitude exceeds %d limit", netExp, decimalScaleLimit)
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, false, nil
	}
	return r, true, nil
}

// schemaMinBytes returns the minimum number of wire bytes required to
// encode one value of node's type. Used at decode time to bound array
// block counts. Cycles fall back to 1 (conservative — defaults to the
// existing tight buffer-relative guard).
func schemaMinBytes(n *schemaNode) int {
	return schemaMinBytesSeen(n, map[*schemaNode]struct{}{})
}

func schemaMinBytesSeen(n *schemaNode, seen map[*schemaNode]struct{}) int {
	if n == nil {
		return 1
	}
	if _, cycle := seen[n]; cycle {
		return 1
	}
	seen[n] = struct{}{}
	defer delete(seen, n)
	switch n.kind {
	case "null":
		return 0
	case "boolean", "int", "long", "enum":
		return 1
	case "float":
		return 4
	case "double":
		return 8
	case "bytes", "string":
		return 1
	case "fixed":
		return n.size
	case "array", "map":
		return 1 // empty-collection terminator is 1 byte
	case "union":
		m := math.MaxInt
		for _, b := range n.branches {
			if v := schemaMinBytesSeen(b, seen); v < m {
				m = v
			}
		}
		if m == math.MaxInt {
			return 1
		}
		return 1 + m
	case "record":
		var s int
		for i := range n.fields {
			s += schemaMinBytesSeen(n.fields[i].node, seen)
			if s >= math.MaxInt32 {
				return math.MaxInt32
			}
		}
		return s
	}
	return 1
}

func (s *deserArray) deser(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	if sl.depth >= maxDepth {
		return nil, errTooDeep
	}
	sl.depth++
	defer func() { sl.depth-- }()
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
	// For interface targets, build a []any. sliceVal is populated lazily so
	// the first block's count can serve as a capacity hint (avoids a
	// MakeSlice+reflect.Copy on the typical single-block path).
	var (
		sliceVal  reflect.Value
		sliceType reflect.Type
	)
	if iface {
		sliceType = sliceAnyType
	} else {
		v.SetLen(0)
		sliceVal = v
		sliceType = v.Type()
	}
	// For primitive item types with matching Go element types, use
	// a specialized loop that avoids per-element function pointer calls.
	useFast := !iface && s.fastLoop != nil && sliceType.Elem().Kind() == s.fastElemKind
	// For interface targets with primitive avro items, use the iface
	// fast loop that operates directly on []any.
	useFastIface := iface && s.fastIfaceLoop != nil
	// Avro arrays are encoded as a series of blocks. Each block starts
	// with a count: positive means N elements follow, zero means end of
	// array, negative means |N| elements follow and the next varint is
	// the block's byte size (for skipping without decoding).
	var err error
	var totalItems int64
	for {
		var count int64
		count, src, err = readVarlong(src)
		if err != nil {
			return nil, err
		}
		if count == 0 {
			if iface {
				if !sliceVal.IsValid() {
					sliceVal = reflect.MakeSlice(sliceType, 0, 0)
				}
				return src, setIface(v, sliceVal, "array")
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
		if err := checkArrayBlockBounds(count, totalItems, len(src), s.minItemBytes); err != nil {
			return nil, err
		}
		totalItems += count
		if count > math.MaxInt {
			return nil, fmt.Errorf("array block count %d exceeds platform max int", count)
		}
		n := int(count)
		// Lazy-allocate the iface backing slice on the first block using
		// its count as a capacity hint.
		if iface && !sliceVal.IsValid() {
			sliceVal = reflect.MakeSlice(sliceType, 0, n)
		}
		start := sliceVal.Len()
		if start > math.MaxInt-n {
			return nil, fmt.Errorf("array length overflows int: start=%d count=%d", start, n)
		}
		newLen := start + n
		switch {
		case sliceVal.Cap() < newLen:
			ns := reflect.MakeSlice(sliceVal.Type(), newLen, newLen)
			reflect.Copy(ns, sliceVal)
			sliceVal = ns
			if !iface {
				v.Set(sliceVal)
			}
		case iface:
			// MakeSlice-derived values are unaddressable, so SetLen
			// would panic. Re-slice instead — same backing memory.
			sliceVal = sliceVal.Slice(0, newLen)
		default:
			sliceVal.SetLen(newLen)
		}
		if useFast {
			src, err = s.fastLoop(src, sliceVal, start, n, sl)
			if err != nil {
				return nil, err
			}
			continue
		}
		if useFastIface {
			// sliceVal wraps a []any; mutating via the extracted slice is
			// visible through sliceVal too (same underlying array). The
			// final setIface(v, sliceVal, "array") picks up all writes.
			slice := sliceVal.Interface().([]any)
			src, err = s.fastIfaceLoop(src, slice, start, n, sl)
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
		// Compare in int64-sized arithmetic before narrowing to int.
		// idx+int(count) > arrLen wraps for huge count (e.g. count
		// near MaxInt64), bypassing the bound and panicking on
		// v.Index(idx) when idx exceeds arrLen. arrLen-idx is
		// non-negative by loop invariant.
		if count > int64(arrLen-idx) {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "array", Err: fmt.Errorf("expected %d elements, got more", arrLen)}
		}
		n := int(count)
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
		var n int
		n, src, err = readLength(src, "string")
		if err != nil {
			return nil, err
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
		sliceVal.Index(i).SetBool(src[0] == 1)
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

// The following iface fast loops decode array items directly into a
// []any, bypassing the reflect.Value wrapping that the generic loop
// would do. Selected at schema-build time based on the avro item type.

func deserArrayStringIfaceLoop(src []byte, slice []any, start, count int, sl *slab) ([]byte, error) {
	for i := start; i < start+count; i++ {
		n, rest, err := readLength(src, "string")
		if err != nil {
			return nil, err
		}
		slice[i] = sl.string(rest, n)
		src = rest[n:]
	}
	return src, nil
}

func deserArrayBooleanIfaceLoop(src []byte, slice []any, start, count int, sl *slab) ([]byte, error) {
	// Caller guarantees len(src) >= count via block bounds check.
	for i := start; i < start+count; i++ {
		slice[i] = src[0] == 1
		src = src[1:]
	}
	return src, nil
}

func deserArrayIntIfaceLoop(src []byte, slice []any, start, count int, sl *slab) ([]byte, error) {
	for i := start; i < start+count; i++ {
		val, rest, err := readVarint(src)
		if err != nil {
			return nil, err
		}
		slice[i] = val
		src = rest
	}
	return src, nil
}

func deserArrayLongIfaceLoop(src []byte, slice []any, start, count int, sl *slab) ([]byte, error) {
	for i := start; i < start+count; i++ {
		val, rest, err := readVarlong(src)
		if err != nil {
			return nil, err
		}
		slice[i] = val
		src = rest
	}
	return src, nil
}

func deserArrayFloatIfaceLoop(src []byte, slice []any, start, count int, sl *slab) ([]byte, error) {
	for i := start; i < start+count; i++ {
		u, rest, err := readUint32(src)
		if err != nil {
			return nil, err
		}
		slice[i] = math.Float32frombits(u)
		src = rest
	}
	return src, nil
}

func deserArrayDoubleIfaceLoop(src []byte, slice []any, start, count int, sl *slab) ([]byte, error) {
	for i := start; i < start+count; i++ {
		u, rest, err := readUint64(src)
		if err != nil {
			return nil, err
		}
		slice[i] = math.Float64frombits(u)
		src = rest
	}
	return src, nil
}

type deserMap struct {
	deserItem    deserfn
	fastBlock    func(src []byte, mapVal, keyVal, elemVal reflect.Value, count int, sl *slab) ([]byte, error)
	fastElemKind reflect.Kind
	// fastIfaceBlock decodes one block of entries directly into a
	// map[string]any, bypassing reflect for primitive values. Selected
	// at schema-build time based on the avro value type. nil for
	// non-primitive value types; the generic reflect path handles those.
	fastIfaceBlock func(src []byte, m map[string]any, count int, sl *slab) ([]byte, error)
	// minEntryBytes is 1 (key length varint, ≥1 byte for empty key)
	// + schemaMinBytes(values). Used to bound block-count against
	// remaining buffer length, preventing the same memory-amplification
	// DoS shape that the array path's minItemBytes guards against.
	minEntryBytes int
}

// maxMapPreAllocSize caps the size hint passed to reflect.MakeMapWithSize
// so an attacker-controlled block count can't drive bucket overhead
// proportional to the wire count. Legitimate larger maps still grow
// dynamically (incremental rehash) — the cap costs a small amount of
// rehashing work for maps above this size, which is far cheaper than
// the worst-case ~40x amplification we'd otherwise pay for hostile
// input. Matches maxZeroByteItems in spirit (bound a single hostile
// dimension by an absolute small constant).
const maxMapPreAllocSize = 4 << 10

func (s *deserMap) deser(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	if sl.depth >= maxDepth {
		return nil, errTooDeep
	}
	sl.depth++
	defer func() { sl.depth-- }()
	v = indirectAlloc(v)
	iface := v.Kind() == reflect.Interface
	// mapVal is populated lazily so we can size the map with the first
	// block's count as a hint (avoids rehash on the typical single-block
	// path). For non-iface maps the user may pass a pre-populated map; we
	// merge into it as-is and skip the hint.
	var (
		mapVal  reflect.Value
		mapTyp  reflect.Type
		elemTyp reflect.Type
	)
	if iface {
		mapTyp = mapStringAnyType
		elemTyp = anyType
	} else {
		t := v.Type()
		if t.Kind() != reflect.Map || t.Key().Kind() != reflect.String {
			return nil, &SemanticError{GoType: t, AvroType: "map"}
		}
		mapTyp = t
		elemTyp = t.Elem()
		if !v.IsNil() {
			mapVal = v
		}
	}
	// For primitive value types with matching Go element types, use
	// reusable reflect.Value containers to avoid per-entry allocations.
	useFast := !iface && s.fastBlock != nil && elemTyp.Kind() == s.fastElemKind
	// For interface targets with primitive avro values, use the
	// iface-block fast path that operates directly on map[string]any.
	useFastIface := iface && s.fastIfaceBlock != nil
	// Pre-allocate reusable key and elem containers to avoid
	// per-entry reflect.ValueOf / reflect.New allocations.
	// Construct keyVal with the user's actual map key type (e.g.
	// `type UserID string`); reusing this Value for SetMapIndex
	// avoids the panic that would fire on plain reflect.ValueOf(s)
	// when the map's key is a named string subtype.
	var keyVal, elemVal reflect.Value
	if !useFastIface {
		keyVal = reflect.New(mapTyp.Key()).Elem()
		elemVal = reflect.New(elemTyp).Elem()
	}
	var err error
	for {
		var count int64
		count, src, err = readVarlong(src)
		if err != nil {
			return nil, err
		}
		if count == 0 {
			// Empty map: allocate zero-sized backing if we never saw a block.
			if !mapVal.IsValid() {
				mapVal = reflect.MakeMap(mapTyp)
				if !iface {
					v.Set(mapVal)
				}
			}
			if iface {
				return src, setIface(v, mapVal, "map")
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
		if count > int64(len(src))/int64(s.minEntryBytes) {
			return nil, fmt.Errorf("map block count %d exceeds remaining buffer length %d (min %d byte/entry)", count, len(src), s.minEntryBytes)
		}
		// Lazy-allocate on first block using its count as a size hint,
		// capped to bound bucket-overhead amplification on hostile
		// input. The block-count bound above admits valid wire shapes
		// where many entries fit in few bytes (e.g. map<long> entries
		// at the 2-byte minimum); without the cap the size hint would
		// drive ~40x heap allocation per input byte for map[string]any.
		if !mapVal.IsValid() {
			hint := int(count)
			if hint > maxMapPreAllocSize {
				hint = maxMapPreAllocSize
			}
			mapVal = reflect.MakeMapWithSize(mapTyp, hint)
			if !iface {
				v.Set(mapVal)
			}
		}
		if useFast {
			src, err = s.fastBlock(src, mapVal, keyVal, elemVal, int(count), sl)
			if err != nil {
				return nil, err
			}
			continue
		}
		if useFastIface {
			// mapVal wraps the same map[string]any header; mutating
			// via the extracted Go map is visible through mapVal too,
			// so the trailing setIface picks up all entries.
			m := mapVal.Interface().(map[string]any)
			src, err = s.fastIfaceBlock(src, m, int(count), sl)
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
	if keyLen < 0 || keyLen > int64(len(src)) {
		return nil, fmt.Errorf("invalid map key length %d", keyLen)
	}
	n := int(keyLen)
	keyVal.SetString(sl.string(src, n))
	return src[n:], nil
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
		if valLen < 0 || valLen > int64(len(src)) {
			return nil, &ShortBufferError{Type: "string", Need: int(valLen), Have: len(src)}
		}
		n := int(valLen)
		elemVal.SetString(sl.string(src, n))
		src = src[n:]

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
		elemVal.SetBool(src[0] == 1)
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

// The following iface-block functions decode map entries directly into a
// map[string]any, bypassing reflect.Value containers for primitive value
// types. They mirror the typed deserMap*Block helpers above, save for
// reading into the native Go map. Selected at schema build time based
// on the avro value type.

func deserMapStringIfaceBlock(src []byte, m map[string]any, count int, sl *slab) ([]byte, error) {
	for range count {
		keyLen, rest, err := readVarlong(src)
		if err != nil {
			return nil, err
		}
		if keyLen < 0 || keyLen > int64(len(rest)) {
			return nil, fmt.Errorf("invalid map key length %d", keyLen)
		}
		kn := int(keyLen)
		key := sl.string(rest, kn)
		src = rest[kn:]

		valLen, rest2, err := readVarlong(src)
		if err != nil {
			return nil, err
		}
		if valLen < 0 || valLen > int64(len(rest2)) {
			return nil, &ShortBufferError{Type: "string", Need: int(valLen), Have: len(rest2)}
		}
		vn := int(valLen)
		m[key] = sl.string(rest2, vn)
		src = rest2[vn:]
	}
	return src, nil
}

func deserMapBooleanIfaceBlock(src []byte, m map[string]any, count int, sl *slab) ([]byte, error) {
	for range count {
		keyLen, rest, err := readVarlong(src)
		if err != nil {
			return nil, err
		}
		if keyLen < 0 || keyLen > int64(len(rest)) {
			return nil, fmt.Errorf("invalid map key length %d", keyLen)
		}
		kn := int(keyLen)
		key := sl.string(rest, kn)
		src = rest[kn:]
		if len(src) < 1 {
			return nil, &ShortBufferError{Type: "boolean"}
		}
		m[key] = src[0] == 1
		src = src[1:]
	}
	return src, nil
}

func deserMapIntIfaceBlock(src []byte, m map[string]any, count int, sl *slab) ([]byte, error) {
	for range count {
		keyLen, rest, err := readVarlong(src)
		if err != nil {
			return nil, err
		}
		if keyLen < 0 || keyLen > int64(len(rest)) {
			return nil, fmt.Errorf("invalid map key length %d", keyLen)
		}
		kn := int(keyLen)
		key := sl.string(rest, kn)
		src = rest[kn:]

		var val int32
		val, src, err = readVarint(src)
		if err != nil {
			return nil, err
		}
		m[key] = val
	}
	return src, nil
}

func deserMapLongIfaceBlock(src []byte, m map[string]any, count int, sl *slab) ([]byte, error) {
	for range count {
		keyLen, rest, err := readVarlong(src)
		if err != nil {
			return nil, err
		}
		if keyLen < 0 || keyLen > int64(len(rest)) {
			return nil, fmt.Errorf("invalid map key length %d", keyLen)
		}
		kn := int(keyLen)
		key := sl.string(rest, kn)
		src = rest[kn:]

		var val int64
		val, src, err = readVarlong(src)
		if err != nil {
			return nil, err
		}
		m[key] = val
	}
	return src, nil
}

func deserMapFloatIfaceBlock(src []byte, m map[string]any, count int, sl *slab) ([]byte, error) {
	for range count {
		keyLen, rest, err := readVarlong(src)
		if err != nil {
			return nil, err
		}
		if keyLen < 0 || keyLen > int64(len(rest)) {
			return nil, fmt.Errorf("invalid map key length %d", keyLen)
		}
		kn := int(keyLen)
		key := sl.string(rest, kn)
		src = rest[kn:]

		var u uint32
		u, src, err = readUint32(src)
		if err != nil {
			return nil, err
		}
		m[key] = math.Float32frombits(u)
	}
	return src, nil
}

// The following deserIfaceFn implementations decode primitive values
// directly into Go `any`, skipping the reflect.Value wrapping that the
// generic deserfn would do. They are wired into record/map iface paths
// at schema-build time alongside the existing deserfn.

func deserBooleanIface(src []byte, sl *slab) (any, []byte, error) {
	if len(src) < 1 {
		return nil, nil, &ShortBufferError{Type: "boolean"}
	}
	return src[0] == 1, src[1:], nil
}

func deserIntIface(src []byte, sl *slab) (any, []byte, error) {
	v, src, err := readVarint(src)
	if err != nil {
		return nil, nil, err
	}
	return v, src, nil
}

func deserLongIface(src []byte, sl *slab) (any, []byte, error) {
	v, src, err := readVarlong(src)
	if err != nil {
		return nil, nil, err
	}
	return v, src, nil
}

func deserFloatIface(src []byte, sl *slab) (any, []byte, error) {
	u, src, err := readUint32(src)
	if err != nil {
		return nil, nil, err
	}
	return math.Float32frombits(u), src, nil
}

func deserDoubleIface(src []byte, sl *slab) (any, []byte, error) {
	u, src, err := readUint64(src)
	if err != nil {
		return nil, nil, err
	}
	return math.Float64frombits(u), src, nil
}

func deserStringIface(src []byte, sl *slab) (any, []byte, error) {
	n, src, err := readLength(src, "string")
	if err != nil {
		return nil, nil, err
	}
	return sl.string(src, n), src[n:], nil
}

// ifaceFnForPrimitive returns the iface-direct decoder for a plain
// primitive avro type, or nil for complex/logical/custom types whose
// deser dispatch can't be short-circuited.
func ifaceFnForPrimitive(meta *fieldMeta) deserIfaceFn {
	if meta == nil || meta.logical != "" || meta.hasCustomType {
		return nil
	}
	return ifaceFnForKind(meta.avroType)
}

// ifaceFnForKind returns the iface-direct decoder for an avro kind
// name, or nil if the kind isn't a plain primitive. Callers must verify
// no logical type / custom decoder applies before using the result.
func ifaceFnForKind(kind string) deserIfaceFn {
	switch kind {
	case "boolean":
		return deserBooleanIface
	case "int":
		return deserIntIface
	case "long":
		return deserLongIface
	case "float":
		return deserFloatIface
	case "double":
		return deserDoubleIface
	case "string":
		return deserStringIface
	}
	return nil
}

func deserMapDoubleIfaceBlock(src []byte, m map[string]any, count int, sl *slab) ([]byte, error) {
	for range count {
		keyLen, rest, err := readVarlong(src)
		if err != nil {
			return nil, err
		}
		if keyLen < 0 || keyLen > int64(len(rest)) {
			return nil, fmt.Errorf("invalid map key length %d", keyLen)
		}
		kn := int(keyLen)
		key := sl.string(rest, kn)
		src = rest[kn:]

		var u uint64
		u, src, err = readUint64(src)
		if err != nil {
			return nil, err
		}
		m[key] = math.Float64frombits(u)
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
	case v.Kind() == reflect.Interface:
		if err := setIface(v, reflect.ValueOf(b), "fixed"); err != nil {
			return nil, err
		}
	case isUUIDType(v.Type()):
		// Concrete [16]byte target: rv.Type() == v.Type() by
		// isUUIDType, so direct Set is safe (no interface check).
		v.Set(reflect.ValueOf(b))
	case v.Kind() == reflect.String:
		v.SetString(uuidToString(b))
	case v.Type().Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8:
		// Copy: SetBytes(src[:16]) would alias the caller's input buffer,
		// so a later overwrite of src would silently corrupt the decoded
		// value. The deserFixed slice path already does this; mirror it.
		buf := make([]byte, 16)
		copy(buf, src[:16])
		v.SetBytes(buf)
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
		return src[s.n:], setIface(v, reflect.ValueOf(b), "fixed")
	}
	t := v.Type()
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		b := make([]byte, s.n)
		copy(b, src[:s.n])
		v.Set(reflect.ValueOf(b))
		return src[s.n:], nil
	}
	if t.Kind() == reflect.String {
		// Mirror serSize's reflect.String arm: encoder accepts a
		// string of the right length and writes raw bytes; decoder
		// reads raw bytes and materializes them as a string. Same
		// shape as deserBytes's reflect.String arm.
		v.SetString(string(src[:s.n]))
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

// setFloatValue sets v to f, handling interface, float, integer (whole-number),
// and json.Number targets. bits is 32 or 64, the source width — used for
// interface assignment, the float32-overflow check, and json.Number formatting.
// Shared between natural float/double deser and float-promotion deserializers
// so target-set parity stays in lock-step (regression: promote*To{Float,Double}
// previously rejected integer + json.Number targets that deserFloat/deserDouble
// accepted).
func setFloatValue(v reflect.Value, f float64, avroType string, bits int) error {
	if v.Kind() == reflect.Interface {
		var rv reflect.Value
		if bits == 32 {
			rv = reflect.ValueOf(float32(f))
		} else {
			rv = reflect.ValueOf(f)
		}
		if v.Type().NumMethod() == 0 {
			v.Set(rv)
			return nil
		}
		if !rv.Type().AssignableTo(v.Type()) {
			return &SemanticError{GoType: v.Type(), AvroType: avroType}
		}
		v.Set(rv)
		return nil
	}
	if v.CanFloat() {
		if bits == 64 && v.Kind() == reflect.Float32 && finiteFloat32Overflows(f) {
			return &SemanticError{GoType: v.Type(), AvroType: avroType, Err: fmt.Errorf("value %g overflows float32", f)}
		}
		v.SetFloat(f)
		return nil
	}
	// Reverse of the appendAvroFloat32/64 CanInt/CanUint encode arms:
	// whole-number float values can be assigned to integer targets.
	if v.CanInt() || v.CanUint() {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return &SemanticError{GoType: v.Type(), AvroType: avroType, Err: fmt.Errorf("non-finite %g into integer target", f)}
		}
		if f != float64(int64(f)) {
			return &SemanticError{GoType: v.Type(), AvroType: avroType, Err: fmt.Errorf("non-whole %g into integer target", f)}
		}
		n := int64(f)
		if v.CanInt() {
			if v.OverflowInt(n) {
				return &SemanticError{GoType: v.Type(), AvroType: avroType, Err: fmt.Errorf("value %d overflows %s", n, v.Type())}
			}
			v.SetInt(n)
			return nil
		}
		if n < 0 || v.OverflowUint(uint64(n)) {
			return &SemanticError{GoType: v.Type(), AvroType: avroType, Err: fmt.Errorf("value %d overflows %s", n, v.Type())}
		}
		v.SetUint(uint64(n))
		return nil
	}
	if v.Type() == jsonNumberType {
		v.Set(reflect.ValueOf(json.Number(strconv.FormatFloat(f, 'g', -1, bits))))
		return nil
	}
	return &SemanticError{GoType: v.Type(), AvroType: avroType}
}

// setBytesValue sets v to b, handling []byte slice, fixed-length byte array,
// string, and (when applicable) empty/typed-interface targets. avroType is the
// declared wire type ("bytes" / "fixed") and only affects error tagging — the
// accepted target set is the same. Shared between natural deserBytes,
// promoteStringToBytes, and JSON assignBytes so all paths agree on which Go
// targets accept Avro bytes/fixed.
func setBytesValue(v reflect.Value, b []byte, avroType string) error {
	if v.Kind() == reflect.Interface {
		if v.Type().NumMethod() == 0 {
			v.Set(reflect.ValueOf(b))
			return nil
		}
		rv := reflect.ValueOf(b)
		if !rv.Type().AssignableTo(v.Type()) {
			return &SemanticError{GoType: v.Type(), AvroType: avroType}
		}
		v.Set(rv)
		return nil
	}
	switch v.Kind() {
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.Uint8 {
			return &SemanticError{GoType: v.Type(), AvroType: avroType}
		}
		v.SetBytes(b)
	case reflect.Array:
		if v.Type().Elem().Kind() != reflect.Uint8 {
			return &SemanticError{GoType: v.Type(), AvroType: avroType}
		}
		if v.Len() != len(b) {
			return &SemanticError{GoType: v.Type(), AvroType: avroType, Err: fmt.Errorf("cannot decode %d bytes into array of length %d", len(b), v.Len())}
		}
		reflect.Copy(v, reflect.ValueOf(b))
	case reflect.String:
		v.SetString(string(b))
	default:
		return &SemanticError{GoType: v.Type(), AvroType: avroType}
	}
	return nil
}

// setStringValue sets v to the string view of src[:n] (or to a fresh copy when
// the target borrows past the source buffer). Shared between natural
// deserString and promoteBytesToString. The slab is used for the interface and
// string-target paths to avoid an extra copy; the TextUnmarshaler and []byte
// arms allocate fresh storage so the target owns its bytes.
func setStringValue(v reflect.Value, src []byte, n int, sl *slab) error {
	if v.Kind() == reflect.Interface {
		s := sl.string(src, n)
		if v.Type().NumMethod() == 0 {
			v.Set(reflect.ValueOf(s))
			return nil
		}
		rv := reflect.ValueOf(s)
		if !rv.Type().AssignableTo(v.Type()) {
			return &SemanticError{GoType: v.Type(), AvroType: "string"}
		}
		v.Set(rv)
		return nil
	}
	if v.Kind() == reflect.String {
		v.SetString(sl.string(src, n))
		return nil
	}
	// TextUnmarshaler before []byte: named []byte subtypes like net.IP
	// should use their text parsing, not raw byte assignment.
	if v.CanAddr() && v.Addr().Type().Implements(textUnmarshalerType) {
		b := make([]byte, n)
		copy(b, src[:n])
		return v.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText(b)
	}
	if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		b := make([]byte, n)
		copy(b, src[:n])
		v.SetBytes(b)
		return nil
	}
	return &SemanticError{GoType: v.Type(), AvroType: "string"}
}

// setLongValue sets v to val, handling interface, int, and uint targets.
// Returns an error if val does not fit in v's Go type.
func setLongValue(v reflect.Value, val int64) error {
	if v.Kind() == reflect.Interface {
		if v.Type().NumMethod() == 0 {
			v.Set(reflect.ValueOf(val))
			return nil
		}
		rv := reflect.ValueOf(val)
		if !rv.Type().AssignableTo(v.Type()) {
			return &SemanticError{GoType: v.Type(), AvroType: "long"}
		}
		v.Set(rv)
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
	if v.CanFloat() {
		// Mirrors the documented whole-number-float-as-int encode
		// leniency: AppendEncode(float64(42), "long") succeeds, so
		// Decode("long" wire, *float64) must round-trip. Mantissa
		// bounds protect the lossless guarantee.
		precLimit := int64(1) << 53
		if v.Type().Bits() == 32 {
			precLimit = 1 << 24
		}
		if val < -precLimit || val > precLimit {
			return &SemanticError{GoType: v.Type(), AvroType: "long", Err: fmt.Errorf("value %d exceeds %s exact-precision range", val, v.Type())}
		}
		v.SetFloat(float64(val))
		return nil
	}
	if v.Type() == jsonNumberType {
		v.Set(reflect.ValueOf(json.Number(strconv.FormatInt(val, 10))))
		return nil
	}
	return &SemanticError{GoType: v.Type(), AvroType: "long"}
}

// setIntValue sets v to val, handling interface, int, and uint targets.
// Returns an error if val does not fit in v's Go type.
func setIntValue(v reflect.Value, val int32) error {
	if v.Kind() == reflect.Interface {
		if v.Type().NumMethod() == 0 {
			v.Set(reflect.ValueOf(val))
			return nil
		}
		rv := reflect.ValueOf(val)
		if !rv.Type().AssignableTo(v.Type()) {
			return &SemanticError{GoType: v.Type(), AvroType: "int"}
		}
		v.Set(rv)
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
	if v.CanFloat() {
		// int32 always fits in float64; for float32 the int24 mantissa
		// boundary is enforced for symmetry with the encoder's
		// appendAvroFloat32 CanInt arm.
		if v.Type().Bits() == 32 && (val < -(1<<24) || val > 1<<24) {
			return &SemanticError{GoType: v.Type(), AvroType: "int", Err: fmt.Errorf("value %d exceeds float32 exact-precision range", val)}
		}
		v.SetFloat(float64(val))
		return nil
	}
	if v.Type() == jsonNumberType {
		v.Set(reflect.ValueOf(json.Number(strconv.FormatInt(int64(val), 10))))
		return nil
	}
	return &SemanticError{GoType: v.Type(), AvroType: "int"}
}

// deserTimeAsLong is the shared decoder for long-typed time logical
// types (timestamp-millis/micros/nanos). It accepts time.Time targets,
// empty/typed interfaces (subject to setIface's assignability check),
// and falls back to setLongValue for plain long targets. conv produces
// the time.Time from the wire-decoded long; conv must be total — the
// nanos variant's encode-side error path (timeToTimestampNanos overflow)
// has no decode-side analogue.
func deserTimeAsLong(src []byte, v reflect.Value, conv func(int64) time.Time) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	return src, setTimeAsLongTarget(indirectAlloc(v), val, conv)
}

// setTimeAsLongTarget applies the reader's logical-type conversion to a
// long-typed wire value already read. Shared between the natural
// long-time deserializers (deserTimeAsLong / deserDate's analogue) and
// the promotion path (int→long with a long-logical reader): factoring
// here means a promoted int→long timestamp-millis decode hits the
// SAME target-arm dispatch as a natural long+timestamp-millis decode.
func setTimeAsLongTarget(v reflect.Value, val int64, conv func(int64) time.Time) error {
	if v.Kind() == reflect.Interface {
		return setIface(v, reflect.ValueOf(conv(val)), "long")
	}
	if v.Type() == timeType {
		v.Set(reflect.ValueOf(conv(val)))
		return nil
	}
	if v.Kind() == reflect.String {
		v.SetString(conv(val).Format(time.RFC3339Nano))
		return nil
	}
	return setLongValue(v, val)
}

func deserTimestampMillis(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	return deserTimeAsLong(src, v, timestampMillisToTime)
}

func deserTimestampMicros(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	return deserTimeAsLong(src, v, timestampMicrosToTime)
}

func deserTimestampNanos(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	return deserTimeAsLong(src, v, timestampNanosToTime)
}

func deserDate(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	val, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		return src, setIface(v, reflect.ValueOf(dateToTime(val)), "int")
	}
	if v.Type() == timeType {
		v.Set(reflect.ValueOf(dateToTime(val)))
		return src, nil
	}
	// String target mirrors serDate's tryParseDateString leniency
	// (ser.go's date arm + JSON "int" date arm both accept a date
	// string on encode); the decoder emits ISO 8601 date-only.
	if v.Kind() == reflect.String {
		v.SetString(dateToTime(val).Format(time.DateOnly))
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
	if v.Kind() == reflect.Interface {
		return src, setIface(v, reflect.ValueOf(timeMillisToDuration(val)), "int")
	}
	if v.Type() == durationType {
		v.Set(reflect.ValueOf(timeMillisToDuration(val)))
		return src, nil
	}
	if v.Type() == timeType {
		// Mirrors serTimeMillis's timeType arm: encoder extracts
		// time-of-day fields, decoder materializes them at epoch UTC.
		v.Set(reflect.ValueOf(timeOfDayToTime(timeMillisToDuration(val))))
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
	// time.Duration / time.Time / *any targets get the converted value;
	// the overflow guard lives in timeMicrosToDuration so every caller
	// (binary, unsafe, JSON-any, JSON-typed) rejects out-of-range
	// uniformly. Plain integer targets bypass the conversion and
	// store the raw long.
	if v.Type() == durationType || v.Type() == timeType || v.Kind() == reflect.Interface {
		d, err := timeMicrosToDuration(val)
		if err != nil {
			return nil, err
		}
		switch {
		case v.Type() == durationType:
			v.Set(reflect.ValueOf(d))
		case v.Type() == timeType:
			// Mirrors serTimeMicros's timeType arm.
			v.Set(reflect.ValueOf(timeOfDayToTime(d)))
		default:
			return src, setIface(v, reflect.ValueOf(d), "long")
		}
		return src, nil
	}
	return src, setLongValue(v, val)
}

func deserDuration(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	if len(src) < 12 {
		return nil, &ShortBufferError{Type: "duration", Need: 12, Have: len(src)}
	}
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		return src[12:], setIface(v, reflect.ValueOf(DurationFromBytes(src[:12])), "fixed")
	}
	if v.Type() == avroDurationType {
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
	return setDecimalRat(v, bytesToRat(b, scale), scale)
}

// setDecimalRat is the rat-input variant of setDecimalValue. The
// binary callers always have bytes (which they convert through
// bytesToRat); the JSON path's bare-number form already has a parsed
// *big.Rat. Both paths share this helper so the supported target
// types — *big.Rat / big.Rat / json.Number / *float32 / *float64 /
// *string / interface{} — and the float overflow guards stay in
// lockstep across binary and JSON decode.
func setDecimalRat(v reflect.Value, r *big.Rat, scale int) (bool, error) {
	if v.Kind() == reflect.Interface {
		return true, setIface(v, reflect.ValueOf(r), "decimal")
	}
	if v.Type() == bigRatType {
		v.Set(reflect.ValueOf(*r))
		return true, nil
	}
	if v.Type() == jsonNumberType {
		v.Set(reflect.ValueOf(json.Number(r.FloatString(scale))))
		return true, nil
	}
	if v.CanFloat() {
		f, _ := r.Float64()
		// big.Rat.Float64 returns ±Inf when the rational is too large
		// for float64; reject rather than silently writing Inf.
		if math.IsInf(f, 0) {
			return true, &SemanticError{GoType: v.Type(), AvroType: "decimal", Err: fmt.Errorf("decimal value %s overflows %s", r.RatString(), v.Kind())}
		}
		if v.Kind() == reflect.Float32 && finiteFloat32Overflows(f) {
			return true, &SemanticError{GoType: v.Type(), AvroType: "decimal", Err: fmt.Errorf("value %g overflows float32", f)}
		}
		v.SetFloat(f)
		return true, nil
	}
	if v.Kind() == reflect.String {
		v.SetString(r.FloatString(scale))
		return true, nil
	}
	return false, nil
}

// assignBytesTarget materializes payload b into a []byte / [N]byte /
// string target, mirroring deserBytes's target-type dispatch. Shared
// fall-back for the decimal/big-decimal opaque-bytes pass-through —
// the ser side falls through to serBytes when the input isn't
// coercible, so the de side must accept the same target shapes when
// the structured-decode fails. Returns SemanticError naming the
// schema's logical avroType when no target matches.
func assignBytesTarget(v reflect.Value, b []byte, avroType string) ([]byte, error) {
	switch {
	case v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8:
		out := make([]byte, len(b))
		copy(out, b)
		v.SetBytes(out)
	case v.Kind() == reflect.String:
		v.SetString(string(b))
	case v.Kind() == reflect.Array && v.Type().Elem().Kind() == reflect.Uint8:
		if v.Len() != len(b) {
			return nil, &SemanticError{GoType: v.Type(), AvroType: avroType, Err: fmt.Errorf("cannot decode %d bytes into array of length %d", len(b), v.Len())}
		}
		reflect.Copy(v, reflect.ValueOf(b))
	default:
		return nil, &SemanticError{GoType: v.Type(), AvroType: avroType}
	}
	return nil, nil
}

type deserBytesDecimal struct {
	scale int
}

func (s *deserBytesDecimal) deser(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	n, src, err := readLength(src, "decimal")
	if err != nil {
		return nil, err
	}
	b := src[:n]
	src = src[n:]
	v = indirectAlloc(v)
	if ok, err := setDecimalValue(v, b, s.scale); ok {
		return src, err
	}
	// Opaque-bytes pass-through: mirrors serBytesDecimal's fall-through
	// to serBytes when the input isn't a coercible numeric type — see
	// also deserFixedDecimal below. Without this, an []byte/string
	// target encoded via the pass-through can't be decoded back.
	return assignBytesTarget(v, b, "decimal")
}

type deserBigDecimal struct{}

func (s *deserBigDecimal) deser(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	n, src, err := readLength(src, "big-decimal")
	if err != nil {
		return nil, err
	}
	payload := src[:n]
	src = src[n:]
	v = indirectAlloc(v)
	// Try the structured-rat decode first; if the target can't take a
	// *big.Rat (e.g. []byte/string for opaque pass-through, mirroring
	// serBigDecimal's serBytes fall-through), fall back to raw bytes.
	// For raw-byte targets the validity of the payload doesn't matter
	// (user is intentionally bypassing the framing), so the parse
	// error is only surfaced when a structured target is rejected.
	if r, displayScale, perr := parseBigDecimalPayload(payload); perr == nil {
		if ok, err := setDecimalRat(v, r, displayScale); ok {
			return src, err
		}
	} else if v.Kind() != reflect.Slice && v.Kind() != reflect.String && v.Kind() != reflect.Array {
		return nil, perr
	}
	return assignBytesTarget(v, payload, "big-decimal")
}

// parseBigDecimalPayload parses the big-decimal inner payload bytes
// (length-prefixed unscaled || zigzag scale) into a *big.Rat and a
// display scale for FloatString. Negative wire scale is accepted
// (value = unscaled * 10^|scale|); displayScale clamps to 0.
func parseBigDecimalPayload(payload []byte) (*big.Rat, int, error) {
	uLen, p, err := readLength(payload, "big-decimal unscaled")
	if err != nil {
		return nil, 0, err
	}
	uBytes := p[:uLen]
	p = p[uLen:]
	scale, p, err := readVarlong(p)
	if err != nil {
		return nil, 0, fmt.Errorf("big-decimal scale: %w", err)
	}
	if len(p) != 0 {
		return nil, 0, fmt.Errorf("big-decimal: %d trailing bytes after scale", len(p))
	}
	if scale > decimalScaleLimit || scale < -decimalScaleLimit {
		return nil, 0, fmt.Errorf("big-decimal scale %d exceeds %d limit", scale, decimalScaleLimit)
	}
	unscaled := bytesToBigInt(uBytes)
	r := new(big.Rat).SetInt(unscaled)
	if scale > 0 {
		denom := new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil)
		r.Quo(r, new(big.Rat).SetInt(denom))
	} else if scale < 0 {
		mult := new(big.Int).Exp(big.NewInt(10), big.NewInt(-scale), nil)
		r.Mul(r, new(big.Rat).SetInt(mult))
	}
	displayScale := int(scale)
	if displayScale < 0 {
		displayScale = 0
	}
	return r, displayScale, nil
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
//
// Negative scale is interpreted as `unscaled * 10^|scale|` (matching
// Java/avro-rs big-decimal semantics). |scale| is bounded by
// decimalScaleLimit; scales beyond produce a zero *big.Rat rather
// than allocating unbounded.
func RatFromBytes(b []byte, scale int) *big.Rat {
	return bytesToRat(b, scale)
}

func bytesToRat(b []byte, scale int) *big.Rat {
	unscaled := bytesToBigInt(b)
	if scale > decimalScaleLimit || scale < -decimalScaleLimit {
		// Public-API safety: bound the public RatFromBytes surface
		// against attacker-controlled scale. Internal callers pass
		// schema-validated non-negative scale already bounded by
		// validateLogical, so this guard only fires for direct
		// RatFromBytes use with hostile input.
		return new(big.Rat)
	}
	if scale < 0 {
		mult := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-scale)), nil)
		return new(big.Rat).SetFrac(new(big.Int).Mul(unscaled, mult), big.NewInt(1))
	}
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
// It is alloc-free: the string is reinterpreted as bytes for hex.Decode
// without copying, and hex.Decode only reads from src.
func parseUUID(s string) ([16]byte, error) {
	return parseUUIDBytes(unsafe.Slice(unsafe.StringData(s), len(s)))
}

// parseUUIDBytes parses an RFC 4122 hex-dash UUID byte slice into a [16]byte.
// hex.Decode does not retain or mutate src, so callers may pass a borrowed
// slice (e.g. directly from the wire buffer).
func parseUUIDBytes(s []byte) ([16]byte, error) {
	var u [16]byte
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return u, fmt.Errorf("invalid UUID %q", s)
	}
	if _, err := hex.Decode(u[0:4], s[0:8]); err != nil {
		return u, fmt.Errorf("invalid UUID %q: %w", s, err)
	}
	if _, err := hex.Decode(u[4:6], s[9:13]); err != nil {
		return u, fmt.Errorf("invalid UUID %q: %w", s, err)
	}
	if _, err := hex.Decode(u[6:8], s[14:18]); err != nil {
		return u, fmt.Errorf("invalid UUID %q: %w", s, err)
	}
	if _, err := hex.Decode(u[8:10], s[19:23]); err != nil {
		return u, fmt.Errorf("invalid UUID %q: %w", s, err)
	}
	if _, err := hex.Decode(u[10:16], s[24:36]); err != nil {
		return u, fmt.Errorf("invalid UUID %q: %w", s, err)
	}
	return u, nil
}

func deserUUID(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	n, src, err := readLength(src, "string")
	if err != nil {
		return nil, err
	}
	v = indirectAlloc(v)
	if isUUIDType(v.Type()) {
		u, err := parseUUIDBytes(src[:n])
		if err != nil {
			return nil, err
		}
		reflect.Copy(v, reflect.ValueOf(u))
		return src[n:], nil
	}
	if v.Kind() == reflect.Interface {
		return src[n:], setIface(v, reflect.ValueOf(sl.string(src, n)), "string")
	}
	if v.Kind() == reflect.String {
		v.SetString(sl.string(src, n))
		return src[n:], nil
	}
	if v.CanAddr() && v.Addr().Type().Implements(textUnmarshalerType) {
		b := make([]byte, n)
		copy(b, src[:n])
		if err := v.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText(b); err != nil {
			return nil, err
		}
		return src[n:], nil
	}
	// []byte target: mirror deserString's Slice byte arm. UUID-on-
	// string is wire-equivalent to plain string, and serUUID falls
	// through to serString (which accepts []byte source); the decode
	// side needs the symmetric Slice byte arm for round-trip parity.
	// TextUnmarshaler is checked first per deserString's precedence.
	if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		b := make([]byte, n)
		copy(b, src[:n])
		v.SetBytes(b)
		return src[n:], nil
	}
	return nil, &SemanticError{GoType: v.Type(), AvroType: "string"}
}
