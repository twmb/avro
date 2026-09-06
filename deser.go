package avro

import (
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
	"time"
	"unsafe"
)

type deserfn func(src []byte, v reflect.Value, sl *slab) ([]byte, error)

// readLength reads a length-prefixed varlong, validates that it is
// non-negative and fits in the remaining buffer, then returns the narrowed
// length and the advanced buffer. typeName populates ShortBufferError.Type,
// so keep it short. We compare in int64 to keep the bound correct on 32-bit.
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

// readBlockHeader reads an Avro array/map block header: a varlong count,
// plus a varlong byte size when the count is negative. It returns
// (count, byteSize, rest, false, nil) when a block follows, (0, 0, rest,
// true, nil) at the terminator, and an error for a malformed header.
// byteSize is 0 unless the wire used negative-count framing; skip paths use
// it to skip the block, and validateByteSize bounds it against len(src) for
// them.
func readBlockHeader(src []byte, blockType string, validateByteSize bool) (absCount int64, byteSize int64, _ []byte, end bool, err error) {
	count, src, err := readVarlong(src)
	if err != nil {
		return 0, 0, nil, false, err
	}
	if count == 0 {
		return 0, 0, src, true, nil
	}
	if count < 0 {
		count = -count
		if count < 0 {
			return 0, 0, nil, false, fmt.Errorf("invalid %s block count", blockType)
		}
		byteSize, src, err = readVarlong(src)
		if err != nil {
			return 0, 0, nil, false, err
		}
		if validateByteSize {
			if byteSize < 0 || byteSize > int64(len(src)) {
				return 0, 0, nil, false, &ShortBufferError{Type: blockType, Need: int(byteSize), Have: len(src)}
			}
		}
	}
	return count, byteSize, src, false, nil
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
	skipUnknown     bool
	// alias replaces carving with pointing: string and bytes return a view of
	// the decode input instead of a copy of it. See [AliasInput].
	alias bool
	// customMatches counts CustomType decoders that *matched* (returned a
	// result rather than ErrSkipCustomType) during a decode. A custom-decoder
	// wrapper saves it before probing and compares after. An unchanged count
	// means no custom matched anywhere in the probed subtree, so the all-skip
	// re-decode can bypass the chain for a single O(subtree) pass. See
	// wrapDeserWithCustomDecoders.
	customMatches int
	// bypassCustom, when set, makes a custom-decoder wrapper skip its probe and
	// chain and decode straight through its base deserializer. A no-match
	// all-skip re-decode sets it so nested wrappers don't re-probe, keeping the
	// re-decode O(subtree). Skipping the chain is faithful because no
	// custom matched in the subtree.
	bypassCustom bool
}

// slabSize is the slab batch: short decoded strings and byte slices are
// sub-allocated from one shared buffer to amortize allocation. This is
// perf-only, not a correctness or safety bound; a larger value batches more,
// a smaller one less.
const slabSize = 1024

// carve advances past what it returns: a handed-out region is never revisited,
// which is what makes string's unsafe.String safe across a pooled slab.
func (s *slab) carve(n int) []byte {
	if len(s.buf) < n {
		s.buf = make([]byte, max(slabSize, n))
	}
	b := s.buf[:n:n]
	s.buf = s.buf[n:]
	return b
}

// aliases reports whether decoded strings and byte slices may point into the
// decode input. Nil-safe for the arms that reach it off the slab-free path,
// where no option was ever parsed. See [AliasInput].
func (s *slab) aliases() bool { return s != nil && s.alias }

func (s *slab) string(src []byte, n int) string {
	if s.alias {
		return unsafe.String(unsafe.SliceData(src), n)
	}
	b := s.carve(n)
	copy(b, src[:n])
	return unsafe.String(unsafe.SliceData(b), n)
}

func (s *slab) bytes(src []byte, n int) []byte {
	if s == nil || n == 0 {
		// n == 0 ahead of the alias arm, and not only for the nil slab: empty
		// wire bytes must come back non-nil, and src[:0:0] over an exhausted
		// input is nil.
		b := make([]byte, n)
		copy(b, src[:n])
		return b
	}
	if s.alias {
		return src[:n:n]
	}
	b := s.carve(n)
	copy(b, src[:n])
	return b
}

var slabPool = sync.Pool{New: func() any { return &slab{} }}

// slabFreeKinds are the scalar leaf kinds whose desers never touch the slab
// for any logical type and any target: only string decodes use its string
// buffer, only recursive dispatch bumps its depth, and only custom-decoder
// wrappers write its remaining state. "string" is absent; "bytes" is safe
// because a string target of a bytes schema copies via string(b).
var slabFreeKinds = map[string]bool{
	"null":    true,
	"boolean": true,
	"int":     true,
	"long":    true,
	"float":   true,
	"double":  true,
	"bytes":   true,
	"fixed":   true,
	"enum":    true,
}

// put resets sl's per-call state and returns it to the pool. We keep buf
// so the next caller reuses its backing memory.
func (sl *slab) put() {
	sl.depth = 0
	sl.taggedUnions = false
	sl.tagLogicalTypes = false
	sl.skipUnknown = false
	sl.alias = false
	sl.customMatches = 0
	sl.bypassCustom = false
	slabPool.Put(sl)
}

// Decode reads Avro binary from src into v and returns the remaining bytes.
// v must be a non-nil pointer to a type compatible with the schema:
//
//   - null: any (always decodes to nil)
//   - boolean: bool, any
//   - int, long: int, int8-int64, uint8-uint64, any
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
// To produce JSON from decoded *any data use [Schema.EncodeJSON], not a generic
// JSON encoder: it is schema-aware and converts these types back to their Avro
// representations (time.Time to epoch integers, []byte to \uXXXX strings).
//
// Decode is liberal in what it accepts: we tolerate non-canonical input rather
// than rejecting it, such as a non-0/1 boolean byte that Java also reads as
// false. Encode is canonical, so such input round-trips to the canonical form.
func (s *Schema) Decode(src []byte, v any, opts ...Opt) ([]byte, error) {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return nil, errors.New("decode requires a non-nil pointer")
	}
	// Slab-free schemas (scalar leaves, no custom wiring) never touch the
	// slab, so we skip the pool entirely. A nil slab keeps scalar decodes
	// allocation-free even when GC has drained the pool. Opts only ever alter
	// slab state, and they are inert outside union and record paths, which are
	// never slab-free. So their mere presence takes the pooled path, keeping
	// the nil-slab proof trivial.
	if s.slabFree && len(opts) == 0 {
		return s.deser(src, rv.Elem(), nil)
	}
	sl := slabPool.Get().(*slab)
	if len(opts) > 0 {
		cfg := parseOpts(opts)
		sl.taggedUnions = cfg.tagged
		sl.tagLogicalTypes = cfg.tagLogical
		sl.skipUnknown = cfg.skipUnknown
		sl.alias = cfg.alias
	}
	rest, err := s.deser(src, rv.Elem(), sl)
	sl.put()
	return rest, err
}

///////////
// UNION //
///////////

type deserUnion struct {
	fns []deserfn
	// branchNames and logicalNames are the tags we emit, indexed by branch:
	// the standard name and the same name qualified by any logical type. Both
	// are always full length, a branch with no logical type or whose qualified
	// spelling another branch owns repeating its standard name, since
	// maybeWrap indexes without a length check.
	branchNames  []string
	logicalNames []string
	// noWrap disables maybeWrap. Set by resolveWriterUnion when the
	// reader is non-union: wrapping there would leak the writer's branch
	// name onto a target that has no union to dispatch through.
	// Default (zero value, false) is the natural-union and
	// resolveUnionUnion behavior: wrap when TaggedUnions is active
	// and the target is *any (or compatible interface).
	noWrap bool
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

// maybeWrap wraps a decoded union value with its branch name when TaggedUnions
// is enabled and the target is an interface type that map[string]any can be
// assigned to. In practice that means *any: a plain map does not satisfy any
// non-empty interface's method set. We skip non-interface targets and
// interfaces with methods.
func (s *deserUnion) maybeWrap(v reflect.Value, sl *slab, idx int32) {
	if s.noWrap || !sl.taggedUnions || v.Kind() != reflect.Interface || !v.Elem().IsValid() {
		return
	}
	// Skip if the wrapping map[string]any can't be assigned to v's
	// interface type. We use the cached type rather than building a throwaway
	// reflect.Value(map[string]any{}), which allocates per call.
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
// 0x00 = index 0 (null), 0x02 = index 1 (T). Canonically that is one byte,
// but we accept the non-canonical multi-byte spellings too; readNullUnionIndex
// below is what reads them.
func deserNullUnion(u *deserUnion) deserfn { return deserNullUnionAt(u, 1, 0, 2) }

func deserNullSecondUnion(u *deserUnion) deserfn { return deserNullUnionAt(u, 0, 2, 0) }

// readNullUnionIndex decodes the wire-encoded branch index in a 2-branch
// null-union. Avro encodes union indices as a zigzag varint, so a producer
// may emit a non-canonical multi-byte form like 0x80 0x00 for 0, which
// Java accepts; we fast-path the canonical byte and fall through to
// readVarint. The reflect and unsafe null-union paths share it.
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
		// The union is a schema node and costs one depth unit, as on every
		// other path; without the bump a ["null", Self] list would decode
		// deeper than it can encode.
		if sl.depth >= maxDepth {
			return nil, errTooDeep
		}
		sl.depth++
		defer func() { sl.depth-- }()
		isVal, src, err := readNullUnionIndex(src, valIdx, nullByte, valByte)
		if err != nil {
			return nil, err
		}
		if !isVal {
			v.Set(reflect.Zero(v.Type()))
			return src, nil
		}
		// We pass the un-indirected target to the branch fn and let it
		// indirect itself, as deserUnion.deser does, so a CustomType.Decode
		// returning a pointer goes into a *T target here as it does in a
		// 3-branch union.
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
		return src[1:], setIface(v, reflect.ValueOf(b), "boolean")
	}
	if v.Kind() != reflect.Bool {
		return nil, semErr(v, "boolean")
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

// setFloat32WireValue stores a 32-bit "float" wire value into v. A float32
// target takes the exact bit pattern, preserving signaling-NaN payloads as
// Java does; SetFloat would round-trip through float64 and quiet them.
// Other targets go through setFloatValue.
func setFloat32WireValue(v reflect.Value, u uint32) error {
	if v.Kind() == reflect.Float32 && v.CanAddr() {
		*(*uint32)(unsafe.Pointer(v.UnsafeAddr())) = u
		return nil
	}
	if v.Kind() == reflect.Interface {
		return setIface(v, reflect.ValueOf(math.Float32frombits(u)), "float")
	}
	return setFloatValue(v, float64(math.Float32frombits(u)), "float", 32)
}

func deserFloat(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	u, src, err := readUint32(src)
	if err != nil {
		return nil, err
	}
	return src, setFloat32WireValue(indirectAlloc(v), u)
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
	if err := setBytesValue(indirectAlloc(v), src[:n], "bytes", sl); err != nil {
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

// deserIfaceFn decodes a primitive Avro value and returns it as a Go `any`.
// The record/map/array paths use it for interface targets to skip the
// reflect.ValueOf alloc the generic deserfn pays boxing a primitive into a
// reflect.Value of interface kind. It is nil for complex types
// (record/array/map/union/logical-no-fast); the caller falls back to deserfn
// for those.
type deserIfaceFn func(src []byte, sl *slab) (any, []byte, error)

type deserRecordField struct {
	name       string
	nameVal    reflect.Value // pre-computed reflect.ValueOf(name); avoids alloc per map lookup
	fn         deserfn
	fnIface    deserIfaceFn // non-nil iff f.fn handles a primitive that benefits from iface-direct decode
	meta       *fieldMeta
	defaultVal any
	hasDefault bool
}

// avroType names the field's Avro type, or "" when the field carries no
// metadata to name it with. Decode twin of [serRecordField.avroType]; see
// that comment for why the type is asked rather than copied.
func (f *deserRecordField) avroType() string {
	if f.meta == nil {
		return ""
	}
	return f.meta.avroType
}

type deserRecord struct {
	fields []deserRecordField
	names  []string
	cache  sync.Map // map[mappingKey]*cachedMapping
	fast   sync.Map // map[mappingKey]*fastRecordDeser, per-Go-type compiled unsafe path
	// node and mbw back the per-field skippers [SkipUnknown] needs; see
	// fieldSkips. mbw is the parse's skip walk, shared by every record it
	// built.
	node     *schemaNode
	mbw      *minBytesWalk
	skipOnce sync.Once
	skips    []skipfn
}

// fieldSkips compiles one skipper per record field, once. A record that never
// decodes into a partial struct never compiles any: the schema chooses how many
// records exist, so this must stay off the unconditional build path.
func (s *deserRecord) fieldSkips() []skipfn {
	s.skipOnce.Do(func() {
		s.skips = make([]skipfn, len(s.fields))
		for i := range s.skips {
			// No fresh walk when mbw is nil: a per-record walk would multiply
			// the per-walk allowance by a record count the schema picks. Every
			// record built by a parse carries the parse's walk, so nil here is
			// a wiring bug, and refusing is the only sound answer.
			if s.mbw == nil || s.node == nil || i >= len(s.node.fields) || s.node.fields[i].node == nil {
				s.skips[i] = skipUnbuildable
				continue
			}
			s.skips[i] = buildSkip(s.node.fields[i].node, s.mbw)
		}
	})
	return s.skips
}

// Sibling of [serRecord.fastFor]; see that comment.
func (s *deserRecord) fastFor(t reflect.Type, skipUnknown bool) *fastRecordDeser {
	if v, ok := s.fast.Load(mappingKey{t, skipUnknown}); ok {
		return v.(*fastRecordDeser)
	}
	return nil
}

// Sibling of [serRecord.loadOrCompileFast]; see that comment.
func (s *deserRecord) loadOrCompileFast(t reflect.Type, skipUnknown bool) *fastRecordDeser {
	if fast := s.fastFor(t, skipUnknown); fast != nil {
		return fast
	}
	fast := compileFastDeser(s, t, skipUnknown)
	if fast == nil {
		return nil
	}
	actual, _ := s.fast.LoadOrStore(mappingKey{t, skipUnknown}, fast)
	return actual.(*fastRecordDeser)
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
			return nil, semErr(v, "record")
		}
		// Reuse the existing map[string]any if v already wraps one
		// (streaming decode, OCF reader, batch consumer reusing &out). We
		// do it here rather than in indirectAlloc because the unwrapped
		// non-addressable interface payload would break decoders that
		// v.Set(...) on the result. Here we only SetMapIndex, which works
		// on the non-addressable Map. See [reuseOrMakeStringAnyMap].
		m := reuseOrMakeStringAnyMap(v, len(s.fields))
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
		keyType := t.Key()
		if v.IsNil() {
			v.Set(reflect.MakeMapWithSize(t, len(s.fields)))
		}
		elem := reflect.New(t.Elem()).Elem()
		for _, f := range s.fields {
			if err := validateJSONNumberMapKey(f.name, keyType, "record"); err != nil {
				return nil, err
			}
			if src, err = f.fn(src, elem, sl); err != nil {
				return nil, recordFieldError(nil, f.name, err)
			}
			v.SetMapIndex(mapKeyAs(t, f.nameVal), elem)
			elem.SetZero()
		}
		return src, nil
	}
	if v.CanAddr() {
		if fast := s.loadOrCompileFast(t, sl.skipUnknown); fast != nil {
			return deserRecordFast(src, fast, v, sl)
		}
	}
	// compileFastDeser returned nil because typeFieldMapping failed; we
	// re-call to report the error.
	_, err = typeFieldMappingSkip(s.names, &s.cache, t, sl.skipUnknown)
	return nil, err
}

type deserEnum struct {
	symbols []string
}

// setEnumTarget assigns the (idx, symbol) pair to v per the enum target
// matrix: Interface->symbol-as-string; String->symbol; Int/Uint->ordinal;
// TextUnmarshaler->UnmarshalText(symbol). Shared by deserEnum (binary),
// resolveEnum (binary with symbol remap), and decodeEnum (JSON) so the
// target arms agree on overflow checks and SemanticError shapes.
func setEnumTarget(v reflect.Value, idx int, symbol string) error {
	if v.Kind() == reflect.Interface {
		return setIface(v, reflect.ValueOf(symbol), "enum")
	}
	// TextUnmarshaler before the String/int arms: an enum target receiving
	// a symbol name uses its text parsing, mirroring serEnum (which matches
	// by symbol name first). A plain int target with no UnmarshalText falls
	// through to the ordinal arm below. The implements-check gates the
	// []byte(symbol) allocation off the common string/int paths.
	if v.CanAddr() && v.Addr().Type().Implements(textUnmarshalerType) {
		_, err := tryTextUnmarshal(v, []byte(symbol))
		return err
	}
	switch {
	case v.Kind() == reflect.String:
		return setStringTarget(v, symbol, "enum")
	case v.CanInt():
		if v.OverflowInt(int64(idx)) {
			return &SemanticError{GoType: v.Type(), AvroType: "enum", Err: fmt.Errorf("ordinal %d overflows %s", idx, v.Type())}
		}
		v.SetInt(int64(idx))
		return nil
	case v.CanUint():
		if v.OverflowUint(uint64(idx)) {
			return &SemanticError{GoType: v.Type(), AvroType: "enum", Err: fmt.Errorf("ordinal %d overflows %s", idx, v.Type())}
		}
		v.SetUint(uint64(idx))
		return nil
	}
	return semErr(v, "enum")
}

func (s *deserEnum) deser(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	idx, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	if idx < 0 || int(idx) >= len(s.symbols) {
		return nil, fmt.Errorf("enum index %d out of range [0, %d)", idx, len(s.symbols))
	}
	return src, setEnumTarget(indirectAlloc(v), int(idx), s.symbols[idx])
}

type deserArray struct {
	deserItem deserfn
	fastLoop  func(src []byte, sliceVal reflect.Value, start, count int, sl *slab) ([]byte, error)
	// nativeLoop decodes one block straight into the concrete Go slice
	// (s[i]=v) when its dynamic type is exactly []V. Named slice/elem
	// types get handled=false and fall back to fastLoop.
	nativeLoop   func(sliceVal reflect.Value, src []byte, start, count int, sl *slab) (bool, []byte, error)
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

// checkArrayBlockBounds validates a block's count against the
// buffer-relative cap for non-zero-minimum items and the cumulative
// zero-byte-element cap. The zero-byte branch uses the pre-add form, since
// the post-add total wraps negative for a hostile count near MaxInt64.
// minItemBytes selects the rule, not only the magnitude, and neither rule
// is uniformly looser, so a per-item minimum may never be rounded up;
// minBytesUnknown is the third rule.
func checkArrayBlockBounds(count int64, totalItems int64, srcLen int, minItemBytes int) error {
	if minItemBytes == minBytesUnknown {
		// Unknown: admit the union of what both rules admit, so an
		// uncomputed minimum can only ever loosen. A valid wire satisfies
		// one of them: count <= srcLen/m for a true minimum m >= 1 (hence
		// count <= srcLen), or count <= the zero-byte cap when m is 0.
		// Their union false-rejects neither, and it still bounds the count
		// by the input rather than by the declared number.
		if lim := max(int64(srcLen), int64(maxZeroByteItems)-totalItems); count > lim {
			return fmt.Errorf("array block count %d exceeds remaining buffer length %d with an uncomputed per-item minimum", count, srcLen)
		}
		return nil
	}
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

// mapEntryMinBytes converts a map value's minimum into the per-entry minimum
// every map block bound uses, so no site can reach checkMapBlockBounds'
// divisor with an unknown. Unknown collapses to a 1-byte entry, which is
// exact: a map entry always carries its key's length varint.
func mapEntryMinBytes(valueMin int) int {
	if valueMin == minBytesUnknown {
		return 1
	}
	return 1 + valueMin
}

// checkMapBlockBounds bounds a map block's declared entry count against the
// remaining buffer. A map entry's key is at least 1 byte (minEntryBytes >= 1),
// so unlike arrays there is no zero-byte-item case. The bound is always
// buffer-relative and never false-rejects a valid block. Shared by deserMap and
// skipMap so the two block-bound checks cannot drift (the array path already
// factored its bound into checkArrayBlockBounds).
func checkMapBlockBounds(count int64, srcLen int, minEntryBytes int) error {
	if count > int64(srcLen)/int64(minEntryBytes) {
		return fmt.Errorf("map block count %d exceeds remaining buffer length %d (min %d byte/entry)", count, srcLen, minEntryBytes)
	}
	return nil
}

// decimalScaleLimit caps decimal/big-decimal scale and precision so a
// hostile scale cannot drive a 10^scale allocation. 10^(1<<16) is a ~27 KB
// big.Int. Java and avro-rs never eagerly evaluate 10^scale; we materialize
// the magnitude at decode time, so the cap has to live here.
const decimalScaleLimit = 1 << 16

// maxRatInputLen caps the byte length boundedRatFromString routes through
// big.Rat.SetString, which is quadratic in the input. Legitimate decimal
// input is bounded by decimalScaleLimit, so twice that magnitude covers
// every schema-conforming input.
const maxRatInputLen = 1 << 17 // 128 KiB

// maxDecimalUnscaledBytes caps a decimal unscaled value on decode, the
// orthogonal axis to decimalScaleLimit. Precision is parse-capped at
// decimalScaleLimit, so a value within the declared precision needs at most
// ~27 KiB. Past that, materializing the big.Int and base-converting is
// superlinear; Java, fastavro and avro-rs never base-convert.
const maxDecimalUnscaledBytes = 32 << 10

// checkDecimalUnscaledLen rejects an over-long decimal unscaled value before
// the big.Int materialization it would drive. Every decimal decode and
// encode path on both wire formats asks it, so encode rejects exactly the
// payloads decode rejects and a wire we produce is a wire we can read.
func checkDecimalUnscaledLen(b []byte) error {
	return checkDecimalUnscaledSize(len(b))
}

// checkDecimalUnscaledSize is checkDecimalUnscaledLen for a caller that knows
// the length before it has the bytes. That is a fixed carrier: its padded
// width is the schema's size, decidable without building the payload.
func checkDecimalUnscaledSize(n int) error {
	if n > maxDecimalUnscaledBytes {
		return fmt.Errorf("decimal unscaled value of %d bytes exceeds %d byte limit", n, maxDecimalUnscaledBytes)
	}
	return nil
}

// isJSONNumber reports whether s is a JSON number per RFC 8259. json.Valid
// validates the grammar; the checks around it reject whitespace padding and
// non-numeric JSON values. Only trailing whitespace needs its own test, since
// no whitespace byte can start a number. boundedRatFromString needs this
// gate because big.Rat.SetString accepts hex, binary, octal, underscores,
// rationals and hex floats, none of which is a JSON number.
func isJSONNumber(s string) bool {
	if len(s) == 0 {
		return false
	}
	if first := s[0]; first != '-' && (first < '0' || first > '9') {
		return false
	}
	switch s[len(s)-1] {
	case ' ', '\t', '\n', '\r':
		return false
	}
	// json.Valid is read-only; alias s's bytes to avoid the []byte(s) copy.
	// Mirrors the parseUUID/parseUUIDBytes unsafe-slice pattern.
	return json.Valid(unsafe.Slice(unsafe.StringData(s), len(s)))
}

// boundedRatFromString parses s into a *big.Rat, gating on isJSONNumber and
// rejecting decimal forms whose net exponent exceeds decimalScaleLimit,
// since SetString materializes 10^|exp| eagerly. It returns (rat, true, nil)
// on success, (nil, false, nil) when s is no number form at all so you may
// fall back to raw bytes, and (nil, false, err) when s is number-shaped but
// rejected. You must propagate that err, or hostile numeric-looking input
// re-encodes as raw bytes.
func boundedRatFromString(s string) (*big.Rat, bool, error) {
	if len(s) > maxRatInputLen {
		return nil, false, fmt.Errorf("decimal value exceeds %d byte length cap", maxRatInputLen)
	}
	if !isJSONNumber(s) {
		// Numeric-looking but JSON-invalid input ("0x10", "+5", ".5") errors
		// so the caller's typed path does not drop into the raw-bytes
		// fallback. The predicate is broader than JSON's number start: '+'
		// and '.' count as numeric-looking too.
		if len(s) > 0 {
			c := s[0]
			if c == '-' || c == '+' || c == '.' || (c >= '0' && c <= '9') {
				return nil, false, fmt.Errorf("invalid JSON number %q", truncForError(s))
			}
		}
		return nil, false, nil
	}
	netExp := int64(0)
	body := s
	if i := strings.IndexAny(body, "eE"); i >= 0 {
		exp, err := strconv.ParseInt(body[i+1:], 10, 64)
		if err != nil {
			// isJSONNumber already accepted the grammar, so ParseInt can fail
			// only on range. That is the "numeric but rejected" lane, not the
			// "non-numeric" lane you may fall back to raw bytes on.
			return nil, false, fmt.Errorf("decimal exponent %s overflows int64", truncForError(body[i+1:]))
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

// magnitudeWidestMultiplier is the largest constant factor this package
// applies to a single schema-declared magnitude: bits per byte, in
// maxDecimalDigits. maxSchemaMagnitude is chosen against it. A consumer
// needing a wider factor belongs here, lowering the ceiling for everyone.
const magnitudeWidestMultiplier = 8

// maxSchemaMagnitude is the one ceiling every schema-declared magnitude is
// saturated to before it enters arithmetic. A fixed size is the only
// parse-time quantity not bounded by the length of the text declaring it,
// so an unsaturated one is the one way arithmetic here leaves the int range.
// 1<<27 is the largest power of two that survives
// magnitudeWidestMultiplier inside a 32-bit int, and 128 MiB is far above
// any fixed anyone declares. Clipping an absurd one only loosens a
// buffer-relative block bound, so a count can fail at the element decode
// instead; both reject.
const maxSchemaMagnitude = 1 << 27

// saturateSchemaMagnitude clamps a schema-declared magnitude into
// [0, maxSchemaMagnitude] so arithmetic on the result cannot wrap. It is
// total, so callers need not know whether theirs was validated upstream. It
// bounds arithmetic range, not allocation size: a magnitude that becomes a
// make length needs its own far tighter bound at the allocating site.
func saturateSchemaMagnitude(n int) int {
	if n < 0 {
		return 0
	}
	if n > maxSchemaMagnitude {
		return maxSchemaMagnitude
	}
	return n
}

// schemaMinBytes returns the minimum number of wire bytes required to encode
// one value of node's type, used at decode time to bound block counts. The
// result is always in [0, maxSchemaMagnitude]: three callers compute 1 plus
// it and one divides by it, so a wrapped or negative return would crash or
// misclassify. This spins up a fresh walk for one node; computing minimums
// for several containers in one operation must share one walk
// (newMinBytesWalk), or the operation costs the container count times the
// per-walk allowance.
func schemaMinBytes(n *schemaNode) int {
	return newMinBytesWalk().minBytesOf(n)
}

// newMinBytesWalk returns a walk carrying a full allowance and an empty memo.
// One walk threads through all the container sites of a single operation,
// so maxMinBytesWork bounds the operation's total min-bytes work and a
// schema pointing N containers at one shared subtree pays once.
func newMinBytesWalk() *minBytesWalk {
	return &minBytesWalk{
		path:      make(map[*schemaNode]bool),
		done:      make(map[*schemaNode]int),
		allowance: maxMinBytesWork,
	}
}

// minBytesOf returns node n's minimum wire bytes, consuming this walk's shared
// memo and allowance so the cost joins that of every prior container computed
// on the same walk.
func (w *minBytesWalk) minBytesOf(n *schemaNode) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	v, _ := w.minBytes(n)
	return v
}

// maxMinBytesWork bounds the work one walk may perform, counted in children
// examined, across every container of an operation. The memo makes an
// acyclic graph linear however many paths reach a node, but a cyclic one
// cannot be memoized and mutually recursive levels fan out per reference;
// this is the backstop. It is charged per child, not per node entered,
// since a record's field count is a second author-picked magnitude.
// Exhausting it is sound: reaching the cap requires cyclic references, and
// the node asked about then costs at least one wire byte, so the stand-in
// is never above the true minimum. The value sits far above what an honest
// schema costs.
const maxMinBytesWork = 1 << 22

// minBytesUnknown is what the walk reports when it cannot compute a node's
// minimum: an unwired forward reference, or an exhausted allowance. It is a
// distinct rule rather than a number because checkArrayBlockBounds switches
// between two incomparable rules on whether the minimum is 0, so no numeric
// stand-in is safe in both directions. A reported minimum must be a sound
// lower bound on the true per-value wire size: under-reporting only loosens
// a bound, over-reporting changes which bound applies.
const minBytesUnknown = -1

// minBytesWalk carries the shared state of one operation's min-bytes work.
// A named type referenced twice binds both references to one *schemaNode,
// so the walk descends a DAG, and re-descending per reference is 2^depth
// work on a schema whose text grows linearly; the memo bounds it. The memo
// is separate from cycle detection, since a cycle mark must come off on the
// way back out while a memo entry must survive. A back-edge returns a
// conservative stand-in, so any result computed through one is a property
// of the path rather than the node and must not be remembered; the exact
// condition is that n's subtree is entirely cycle-free, which minBytes
// reports alongside the value. A weaker condition is wrong in a way no cost
// test can see, since a wrong memo is faster, so a parity check against an
// unmemoized walk settles it.
type minBytesWalk struct {
	// mu guards the three fields below. The skip path's share of an operation
	// runs at decode time, inside a record's sync.Once, so two records of one
	// resolved schema can compile concurrently on one walk. It is uncontended
	// in steady state. Drain order is nondeterministic and need not be
	// otherwise: an exhausted allowance yields minBytesUnknown, so which record
	// drains the walk can only loosen a later bound.
	mu        sync.Mutex
	path      map[*schemaNode]bool // the nodes currently being computed
	done      map[*schemaNode]int  // results for nodes whose subtree has no cycle
	allowance int                  // children left to examine; see maxMinBytesWork
}

// take charges n children about to be examined, reporting false once the
// allowance cannot cover them. Mirrors walkBudget.takeNodes: an over-large
// request drives the allowance to zero rather than partially spending it, so
// exhaustion is permanent and every later entry takes the stand-in.
func (w *minBytesWalk) take(n int) bool {
	if n > w.allowance {
		w.allowance = 0
		return false
	}
	w.allowance -= n
	return true
}

// minBytesChildren is how many children entering n will examine: the cost of
// one entry, and so what it is charged. We read it off the same fields
// minBytesFromChildren iterates, so the charge cannot drift from the work. An
// arm that grows a new child list has to be added to both or neither. The
// container arms are absent from both, since an array or a map
// answers with its own terminator byte and never descends into its element.
func minBytesChildren(n *schemaNode) int {
	switch n.kind {
	case "union":
		return len(n.branches)
	case "record":
		return len(n.fields)
	}
	return 0
}

// minBytes returns node n's minimum wire bytes and whether n's subtree is
// entirely free of cycles, the condition for remembering the result.
func (w *minBytesWalk) minBytes(n *schemaNode) (int, bool) {
	if n == nil {
		// An unwired forward reference during build. Its subtree is not
		// reachable yet, so its minimum is not merely expensive but unknown.
		// Unknown is the only sound answer: 0 and 1 select different
		// block-bound rules (see checkArrayBlockBounds), and this node's
		// true minimum may be either.
		return minBytesUnknown, false
	}
	if w.path[n] {
		// A cycle through n. The stand-in is 1 because every finite value
		// of n must somewhere decline to re-enter it, through a union's
		// branch index or a container's terminating block count, each at
		// least one byte of n's own encoding. The false travels back so
		// every enclosing node knows its result came through a back-edge
		// and must not be remembered.
		return 1, false
	}
	if v, ok := w.done[n]; ok {
		return v, true
	}
	// Charged before descending, and charged for the whole child list. The
	// loops below iterate a count the schema author chose, so charging one
	// unit for the entry would bound the number of entries and leave the work
	// each one does unbounded.
	if !w.take(1 + minBytesChildren(n)) {
		// Exhausted. Unknown, not a magnitude: exhaustion is permanent and
		// global to the walk, so every later node takes this arm, including
		// nodes with nothing cyclic about them, whose true minimum is 0. A
		// numeric stand-in here would be a claim about a node the walk never
		// looked at.
		return minBytesUnknown, false
	}
	w.path[n] = true
	v, acyclic := w.minBytesFromChildren(n)
	delete(w.path, n)
	if acyclic {
		w.done[n] = v
	}
	return v, acyclic
}

func (w *minBytesWalk) minBytesFromChildren(n *schemaNode) (int, bool) {
	switch n.kind {
	case "null":
		return 0, true
	case "boolean", "int", "long", "enum":
		return 1, true
	case "float":
		return 4, true
	case "double":
		return 8, true
	case "bytes", "string":
		return 1, true
	case "fixed":
		// The declared size is the one magnitude here that the schema text
		// names outright and the parser leaves unbounded. Every wrap below is
		// built from it, so we saturate it at the point it enters.
		return saturateSchemaMagnitude(n.size), true
	case "array", "map":
		return 1, true // empty-collection terminator is 1 byte
	case "union":
		// found, not a sentinel value: a branch minimum that happened to equal
		// the sentinel would otherwise read as "no branches at all" and report
		// the union as costing one byte.
		m, found := 0, false
		acyclic := true
		unknown := false
		for _, b := range n.branches {
			v, ba := w.minBytes(b)
			acyclic = acyclic && ba
			if v == minBytesUnknown {
				unknown = true
				continue
			}
			if !found || v < m {
				m, found = v, true
			}
		}
		if unknown {
			// A union never needs the unknown rule: whatever the unreadable
			// branch costs, it is at least 0, so the union is at least its own
			// branch-index varint. 1 is a sound lower bound and, unlike a
			// record's, it is positive, so it keeps the buffer-relative rule
			// that a union's guaranteed byte justifies.
			return 1, false
		}
		if !found {
			return 1, acyclic
		}
		return saturateSchemaMagnitude(1 + m), acyclic
	case "record":
		// Saturating the running sum, not just the result: every term is
		// already in range, so `s + term` cannot wrap, and the clamp keeps it
		// that way for the next field. A guard on the result alone would test
		// a value the wrap already destroyed.
		var s int
		acyclic := true
		unknown := false
		for i := range n.fields {
			v, fa := w.minBytes(n.fields[i].node)
			acyclic = acyclic && fa
			if v == minBytesUnknown {
				// Keep summing the readable fields: their total is still a
				// sound lower bound on the record, since the unknown ones
				// contribute at least 0.
				unknown = true
				continue
			}
			s = saturateSchemaMagnitude(s + v)
			if s == maxSchemaMagnitude {
				// Already at the ceiling from the readable fields alone, so
				// the ceiling is a sound lower bound whatever the rest cost.
				return maxSchemaMagnitude, acyclic && !unknown
			}
		}
		if unknown && s == 0 {
			// Nothing readable to stand on. The record's true minimum may be
			// 0 or positive, and those select different rules, so neither can
			// be guessed.
			return minBytesUnknown, false
		}
		if unknown {
			// The readable fields alone guarantee s bytes; the unknown ones
			// only add. A positive lower bound is all the buffer-relative
			// rule needs.
			return s, false
		}
		return s, acyclic
	}
	return 1, true
}

// fastPathSafeForElem reports whether a primitive fast loop with expected
// kind fastElemKind is safe for elements of type elemType. The string fast
// loops bypass setStringValue, so they share stringFastPathEligibleDecode
// with the unsafe struct gates. Evaluated once per decode call.
func fastPathSafeForElem(elemType reflect.Type, fastElemKind reflect.Kind) bool {
	if elemType.Kind() != fastElemKind {
		return false
	}
	if fastElemKind == reflect.String {
		return stringFastPathEligibleDecode(elemType)
	}
	return true
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
		return nil, semErr(v, "array")
	}
	// Fixed-size Go arrays: we decode straight into the array elements and
	// verify the wire holds exactly that many.
	if fixedArray {
		return s.deserFixedArray(src, v, sl)
	}
	// For interface targets we build a []any. We populate sliceVal lazily so
	// the first block's count can serve as a capacity hint, avoiding a
	// MakeSlice+reflect.Copy on the typical single-block path.
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
	// For primitive item types with matching Go element types we use a
	// specialized loop that avoids per-element function pointer calls.
	// fastPathSafeForElem screens both the Kind match and the json.Number
	// guard-bypass case; see its docstring.
	useFast := !iface && s.fastLoop != nil && fastPathSafeForElem(sliceType.Elem(), s.fastElemKind)
	// Native concrete path: write straight into []V. The unnamed-slice
	// assertion in nativeLoop returns handled=false for named slice/elem
	// types, which fall back to fastLoop.
	useFastNative := useFast && s.nativeLoop != nil
	// For interface targets with primitive avro items, use the iface
	// fast loop that operates directly on []any.
	useFastIface := iface && s.fastIfaceLoop != nil
	// Avro arrays are encoded as a series of blocks. Each block starts
	// with a count. Positive means N elements follow, zero means end of
	// array. Negative means |N| elements follow, and the next varint is
	// the block's byte size, for skipping without decoding.
	var err error
	var totalItems int64
	for {
		count, _, rest, end, headerErr := readBlockHeader(src, "array", false)
		if headerErr != nil {
			return nil, headerErr
		}
		src = rest
		if end {
			if iface {
				if !sliceVal.IsValid() {
					sliceVal = reflect.MakeSlice(sliceType, 0, 0)
				}
				return src, setIface(v, sliceVal, "array")
			}
			// An empty Avro array decodes to a non-nil empty slice, as on
			// every other decode path, so you can tell it from an absent
			// value.
			if v.IsNil() {
				v.Set(reflect.MakeSlice(sliceType, 0, 0))
			}
			return src, nil
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
			// would panic. Re-slice instead, on the same backing memory.
			sliceVal = sliceVal.Slice(0, newLen)
		default:
			sliceVal.SetLen(newLen)
		}
		if useFastNative {
			handled, rest, nerr := s.nativeLoop(sliceVal, src, start, n, sl)
			if nerr != nil {
				return nil, nerr
			}
			if handled {
				src = rest
				continue
			}
			useFastNative = false // named slice/elem type: fall back to fastLoop henceforth
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
			// We batch-allocate backing for nil slots only and reuse any
			// non-nil retained pointer, so an element aliased from a prior
			// decode is updated in place. That matches the unsafe struct-field
			// path (udArrayPtrRecord) and the documented pointer-reuse
			// contract. On a freshly grown slice the new slots are nil, so
			// they all get fresh backing.
			var need int
			for i := range n {
				if sliceVal.Index(start + i).IsNil() {
					need++
				}
			}
			if need > 0 {
				backing := reflect.MakeSlice(reflect.SliceOf(innerType), need, need)
				j := 0
				for i := range n {
					slot := sliceVal.Index(start + i)
					if slot.IsNil() {
						slot.Set(backing.Index(j).Addr())
						j++
					}
				}
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

func (s *deserArray) deserFixedArray(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	arrLen := v.Len()
	idx := 0
	var err error
	for {
		count, _, rest, end, headerErr := readBlockHeader(src, "array", false)
		if headerErr != nil {
			return nil, headerErr
		}
		src = rest
		if end {
			if idx != arrLen {
				return nil, &SemanticError{GoType: v.Type(), AvroType: "array", Err: fmt.Errorf("expected %d elements, got %d", arrLen, idx)}
			}
			return src, nil
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

// deserArrayLoop builds a per-primitive fast loop for typed slice targets.
// readOne reads one wire element from src; set stores it into the slice slot
// (sliceVal.Index(i)). One helper replaces six near-identical loops
// (String/Boolean/Int/Long/Float/Double), and we pre-bind the package vars
// below so schema build pays no allocation per array.
func deserArrayLoop[T any](readOne func(src []byte, sl *slab) (T, []byte, error), set func(reflect.Value, T)) func(src []byte, sliceVal reflect.Value, start, count int, sl *slab) ([]byte, error) {
	return func(src []byte, sliceVal reflect.Value, start, count int, sl *slab) ([]byte, error) {
		for i := start; i < start+count; i++ {
			v, rest, err := readOne(src, sl)
			if err != nil {
				return nil, err
			}
			set(sliceVal.Index(i), v)
			src = rest
		}
		return src, nil
	}
}

func deserArrayIfaceLoop[T any](readOne func(src []byte, sl *slab) (T, []byte, error)) func(src []byte, slice []any, start, count int, sl *slab) ([]byte, error) {
	return func(src []byte, slice []any, start, count int, sl *slab) ([]byte, error) {
		for i := start; i < start+count; i++ {
			v, rest, err := readOne(src, sl)
			if err != nil {
				return nil, err
			}
			slice[i] = v
			src = rest
		}
		return src, nil
	}
}

// Per-primitive readOne functions feed both the typed-slice and iface loops.
// Each checks its own bounds, so a caller needs no guard of its own.
func readOneString(src []byte, sl *slab) (string, []byte, error) {
	n, src, err := readLength(src, "string")
	if err != nil {
		return "", nil, err
	}
	return sl.string(src, n), src[n:], nil
}

func readOneBool(src []byte, _ *slab) (bool, []byte, error) {
	if len(src) < 1 {
		return false, nil, &ShortBufferError{Type: "boolean"}
	}
	return src[0] == 1, src[1:], nil
}

func readOneInt(src []byte, _ *slab) (int32, []byte, error) {
	return readVarint(src)
}

func readOneLong(src []byte, _ *slab) (int64, []byte, error) {
	return readVarlong(src)
}

func readOneFloat(src []byte, _ *slab) (float32, []byte, error) {
	u, src, err := readUint32(src)
	if err != nil {
		return 0, nil, err
	}
	return math.Float32frombits(u), src, nil
}

func readOneDouble(src []byte, _ *slab) (float64, []byte, error) {
	u, src, err := readUint64(src)
	if err != nil {
		return 0, nil, err
	}
	return math.Float64frombits(u), src, nil
}

var (
	deserArrayStringLoop  = deserArrayLoop(readOneString, reflect.Value.SetString)
	deserArrayBooleanLoop = deserArrayLoop(readOneBool, reflect.Value.SetBool)
	deserArrayIntLoop     = deserArrayLoop(readOneInt, func(v reflect.Value, x int32) { v.SetInt(int64(x)) })
	deserArrayLongLoop    = deserArrayLoop(readOneLong, reflect.Value.SetInt)
	deserArrayFloatLoop   = deserArrayLoop(readOneFloat, func(v reflect.Value, x float32) { *(*uint32)(unsafe.Pointer(v.UnsafeAddr())) = math.Float32bits(x) })
	deserArrayDoubleLoop  = deserArrayLoop(readOneDouble, reflect.Value.SetFloat)

	deserArrayStringIfaceLoop  = deserArrayIfaceLoop(readOneString)
	deserArrayBooleanIfaceLoop = deserArrayIfaceLoop(readOneBool)
	deserArrayIntIfaceLoop     = deserArrayIfaceLoop(readOneInt)
	deserArrayLongIfaceLoop    = deserArrayIfaceLoop(readOneLong)
	deserArrayFloatIfaceLoop   = deserArrayIfaceLoop(readOneFloat)
	deserArrayDoubleIfaceLoop  = deserArrayIfaceLoop(readOneDouble)
)

type deserMap struct {
	deserItem deserfn
	fastBlock func(src []byte, mapVal, keyVal, elemVal reflect.Value, count int, sl *slab) ([]byte, error)
	// nativeBlock decodes one block straight into the concrete Go map
	// (m[k]=v) when its dynamic type is exactly map[string]V. Named
	// map types get handled=false and fall back to fastBlock.
	nativeBlock  func(mapVal reflect.Value, src []byte, count int, sl *slab) (bool, []byte, error)
	fastElemKind reflect.Kind
	// fastIfaceVal decodes one primitive value directly into a Go any,
	// bypassing reflect; deserMapPrimitiveIfaceBlock drives the per-block
	// loop. nil for non-primitive value types; the generic reflect path
	// handles those.
	fastIfaceVal deserIfaceFn
	// minEntryBytes is 1 (key length varint, >=1 byte for empty key)
	// + schemaMinBytes(values). Used to bound block-count against
	// remaining buffer length, preventing the same memory-amplification
	// DoS shape that the array path's minItemBytes guards against.
	minEntryBytes int
}

// maxMapPreAllocSize caps the size hint passed to reflect.MakeMapWithSize so
// an attacker-controlled block count cannot drive bucket overhead
// proportional to the wire count. Larger maps still grow dynamically.
const maxMapPreAllocSize = 4 << 10

func (s *deserMap) deser(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	if sl.depth >= maxDepth {
		return nil, errTooDeep
	}
	sl.depth++
	defer func() { sl.depth-- }()
	v = indirectAlloc(v)
	iface := v.Kind() == reflect.Interface
	// We populate mapVal lazily so we can size the map with the first block's
	// count as a hint, avoiding a rehash on the typical single-block path. For
	// non-iface maps you may pass a pre-populated map; we merge into it as-is
	// and skip the hint.
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
	// Primitive value types with matching Go element types reuse reflect
	// containers to avoid per-entry allocations. A json.Number map key needs
	// per-key validation the fast loops cannot perform, so it takes the slow
	// path.
	useFast := !iface && s.fastBlock != nil && fastPathSafeForElem(elemTyp, s.fastElemKind) && mapTyp.Key() != jsonNumberType
	// Native concrete path: exact string key (so the unnamed map[string]V
	// assertion can succeed) on top of the useFast eligibility. A named map
	// or named key type returns handled=false and falls back to fastBlock.
	useFastNative := useFast && s.nativeBlock != nil && mapTyp.Key() == stringType
	// For interface targets with primitive avro values, use the
	// iface-block fast path that operates directly on map[string]any.
	useFastIface := iface && s.fastIfaceVal != nil
	// We pre-allocate reusable key and elem containers to avoid per-entry
	// reflect.ValueOf / reflect.New allocations. keyVal gets your actual map
	// key type (e.g. `type UserID string`); reusing this Value for SetMapIndex
	// avoids the panic that plain reflect.ValueOf(s) would fire when the map's
	// key is a named string subtype.
	var keyVal, elemVal reflect.Value
	if !useFastIface {
		keyVal = reflect.New(mapTyp.Key()).Elem()
		elemVal = reflect.New(elemTyp).Elem()
	}
	var err error
	for {
		count, _, rest, end, headerErr := readBlockHeader(src, "map", false)
		if headerErr != nil {
			return nil, headerErr
		}
		src = rest
		if end {
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
		if err := checkMapBlockBounds(count, len(src), s.minEntryBytes); err != nil {
			return nil, err
		}
		// We lazy-allocate on the first block using its count as a size
		// hint, capped to bound bucket-overhead amplification on hostile
		// input. The block-count bound above admits valid wire shapes
		// where many entries fit in few bytes (e.g. map<long> entries
		// at the 2-byte minimum); without the cap the size hint would
		// drive ~40x heap allocation per input byte for map[string]any.
		if !mapVal.IsValid() {
			hint := min(int(count), maxMapPreAllocSize)
			mapVal = reflect.MakeMapWithSize(mapTyp, hint)
			if !iface {
				v.Set(mapVal)
			}
		}
		if useFastNative {
			handled, rest, nerr := s.nativeBlock(mapVal, src, int(count), sl)
			if nerr != nil {
				return nil, nerr
			}
			if handled {
				src = rest
				continue
			}
			useFastNative = false // named map type: fall back to fastBlock henceforth
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
			src, err = deserMapPrimitiveIfaceBlock(src, m, int(count), sl, s.fastIfaceVal)
			if err != nil {
				return nil, err
			}
			continue
		}
		keyType := mapTyp.Key()
		for range int(count) {
			src, err = readMapKey(src, keyVal, sl)
			if err != nil {
				return nil, err
			}
			if err := validateJSONNumberMapKey(keyVal.String(), keyType, "map"); err != nil {
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

// readMapKey reads an Avro map key from src into keyVal. We call it once per
// map entry; the work inside (readVarlong, slab string copy) dominates the
// call overhead.
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

func readMapKeyString(src []byte, sl *slab) (string, []byte, error) {
	keyLen, src, err := readVarlong(src)
	if err != nil {
		return "", nil, err
	}
	if keyLen < 0 || keyLen > int64(len(src)) {
		return "", nil, fmt.Errorf("invalid map key length %d", keyLen)
	}
	n := int(keyLen)
	return sl.string(src, n), src[n:], nil
}

// deserMapBlock decodes one map block (count entries) for a primitive value
// type, filling mapVal via SetMapIndex with reusable keyVal/elemVal
// containers. readOne returns the next wire value of T, set assigns it to
// elemVal. One helper replaces six near-identical blocks
// (String/Boolean/Int/Long/Float/Double).
func deserMapBlock[T any](readOne func(src []byte, sl *slab) (T, []byte, error), set func(reflect.Value, T)) func(src []byte, mapVal, keyVal, elemVal reflect.Value, count int, sl *slab) ([]byte, error) {
	return func(src []byte, mapVal, keyVal, elemVal reflect.Value, count int, sl *slab) ([]byte, error) {
		var err error
		for range count {
			src, err = readMapKey(src, keyVal, sl)
			if err != nil {
				return nil, err
			}
			v, rest, err := readOne(src, sl)
			if err != nil {
				return nil, err
			}
			set(elemVal, v)
			src = rest
			mapVal.SetMapIndex(keyVal, elemVal)
		}
		return src, nil
	}
}

var (
	deserMapStringBlock  = deserMapBlock(readOneString, reflect.Value.SetString)
	deserMapBooleanBlock = deserMapBlock(readOneBool, reflect.Value.SetBool)
	deserMapIntBlock     = deserMapBlock(readOneInt, func(v reflect.Value, x int32) { v.SetInt(int64(x)) })
	deserMapLongBlock    = deserMapBlock(readOneLong, reflect.Value.SetInt)
	deserMapFloatBlock   = deserMapBlock(readOneFloat, func(v reflect.Value, x float32) { *(*uint32)(unsafe.Pointer(v.UnsafeAddr())) = math.Float32bits(x) })
	deserMapDoubleBlock  = deserMapBlock(readOneDouble, reflect.Value.SetFloat)
)

// Native concrete decode: write directly into the Go map or slice, bypassing
// reflect SetMapIndex and Index(i).Set. We select it at decode time when the
// container's dynamic type is the unnamed map[string]V or []V; a named type
// falls back to the reflect loop. The readOneX leaves are reused, so
// coercion is identical, float32 raw bits included.

func nativeMapBlockFor[V any](readOne func([]byte, *slab) (V, []byte, error)) func(reflect.Value, []byte, int, *slab) (bool, []byte, error) {
	return func(mapVal reflect.Value, src []byte, count int, sl *slab) (bool, []byte, error) {
		m, ok := mapVal.Interface().(map[string]V)
		if !ok {
			return false, src, nil
		}
		var err error
		for range count {
			var k string
			k, src, err = readMapKeyString(src, sl)
			if err != nil {
				return true, nil, err
			}
			var val V
			val, src, err = readOne(src, sl)
			if err != nil {
				return true, nil, err
			}
			m[k] = val
		}
		return true, src, nil
	}
}

func nativeArrayLoopFor[V any](readOne func([]byte, *slab) (V, []byte, error)) func(reflect.Value, []byte, int, int, *slab) (bool, []byte, error) {
	return func(sliceVal reflect.Value, src []byte, start, count int, sl *slab) (bool, []byte, error) {
		s, ok := sliceVal.Interface().([]V)
		if !ok {
			return false, src, nil
		}
		var err error
		for i := start; i < start+count; i++ {
			var val V
			val, src, err = readOne(src, sl)
			if err != nil {
				return true, nil, err
			}
			s[i] = val
		}
		return true, src, nil
	}
}

var (
	deserNativeMapStringBlock  = nativeMapBlockFor(readOneString)
	deserNativeMapBooleanBlock = nativeMapBlockFor(readOneBool)
	deserNativeMapIntBlock     = nativeMapBlockFor(readOneInt)
	deserNativeMapLongBlock    = nativeMapBlockFor(readOneLong)
	deserNativeMapFloatBlock   = nativeMapBlockFor(readOneFloat)
	deserNativeMapDoubleBlock  = nativeMapBlockFor(readOneDouble)

	deserNativeArrayStringLoop  = nativeArrayLoopFor(readOneString)
	deserNativeArrayBooleanLoop = nativeArrayLoopFor(readOneBool)
	deserNativeArrayIntLoop     = nativeArrayLoopFor(readOneInt)
	deserNativeArrayLongLoop    = nativeArrayLoopFor(readOneLong)
	deserNativeArrayFloatLoop   = nativeArrayLoopFor(readOneFloat)
	deserNativeArrayDoubleLoop  = nativeArrayLoopFor(readOneDouble)
)

// The following iface-block functions decode map entries directly into a
// map[string]any, bypassing reflect.Value containers for primitive value
// types. They mirror the typed deserMap*Block helpers above, save for
// reading into the native Go map. We select them at schema build time based
// on the avro value type.

func deserMapPrimitiveIfaceBlock(src []byte, m map[string]any, count int, sl *slab, decodeVal deserIfaceFn) ([]byte, error) {
	for range count {
		key, rest, err := readMapKeyString(src, sl)
		if err != nil {
			return nil, err
		}
		val, rest, err := decodeVal(rest, sl)
		if err != nil {
			return nil, err
		}
		m[key] = val
		src = rest
	}
	return src, nil
}

// deserIface adapts a typed readOne into the (any, []byte, error)
// deserIfaceFn shape: boxing T into any happens at the assignment, so each
// primitive does not need its own near-identical wrapper. Every readOne
// bounds-checks itself, so we add nothing here.
func deserIface[T any](readOne func(src []byte, sl *slab) (T, []byte, error)) deserIfaceFn {
	return func(src []byte, sl *slab) (any, []byte, error) {
		v, src, err := readOne(src, sl)
		if err != nil {
			return nil, nil, err
		}
		return v, src, nil
	}
}

var (
	deserBooleanIface = deserIface(readOneBool)
	deserIntIface     = deserIface(readOneInt)
	deserLongIface    = deserIface(readOneLong)
	deserFloatIface   = deserIface(readOneFloat)
	deserDoubleIface  = deserIface(readOneDouble)
	deserStringIface  = deserIface(readOneString)
)

func ifaceFnForPrimitive(meta *fieldMeta) deserIfaceFn {
	if meta == nil || meta.logical != "" || meta.hasCustomType {
		return nil
	}
	return ifaceFnForKind(meta.avroType)
}

// deserIfaceFnByKind maps an Avro primitive kind name to its iface-direct
// decoder. We build it from the deser*Iface vars above so a new primitive
// gets wired in one place only.
var deserIfaceFnByKind = map[string]deserIfaceFn{
	"boolean": deserBooleanIface,
	"int":     deserIntIface,
	"long":    deserLongIface,
	"float":   deserFloatIface,
	"double":  deserDoubleIface,
	"string":  deserStringIface,
}

// ifaceFnForKind returns the iface-direct decoder for an avro kind name, or
// nil if the kind isn't a plain primitive. You must verify no logical type or
// custom decoder applies before using the result.
func ifaceFnForKind(kind string) deserIfaceFn { return deserIfaceFnByKind[kind] }

// deserFixedUUIDReflect decodes a fixed(16) UUID. Into any it returns
// [16]byte; into [16]byte it copies the raw bytes; into string it
// formats a hex-dash UUID string; into []byte it falls back to raw bytes.
func deserFixedUUIDReflect(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	if err := needLen(src, 16, "uuid"); err != nil {
		return nil, err
	}
	b := [16]byte(src[:16])
	v = indirectAlloc(v)
	switch {
	case v.Kind() == reflect.Interface:
		if err := setIface(v, reflect.ValueOf(b), "fixed"); err != nil {
			return nil, err
		}
	case isUUIDType(v.Type()):
		// [16]byte-shaped target trusts the raw bytes: no interface check and
		// no UnmarshalText round trip; the bytes *are* the UUID. copyBytesToArray
		// (not Set(reflect.ValueOf(b))) so a named byte element ([16]B, type B
		// byte), Kind Uint8 but not assignable from [16]byte, does not panic.
		copyBytesToArray(v, b[:])
	case v.CanAddr() && v.Addr().Type().Implements(textUnmarshalerType):
		// TextUnmarshaler before the String / []byte arms, parity with the
		// string decoders and serFixedUUIDReflect's text-before-string-kind
		// order. We pass the canonical hex-dash form so the same Go type
		// can decode from either schema shape (fixed+uuid or string+uuid).
		if _, err := tryTextUnmarshal(v, []byte(uuidToString(b))); err != nil {
			return nil, err
		}
	case v.Kind() == reflect.String:
		if err := setStringTarget(v, uuidToString(b), "fixed"); err != nil {
			return nil, err
		}
	case v.Type().Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8:
		// Raw bytes, so the same rule as deserFixed's slice arm: alias only
		// under AliasInput. Without it, SetBytes(src[:16]) would leave the
		// decoded value at the mercy of a later overwrite of src.
		if sl.aliases() {
			v.SetBytes(sl.bytes(src, 16))
			break
		}
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
	if err := needLen(src, s.n, "fixed"); err != nil {
		return nil, err
	}
	v = indirectAlloc(v)
	if v.Kind() == reflect.Interface {
		return src[s.n:], setIface(v, reflect.ValueOf(sl.bytes(src, s.n)), "fixed")
	}
	t := v.Type()
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		// SetBytes, not Set(reflect.ValueOf(b)): a named byte-slice or
		// named-byte-element slice (type B byte; []B) has element Kind Uint8 but
		// is not assignable from []byte, so Set panics. SetBytes writes through
		// the Kind. Mirrors setBytesValue's Slice arm.
		v.SetBytes(sl.bytes(src, s.n))
		return src[s.n:], nil
	}
	if t.Kind() == reflect.String {
		// Mirror serSize's reflect.String arm: encoder accepts a
		// string of the right length and writes raw bytes; decoder
		// reads raw bytes and materializes them as a string. Same
		// shape as deserBytes's reflect.String arm, alias branch included.
		str := ""
		if sl.aliases() {
			str = sl.string(src, s.n)
		} else {
			str = string(src[:s.n])
		}
		if err := setStringTarget(v, str, "fixed"); err != nil {
			return nil, err
		}
		return src[s.n:], nil
	}
	if t.Kind() != reflect.Array || t.Elem().Kind() != reflect.Uint8 {
		return nil, &SemanticError{GoType: t, AvroType: "fixed"}
	}
	if t.Len() != s.n {
		return nil, &SemanticError{GoType: t, AvroType: "fixed"}
	}
	copyBytesToArray(v, src[:s.n])
	return src[s.n:], nil
}

///////////////////////////////
// LOGICAL TYPE DESERIALIZERS //
///////////////////////////////

// rejectJSONNumberStringTarget reports a SemanticError when v is a
// json.Number target receiving string-like wire content. json.Number's
// contract requires a valid number literal, and json.Marshal would reject
// the value later, far from the decode site. Mirrors appendAvroString's
// encode-side reject.
func rejectJSONNumberStringTarget(v reflect.Value, content, avroType string) error {
	if v.Type() != jsonNumberType {
		return nil
	}
	return &SemanticError{GoType: v.Type(), AvroType: avroType,
		Err: fmt.Errorf("string-like value %q has no JSON number representation", truncForError(content))}
}

// setStringTarget is the combined "guard + SetString" applied at every
// string-like-wire setter. Single entry point for the
// rejectJSONNumberStringTarget + SetString pair across all 12 setter
// sites, so a future setter can't accidentally call SetString without
// the guard. v.Kind() must be reflect.String.
func setStringTarget(v reflect.Value, s, avroType string) error {
	if err := rejectJSONNumberStringTarget(v, s, avroType); err != nil {
		return err
	}
	v.SetString(s)
	return nil
}

// validateJSONNumberMapKey is rejectJSONNumberStringTarget's key-axis
// variant: when keyType is json.Number, key must be a valid JSON number
// literal. It runs per key on both decode and encode, so every
// map[json.Number]V key that encodes decodes back. Avro field names never
// parse as numbers, so the record-as-map case fails on the first key.
func validateJSONNumberMapKey(key string, keyType reflect.Type, avroType string) error {
	if keyType != jsonNumberType {
		return nil
	}
	if !isJSONNumber(key) {
		return &SemanticError{GoType: keyType, AvroType: avroType,
			Err: fmt.Errorf("map key %q is not a valid JSON number literal", truncForError(key))}
	}
	return nil
}

// formatToStringKindTarget formats content into v if v is a string-kind
// target other than json.Number, whose contract arbitrary formatted content
// would violate. A json.Number target falls through (wrote=false) so the
// caller routes the numeric wire value through its integer or float arm.
func formatToStringKindTarget(v reflect.Value, content, avroType string) (wrote bool, err error) {
	if v.Kind() != reflect.String || v.Type() == jsonNumberType {
		return false, nil
	}
	return true, setStringTarget(v, content, avroType)
}

// setFloatValue sets v to f, handling interface, float, whole-number integer,
// and json.Number targets. bits is the source width, 32 or 64. Every
// float-emitting deserializer shares it, so the target set stays uniform. We
// reject non-finite floats for integer and json.Number targets, neither of
// which can hold them; decode into a typed float if you need them.
func setFloatValue(v reflect.Value, f float64, avroType string, bits int) error {
	if v.Kind() == reflect.Interface {
		var rv reflect.Value
		if bits == 32 {
			rv = reflect.ValueOf(float32(f))
		} else {
			rv = reflect.ValueOf(f)
		}
		return setIface(v, rv, avroType)
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
		if f != math.Trunc(f) {
			return &SemanticError{GoType: v.Type(), AvroType: avroType, Err: fmt.Errorf("non-whole %g into integer target", f)}
		}
		// Bound in float space before the integer conversion: Go's
		// float-to-integer conversion is implementation-defined on overflow,
		// and a round-trip check silently accepts 2^63 on saturating
		// platforms. Mirrors the encode-side floatFitsInt64.
		if v.CanInt() {
			if f < -(1<<63) || f >= (1<<63) {
				return &SemanticError{GoType: v.Type(), AvroType: avroType, Err: fmt.Errorf("value %g overflows %s", f, v.Type())}
			}
			n := int64(f)
			if v.OverflowInt(n) {
				return &SemanticError{GoType: v.Type(), AvroType: avroType, Err: fmt.Errorf("value %d overflows %s", n, v.Type())}
			}
			v.SetInt(n)
			return nil
		}
		// Unsigned target: uint64(f) is well-defined for f in [0, 2^64) on
		// every platform; the full uint64 range is supported.
		if f < 0 || f >= (1<<64) {
			return &SemanticError{GoType: v.Type(), AvroType: avroType, Err: fmt.Errorf("value %g overflows %s", f, v.Type())}
		}
		n := uint64(f)
		if v.OverflowUint(n) {
			return &SemanticError{GoType: v.Type(), AvroType: avroType, Err: fmt.Errorf("value %d overflows %s", n, v.Type())}
		}
		v.SetUint(n)
		return nil
	}
	if v.Type() == jsonNumberType {
		// Non-finite floats have no valid JSON number literal, which
		// json.Number's contract requires.
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return &SemanticError{GoType: v.Type(), AvroType: avroType, Err: fmt.Errorf("non-finite %g has no JSON number representation", f)}
		}
		v.Set(reflect.ValueOf(json.Number(strconv.FormatFloat(f, 'g', -1, bits))))
		return nil
	}
	return &SemanticError{GoType: v.Type(), AvroType: avroType}
}

// setBytesValue sets v to a fresh copy of b, handling interface, []byte,
// [N]byte, and string targets. b may alias the wire buffer, so the slice and
// interface arms allocate owned storage. Every bytes and fixed decode path
// shares it, so all agree on the accepted targets and the never-alias rule.
func setBytesValue(v reflect.Value, b []byte, avroType string, sl *slab) error {
	switch v.Kind() {
	case reflect.Interface:
		// Empty wire bytes must come back as a non-nil empty []byte. A nil
		// result is nil-equivalent on re-encode, so the nil-first union
		// dispatch would flip a decoded {"bytes": ""} onto the null branch.
		return setIface(v, reflect.ValueOf(sl.bytes(b, len(b))), avroType)
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.Uint8 {
			return &SemanticError{GoType: v.Type(), AvroType: avroType}
		}
		v.SetBytes(sl.bytes(b, len(b)))
	case reflect.Array:
		if v.Type().Elem().Kind() != reflect.Uint8 {
			return &SemanticError{GoType: v.Type(), AvroType: avroType}
		}
		if v.Len() != len(b) {
			return &SemanticError{GoType: v.Type(), AvroType: avroType, Err: fmt.Errorf("cannot decode %d bytes into array of length %d", len(b), v.Len())}
		}
		copyBytesToArray(v, b)
	case reflect.String:
		// string(b) copies; the slab's string is the one that can alias. The
		// branch keeps this arm off the slab entirely without AliasInput, which
		// is what lets the bytes and fixed kinds decode with no slab at all.
		if sl.aliases() {
			return setStringTarget(v, sl.string(b, len(b)), avroType)
		}
		return setStringTarget(v, string(b), avroType)
	default:
		return &SemanticError{GoType: v.Type(), AvroType: avroType}
	}
	return nil
}

// setStringValue sets v to the string view of src[:n] (or to a fresh copy when
// the target borrows past the source buffer). Shared between natural
// deserString and promoteBytesToString. Every arm but TextUnmarshaler carves
// from the slab; that one allocates because it parses the bytes rather than
// keeping them.
func setStringValue(v reflect.Value, src []byte, n int, sl *slab) error {
	if v.Kind() == reflect.Interface {
		return setIface(v, reflect.ValueOf(sl.string(src, n)), "string")
	}
	// TextUnmarshaler wins over the string kind, as in appendAvroString and
	// encoding/json; this also covers named []byte types like net.IP. The
	// implements check gates the []byte allocation so the plain-string path
	// stays alloc-free. json.Number has no UnmarshalText and falls through
	// to setStringTarget's guard.
	if v.CanAddr() && v.Addr().Type().Implements(textUnmarshalerType) {
		b := make([]byte, n)
		copy(b, src[:n])
		_, err := tryTextUnmarshal(v, b)
		return err
	}
	if v.Kind() == reflect.String {
		return setStringTarget(v, sl.string(src, n), "string")
	}
	if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		v.SetBytes(sl.bytes(src, n))
		return nil
	}
	return semErr(v, "string")
}

// setIntegerWire stores the wire integer into v, handling interface, int,
// uint, float (whole-number with mantissa bound), and json.Number targets.
// avroType is "int" or "long" for the SemanticError tag. Shared body of
// setIntValue / setLongValue so the target-set dispatch and float-mantissa
// bound live in one place.
func setIntegerWire[T int32 | int64](v reflect.Value, val T, avroType string) error {
	if v.Kind() == reflect.Interface {
		return setIface(v, reflect.ValueOf(val), avroType)
	}
	v64 := int64(val)
	if v.CanInt() {
		if v.OverflowInt(v64) {
			return &SemanticError{GoType: v.Type(), AvroType: avroType, Err: fmt.Errorf("value %d overflows %s", val, v.Type())}
		}
		v.SetInt(v64)
		return nil
	}
	if v.CanUint() {
		if v64 < 0 || v.OverflowUint(uint64(v64)) {
			return &SemanticError{GoType: v.Type(), AvroType: avroType, Err: fmt.Errorf("value %d overflows %s", val, v.Type())}
		}
		v.SetUint(uint64(v64))
		return nil
	}
	if v.CanFloat() {
		// The natural decoder rejects a wire value the Go float target
		// cannot hold exactly. The resolved-promotion path IEEE-rounds
		// instead, because there the reader schema itself is float/double
		// and you opted into that precision at the schema layer.
		f, err := intFitsFloat(v64, v.Type().Bits())
		if err != nil {
			return &SemanticError{GoType: v.Type(), AvroType: avroType, Err: err}
		}
		v.SetFloat(f)
		return nil
	}
	if v.Type() == jsonNumberType {
		v.Set(reflect.ValueOf(json.Number(strconv.FormatInt(v64, 10))))
		return nil
	}
	return &SemanticError{GoType: v.Type(), AvroType: avroType}
}

func setLongValue(v reflect.Value, val int64) error { return setIntegerWire(v, val, "long") }
func setIntValue(v reflect.Value, val int32) error  { return setIntegerWire(v, val, "int") }

// deserTimeAsLong is the shared decoder for the long-typed timestamp
// logicals. It accepts time.Time and interface targets and falls back to
// setLongValue for plain long targets. conv must be total.
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
// the promotion path (int->long with a long-logical reader). Factoring
// here means a promoted int->long timestamp-millis decode hits the
// *same* target-arm dispatch as a natural long+timestamp-millis decode.
func setTimeAsLongTarget(v reflect.Value, val int64, conv func(int64) time.Time) error {
	if v.Kind() == reflect.Interface {
		return setIface(v, reflect.ValueOf(conv(val)), "long")
	}
	if v.Type() == timeType {
		v.Set(reflect.ValueOf(conv(val)))
		return nil
	}
	// A string target takes the formatted timestamp, as serTimeAsLong
	// accepts one on encode; json.Number falls through to the raw integer.
	if wrote, err := formatToStringKindTarget(v, conv(val).Format(time.RFC3339Nano), "long"); wrote {
		return err
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
	// string on encode); the decoder emits ISO 8601 date-only. json.Number
	// targets are excluded so they get the raw days-since-epoch as a JSON
	// number literal via setIntValue's json.Number arm.
	if wrote, err := formatToStringKindTarget(v, dateToTime(val).Format(time.DateOnly), "int"); wrote {
		if err != nil {
			return nil, err
		}
		return src, nil
	}
	return src, setIntValue(v, val)
}

// setTimeMillisTarget assigns an int32 time-millis wire value to v per
// the documented target matrix: Interface->Duration; durationType->Duration;
// timeType->time.Time at epoch UTC via timeOfDayToTime; integer fallback
// -> setIntValue. Shared by binary deserTimeMillis and JSON decodeInt's
// time-millis arm (and udTimeMillisTime for the time-target unsafe path).
func setTimeMillisTarget(v reflect.Value, val int32) error {
	if v.Kind() == reflect.Interface {
		return setIface(v, reflect.ValueOf(timeMillisToDuration(val)), "int")
	}
	if v.Type() == durationType {
		v.Set(reflect.ValueOf(timeMillisToDuration(val)))
		return nil
	}
	if v.Type() == timeType {
		v.Set(reflect.ValueOf(timeOfDayToTime(timeMillisToDuration(val))))
		return nil
	}
	return setIntValue(v, val)
}

// setTimeMicrosTarget mirrors setTimeMillisTarget for int64 time-micros.
// The overflow guard lives in timeMicrosToDuration so every caller
// (binary, unsafe, JSON-any, JSON-typed) rejects out-of-range uniformly.
func setTimeMicrosTarget(v reflect.Value, val int64) error {
	if v.Type() == durationType || v.Type() == timeType || v.Kind() == reflect.Interface {
		d, err := timeMicrosToDuration(val)
		if err != nil {
			return err
		}
		switch {
		case v.Type() == durationType:
			v.Set(reflect.ValueOf(d))
		case v.Type() == timeType:
			v.Set(reflect.ValueOf(timeOfDayToTime(d)))
		default:
			return setIface(v, reflect.ValueOf(d), "long")
		}
		return nil
	}
	return setLongValue(v, val)
}

func deserTimeMillis(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	val, src, err := readVarint(src)
	if err != nil {
		return nil, err
	}
	return src, setTimeMillisTarget(indirectAlloc(v), val)
}

func deserTimeMicros(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	return src, setTimeMicrosTarget(indirectAlloc(v), val)
}

func deserDuration(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	if err := needLen(src, 12, "duration"); err != nil {
		return nil, err
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
	// Bound the unscaled length before bytesToRat materializes a big.Int and
	// setDecimalRat base-converts it (see maxDecimalUnscaledBytes). Returning
	// (true, err) makes the binary deserBytesDecimal / deserFixedDecimal and
	// JSON assignBytes callers return the error instead of falling through to
	// the opaque-bytes path.
	if err := checkDecimalUnscaledLen(b); err != nil {
		return true, err
	}
	return setDecimalRat(v, bytesToRat(b, scale), scale)
}

// setDecimalRat is the rat-input variant of setDecimalValue; the JSON
// bare-number form already has a parsed *big.Rat. Both share this so the
// target set and the float overflow guards stay the same on both wires.
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
		// big.Rat.Float64 returns +/-Inf when the rational is too large
		// for float64; reject rather than write Inf.
		if math.IsInf(f, 0) {
			return true, &SemanticError{GoType: v.Type(), AvroType: "decimal", Err: fmt.Errorf("decimal value %s overflows %s", truncRatForError(r), v.Kind())}
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
	// Opaque-bytes pass-through for a []byte target, mirroring
	// serBytesDecimal's fall-through to serBytes. A string target never gets
	// here: setDecimalRat's string arm always reads the wire as numeric decimal
	// text, and the encoder rejects a non-numeric string for a decimal. So
	// string is numeric-text-only and []byte the sole opaque carrier,
	// symmetric on both sides.
	return src, setBytesValue(v, b, "decimal", sl)
}

type deserBigDecimal struct{}

func (s *deserBigDecimal) deser(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	n, src, err := readLength(src, "big-decimal")
	if err != nil {
		return nil, err
	}
	if err := setBigDecimalTarget(v, src[:n], sl); err != nil {
		return nil, err
	}
	return src[n:], nil
}

// setBigDecimalTarget stores a big-decimal payload into v: the structured
// decode where a target takes it, else the raw bytes for an opaque
// pass-through target. The natural decode and the string-to-bytes
// promotion share it.
func setBigDecimalTarget(v reflect.Value, payload []byte, sl *slab) error {
	v = indirectAlloc(v)
	done, err := applyBigDecimalPayload(v, payload)
	if !done {
		err = setBytesValue(v, payload, "big-decimal", sl)
	}
	return err
}

// applyBigDecimalPayload tries the structured big-decimal decode first.
// (true, nil) means the structured set succeeded; (true, err) is final, from
// setDecimalRat or from a parse failure against a structured-only target;
// (false, nil) means fall through to setBytesValue for the opaque-bytes
// pass-through, because no structured target matched or the parse failed
// against a byte-like target, where the framing is bypassed on purpose. The
// binary, JSON and promotion paths share it.
func applyBigDecimalPayload(v reflect.Value, payload []byte) (done bool, err error) {
	r, displayScale, perr := parseBigDecimalPayload(payload)
	if perr == nil {
		if ok, serr := setDecimalRat(v, r, displayScale); ok {
			return true, serr
		}
		return false, nil
	}
	if v.Kind() != reflect.Slice && v.Kind() != reflect.String && v.Kind() != reflect.Array {
		return true, perr
	}
	return false, nil
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
	if err := checkDecimalUnscaledLen(uBytes); err != nil {
		return nil, 0, err
	}
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
	r := scaledRat(bytesToBigInt(uBytes), int(scale))
	displayScale := max(int(scale), 0)
	return r, displayScale, nil
}

type deserFixedDecimal struct {
	size  int
	scale int
}

func (s *deserFixedDecimal) deser(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	if err := needLen(src, s.size, "decimal"); err != nil {
		return nil, err
	}
	b := src[:s.size]
	src = src[s.size:]
	v = indirectAlloc(v)
	if ok, err := setDecimalValue(v, b, s.scale); ok {
		return src, err
	}
	// Fall back to the opaque [N]byte or []byte fixed. The delegate gets a
	// copy of only the payload, so the target never aliases your src, and we
	// return the outer src advanced above rather than the delegate's
	// remainder, which would drop every byte after this value.
	if _, err := (&deserFixed{s.size}).deser(append(b[:0:0], b...), v, sl); err != nil {
		return nil, err
	}
	return src, nil
}

// RatFromBytes converts Avro decimal bytes (big-endian two's complement) to a
// *big.Rat with the given scale. This is the conversion you would otherwise
// write yourself in a [CustomType] Decode callback that overrides our built-in
// decimal handling, since such a callback receives the raw []byte.
//
// We read a negative scale as unscaled * 10^|scale|, matching Java and
// avro-rs. We bound |scale| and the unscaled byte length; input past either
// bound returns a zero *big.Rat rather than allocating unbounded.
func RatFromBytes(b []byte, scale int) *big.Rat {
	return bytesToRat(b, scale)
}

func bytesToRat(b []byte, scale int) *big.Rat {
	if scale > decimalScaleLimit || scale < -decimalScaleLimit {
		// Public-API safety: bound the public RatFromBytes entry
		// against attacker-controlled scale. Internal callers pass
		// schema-validated non-negative scale already bounded by
		// validateLogical, so this guard only fires for direct
		// RatFromBytes use with hostile input.
		return new(big.Rat)
	}
	if len(b) > maxDecimalUnscaledBytes {
		// Public-API safety: the decode paths reject an over-long unscaled
		// value (via checkDecimalUnscaledLen) before reaching here, so this
		// fires only for direct RatFromBytes use with hostile input. Return a
		// zero rat rather than driving an unbounded base conversion, mirroring
		// the scale guard above.
		return new(big.Rat)
	}
	return scaledRat(bytesToBigInt(b), scale)
}

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

// parseUUID parses an RFC 4122 hex-dash UUID string into a [16]byte. It is
// alloc-free: we reinterpret the string as bytes for hex.Decode without
// copying, and hex.Decode only reads from src.
func parseUUID(s string) ([16]byte, error) {
	return parseUUIDBytes(unsafe.Slice(unsafe.StringData(s), len(s)))
}

// parseUUIDBytes parses an RFC 4122 hex-dash UUID byte slice into a [16]byte.
// hex.Decode does not retain or mutate src, so you may pass a borrowed slice
// (e.g. directly from the wire buffer).
func parseUUIDBytes(s []byte) ([16]byte, error) {
	var u [16]byte
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return u, fmt.Errorf("invalid UUID %q", truncBytesForError(s))
	}
	if _, err := hex.Decode(u[0:4], s[0:8]); err != nil {
		return u, fmt.Errorf("invalid UUID %q: %w", truncBytesForError(s), err)
	}
	if _, err := hex.Decode(u[4:6], s[9:13]); err != nil {
		return u, fmt.Errorf("invalid UUID %q: %w", truncBytesForError(s), err)
	}
	if _, err := hex.Decode(u[6:8], s[14:18]); err != nil {
		return u, fmt.Errorf("invalid UUID %q: %w", truncBytesForError(s), err)
	}
	if _, err := hex.Decode(u[8:10], s[19:23]); err != nil {
		return u, fmt.Errorf("invalid UUID %q: %w", truncBytesForError(s), err)
	}
	if _, err := hex.Decode(u[10:16], s[24:36]); err != nil {
		return u, fmt.Errorf("invalid UUID %q: %w", truncBytesForError(s), err)
	}
	return u, nil
}

func deserUUID(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	n, src, err := readLength(src, "string")
	if err != nil {
		return nil, err
	}
	if err := setUUIDTarget(v, src[:n], sl); err != nil {
		return nil, err
	}
	return src[n:], nil
}

// setUUIDTarget stores the wire's UUID text s into v: parsed into a
// [16]byte target, else through setStringValue's Interface / String /
// TextUnmarshaler / []byte chain, since uuid-on-string is wire-equivalent
// to plain string (serUUID falls through to serString on encode). The
// natural decode and the bytes-to-string promotion share it.
func setUUIDTarget(v reflect.Value, s []byte, sl *slab) error {
	v = indirectAlloc(v)
	if isUUIDType(v.Type()) {
		u, err := parseUUIDBytes(s)
		if err != nil {
			return err
		}
		copyBytesToArray(v, u[:])
		return nil
	}
	return setStringValue(v, s, len(s), sl)
}
