package avro

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"slices"
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
// Strings are immutable so sharing backing memory is safe. nums does the same
// for numbers decoded into an any.
type slab struct {
	buf []byte
	// nums is the rest of the current number slab. See boxNum.
	nums            []uint64
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

// numSlabLen is how many numbers decoded into an any share one allocation.
// One number you keep keeps its whole slab alive, so the slab is small: 64
// numbers is 512 bytes.
const numSlabLen = 64

// eface is the layout of an interface value with no methods: a type word and
// a data word.
type eface struct{ typ, data unsafe.Pointer }

// sliceHeader is the layout of a slice, which a decode into a typed slice
// copies and puts back when the decode fails.
type sliceHeader struct {
	data     unsafe.Pointer
	len, cap int
}

// These fail to compile if an any is not the size of an eface.
var (
	_ [unsafe.Sizeof(eface{}) - unsafe.Sizeof(any(nil))]struct{}
	_ [unsafe.Sizeof(any(nil)) - unsafe.Sizeof(eface{})]struct{}
	_ [unsafe.Sizeof(sliceHeader{}) - unsafe.Sizeof([]byte(nil))]struct{}
	_ [unsafe.Sizeof([]byte(nil)) - unsafe.Sizeof(sliceHeader{})]struct{}
)

// numSlabOK reports whether an any that we point at a number reads back as
// that number. It is false only for a toolchain whose interface value is not
// laid out as eface is, and boxNum then boxes with any(v).
var numSlabOK = func() bool {
	const want = int64(1) << 40
	slot := want
	boxed := any(int64(0))
	(*eface)(unsafe.Pointer(&boxed)).data = unsafe.Pointer(&slot)
	return boxed == any(want)
}()

// hasTarget reports whether an element of s is a target, as isTarget defines
// it. The elements an earlier decode stored mostly have one type, so we
// compare each element's type word with that of the last element whose type
// is never a target, and ask isTarget only for an element of another type.
func hasTarget(s []any) bool {
	if !numSlabOK {
		return slices.ContainsFunc(s, isTarget)
	}
	var never unsafe.Pointer
	for i := range s {
		typ := (*eface)(unsafe.Pointer(&s[i])).typ
		if typ == nil || typ == never {
			continue
		}
		switch s[i].(type) {
		case int64, int32, string, float64, float32, bool, []byte, time.Time, time.Duration:
			never = typ
			continue
		}
		if isTarget(s[i]) {
			return true
		}
	}
	return false
}

// numLike is a number type we decode into an any. ~int64 takes in
// time.Duration, the any value of a time-millis or time-micros.
type numLike interface {
	int32 | ~int64 | float32 | float64
}

// boxNum returns v in an any, as any(v) does. any(v) allocates for each
// number. We take 8 bytes from the slab's number slab instead, so that
// numSlabLen numbers share one allocation, and point the any's data word at
// them. We never write a slot again after we hand it out, since the runtime
// treats the value behind an any as immutable.
//
// Without a slab, or for a value whose bits are under 256, we use any(v): the
// runtime boxes such a value with no allocation.
func boxNum[T numLike](sl *slab, v T) any {
	var small bool
	if unsafe.Sizeof(v) == 4 {
		small = *(*uint32)(unsafe.Pointer(&v)) < 256
	} else {
		small = *(*uint64)(unsafe.Pointer(&v)) < 256
	}
	if sl == nil || small || !numSlabOK {
		return v
	}
	if len(sl.nums) == 0 {
		sl.nums = make([]uint64, numSlabLen)
	}
	slot := unsafe.Pointer(&sl.nums[0])
	sl.nums = sl.nums[1:]
	*(*T)(slot) = v
	// A zero T is boxed with no allocation and has T's type word.
	boxed := any(T(0))
	(*eface)(unsafe.Pointer(&boxed)).data = slot
	return boxed
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

// slabFreeKinds are the scalar leaf kinds whose desers never need the slab
// for any logical type and any target: only string decodes use its string
// buffer, only recursive dispatch bumps its depth, and only custom-decoder
// wrappers write its remaining state. A number decoded into an any uses the
// number slab when there is a slab, and boxes with any(v) when there is none.
// "string" is absent; "bytes" is safe because a string target of a bytes
// schema copies via string(b).
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

// put resets sl's per-call state and returns it to the pool. We keep buf and
// nums so the next caller reuses their backing memory.
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
//   - record: struct (matched by field name or `avro` tag), map[string]any,
//     [ArrayRecord] (matched by slice index), any
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
// Decode reuses addressable values in v, with the following rules:
//
//   - A map keeps the keys it already has.
//   - When a union has two or more Avro records or maps, decoding one of
//     them into a Go map always creates a new map.
//
// Into an any:
//
//   - Decoding into an existing pointer, map, or slice, where the type is
//     incompatible, is an error.
//   - Avro primitives are replaced (including []byte).
//   - In a union, Decode replaces a value in the any that has another
//     branch's type.
//
// If you reuse v, copy out what you need before you decode into v again.
//
// To produce JSON from decoded *any data use [Schema.EncodeJSON], not a
// generic JSON encoder: EncodeJSON is schema-aware and converts decoded values
// back to their Avro representations (time.Time to epoch integers, []byte to
// \uXXXX strings).
//
// Decode is liberal in what it accepts: we tolerate non-canonical input rather
// than rejecting it, such as a non-0/1 boolean byte that Java also reads as
// false. Encode is canonical, so such input round-trips to the canonical form.
func (s *Schema) Decode(src []byte, v any, opts ...Opt) ([]byte, error) {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return nil, errors.New("decode requires a non-nil pointer")
	}
	// Slab-free schemas (scalar leaves, no custom wiring) never need the
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
	// shape is what each branch decodes to in an any, which the `branch`
	// method reads to apply the union rules.
	shape unionShape
}

// unionShape says what each branch of a union decodes to in an any: a
// map[string]any for a record or a map, a []any for an array, a *big.Rat for
// a decimal, or a value with nothing to decode into. The shape serves two
// rules about the value a target already has: a value with another branch's
// type is replaced, and a union with two or more records or maps always
// decodes one of them into a new map.
type unionShape struct {
	class              []byte // per branch: 'm', 'a', 'r', 'n' for null, or 0
	maps, arrays, rats int    // how many branches have each class
	// customGo is the Go type each custom type on a branch declares for
	// what its Decode returns.
	customGo []reflect.Type
}

// unionClass returns the unionShape class of one branch.
func unionClass(n *schemaNode) byte {
	if n == nil {
		return 0
	}
	if n.logical == "decimal" || n.logical == "big-decimal" {
		return 'r'
	}
	switch n.kind {
	case "record", "map":
		return 'm'
	case "array":
		return 'a'
	case "null":
		return 'n'
	}
	return 0
}

// newUnionShape returns the shape of a union with these branches, where custom
// is the custom-type overlay of the schema the union is in.
func newUnionShape(branches []*schemaNode, custom map[*schemaNode]*customWiring) unionShape {
	u := unionShape{class: make([]byte, len(branches))}
	for i, b := range branches {
		u.class[i] = unionClass(b)
		if w := custom[b]; w != nil {
			// We add each type once, so the list is as long as the custom
			// types you registered and not as wide as the union.
			for _, t := range w.goTypes {
				if !slices.Contains(u.customGo, t) {
					u.customGo = append(u.customGo, t)
				}
			}
		}
		switch u.class[i] {
		case 'm':
			u.maps++
		case 'a':
			u.arrays++
		case 'r':
			u.rats++
		}
	}
	return u
}

// replaces reports whether a value of a branch with class c replaces x, the
// value an any has, where a decode outside a union would decode into x or
// return an error: x has the type another branch decodes to.
func (u *unionShape) replaces(x any, c byte) bool {
	if x == nil {
		return false
	}
	// A custom type on a branch decodes to the Go type it declares.
	if len(u.customGo) > 0 && slices.Contains(u.customGo, reflect.TypeOf(x)) {
		return true
	}
	switch x.(type) {
	case map[string]any:
		return c != 'm' && u.maps > 0 || c == 'm' && u.maps > 1
	case []any:
		return c != 'a' && u.arrays > 0
	case *big.Rat:
		return c != 'r' && u.rats > 0
	}
	// A map of your own type is still a Go map.
	return c == 'm' && u.maps > 1 && reflect.ValueOf(x).Kind() == reflect.Map
}

// unionTarget says where the value of one branch of a union goes in a
// target: straight into the target, or into a cell, a new value of slot's
// type that starts from seed, which store puts in the target after a decode
// that worked, so that a decode that fails or panics leaves the target as it
// was.
type unionTarget struct {
	slot   reflect.Value // the any or the map the value goes in
	seed   any           // what the value decodes into: what slot has, or nil
	env    reflect.Value // an any whose TaggedUnions envelope has a pointer to slot
	inside any           // that pointer
}

// target applies the union rules to what the target v has, for a value of a
// branch with class c, fills t with where the value goes, and reports whether
// the value decodes straight into the target, at dst. tagged says a
// TaggedUnions envelope goes around the value, so an any with the envelope of
// an earlier decode stands for the value inside it.
func (u *unionShape) target(t *unionTarget, v reflect.Value, c byte, tagged bool) (direct bool) {
	// A null decodes into no value, so it decodes straight into v.
	if c == 'n' {
		return true
	}
	direct = true
	// We follow the pointers the decoders follow, a pointer and a pointer in
	// an any, without allocating one.
	for range maxIndirectDepth {
		if v.Kind() == reflect.Pointer && !v.IsNil() {
			v = v.Elem()
			continue
		}
		if v.Kind() != reflect.Interface || !v.CanSet() || !v.CanInterface() {
			break
		}
		x := v.Interface()
		env := false
		if w, ok := x.(map[string]any); ok && len(w) == 1 && tagged {
			// The rules apply to the value inside.
			tagged, env, direct = false, true, false
			for _, x = range w {
			}
		}
		replace := u.replaces(x, c)
		if p := reflect.ValueOf(x); !replace && p.Kind() == reflect.Pointer && !p.IsNil() {
			if env {
				t.env, t.inside = v, x
			}
			v = p.Elem()
			continue
		}
		t.slot, t.seed = v, x
		if replace {
			t.seed, direct = nil, false
		}
		return direct
	}
	switch {
	case v.Kind() == reflect.Map && c == 'm' && u.maps > 1 && !v.IsNil() && v.CanSet():
		// The cell starts as a nil map.
		t.slot, direct = v, false
	case t.env.IsValid():
		// A value behind the pointer in the envelope that is not an any
		// decodes through the pointer.
		t.slot, t.seed, t.env = t.env, t.inside, reflect.Value{}
	}
	return direct
}

// dst returns where a value that decodes straight into the target v goes: the
// any that target stopped at, which is where v's pointers lead, so that a
// custom type's branch decodes into the same any as every other branch.
func (t *unionTarget) dst(v reflect.Value) reflect.Value {
	if t.slot.IsValid() {
		return t.slot
	}
	return v
}

// cell returns a new value of slot's type that has seed.
func (t *unionTarget) cell() reflect.Value {
	if t.slot.Type() == anyType {
		p := new(any)
		*p = t.seed
		return reflect.ValueOf(p).Elem()
	}
	c := reflect.New(t.slot.Type()).Elem()
	if t.seed != nil {
		c.Set(reflect.ValueOf(t.seed))
	}
	return c
}

// store puts the value that cell c decoded into the target. An any whose
// envelope had a pointer to slot gets the pointer back, for the caller to
// wrap.
func (t *unionTarget) store(c reflect.Value) {
	t.slot.Set(c)
	if t.env.IsValid() {
		t.env.Set(reflect.ValueOf(t.inside))
	}
}

// wrapEnvelope puts the value of the any at v, or of the any that v's
// pointers point at, in a TaggedUnions envelope named name. An interface with
// methods, a nil any, and any other target keep the bare value: a
// map[string]any does not have an interface's methods.
func wrapEnvelope(v reflect.Value, name string) {
	v = derefNoAlloc(v)
	if v.Kind() != reflect.Interface || !v.Elem().IsValid() || !mapStringAnyType.AssignableTo(v.Type()) {
		return
	}
	v.Set(reflect.ValueOf(map[string]any{name: v.Elem().Interface()}))
}

// unionShapeOf returns the shape of the union node n, which Parse builds with
// the union's tag tables. A hand-assembled node has none, and no rule applies
// to it.
func unionShapeOf(n *schemaNode) *unionShape {
	if n.tags == nil {
		return new(unionShape)
	}
	return &n.tags.shape
}

// readerShape returns the shape a resolved decoder uses for the reader union
// r, where custom is the reader's overlay: the counts are r's, and class has
// the one class each entry of branches has, which are the reader branches the
// writer's values decode as.
func readerShape(r *schemaNode, custom map[*schemaNode]*customWiring, branches ...*schemaNode) unionShape {
	u := newUnionShape(r.branches, custom)
	u.class = make([]byte, len(branches))
	for i, b := range branches {
		u.class[i] = unionClass(b)
	}
	return u
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
	return s.branch(src, v, sl, int(idx), s.fns[idx])
}

// branch decodes a value of branch idx into v with fn. The union rules apply
// to the value v has, and TaggedUnions wraps the result.
func (s *deserUnion) branch(src []byte, v reflect.Value, sl *slab, idx int, fn deserfn) ([]byte, error) {
	var t unionTarget
	dst, inCell := v, false
	// A nil any has no value for the rules to apply to.
	if v.Kind() != reflect.Interface || !v.IsNil() {
		var c byte
		if idx < len(s.shape.class) {
			c = s.shape.class[idx]
		}
		if s.shape.target(&t, v, c, sl.taggedUnions && !s.noWrap) {
			dst = t.dst(v)
		} else {
			dst, inCell = t.cell(), true
		}
	}
	src, err := fn(src, dst, sl)
	if err != nil {
		return src, err
	}
	if inCell {
		t.store(dst)
	}
	s.maybeWrap(v, sl, int32(idx))
	return src, nil
}

// maybeWrap wraps a decoded union value with its branch name when TaggedUnions
// is enabled.
func (s *deserUnion) maybeWrap(v reflect.Value, sl *slab, idx int32) {
	if s.noWrap || !sl.taggedUnions {
		return
	}
	name := s.branchNames[idx]
	if sl.tagLogicalTypes {
		name = s.logicalNames[idx]
	}
	wrapEnvelope(v, name)
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
		return u.branch(src, v, sl, valIdx, u.fns[valIdx])
	}
}

// deserNullUnionIface returns the iface-direct decoder of the ["null", T] or
// [T, "null"] union that meta describes and fn decodes, or nil unless T is a
// plain primitive. With TaggedUnions we decode through fn, so that maybeWrap
// stays the one place that tags a value.
func deserNullUnionIface(meta *fieldMeta, fn deserfn) deserIfaceFn {
	inner := ifaceFnForPrimitive(meta.inner)
	if inner == nil {
		return nil
	}
	nullByte, valByte := nullUnionBytes(meta.nullSecond)
	valIdx := 1
	if meta.nullSecond {
		valIdx = 0
	}
	return func(src []byte, sl *slab) (any, []byte, error) {
		if sl.taggedUnions {
			cell := reflect.New(anyType).Elem()
			src, err := fn(src, cell, sl)
			if err != nil {
				return nil, nil, err
			}
			return cell.Interface(), src, nil
		}
		// deserNullUnionAt bumps the depth around T's decode. A primitive T
		// never reads the depth, so the check alone does the same here.
		if sl.depth >= maxDepth {
			return nil, nil, errTooDeep
		}
		isVal, src, err := readNullUnionIndex(src, valIdx, nullByte, valByte)
		if err != nil {
			return nil, nil, err
		}
		if !isVal {
			return nil, src, nil
		}
		return inner(src, sl)
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
	if v = indirectAlloc(v); v.Kind() == reflect.Interface {
		return src, setIface(v, reflect.ValueOf(boxNum(sl, val)), "int")
	}
	return src, setIntValue(v, val)
}

func deserLong(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	if v = indirectAlloc(v); v.Kind() == reflect.Interface {
		return src, setIface(v, reflect.ValueOf(boxNum(sl, val)), "long")
	}
	return src, setLongValue(v, val)
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
	return setFloatValue(v, float64(math.Float32frombits(u)), "float", 32, nil)
}

func deserFloat(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	u, src, err := readUint32(src)
	if err != nil {
		return nil, err
	}
	if v = indirectAlloc(v); v.Kind() == reflect.Interface {
		return src, setIface(v, reflect.ValueOf(boxNum(sl, math.Float32frombits(u))), "float")
	}
	return src, setFloat32WireValue(v, u)
}

func deserDouble(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	u, src, err := readUint64(src)
	if err != nil {
		return nil, err
	}
	if v = indirectAlloc(v); v.Kind() == reflect.Interface {
		return src, setIface(v, reflect.ValueOf(boxNum(sl, math.Float64frombits(u))), "double")
	}
	return src, setFloatValue(v, math.Float64frombits(u), "double", 64, nil)
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
	// fast is the compiled unsafe path per Go type, one map per SkipUnknown
	// mode: map[reflect.Type]*fastRecordDeser. A reflect.Type key hashes as
	// one pointer, and a mappingKey key hashes field by field.
	fast [2]sync.Map
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

func (s *deserRecord) fastMap(skipUnknown bool) *sync.Map {
	if skipUnknown {
		return &s.fast[1]
	}
	return &s.fast[0]
}

// Sibling of [serRecord.fastFor]; see that comment.
func (s *deserRecord) fastFor(t reflect.Type, skipUnknown bool) *fastRecordDeser {
	if v, ok := s.fastMap(skipUnknown).Load(t); ok {
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
	actual, _ := s.fastMap(skipUnknown).LoadOrStore(t, fast)
	return actual.(*fastRecordDeser)
}

func (s *deserRecord) deser(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	if sl.depth >= maxDepth {
		return nil, errTooDeep
	}
	sl.depth++
	// While scratch is set, your any target v has a field's value and orig
	// has yours. We put orig back on an error or a panic.
	var orig any
	scratch := false
	defer func() {
		sl.depth--
		if scratch {
			setAny(v, orig)
		}
	}()
	v = indirectAlloc(v)
	k := v.Kind()
	if k == reflect.Interface {
		if v.Type().NumMethod() != 0 && !mapStringAnyType.AssignableTo(v.Type()) {
			return nil, semErr(v, "record")
		}
		// A map or slice that v has is a typed target. We decode into a
		// map[string]any here, which is the streaming-decode pattern, and
		// into any other map or an ArrayRecord below, as into a typed
		// target. We take the map out of v rather than unwrap v in
		// indirectAlloc, because what an interface has is not addressable,
		// and a decoder that calls v.Set on it would panic.
		m, typed, err := anyMap(v, "record")
		if err != nil {
			return nil, err
		}
		if m == nil && !typed.IsValid() {
			m = make(map[string]any, len(s.fields))
		}
		if typed.IsValid() {
			if typed.Kind() == reflect.Map {
				return s.deserMapTarget(src, typed, sl)
			}
			row := reuseOrMakeArrayRecord(typed.Interface().(ArrayRecord), len(s.fields))
			if src, err = s.deserArrayRecord(src, row, sl); err != nil {
				return nil, err
			}
			v.Set(reflect.ValueOf(row))
			return src, nil
		}
		// A field with no fnIface decodes into v itself, so we allocate no
		// cell for it. From the first such field to the v.Set below, v has nil
		// or a field's value: code in between must read orig, not v, for
		// your value.
		//
		// A field decodes into the value m has for it, which only a map m
		// had before this decode can have. fnIface stores a new value
		// without reading the old one, so a field whose old value is a
		// target takes fn.
		look := len(m) > 0
		for i := range s.fields {
			f := &s.fields[i]
			var old any
			if look {
				old = m[f.name]
			}
			if f.fnIface != nil && !isTarget(old) {
				var val any
				val, src, err = f.fnIface(src, sl)
				if err != nil {
					return nil, recordFieldError(nil, f.name, err)
				}
				m[f.name] = val
				continue
			}
			if !scratch {
				orig, scratch = v.Interface(), true
			}
			setAny(v, old)
			if src, err = f.fn(src, v, sl); err != nil {
				return nil, recordFieldError(nil, f.name, err)
			}
			m[f.name] = v.Interface()
		}
		scratch = false
		v.Set(reflect.ValueOf(m))
		return src, nil
	}
	t := v.Type()
	if t == arrayRecordType && v.CanAddr() {
		target := (*ArrayRecord)(v.Addr().UnsafePointer())
		row := reuseOrMakeArrayRecord(*target, len(s.fields))
		src, err := s.deserArrayRecord(src, row, sl)
		if err != nil {
			return nil, err
		}
		*target = row
		return src, nil
	}
	if k != reflect.Struct && (k != reflect.Map || t.Key().Kind() != reflect.String) {
		return nil, &SemanticError{GoType: t, AvroType: "record"}
	}
	if k == reflect.Map {
		return s.deserMapTarget(src, v, sl)
	}
	if v.CanAddr() {
		if fast := s.loadOrCompileFast(t, sl.skipUnknown); fast != nil {
			return deserRecordFast(src, fast, v, sl)
		}
	}
	// compileFastDeser returned nil because typeFieldMapping failed; we
	// re-call to report the error.
	_, err := typeFieldMappingSkip(s.names, &s.cache, t, sl.skipUnknown)
	return nil, err
}

// deserMapTarget decodes the record's fields into v, a map target or a map
// of your own type that an any has. A field decodes into the value v has for
// it.
func (s *deserRecord) deserMapTarget(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	t := v.Type()
	if t.Key().Kind() != reflect.String {
		return nil, &SemanticError{GoType: t, AvroType: "record"}
	}
	keyType := t.Key()
	// A nil map target gets a new map only from a decode that worked.
	m := v
	if v.IsNil() {
		m = reflect.MakeMapWithSize(t, len(s.fields))
	}
	look := m.Len() > 0 && reusesMapValue(t.Elem())
	elem := reflect.New(t.Elem()).Elem()
	var err error
	for _, f := range s.fields {
		if err := validateJSONNumberMapKey(f.name, keyType, "record"); err != nil {
			return nil, err
		}
		key := mapKeyAs(t, f.nameVal)
		if look {
			mapValueInto(m, key, elem)
		}
		if src, err = f.fn(src, elem, sl); err != nil {
			return nil, recordFieldError(nil, f.name, err)
		}
		m.SetMapIndex(key, elem)
		elem.SetZero()
	}
	if v.IsNil() {
		v.Set(m)
	}
	return src, nil
}

// deserArrayRecord decodes the record's fields into row, one per position.
// row has one slot per field, and a field decodes into the value its slot
// has. fnIface stores a new value without reading the old one, so a slot
// whose old value is a target takes fn.
func (s *deserRecord) deserArrayRecord(src []byte, row ArrayRecord, sl *slab) ([]byte, error) {
	var err error
	for i := range s.fields {
		f := &s.fields[i]
		if f.fnIface != nil && !isTarget(row[i]) {
			val, rest, err := f.fnIface(src, sl)
			if err != nil {
				return nil, recordFieldError(arrayRecordType, f.name, err)
			}
			row[i], src = val, rest
			continue
		}
		if src, err = f.fn(src, reflect.ValueOf(&row[i]).Elem(), sl); err != nil {
			return nil, recordFieldError(arrayRecordType, f.name, err)
		}
	}
	return src, nil
}

// reuseOrMakeArrayRecord returns n slots: row's own backing array when it has
// the room, which is the streaming-decode pattern, and a new one otherwise.
// Every record-into-ArrayRecord decoder uses it, so they agree on reuse. A slot
// keeps its value, and a field decodes into it as into any other any. The
// result is never nil, even for a record with no fields: a nil ArrayRecord is
// null, and a record is not.
func reuseOrMakeArrayRecord(row ArrayRecord, n int) ArrayRecord {
	if row == nil || cap(row) < n {
		return make(ArrayRecord, n)
	}
	return row[:n]
}

type deserEnum struct {
	symbols []string
	// boxed has each symbol in an any, so that a decode into an any does not
	// allocate a string header for each value.
	boxed []any
}

func newDeserEnum(symbols []string) *deserEnum {
	boxed := make([]any, len(symbols))
	for i, s := range symbols {
		boxed[i] = s
	}
	return &deserEnum{symbols: symbols, boxed: boxed}
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
	// setEnumTarget's interface arm, with the symbol boxed at parse.
	if v = indirectAlloc(v); v.Kind() == reflect.Interface {
		return src, setIface(v, reflect.ValueOf(s.boxed[idx]), "enum")
	}
	return src, setEnumTarget(v, int(idx), s.symbols[idx])
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
	// bypassing reflect. Selected at schema-build time based on the avro
	// item type. nil for items with no iface-direct decoder; see ifaceFnFor.
	fastIfaceLoop func(src []byte, slice []any, start, count int, sl *slab) ([]byte, error)
	// rec is the items' record when deserItem is that record's own deser, with
	// no custom type wrapped around it. A slice of structs or of single
	// pointers to structs then decodes each block through the record's
	// compiled fields, which we look up once per block.
	rec *deserRecord
	// itemMeta is the items' metadata, which a struct field compiles its
	// unsafe decoder from. typedBlock reads elemFns for a block of primitive
	// items that fastLoop did not take. It has, for each element type, the
	// unsafe decoder of one item into one element, or a nil udeserfn where the
	// unsafe path declines that type, one map per SkipUnknown mode:
	// map[reflect.Type]udeserfn.
	itemMeta *fieldMeta
	elemFns  [2]sync.Map
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
	// While at is set, the typed slice at it may have a new length, and the
	// deferred call puts saved back when the decode fails or panics.
	var saved sliceHeader
	var at *sliceHeader
	defer func() {
		sl.depth--
		if at != nil {
			*at = saved
		}
	}()
	v = indirectAlloc(v)
	iface := v.Kind() == reflect.Interface
	fixedArray := v.Kind() == reflect.Array
	if (!iface && !fixedArray && v.Kind() != reflect.Slice) || v.Type() == arrayRecordType {
		return nil, semErr(v, "array")
	}
	// Fixed-size Go arrays: we decode straight into the array elements and
	// verify the wire holds exactly that many.
	if fixedArray {
		return s.deserFixedArray(src, v, sl)
	}
	// For an interface target we build a []any and box it once at the end,
	// in the backing array of the []any v has, if v has one. A typed target
	// is decoded in place through sliceVal.
	var (
		anys      []any
		sliceVal  reflect.Value
		sliceType reflect.Type
	)
	if !iface {
		if v.CanAddr() {
			at = (*sliceHeader)(unsafe.Pointer(v.UnsafeAddr()))
			saved = *at
		}
		v.SetLen(0)
		sliceVal = v
		sliceType = v.Type()
	} else if typed, err := anySlice(v, &anys); err != nil || typed.IsValid() {
		return s.deserTypedInAny(src, v, typed, err, sl)
	}
	// Only the elements the old []any had can have something to decode into.
	oldCap := cap(anys)
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
	// fast loop that operates directly on []any. The loop stores each
	// element without reading the old one, so a block whose old elements
	// have a target takes the generic loop below.
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
				if anys == nil {
					anys = []any{}
				}
				return src, setIface(v, reflect.ValueOf(anys), "array")
			}
			// An empty Avro array decodes to a non-nil empty slice, as on
			// every other decode path, so you can tell it from an absent
			// value.
			if v.IsNil() {
				v.Set(reflect.MakeSlice(sliceType, 0, 0))
			}
			at = nil
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
		start := len(anys)
		if !iface {
			start = sliceVal.Len()
		}
		if start > math.MaxInt-n {
			return nil, fmt.Errorf("array length overflows int: start=%d count=%d", start, n)
		}
		// A block fills the room the slice has, in place, before it grows the
		// slice for the rest, as DecodeJSON grows it one item at a time, so
		// that a decode that fails has written the same items into your
		// backing array on both decoders. Both arms grow as append does.
		for n > 0 {
			var k int
			if iface {
				if len(anys) == cap(anys) {
					anys = slices.Grow(anys, n)
				}
				k = min(n, cap(anys)-start)
				anys = anys[:start+k]
			} else {
				if sliceVal.Cap() == start {
					sliceVal.Grow(n)
				}
				k = min(n, sliceVal.Cap()-start)
				sliceVal.SetLen(start + k)
			}
			end := start + k
			handled := false
			if useFastNative {
				var rest []byte
				if handled, rest, err = s.nativeLoop(sliceVal, src, start, k, sl); handled {
					src = rest
				} else if err == nil {
					useFastNative = false // named slice/elem type: fall back to fastLoop henceforth
				}
			}
			if err == nil && !handled {
				switch {
				case useFast:
					src, err = s.fastLoop(src, sliceVal, start, k, sl)
				case useFastIface && (start >= oldCap || !hasTarget(anys[start:min(end, oldCap)])):
					src, err = s.fastIfaceLoop(src, anys, start, k, sl)
				case iface:
					for i := start; i < end && err == nil; i++ {
						src, err = s.deserItem(src, reflect.ValueOf(&anys[i]).Elem(), sl)
					}
				default:
					// The block body is a function of its own so that
					// deser keeps few enough returns for the compiler to
					// open-code its defer.
					src, err = s.typedBlock(src, sliceVal, start, k, sl)
				}
			}
			if err != nil {
				return nil, err
			}
			start += k
			n -= k
		}
	}
}

// deserTypedInAny finishes a decode into the interface v after anySlice
// found an error, or found typed, a slice of a type other than []any, which
// is a typed target. What an interface has is not addressable, so we decode
// into a copy of the slice header, which reuses the backing array, and store
// the copy. It takes anySlice's err, rather than deser returning it, so that
// deser keeps few enough returns for the compiler to open-code its defer.
func (s *deserArray) deserTypedInAny(src []byte, v, typed reflect.Value, err error, sl *slab) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	tmp := reflect.New(typed.Type()).Elem()
	tmp.Set(typed)
	// The caller counted this array's depth already.
	sl.depth--
	src, err = s.deser(src, tmp, sl)
	sl.depth++
	if err != nil {
		return nil, err
	}
	v.Set(tmp)
	return src, nil
}

// elemFor returns the unsafe decoder a struct field of type t gets for one
// item in mode skipUnknown, or nil where that path declines t.
func (s *deserArray) elemFor(t reflect.Type, skipUnknown bool) udeserfn {
	m := &s.elemFns[0]
	if skipUnknown {
		m = &s.elemFns[1]
	}
	if fn, ok := m.Load(t); ok {
		return fn.(udeserfn)
	}
	fn := tryCompileFieldDeser(&deserRecordField{meta: s.itemMeta}, t, skipUnknown)
	actual, _ := m.LoadOrStore(t, fn)
	return actual.(udeserfn)
}

// typedBlock decodes items into slots [start, start+n) of the typed slice
// sliceVal, whose length is at least start+n.
func (s *deserArray) typedBlock(src []byte, sliceVal reflect.Value, start, n int, sl *slab) ([]byte, error) {
	elemType := sliceVal.Type().Elem()
	if s.rec != nil {
		data := sliceVal.UnsafePointer()
		switch {
		case elemType.Kind() == reflect.Struct:
			return udRecordBlock(src, s.rec, elemType, data, start, n, sl)
		case elemType.Kind() == reflect.Pointer && elemType.Elem().Kind() == reflect.Struct:
			ptrs := unsafe.Slice((*unsafe.Pointer)(data), start+n)[start:]
			return udPtrRecordBlock(src, s.rec, elemType.Elem(), ptrs, sl)
		}
	}
	// Primitive items reach here only when fastLoop did not take the block:
	// the element's Go kind differs from the item's, as for an int from a
	// long, or the element is a string type fastLoop must not write. Each
	// element then decodes as a struct field of its type does.
	if s.fastLoop != nil {
		if fn := s.elemFor(elemType, sl.skipUnknown); fn != nil {
			data := sliceVal.UnsafePointer()
			size := elemType.Size()
			var err error
			for i := start; i < start+n; i++ {
				src, err = fn(src, unsafe.Add(data, uintptr(i)*size), sl)
				if err != nil {
					return nil, err
				}
			}
			return src, nil
		}
	}
	// We batch-allocate backing for nil pointer slots only and reuse any
	// non-nil retained pointer, so an element aliased from a prior decode is
	// updated in place. That matches the unsafe struct-field path
	// (udArrayPtrRecord) and the documented pointer-reuse contract. A slice
	// that grows keeps the pointers it had, since Grow copies them, so only
	// the new slots get fresh backing. A slot gets its pointer just before
	// its item decodes, and the deferred call gives it a nil back when the
	// item fails or panics.
	var backing reflect.Value
	if elemType.Kind() == reflect.Pointer {
		var need int
		for i := range n {
			if sliceVal.Index(start + i).IsNil() {
				need++
			}
		}
		if need > 0 {
			backing = reflect.MakeSlice(reflect.SliceOf(elemType.Elem()), need, need)
		}
	}
	var err error
	var fresh reflect.Value // a slot that has a new pointer while its item decodes
	defer func() {
		if fresh.IsValid() {
			fresh.SetZero()
		}
	}()
	j := 0
	for i := start; i < start+n; i++ {
		slot := sliceVal.Index(i)
		if backing.IsValid() && slot.IsNil() {
			slot.Set(backing.Index(j).Addr())
			j++
			fresh = slot
		}
		if src, err = s.deserItem(src, slot, sl); err != nil {
			return nil, err
		}
		fresh = reflect.Value{}
	}
	return src, nil
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

// deserArrayFnIfaceLoop is deserArrayIfaceLoop for items that fn decodes.
func deserArrayFnIfaceLoop(fn deserIfaceFn) func(src []byte, slice []any, start, count int, sl *slab) ([]byte, error) {
	return func(src []byte, slice []any, start, count int, sl *slab) ([]byte, error) {
		for i := start; i < start+count; i++ {
			v, rest, err := fn(src, sl)
			if err != nil {
				return nil, err
			}
			slice[i], src = v, rest
		}
		return src, nil
	}
}

// deserArrayNumIfaceLoop is deserArrayIfaceLoop for a number, which boxNum
// boxes.
func deserArrayNumIfaceLoop[T numLike](readOne func(src []byte, sl *slab) (T, []byte, error)) func(src []byte, slice []any, start, count int, sl *slab) ([]byte, error) {
	return func(src []byte, slice []any, start, count int, sl *slab) ([]byte, error) {
		for i := start; i < start+count; i++ {
			v, rest, err := readOne(src, sl)
			if err != nil {
				return nil, err
			}
			// boxNum makes the same check. We repeat the check here
			// because boxNum is not inlined into this closure, and a
			// small value would cost a second call.
			var small bool
			if unsafe.Sizeof(v) == 4 {
				small = *(*uint32)(unsafe.Pointer(&v)) < 256
			} else {
				small = *(*uint64)(unsafe.Pointer(&v)) < 256
			}
			if small {
				slice[i] = v
			} else {
				slice[i] = boxNum(sl, v)
			}
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
	deserArrayIntIfaceLoop     = deserArrayNumIfaceLoop(readOneInt)
	deserArrayLongIfaceLoop    = deserArrayNumIfaceLoop(readOneLong)
	deserArrayFloatIfaceLoop   = deserArrayNumIfaceLoop(readOneFloat)
	deserArrayDoubleIfaceLoop  = deserArrayNumIfaceLoop(readOneDouble)
)

type deserMap struct {
	deserItem deserfn
	fastBlock func(src []byte, mapVal, keyVal, elemVal reflect.Value, count int, sl *slab) ([]byte, error)
	// nativeBlock decodes one block straight into the concrete Go map
	// (m[k]=v) when its dynamic type is exactly map[string]V. Named
	// map types get handled=false and fall back to fastBlock.
	nativeBlock  func(mapVal reflect.Value, src []byte, count int, sl *slab) (bool, []byte, error)
	fastElemKind reflect.Kind
	// fastIfaceVal decodes one value directly into a Go any, bypassing
	// reflect; deserMapPrimitiveIfaceBlock drives the per-block loop. nil for
	// values with no iface-direct decoder (see ifaceFnFor); the generic
	// reflect path handles those.
	fastIfaceVal deserIfaceFn
	// rec is the values' record when deserItem is that record's own deser,
	// with no custom type wrapped around it. A struct value then decodes
	// through the record's compiled fields, which we look up once per block.
	rec *deserRecord
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
	// count as a hint, avoiding a rehash on the typical single-block path. A
	// map you pass, or the map[string]any your any has, is merged into as-is,
	// with no hint.
	var (
		mapVal  reflect.Value
		mapTyp  reflect.Type
		elemTyp reflect.Type
	)
	if iface {
		// A map v has is a typed target: we merge into a map[string]any
		// here, and into a map of another type as into the typed target
		// below.
		m, typed, err := anyMap(v, "map")
		if err != nil {
			return nil, err
		}
		if typed.IsValid() {
			v, iface = typed, false
		} else if m != nil {
			mapVal = reflect.ValueOf(m)
		}
	}
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
	// look reports whether a value decodes into the value the map has for its
	// key: the map had entries before this decode, and a value of its type
	// can hold something to reuse.
	look := mapVal.IsValid() && mapVal.Len() > 0 && reusesMapValue(elemTyp)
	// For interface targets with primitive avro values, use the
	// iface-block fast path that operates directly on map[string]any.
	useFastIface := iface && s.fastIfaceVal != nil
	// The reflect loops below reuse one key and one elem container to avoid
	// per-entry reflect.ValueOf / reflect.New allocations. keyVal gets your
	// actual map key type (e.g. `type UserID string`); reusing this Value for
	// SetMapIndex avoids the panic that plain reflect.ValueOf(s) would fire
	// when the map's key is a named string subtype. We allocate both on the
	// first block that needs them, because an empty map and the native and
	// iface loops never use them.
	var keyVal, elemVal reflect.Value
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
			}
			if iface {
				return src, setIface(v, mapVal, "map")
			}
			// A nil map target gets the new map only from a decode that
			// worked.
			if v.IsNil() {
				v.Set(mapVal)
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
		if !useFastIface && !keyVal.IsValid() {
			keyVal = reflect.New(mapTyp.Key()).Elem()
			elemVal = reflect.New(elemTyp).Elem()
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
			src, err = deserMapPrimitiveIfaceBlock(src, m, int(count), sl, s.fastIfaceVal, look, s.deserItem)
			if err != nil {
				return nil, err
			}
			continue
		}
		keyType := mapTyp.Key()
		// elemVal comes from one reflect.New, so its address stays the same
		// for the whole decode.
		var fast *fastRecordDeser
		var elemP unsafe.Pointer
		if s.rec != nil && elemTyp.Kind() == reflect.Struct {
			if f := s.rec.fastFor(elemTyp, sl.skipUnknown); f != nil && f.allFast {
				fast, elemP = f, unsafe.Pointer(elemVal.UnsafeAddr())
			}
		}
		for range int(count) {
			src, err = readMapKey(src, keyVal, sl)
			if err != nil {
				return nil, err
			}
			if err := validateJSONNumberMapKey(keyVal.String(), keyType, "map"); err != nil {
				return nil, err
			}
			if look {
				mapValueInto(mapVal, keyVal, elemVal)
			}
			if fast != nil {
				src, err = deserRecordFastPtr(src, fast, elemP, sl)
			} else {
				src, err = s.deserItem(src, elemVal, sl)
			}
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
		s, ok := sliceOf[V](sliceVal)
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

// deserMapPrimitiveIfaceBlock decodes one block of count entries into m.
// decodeVal stores a new value without reading the old one. When look is set,
// m had entries before this decode, and a key whose old value is a target
// takes item, which decodes into a pointer and returns the error of a typed
// target for a map or slice.
func deserMapPrimitiveIfaceBlock(src []byte, m map[string]any, count int, sl *slab, decodeVal deserIfaceFn, look bool, item deserfn) ([]byte, error) {
	for range count {
		key, rest, err := readMapKeyString(src, sl)
		if err != nil {
			return nil, err
		}
		if look {
			if old := m[key]; isTarget(old) {
				// cell is its own variable so that only this path
				// allocates one.
				cell := old
				src = rest
				if src, err = item(src, reflect.ValueOf(&cell).Elem(), sl); err != nil {
					return nil, err
				}
				m[key] = cell
				continue
			}
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

// deserNumIface is deserIface for a number, which boxNum boxes.
func deserNumIface[T numLike](readOne func(src []byte, sl *slab) (T, []byte, error)) deserIfaceFn {
	return func(src []byte, sl *slab) (any, []byte, error) {
		v, src, err := readOne(src, sl)
		if err != nil {
			return nil, nil, err
		}
		return boxNum(sl, v), src, nil
	}
}

var (
	deserBooleanIface = deserIface(readOneBool)
	deserIntIface     = deserNumIface(readOneInt)
	deserLongIface    = deserNumIface(readOneLong)
	deserFloatIface   = deserNumIface(readOneFloat)
	deserDoubleIface  = deserNumIface(readOneDouble)
	deserStringIface  = deserIface(readOneString)
)

// deserBytesIface is deserBytes into an any.
func deserBytesIface(src []byte, sl *slab) (any, []byte, error) {
	n, src, err := readLength(src, "bytes")
	if err != nil {
		return nil, nil, err
	}
	return sl.bytes(src, n), src[n:], nil
}

// ifaceFnFor returns the iface-direct decoder for meta: a plain primitive's,
// or a null union's (see fieldMeta.iface). It is nil otherwise.
func ifaceFnFor(meta *fieldMeta) deserIfaceFn {
	if meta != nil && meta.iface != nil {
		return meta.iface
	}
	return ifaceFnForPrimitive(meta)
}

// ifaceFnForPrimitive returns the iface-direct decoder for a plain primitive
// field, or nil where a logical type or custom decoder applies, or the kind
// is not a primitive.
func ifaceFnForPrimitive(meta *fieldMeta) deserIfaceFn {
	if meta == nil || meta.logical != "" || meta.hasCustomType {
		return nil
	}
	return deserIfaceFnByKind[meta.avroType]
}

// deserIfaceFnByKind maps an Avro primitive kind name to its iface-direct
// decoder. We build it from the deser*Iface vars above so a new primitive
// gets wired in one place only.
var deserIfaceFnByKind = map[string]deserIfaceFn{
	"bytes":   deserBytesIface,
	"boolean": deserBooleanIface,
	"int":     deserIntIface,
	"long":    deserLongIface,
	"float":   deserFloatIface,
	"double":  deserDoubleIface,
	"string":  deserStringIface,
}

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
// which can hold them; decode into a typed float if you need them. An
// interface target gets the number boxed from sl, which may be nil.
func setFloatValue(v reflect.Value, f float64, avroType string, bits int, sl *slab) error {
	if v.Kind() == reflect.Interface {
		var boxed any
		if bits == 32 {
			boxed = boxNum(sl, float32(f))
		} else {
			boxed = boxNum(sl, f)
		}
		return setIface(v, reflect.ValueOf(boxed), avroType)
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
// time-millis arm (and udTimeMillisTime for the time-target unsafe path). An
// interface target gets the Duration boxed from sl, which may be nil.
func setTimeMillisTarget(v reflect.Value, val int32, sl *slab) error {
	if v.Kind() == reflect.Interface {
		return setIface(v, reflect.ValueOf(boxNum(sl, timeMillisToDuration(val))), "int")
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
func setTimeMicrosTarget(v reflect.Value, val int64, sl *slab) error {
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
			return setIface(v, reflect.ValueOf(boxNum(sl, d)), "long")
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
	return src, setTimeMillisTarget(indirectAlloc(v), val, sl)
}

func deserTimeMicros(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	val, src, err := readVarlong(src)
	if err != nil {
		return nil, err
	}
	return src, setTimeMicrosTarget(indirectAlloc(v), val, sl)
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

// ratTarget returns the *big.Rat that a decimal decodes into in place: one
// behind a pointer, such as a *big.Rat field or element, or the one an any
// has. v is the target before indirectAlloc. A big.Rat stored by value is
// not one, although we can address it: a copy of its struct shares its
// storage, and a decode in place would change every copy you kept. That
// includes the big.Rat of Decode(src, r) for an r of type *big.Rat, which we
// cannot tell from the address of a field.
func ratTarget(v reflect.Value) *big.Rat {
	for range maxIndirectDepth {
		switch v.Kind() {
		case reflect.Pointer:
			if v.IsNil() || !v.CanInterface() {
				return nil
			}
			if v.Type() == bigRatPtrType {
				return v.Interface().(*big.Rat)
			}
			v = v.Elem()
		case reflect.Interface:
			if v.IsNil() {
				return nil
			}
			v = v.Elem()
		default:
			return nil
		}
	}
	return nil
}

// decimalInPlace decodes the decimal b into the *big.Rat the target v has,
// and reports whether v has one. That reuses the numerator's storage and
// makes no new big.Rat.
func decimalInPlace(v reflect.Value, b []byte, scale int) (bool, error) {
	r := ratTarget(v)
	if r == nil {
		return false, nil
	}
	if err := checkDecimalUnscaledLen(b); err != nil {
		return true, err
	}
	r.SetFrac(setSigned(r.Num(), b), pow10(scale))
	return true, nil
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
	if ok, err := decimalInPlace(v, b, s.scale); ok {
		return src, err
	}
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
	if ok, err := decimalInPlace(v, b, s.scale); ok {
		return src, err
	}
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
// *big.Rat with the given scale. Use RatFromBytes in a [CustomType] Decode
// callback that overrides our built-in decimal handling, since such a callback
// receives the raw []byte.
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

func bytesToBigInt(b []byte) *big.Int { return setSigned(new(big.Int), b) }

// setSigned sets z to the two's complement big-endian integer b, in z's own
// storage, and returns z.
func setSigned(z *big.Int, b []byte) *big.Int {
	if len(b) == 0 || b[0]&0x80 == 0 {
		return z.SetBytes(b)
	}
	// A negative value: its magnitude is the two's complement of b.
	var stack [64]byte
	mag := append(stack[:0], b...)
	carry := byte(1)
	for i := len(mag) - 1; i >= 0; i-- {
		mag[i] = ^mag[i] + carry
		if mag[i] != 0 {
			carry = 0
		}
	}
	return z.Neg(z.SetBytes(mag))
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
