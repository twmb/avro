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
	"sync"
	"sync/atomic"
	"time"
)

type serfn func([]byte, reflect.Value, int) ([]byte, error)

// maxDepth bounds recursion in both the encoder and decoder. On the
// encoder side this protects against cyclic Go input against recursive
// schemas (stack overflow is fatal in Go). On the decoder side it
// protects against malicious wire data driving unbounded recursion via
// recursive schemas (e.g. linked-list "Node" with all "next" fields
// non-null). 1000 is well below Go's stack growth limit and far above
// any legitimate Avro depth.
const maxDepth = 1000

var errTooDeep = errors.New("avro: recursion limit exceeded (cyclic or pathologically deep input)")


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
			return s.ser(dst, rv, 0)
		default:
			return nil, &SemanticError{AvroType: s.node.kind, Err: errors.New("cannot encode nil")}
		}
	}
	return s.ser(dst, rv, 0)
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
	// branchKinds maps an Avro primitive kind name (boolean, int, long,
	// float, double, string, bytes) to its branch index when the union
	// contains exactly one branch of that kind. Used by ser to prefer
	// a type-name match over try-each — mirrors Java's
	// GenericData.resolveUnion and hamba/fastavro's name-based dispatch.
	branchKinds map[string]int
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
// that fails or v is not a tagged map, the value's Go-type canonical
// Avro name is dispatched directly (Java/fastavro/hamba parity — see
// branchKinds doc); if no name match exists, each branch is tried in
// order, preserving the documented whole-number-float and
// json.Number-into-int promotion paths.
func (s *serUnion) ser(dst []byte, v reflect.Value, depth int) ([]byte, error) {
	if depth >= maxDepth {
		return nil, errTooDeep
	}
	if idx, inner, ok := s.tryUnwrapTagged(v); ok {
		attempt := appendVarint(dst, int32(idx))
		if result, err := s.fns[idx](attempt, inner, depth+1); err == nil {
			return result, nil
		} else if errors.Is(err, errTooDeep) {
			return nil, err
		}
	}

	// Nil-first dispatch: if v is nil-equivalent and the union has a
	// null branch, pick null regardless of arity. Mirrors the 2-branch
	// optimization serNullUnionAt and generalizes the "Go nil = absent
	// → null branch" semantic uniformly across all union arities. Pre-
	// fix, only the 2-branch optimization did this; the generic
	// dispatcher used type-name dispatch first, so nil []byte against
	// ["null","int","bytes"] routed to "bytes" (empty bytes) while the
	// 2-branch sibling ["null","bytes"] routed to null. The two
	// behaviors now agree.
	if nullIdx, ok := s.branchKinds["null"]; ok && isNilValue(v) {
		return appendVarint(dst, int32(nullIdx)), nil
	}

	base := dst
	if name := unionTypeNameForValue(v); name != "" {
		if idx, ok := s.branchKinds[name]; ok {
			attempt := appendVarint(base, int32(idx))
			if result, err := s.fns[idx](attempt, v, depth+1); err == nil {
				return result, nil
			} else if errors.Is(err, errTooDeep) {
				return nil, err
			}
		}
	}

	var err error
	for i, fn := range s.fns {
		attempt := appendVarint(base, int32(i))
		if attempt, err = fn(attempt, v, depth+1); err == nil {
			return attempt, nil
		}
		// Propagate too-deep immediately; trial loop would mask it.
		if errors.Is(err, errTooDeep) {
			return nil, err
		}
	}
	e := &SemanticError{AvroType: "union", Err: errors.New("no matching branch")}
	if v.IsValid() {
		e.GoType = v.Type()
	}
	return nil, e
}

// unionTypeNameForValue returns the Avro primitive kind name that
// matches v's Go type directly, or "" if v should fall through to
// try-each (json.Number, time.Time, time.Duration, *big.Rat, etc. —
// the encoder's lenient/coercion paths handle these via try-each).
// Used by serUnion.ser, appendAvroJSONUnion, and encodeDefault's union
// case for consistent first-pass branch selection.
//
// Pointer/interface chains are unwrapped up to maxIndirectDepth (same
// guard as indirect/indirectAlloc) to avoid stack overflow on cyclic
// inputs like `var p any; p = &p` that the fuzz harness produces.
// Cycles beyond the cap return "" — try-each then handles the eventual
// rejection from indirect().
func unionTypeNameForValue(v reflect.Value) string {
	for range maxIndirectDepth {
		if !v.IsValid() {
			return ""
		}
		if v.Type() == jsonNumberType {
			// json.Number's Kind() is reflect.String but it can also
			// flow into int/long/float/double branches via numeric
			// coercion — let try-each find the right branch rather
			// than locking to "string".
			return ""
		}
		switch v.Kind() {
		case reflect.Pointer, reflect.Interface:
			if v.IsNil() {
				return "null"
			}
			v = v.Elem()
			continue
		case reflect.Bool:
			return "boolean"
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint8, reflect.Uint16:
			return "int"
		case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint32, reflect.Uint64:
			return "long"
		case reflect.Float32:
			return "float"
		case reflect.Float64:
			return "double"
		case reflect.String:
			return "string"
		case reflect.Slice, reflect.Array:
			if v.Type().Elem().Kind() == reflect.Uint8 {
				return "bytes"
			}
		}
		return ""
	}
	return ""
}

// Avro encodes the union branch index as a varint before the value.
// Varint 0 encodes to byte 0x00, varint 1 encodes to byte 0x02
// (zigzag: 1 << 1 = 2). These two-branch null-union helpers inline
// the single-byte varints directly.

// serNullUnion handles ["null", T] unions: null is index 0 (byte 0),
// T is index 1 (byte 2).
func serNullUnion(u *serUnion) serfn { return serNullUnionAt(u, 1, 0, 2) }

// serNullSecondUnion handles ["T", "null"] unions: T is index 0 (byte 0),
// null is index 1 (byte 2).
func serNullSecondUnion(u *serUnion) serfn { return serNullUnionAt(u, 0, 2, 0) }

// serNullUnionAt is the shared implementation. valIdx is the index of T
// in the union; nullByte and valByte are the wire-format bytes for the
// null and value branches respectively.
func serNullUnionAt(u *serUnion, valIdx int, nullByte, valByte byte) serfn {
	return func(dst []byte, v reflect.Value, depth int) ([]byte, error) {
		if isNilValue(v) {
			return append(dst, nullByte), nil
		}
		if idx, inner, ok := u.tryUnwrapTagged(v); ok {
			if idx != valIdx && isNilValue(inner) {
				return append(dst, nullByte), nil
			}
			if idx == valIdx {
				if result, err := u.fns[valIdx](append(dst, valByte), inner, depth+1); err == nil {
					return result, nil
				} else if errors.Is(err, errTooDeep) {
					return nil, err
				}
			}
		}
		return u.fns[valIdx](append(dst, valByte), v, depth+1)
	}
}

// isNilValue reports whether v is nil-equivalent for the purposes of the
// 2-branch [null,T] union optimization. It peels Pointer / Interface
// layers (handling &nilPtr — a **T with non-nil outer pointer wrapping a
// nil *T) and treats nil Map / Slice / Chan / Func as nil. The accept
// set matches serNull (ser.go) and appendAvroJSON's case "null" arm
// (json_codec.go) exactly so the four dispatch sites — binary 2-branch
// optimization (serNullUnionAt), binary 3-branch try-each (serUnion.ser
// → serNull), JSON 2-branch optimization (appendAvroJSONUnion's
// 2-branch nil short-circuit), and JSON try-each (case "null") —
// agree on what counts as nil.
//
// Capped at maxIndirectDepth so a self-referential interface
// (var p any; p = &p) terminates instead of looping forever; treat
// the deeply-nested case as not-nil so the encoder reports a real
// error downstream rather than silently encoding null.
func isNilValue(v reflect.Value) bool {
	if !v.IsValid() {
		return true
	}
	for range maxIndirectDepth {
		switch v.Kind() {
		case reflect.Pointer, reflect.Interface:
			if v.IsNil() {
				return true
			}
			v = v.Elem()
		case reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
			return v.IsNil()
		default:
			return false
		}
	}
	return false
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

func serNull(dst []byte, v reflect.Value, _ int) ([]byte, error) {
	if !v.IsValid() {
		return dst, nil
	}
	// Peel pointer + interface layers so a typed-nil value reaching us
	// inside an any wrapper (iter.Value() on a map[string]any in
	// serUnion.tryUnwrapTagged, serArray.serItem over []any, serMap
	// over map[string]any) or as **T-with-nil-inner (&p where var p
	// *int = nil — a common shape from AppendEncode(&nilPtr)) is
	// recognized as nil. Without the peel, any((*int)(nil)) has
	// Kind()==Interface with IsNil()==false (the interface itself is
	// non-nil — it holds type info) and &nilPtr has Kind()==Pointer
	// with IsNil()==false (the outer pointer is non-nil); the kind
	// switch below would return errNonNil for both even though the
	// user clearly meant "null." Mirrors appendAvroJSON's indirect
	// loop on the JSON side and isNilValue's loop on the 2-branch
	// [null,T] optimization. Bounded by maxIndirectDepth so a self-
	// referential interface (var p any; p = &p) terminates.
	for range maxIndirectDepth {
		if v.Kind() != reflect.Interface && v.Kind() != reflect.Pointer {
			break
		}
		if v.IsNil() {
			return dst, nil
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
		if v.IsNil() {
			return dst, nil
		}
	}
	return dst, errNonNil
}

func serBoolean(dst []byte, v reflect.Value, _ int) ([]byte, error) {
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
var mapStringAnyType = reflect.TypeFor[map[string]any]()

// floatFitsInt32 returns f truncated to int32 and a nil error iff f is a
// whole number within [MinInt32, MaxInt32]. Callers wrap the returned
// error with their SemanticError/context.
func floatFitsInt32(f float64) (int32, error) {
	n := math.Trunc(f)
	if f != n {
		return 0, fmt.Errorf("value %v is not a whole number", f)
	}
	if n < math.MinInt32 || n > math.MaxInt32 {
		return 0, fmt.Errorf("value %v overflows int32", f)
	}
	return int32(n), nil
}

// floatFitsInt64 returns f truncated to int64 and a nil error iff f is a
// whole number within int64 range. Uses inclusive MinInt64 / exclusive
// 1<<63 since the next representable float64 above MaxInt64 is 1<<63.
func floatFitsInt64(f float64) (int64, error) {
	n := math.Trunc(f)
	if f != n {
		return 0, fmt.Errorf("value %v is not a whole number", f)
	}
	if n < -(1<<63) || n >= 1<<63 {
		return 0, fmt.Errorf("value %v overflows int64", f)
	}
	return int64(n), nil
}

// floatFitsInt32From is floatFitsInt32 with an additional source-float
// mantissa-precision check. When the source is float32 (bits == 32),
// values exceeding ±(1<<24) are rejected — those are values the matching
// decoder's float32-target arm in setIntValue would reject, so accepting
// them on encode breaks the same-type round-trip. bits == 64 needs no
// additional bound: int32 fits within float64's 1<<53 mantissa exactly.
// Source-bit-aware mantissa rule lives here (and floatFitsInt64From) so
// every encode arm that takes Go float input — serInt, serArray.serInt,
// serMap.serInt, jsonCoerceToInt32 — agrees with setIntValue's decode
// arm on the symmetric round-trip boundary.
func floatFitsInt32From(f float64, bits int) (int32, error) {
	n, err := floatFitsInt32(f)
	if err != nil {
		return 0, err
	}
	if bits == 32 && (n < -(1<<24) || n > 1<<24) {
		return 0, fmt.Errorf("value %v exceeds float32 exact-precision range", f)
	}
	return n, nil
}

// floatFitsInt64From is floatFitsInt64 with an additional source-float
// mantissa-precision check. Mirrors setLongValue's float-target precLimit:
// 1<<24 for a float32 source, 1<<53 for float64. Same DRY rationale as
// floatFitsInt32From — one rule, every encode-side float→int/long path.
func floatFitsInt64From(f float64, bits int) (int64, error) {
	n, err := floatFitsInt64(f)
	if err != nil {
		return 0, err
	}
	var bound int64 = 1 << 53
	if bits == 32 {
		bound = 1 << 24
	}
	if n < -bound || n > bound {
		return 0, fmt.Errorf("value %v exceeds float%d exact-precision range", f, bits)
	}
	return n, nil
}

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
// Int64() first for full int64 precision, falling back to Float64() for
// exponent notation (e.g. "1.5e3") and fractional detection. The returned
// error is bare; callers wrap it in their SemanticError.
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
	// Fall back to float64 for exponent notation (strconv rejects "1.5e3"
	// as an integer) and fractional detection.
	f, err := jn.Float64()
	if err != nil {
		return 0, true, fmt.Errorf("value %s is not a valid number", jn)
	}
	n, err := floatFitsInt64(f)
	if err != nil {
		return 0, true, err
	}
	return n, true, nil
}

func serInt(dst []byte, v reflect.Value, _ int) ([]byte, error) {
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
		n, err := floatFitsInt32From(v.Float(), v.Type().Bits())
		if err != nil {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "int", Err: err}
		}
		return appendVarint(dst, n), nil
	} else if n, ok, err := jsonNumberToInt64(v); ok {
		if err != nil {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "int", Err: err}
		}
		if n < math.MinInt32 || n > math.MaxInt32 {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "int", Err: fmt.Errorf("value %d overflows int32", n)}
		}
		return appendVarint(dst, int32(n)), nil
	}
	return nil, &SemanticError{GoType: v.Type(), AvroType: "int"}
}

func serLong(dst []byte, v reflect.Value, _ int) ([]byte, error) {
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
		n, err := floatFitsInt64From(v.Float(), v.Type().Bits())
		if err != nil {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "long", Err: err}
		}
		return appendVarlong(dst, n), nil
	} else if n, ok, err := jsonNumberToInt64(v); ok {
		if err != nil {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "long", Err: err}
		}
		return appendVarlong(dst, n), nil
	}
	return nil, &SemanticError{GoType: v.Type(), AvroType: "long"}
}

func serFloat(dst []byte, v reflect.Value, _ int) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	return appendAvroFloat32(dst, v)
}

func serDouble(dst []byte, v reflect.Value, _ int) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	return appendAvroFloat64(dst, v)
}

// finiteFloat32Overflows reports whether f is a finite float64 whose
// float32(f) narrowing is ±Inf. ±Inf and NaN inputs return false: those
// have valid float32 forms and shouldn't be rejected by callers that
// otherwise accept finite-only. Used by every site that narrows a
// float64 (wire value or Go input) to float32 — see deserDouble,
// decodeDouble, encodeDefault, jsonCoerceToFloat64, appendAvroFloat32,
// usFloat(Float64), udDouble(Float32). One predicate, one drift class.
func finiteFloat32Overflows(f float64) bool {
	return !math.IsInf(f, 0) && !math.IsNaN(f) && math.IsInf(float64(float32(f)), 0)
}

// appendAvroFloat32 appends v's Avro-float (4-byte) encoding to dst,
// rejecting:
//   - finite float64 inputs that would silently narrow to ±Inf
//   - integer inputs whose magnitude exceeds float32's 24-bit mantissa
//
// Used by serFloat (top-level), serArray.serFloat / serMap.serFloat
// (specialized container paths), and any other site that encodes a
// reflect-typed value as Avro float. Centralizing the rules ensures
// every encode path agrees on what's accepted vs rejected.
func appendAvroFloat32(dst []byte, v reflect.Value) ([]byte, error) {
	if v.CanFloat() {
		f := v.Float()
		// Narrowing float64 → float32 must not silently clamp to ±Inf.
		// Allow ±Inf and NaN pass-through.
		if v.Kind() == reflect.Float64 && finiteFloat32Overflows(f) {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "float", Err: fmt.Errorf("value %g overflows float32", f)}
		}
		return appendUint32(dst, math.Float32bits(float32(f))), nil
	}
	if v.CanInt() {
		n := v.Int()
		if n < -1<<24 || n > 1<<24 {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "float", Err: errors.New("integer overflows float32 exact precision")}
		}
		return appendUint32(dst, math.Float32bits(float32(n))), nil
	}
	if v.CanUint() {
		n := v.Uint()
		if n > 1<<24 {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "float", Err: errors.New("integer overflows float32 exact precision")}
		}
		return appendUint32(dst, math.Float32bits(float32(n))), nil
	}
	if fv, ok := jsonNumberToFloat(v); ok {
		return appendAvroFloat32(dst, fv)
	}
	return nil, &SemanticError{GoType: v.Type(), AvroType: "float"}
}

// appendAvroFloat64 is the parallel helper for Avro double. Same shape
// as appendAvroFloat32, with the float64 mantissa bound (1<<53) instead
// of float32's (1<<24) and no narrowing-to-Inf risk for float inputs.
func appendAvroFloat64(dst []byte, v reflect.Value) ([]byte, error) {
	if v.CanFloat() {
		return appendUint64(dst, math.Float64bits(v.Float())), nil
	}
	if v.CanInt() {
		n := v.Int()
		if n < -1<<53 || n > 1<<53 {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "double", Err: errors.New("integer overflows float64 exact precision")}
		}
		return appendUint64(dst, math.Float64bits(float64(n))), nil
	}
	if v.CanUint() {
		n := v.Uint()
		if n > 1<<53 {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "double", Err: errors.New("integer overflows float64 exact precision")}
		}
		return appendUint64(dst, math.Float64bits(float64(n))), nil
	}
	if fv, ok := jsonNumberToFloat(v); ok {
		return appendAvroFloat64(dst, fv)
	}
	return nil, &SemanticError{GoType: v.Type(), AvroType: "double"}
}

func serBytes(dst []byte, v reflect.Value, depth int) ([]byte, error) {
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
	return doSerBytes(dst, v, depth), nil
}

func serString(dst []byte, v reflect.Value, _ int) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	return appendAvroString(dst, v)
}

// appendAvroString appends v as an Avro string. The resolution order is
// the canonical contract for any Avro-string-typed encode site:
//
//  1. json.Number is rejected (Kind==String but numeric semantics; let
//     union dispatch route it to a numeric branch).
//  2. reflect.String → write the underlying string.
//  3. encoding.TextAppender (preferred over TextMarshaler when both
//     are implemented; appends directly into dst, saving one alloc).
//  4. encoding.TextMarshaler → MarshalText then write.
//  5. []byte slice → write bytes (named subtypes like net.IP that
//     also implement TextMarshaler are handled at step 3/4 above —
//     the text representation is preferred over the raw bytes).
//  6. Anything else → SemanticError.
//
// Used by serString (top-level), serArray.serString (array items),
// and serMap.serString (map values). The JSON encoder uses
// avroStringValue (parallel helper) since it always materializes
// the string for JSON-escaping; both helpers must remain in
// lockstep on precedence.
func appendAvroString(dst []byte, v reflect.Value) ([]byte, error) {
	if v.Type() == jsonNumberType {
		return nil, &SemanticError{GoType: v.Type(), AvroType: "string"}
	}
	if v.Kind() == reflect.String {
		return doSerString(dst, v.String()), nil
	}
	if v.CanInterface() {
		i := v.Interface()
		if a, ok := i.(encoding.TextAppender); ok {
			mark := len(dst)
			dst = appendVarlong(dst, 0) // placeholder for length
			hdrLen := len(dst) - mark
			var err error
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
	if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		return doSerString(dst, string(v.Bytes())), nil
	}
	return nil, &SemanticError{GoType: v.Type(), AvroType: "string"}
}

// avroStringValue resolves v to its canonical Avro-string textual form
// as a Go string. It is the JSON-encoder's counterpart to appendAvroString
// and must keep the same precedence (json.Number rejected; reflect.String;
// encoding.TextAppender; encoding.TextMarshaler; []byte slice). The JSON
// encoder always materializes the string to apply JSON quoting/escapes,
// so the alloc-free TextAppender-into-buffer optimization in
// appendAvroString does not apply here.
func avroStringValue(v reflect.Value) (string, error) {
	if v.Type() == jsonNumberType {
		return "", &SemanticError{GoType: v.Type(), AvroType: "string"}
	}
	if v.Kind() == reflect.String {
		return v.String(), nil
	}
	if v.CanInterface() {
		i := v.Interface()
		if a, ok := i.(encoding.TextAppender); ok {
			text, err := a.AppendText(nil)
			if err != nil {
				return "", err
			}
			return string(text), nil
		}
		if m, ok := i.(encoding.TextMarshaler); ok {
			text, err := m.MarshalText()
			if err != nil {
				return "", err
			}
			return string(text), nil
		}
	}
	if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		return string(v.Bytes()), nil
	}
	return "", &SemanticError{GoType: v.Type(), AvroType: "string"}
}

////////////////////
// STRING & BYTES //
////////////////////

func doSerBytes(dst []byte, v reflect.Value, _ int) []byte {
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

func (s *serRecord) ser(dst []byte, v reflect.Value, depth int) ([]byte, error) {
	if depth >= maxDepth {
		return nil, errTooDeep
	}
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
		// map[string]any fast path: reflect.Value.MapIndex copies the value
		// through reflect.copyVal which allocates per field for interface{}
		// element maps. Direct map access skips that.
		if t == mapStringAnyType {
			m := v.Interface().(map[string]any)
			for _, f := range s.fields {
				value, exists := m[f.name]
				if !exists {
					if !f.hasDefault {
						return nil, &SemanticError{GoType: t, AvroType: "record", Field: f.name, Err: errors.New("missing key")}
					}
					dst = append(dst, f.defaultBytes...)
					continue
				}
				// reflect.ValueOf(nil) returns the invalid zero Value,
				// which the field fn would have to special-case via
				// .IsValid() before any Type/Kind call. reflect.Zero(any)
				// produces a valid zero `any` Value that flows through
				// indirect()/serUnion's IsNil checks naturally — they
				// recognize a nil interface and route to the union's
				// null branch (or surface errIndirectNil on a non-union).
				var rv reflect.Value
				if value != nil {
					rv = reflect.ValueOf(value)
				} else {
					rv = reflect.Zero(anyType)
				}
				if dst, err = f.fn(dst, rv, depth+1); err != nil {
					return nil, recordFieldError(t, f.name, err)
				}
			}
			return dst, nil
		}
		for _, f := range s.fields {
			value := v.MapIndex(mapKeyAs(t, f.nameVal))
			if !value.IsValid() {
				if !f.hasDefault {
					return nil, &SemanticError{GoType: t, AvroType: "record", Field: f.name, Err: errors.New("missing key")}
				}
				dst = append(dst, f.defaultBytes...)
				continue
			}
			if dst, err = f.fn(dst, value, depth+1); err != nil {
				return nil, recordFieldError(t, f.name, err)
			}
		}
		return dst, nil
	}
	// Struct: try precompiled unsafe fast path. Requires addressable
	// value so we can take a pointer for unsafe field access.
	if v.CanAddr() {
		if fast := s.fast.Load(); fast != nil && fast.typ == t {
			return serRecordFast(dst, fast, v, depth+1)
		}
		if fast := compileFastSer(s.fields, s.names, &s.cache, t); fast != nil {
			s.fast.Store(fast)
			return serRecordFast(dst, fast, v, depth+1)
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
		// the null branch. The wire byte depends on null position:
		// ["null",T] → 0x00 (index 0); ["T","null"] → 0x02
		// (zigzag-encoded index 1). nullByte comes from
		// fieldMeta.nullSecond via nullUnionBytes.
		if mapping.omitzero[i] && f.avroType == "nullunion" && valueIsZero(fv) {
			nullByte, _ := nullUnionBytes(f.meta != nil && f.meta.nullSecond)
			dst = append(dst, nullByte)
			continue
		}
		if dst, err = f.fn(dst, fv, depth+1); err != nil {
			return nil, recordFieldError(t, f.name, err)
		}
	}
	return dst, nil
}

type serEnum struct {
	symbols []string
	// symbolIdx maps symbol → index for O(1) lookup; nil for small enums
	// (≤8) where the linear scan is faster than a map lookup. Built once
	// at schema-build time to avoid lock-or-race choices on the hot path.
	symbolIdx map[string]int
}

// newSerEnum constructs a serEnum with an optional lookup index.
func newSerEnum(symbols []string) *serEnum {
	e := &serEnum{symbols: symbols}
	if len(symbols) > 8 {
		e.symbolIdx = make(map[string]int, len(symbols))
		for i, sym := range symbols {
			e.symbolIdx[sym] = i
		}
	}
	return e
}

// indexOfSymbol resolves a symbol name to its ordinal.
func (s *serEnum) indexOfSymbol(needle string) (int, bool) {
	if s.symbolIdx != nil {
		i, ok := s.symbolIdx[needle]
		return i, ok
	}
	for i, symbol := range s.symbols {
		if symbol == needle {
			return i, true
		}
	}
	return 0, false
}

func (s *serEnum) ser(dst []byte, v reflect.Value, _ int) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	switch {
	case v.Kind() == reflect.String:
		needle := v.String()
		if i, ok := s.indexOfSymbol(needle); ok {
			return appendVarint(dst, int32(i)), nil
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

func (s *serArray) ser(dst []byte, v reflect.Value, depth int) ([]byte, error) {
	if depth >= maxDepth {
		return nil, errTooDeep
	}
	dst, v, l, err := serArrayPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	for i := range l {
		if dst, err = s.serItem(dst, v.Index(i), depth+1); err != nil {
			return nil, err
		}
	}
	return append(dst, 0), nil
}

// unwrapElemPtr unwraps a pointer/interface element for the
// serArray/serMap primitive specializations. One level is unwrapped
// inline; ≥2 levels dispatch to indirect(). Callers inline the Kind
// gate at each site so direct elements pay nothing.
func unwrapElemPtr(v reflect.Value) (reflect.Value, error) {
	if v.IsNil() {
		return v, errIndirectNil
	}
	v = v.Elem()
	if v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		return indirect(v)
	}
	return v, nil
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

func (s *serArray) serString(dst []byte, v reflect.Value, _ int) ([]byte, error) {
	dst, v, l, err := serArrayPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	for i := range l {
		elem := v.Index(i)
		if elem.Kind() == reflect.Interface || elem.Kind() == reflect.Pointer {
			if elem, err = unwrapElemPtr(elem); err != nil {
				return nil, &SemanticError{AvroType: "string", Err: err}
			}
		}
		if dst, err = appendAvroString(dst, elem); err != nil {
			return nil, err
		}
	}
	return append(dst, 0), nil
}

func (s *serArray) serBoolean(dst []byte, v reflect.Value, _ int) ([]byte, error) {
	dst, v, l, err := serArrayPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	for i := range l {
		elem := v.Index(i)
		if elem.Kind() == reflect.Interface || elem.Kind() == reflect.Pointer {
			if elem, err = unwrapElemPtr(elem); err != nil {
				return nil, &SemanticError{AvroType: "boolean", Err: err}
			}
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

func (s *serArray) serInt(dst []byte, v reflect.Value, _ int) ([]byte, error) {
	dst, v, l, err := serArrayPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	for i := range l {
		elem := v.Index(i)
		if elem.Kind() == reflect.Interface || elem.Kind() == reflect.Pointer {
			if elem, err = unwrapElemPtr(elem); err != nil {
				return nil, &SemanticError{AvroType: "int", Err: err}
			}
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
			n, err := floatFitsInt32From(elem.Float(), elem.Type().Bits())
			if err != nil {
				return nil, &SemanticError{GoType: elem.Type(), AvroType: "int", Err: err}
			}
			dst = appendVarint(dst, n)
		} else if n, ok, err := jsonNumberToInt64(elem); ok {
			if err != nil {
				return nil, &SemanticError{GoType: elem.Type(), AvroType: "int", Err: err}
			}
			if n < math.MinInt32 || n > math.MaxInt32 {
				return nil, &SemanticError{GoType: elem.Type(), AvroType: "int", Err: fmt.Errorf("value %d overflows int32", n)}
			}
			dst = appendVarint(dst, int32(n))
		} else {
			return nil, &SemanticError{GoType: elem.Type(), AvroType: "int"}
		}
	}
	return append(dst, 0), nil
}

func (s *serArray) serLong(dst []byte, v reflect.Value, _ int) ([]byte, error) {
	dst, v, l, err := serArrayPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	for i := range l {
		elem := v.Index(i)
		if elem.Kind() == reflect.Interface || elem.Kind() == reflect.Pointer {
			if elem, err = unwrapElemPtr(elem); err != nil {
				return nil, &SemanticError{AvroType: "long", Err: err}
			}
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
			n, err := floatFitsInt64From(elem.Float(), elem.Type().Bits())
			if err != nil {
				return nil, &SemanticError{GoType: elem.Type(), AvroType: "long", Err: err}
			}
			dst = appendVarlong(dst, n)
		} else if n, ok, err := jsonNumberToInt64(elem); ok {
			if err != nil {
				return nil, &SemanticError{GoType: elem.Type(), AvroType: "long", Err: err}
			}
			dst = appendVarlong(dst, n)
		} else {
			return nil, &SemanticError{GoType: elem.Type(), AvroType: "long"}
		}
	}
	return append(dst, 0), nil
}

func (s *serArray) serFloat(dst []byte, v reflect.Value, _ int) ([]byte, error) {
	dst, v, l, err := serArrayPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	for i := range l {
		elem := v.Index(i)
		if elem.Kind() == reflect.Interface || elem.Kind() == reflect.Pointer {
			if elem, err = unwrapElemPtr(elem); err != nil {
				return nil, &SemanticError{AvroType: "float", Err: err}
			}
		}
		if dst, err = appendAvroFloat32(dst, elem); err != nil {
			return nil, err
		}
	}
	return append(dst, 0), nil
}

func (s *serArray) serDouble(dst []byte, v reflect.Value, _ int) ([]byte, error) {
	dst, v, l, err := serArrayPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	for i := range l {
		elem := v.Index(i)
		if elem.Kind() == reflect.Interface || elem.Kind() == reflect.Pointer {
			if elem, err = unwrapElemPtr(elem); err != nil {
				return nil, &SemanticError{AvroType: "double", Err: err}
			}
		}
		if dst, err = appendAvroFloat64(dst, elem); err != nil {
			return nil, err
		}
	}
	return append(dst, 0), nil
}

type serMap struct {
	serItem serfn
}

func (s *serMap) ser(dst []byte, v reflect.Value, depth int) ([]byte, error) {
	if depth >= maxDepth {
		return nil, errTooDeep
	}
	dst, v, l, err := serMapPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	iter := v.MapRange()
	for iter.Next() {
		dst = doSerString(dst, iter.Key().String())
		if dst, err = s.serItem(dst, iter.Value(), depth+1); err != nil {
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

func (s *serMap) serString(dst []byte, v reflect.Value, _ int) ([]byte, error) {
	dst, v, l, err := serMapPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	iter := v.MapRange()
	for iter.Next() {
		dst = doSerString(dst, iter.Key().String())
		val := iter.Value()
		if val.Kind() == reflect.Interface || val.Kind() == reflect.Pointer {
			if val, err = unwrapElemPtr(val); err != nil {
				return nil, &SemanticError{AvroType: "string", Err: err}
			}
		}
		if dst, err = appendAvroString(dst, val); err != nil {
			return nil, err
		}
	}
	return append(dst, 0), nil
}

func (s *serMap) serBoolean(dst []byte, v reflect.Value, _ int) ([]byte, error) {
	dst, v, l, err := serMapPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	iter := v.MapRange()
	for iter.Next() {
		dst = doSerString(dst, iter.Key().String())
		val := iter.Value()
		if val.Kind() == reflect.Interface || val.Kind() == reflect.Pointer {
			if val, err = unwrapElemPtr(val); err != nil {
				return nil, &SemanticError{AvroType: "boolean", Err: err}
			}
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

func (s *serMap) serInt(dst []byte, v reflect.Value, _ int) ([]byte, error) {
	dst, v, l, err := serMapPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	iter := v.MapRange()
	for iter.Next() {
		dst = doSerString(dst, iter.Key().String())
		val := iter.Value()
		if val.Kind() == reflect.Interface || val.Kind() == reflect.Pointer {
			if val, err = unwrapElemPtr(val); err != nil {
				return nil, &SemanticError{AvroType: "int", Err: err}
			}
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
			n, err := floatFitsInt32From(val.Float(), val.Type().Bits())
			if err != nil {
				return nil, &SemanticError{GoType: val.Type(), AvroType: "int", Err: err}
			}
			dst = appendVarint(dst, n)
		} else if n, ok, err := jsonNumberToInt64(val); ok {
			if err != nil {
				return nil, &SemanticError{GoType: val.Type(), AvroType: "int", Err: err}
			}
			if n < math.MinInt32 || n > math.MaxInt32 {
				return nil, &SemanticError{GoType: val.Type(), AvroType: "int", Err: fmt.Errorf("value %d overflows int32", n)}
			}
			dst = appendVarint(dst, int32(n))
		} else {
			return nil, &SemanticError{GoType: val.Type(), AvroType: "int"}
		}
	}
	return append(dst, 0), nil
}

func (s *serMap) serLong(dst []byte, v reflect.Value, _ int) ([]byte, error) {
	dst, v, l, err := serMapPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	iter := v.MapRange()
	for iter.Next() {
		dst = doSerString(dst, iter.Key().String())
		val := iter.Value()
		if val.Kind() == reflect.Interface || val.Kind() == reflect.Pointer {
			if val, err = unwrapElemPtr(val); err != nil {
				return nil, &SemanticError{AvroType: "long", Err: err}
			}
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
			n, err := floatFitsInt64From(val.Float(), val.Type().Bits())
			if err != nil {
				return nil, &SemanticError{GoType: val.Type(), AvroType: "long", Err: err}
			}
			dst = appendVarlong(dst, n)
		} else if n, ok, err := jsonNumberToInt64(val); ok {
			if err != nil {
				return nil, &SemanticError{GoType: val.Type(), AvroType: "long", Err: err}
			}
			dst = appendVarlong(dst, n)
		} else {
			return nil, &SemanticError{GoType: val.Type(), AvroType: "long"}
		}
	}
	return append(dst, 0), nil
}

func (s *serMap) serFloat(dst []byte, v reflect.Value, _ int) ([]byte, error) {
	dst, v, l, err := serMapPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	iter := v.MapRange()
	for iter.Next() {
		dst = doSerString(dst, iter.Key().String())
		val := iter.Value()
		if val.Kind() == reflect.Interface || val.Kind() == reflect.Pointer {
			if val, err = unwrapElemPtr(val); err != nil {
				return nil, &SemanticError{AvroType: "float", Err: err}
			}
		}
		if dst, err = appendAvroFloat32(dst, val); err != nil {
			return nil, err
		}
	}
	return append(dst, 0), nil
}

func (s *serMap) serDouble(dst []byte, v reflect.Value, _ int) ([]byte, error) {
	dst, v, l, err := serMapPreamble(dst, v)
	if err != nil || l == 0 {
		return dst, err
	}
	iter := v.MapRange()
	for iter.Next() {
		dst = doSerString(dst, iter.Key().String())
		val := iter.Value()
		if val.Kind() == reflect.Interface || val.Kind() == reflect.Pointer {
			if val, err = unwrapElemPtr(val); err != nil {
				return nil, &SemanticError{AvroType: "double", Err: err}
			}
		}
		if dst, err = appendAvroFloat64(dst, val); err != nil {
			return nil, err
		}
	}
	return append(dst, 0), nil
}

type serSize struct {
	n int
}

func (s *serSize) ser(dst []byte, v reflect.Value, depth int) ([]byte, error) {
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

// extractTime returns v's time.Time content when v is a time.Time
// directly or a string parseable via RFC 3339. Mirrors the
// timeType-arm/tryParseTimeString-arm pattern used at every
// time-logical encode site (binary ser + JSON ser) so a future change
// to the accepted-input set lands in one place.
func extractTime(v reflect.Value) (time.Time, bool) {
	if v.Type() == timeType {
		return v.Interface().(time.Time), true
	}
	return tryParseTimeString(v)
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

// serTimeAsLong is the shared body of the six time-logical "long"
// serializers (timestamp / local-timestamp at millis, micros, nanos).
// Each per-logical wrapper passes the corresponding timeTo<Logical>
// converter. The pattern mirrors deserTimeAsLong on the decode side.
func serTimeAsLong(dst []byte, v reflect.Value, depth int, conv func(time.Time) (int64, error)) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if t, ok := extractTime(v); ok {
		n, err := conv(t)
		if err != nil {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "long", Err: err}
		}
		return appendVarlong(dst, n), nil
	}
	return serLong(dst, v, depth)
}

func serTimestampMillis(dst []byte, v reflect.Value, depth int) ([]byte, error) {
	return serTimeAsLong(dst, v, depth, timeToTimestampMillis)
}

func serTimestampMicros(dst []byte, v reflect.Value, depth int) ([]byte, error) {
	return serTimeAsLong(dst, v, depth, timeToTimestampMicros)
}

func serTimestampNanos(dst []byte, v reflect.Value, depth int) ([]byte, error) {
	return serTimeAsLong(dst, v, depth, timeToTimestampNanos)
}

// Local-timestamp serializers encode wall-clock fields as if UTC, matching
// Java's TimeConversions.LocalTimestampMillisConversion (toInstant(ZoneOffset.UTC))
// and fastavro. See timeToLocalTimestampMillis in logical.go for rationale.

func serLocalTimestampMillis(dst []byte, v reflect.Value, depth int) ([]byte, error) {
	return serTimeAsLong(dst, v, depth, timeToLocalTimestampMillis)
}

func serLocalTimestampMicros(dst []byte, v reflect.Value, depth int) ([]byte, error) {
	return serTimeAsLong(dst, v, depth, timeToLocalTimestampMicros)
}

func serLocalTimestampNanos(dst []byte, v reflect.Value, depth int) ([]byte, error) {
	return serTimeAsLong(dst, v, depth, timeToLocalTimestampNanos)
}

func serDate(dst []byte, v reflect.Value, depth int) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.Type() == timeType {
		d, err := timeToDate(v.Interface().(time.Time))
		if err != nil {
			return nil, &SemanticError{GoType: timeType, AvroType: "date", Err: err}
		}
		return appendVarint(dst, d), nil
	}
	if t, ok := tryParseDateString(v); ok {
		d, err := timeToDate(t)
		if err != nil {
			return nil, &SemanticError{GoType: v.Type(), AvroType: "date", Err: err}
		}
		return appendVarint(dst, d), nil
	}
	return serInt(dst, v, depth)
}

// serTimeMillis encodes a time-millis (time-of-day milliseconds) value.
// Accepts time.Duration (canonical) and time.Time as a convenience
// escape hatch; the time.Time arm silently discards the date and zone
// since the wire format physically can't represent them. Documented
// in README §Logical Types. time.Duration is always lossless;
// time.Time round-trips preserve time-of-day only.
func serTimeMillis(dst []byte, v reflect.Value, depth int) ([]byte, error) {
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
	return serInt(dst, v, depth)
}

// serTimeMicros mirrors serTimeMillis at microsecond resolution —
// see that function's doc for the time.Time-escape-hatch lossiness.
func serTimeMicros(dst []byte, v reflect.Value, depth int) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.Type() == durationType {
		return appendVarlong(dst, time.Duration(v.Int()).Microseconds()), nil
	}
	if v.Type() == timeType {
		t := v.Interface().(time.Time)
		d := time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute + time.Duration(t.Second())*time.Second + time.Duration(t.Nanosecond())
		return appendVarlong(dst, d.Microseconds()), nil
	}
	return serLong(dst, v, depth)
}

func serDuration(dst []byte, v reflect.Value, depth int) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.Type() == avroDurationType {
		b := v.Interface().(Duration).Bytes()
		return append(dst, b[:]...), nil
	}
	return (&serSize{12}).ser(dst, v, depth)
}

// decimalRatFor extracts a *big.Rat from v for decimal-logical-type
// encoding. Tries the direct big.Rat type first (the canonical input),
// then falls through to tryCoerceToRat which handles float, json.Number,
// and numeric strings. Used by both Avro-binary and Avro-JSON encoders
// so the accepted-input set stays in lockstep across the two paths.
//
// Three-valued return: (rat, true, nil) on success; (nil, false, nil)
// when v was not a recognized number-form (caller may fall back to
// raw-bytes encoding paths); (nil, false, err) when v was clearly a
// number-form but rejected for safety (e.g. bounded exponent) — caller
// must propagate err rather than fall through, so a hostile
// json.Number isn't silently re-encoded as raw bytes.
func decimalRatFor(v reflect.Value) (*big.Rat, bool, error) {
	if v.Type() == bigRatType {
		tmp := v.Interface().(big.Rat)
		return &tmp, true, nil
	}
	return tryCoerceToRat(v)
}

// tryCoerceToRat attempts to convert a value to *big.Rat for decimal logical
// types. Accepts float64, json.Number, and numeric strings (e.g. "3.14").
//
// For floats, the shortest-decimal formatting is used (strconv.FormatFloat
// with prec=-1) rather than (*big.Rat).SetFloat64. SetFloat64 exposes the
// exact binary mantissa: float64(0.33) becomes 5944751508129055/18014398509481984,
// whose natural decimal scale is ~52 digits. ratToUnscaled would then
// reject every non-power-of-2 float against any finite schema scale.
// Java's BigDecimal.valueOf(double) takes the same shortest-decimal
// approach via Double.toString; the user-visible value 0.33 becomes
// the big.Rat 33/100, which rounds exactly at schema scale 2.
//
// Returns (nil, false, err) when the input was clearly a number form
// (json.Number, or a reflect.String that parses as a number) but its
// magnitude exceeds decimalScaleLimit — see boundedRatFromString.
func tryCoerceToRat(v reflect.Value) (*big.Rat, bool, error) {
	if v.CanFloat() {
		f := v.Float()
		// Reject non-finite values: NaN/±Inf are not in the decimal value
		// set. Java's BigDecimal.valueOf(double) rejects with
		// NumberFormatException; fastavro raises InvalidOperation.
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, false, nil
		}
		// Float magnitudes are bounded by float64's ~310-digit FormatFloat
		// output; no decimalScaleLimit guard needed on this arm.
		if r, ok := new(big.Rat).SetString(strconv.FormatFloat(f, 'f', -1, 64)); ok {
			return r, true, nil
		}
		return nil, false, nil
	}
	if v.Type() == jsonNumberType {
		s := v.Interface().(json.Number).String()
		r, ok, err := boundedRatFromString(s)
		if err != nil {
			return nil, false, fmt.Errorf("json.Number %q: %w", s, err)
		}
		if ok {
			return r, true, nil
		}
		// json.Number's type guarantees the input was meant as a number;
		// a parse failure (e.g. malformed exponent) is fatal too.
		return nil, false, fmt.Errorf("invalid decimal number %q", s)
	}
	if v.Kind() == reflect.String {
		s := v.String()
		r, ok, err := boundedRatFromString(s)
		if err != nil {
			return nil, false, fmt.Errorf("decimal string %q: %w", s, err)
		}
		if ok {
			return r, true, nil
		}
		// Plain reflect.String values that don't parse as a number may
		// be legitimately destined for the raw-bytes fallback (a
		// non-numeric string field), so silence is appropriate here.
	}
	return nil, false, nil
}

type serBytesDecimal struct {
	precision int
	scale     int
}

func (s *serBytesDecimal) serRat(dst []byte, r *big.Rat) ([]byte, error) {
	unscaled, err := ratToUnscaled(r, s.scale)
	if err != nil {
		return nil, &SemanticError{GoType: bigRatType, AvroType: "bytes", Err: err}
	}
	if err := checkDecimalPrecision(unscaled, s.precision); err != nil {
		return nil, &SemanticError{GoType: bigRatType, AvroType: "bytes", Err: err}
	}
	b := bigIntToBytes(unscaled)
	dst = appendVarlong(dst, int64(len(b)))
	return append(dst, b...), nil
}

func (s *serBytesDecimal) ser(dst []byte, v reflect.Value, depth int) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.Type() == bigRatType {
		tmp := v.Interface().(big.Rat)
		return s.serRat(dst, &tmp)
	}
	r, ok, err := tryCoerceToRat(v)
	if err != nil {
		return nil, &SemanticError{GoType: v.Type(), AvroType: "bytes", Err: err}
	}
	if ok {
		return s.serRat(dst, r)
	}
	return serBytes(dst, v, depth)
}

type serFixedDecimal struct {
	size      int
	precision int
	scale     int
}

func (s *serFixedDecimal) serRat(dst []byte, r *big.Rat) ([]byte, error) {
	unscaled, err := ratToUnscaled(r, s.scale)
	if err != nil {
		return nil, &SemanticError{GoType: bigRatType, AvroType: "fixed", Err: err}
	}
	if err := checkDecimalPrecision(unscaled, s.precision); err != nil {
		return nil, &SemanticError{GoType: bigRatType, AvroType: "fixed", Err: err}
	}
	b := bigIntToBytes(unscaled)
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

func (s *serFixedDecimal) ser(dst []byte, v reflect.Value, depth int) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if v.Type() == bigRatType {
		tmp := v.Interface().(big.Rat)
		return s.serRat(dst, &tmp)
	}
	r, ok, err := tryCoerceToRat(v)
	if err != nil {
		return nil, &SemanticError{GoType: v.Type(), AvroType: "fixed", Err: err}
	}
	if ok {
		return s.serRat(dst, r)
	}
	return (&serSize{s.size}).ser(dst, v, depth)
}

type serBigDecimal struct{}

func (s *serBigDecimal) ser(dst []byte, v reflect.Value, depth int) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	r, ok, err := decimalRatFor(v)
	if err != nil {
		return nil, &SemanticError{GoType: v.Type(), AvroType: "bytes", Err: err}
	}
	if ok {
		return s.serRat(dst, r, v.Type())
	}
	// Fall back to plain bytes: preserves opaque-bytes pass-through
	// for users who construct the wire payload manually.
	return serBytes(dst, v, depth)
}

func (s *serBigDecimal) serRat(dst []byte, r *big.Rat, srcType reflect.Type) ([]byte, error) {
	inner, err := buildBigDecimalPayload(r)
	if err != nil {
		return nil, &SemanticError{GoType: srcType, AvroType: "bytes", Err: err}
	}
	// Outer bytes framing: zigzag-len(inner) || inner.
	dst = appendVarlong(dst, int64(len(inner)))
	return append(dst, inner...), nil
}

// buildBigDecimalPayload returns the big-decimal inner payload bytes
// (length-prefixed unscaled || zigzag scale). Errors on rationals
// with no finite decimal expansion.
func buildBigDecimalPayload(r *big.Rat) ([]byte, error) {
	scale, ok := finiteScale(r)
	if !ok {
		return nil, fmt.Errorf("big.Rat %s has no finite decimal expansion; big-decimal cannot encode this value", r.RatString())
	}
	mult := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	num := new(big.Int).Mul(r.Num(), mult)
	unscaled, _ := new(big.Int).QuoRem(num, r.Denom(), new(big.Int))
	// Remainder is provably 0 since finiteScale chose s to make
	// 10^s / Denom an integer.
	uBytes := bigIntToBytes(unscaled)
	// Inner payload: zigzag-len(uBytes) || uBytes || zigzag(scale).
	inner := appendVarlong(nil, int64(len(uBytes)))
	inner = append(inner, uBytes...)
	inner = appendVarlong(inner, int64(scale))
	return inner, nil
}

// finiteScale returns the smallest s >= 0 such that r * 10^s is an
// integer, or (0, false) if r has no finite decimal expansion (or
// would require a scale beyond decimalScaleLimit — same outcome
// from the caller's perspective).
// For denominator d = 2^a * 5^b returns max(a, b).
//
// The 2-power factor is extracted via TrailingZeroBits() (one
// optimized call) instead of a per-bit loop. The 5-power factor's
// max possible value is bounded upfront via BitLen()/log2(5) so an
// attacker-constructed denominator like 10^65536 (which would take
// ~65536 iterations of QuoRem on a 152K-digit big.Int and burn ~1.4
// CPU seconds per 6-byte wire input) short-circuits to (0, false)
// in O(1) bit-length math. Without that bound the inner loop is
// O(n²) in scale and gives an attacker ~10^8× CPU amplification on
// the binary serBigDecimal and JSON big-decimal encode paths.
func finiteScale(r *big.Rat) (int, bool) {
	d := new(big.Int).Set(r.Denom())
	a := int(d.TrailingZeroBits())
	if a > decimalScaleLimit {
		return 0, false
	}
	if a > 0 {
		d.Rsh(d, uint(a))
	}
	// Upper bound on b: 5^b ≤ d means b ≤ d.BitLen() / log2(5).
	// log2(5) ≈ 2.3219; conservatively use 2 (slightly overestimates
	// b but stays correct since we only reject when b > limit). If
	// the bound already exceeds the cap, short-circuit.
	if d.BitLen()/2 > decimalScaleLimit {
		return 0, false
	}
	b := 0
	five := big.NewInt(5)
	rem := new(big.Int)
	q := new(big.Int)
	one := big.NewInt(1)
	for d.Cmp(one) != 0 {
		if b >= decimalScaleLimit {
			return 0, false
		}
		q.QuoRem(d, five, rem)
		if rem.Sign() != 0 {
			return 0, false
		}
		d.Set(q)
		b++
	}
	if a > b {
		return a, true
	}
	return b, true
}

// ratToUnscaled returns the unscaled big.Int (rat * 10^scale / denom) when
// the value is exactly representable at the requested scale, or an error
// when the conversion would require rounding. Used by both
// serBytesDecimal/serFixedDecimal (which separately need the unscaled
// value to validate against precision) and the JSON encoder.
//
// Java's DecimalConversion.validate uses RoundingMode.UNNECESSARY,
// throwing AvroTypeException when the value's scale exceeds the schema's
// (Conversions.java:151). fastavro's prepare_bytes_decimal raises
// ValueError when delta = exp + scale < 0 (_logical_writers_py.py:131).
// big.NewRat(1, 3) at scale=2 has remainder 1 after multiplying by 100,
// matching Java's "scale=infinite > schema scale=2" rejection.
func ratToUnscaled(r *big.Rat, scale int) (*big.Int, error) {
	s := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	num := new(big.Int).Mul(r.Num(), s)
	unscaled, rem := new(big.Int).QuoRem(num, r.Denom(), new(big.Int))
	if rem.Sign() != 0 {
		return nil, fmt.Errorf("decimal value %s cannot be represented at scale %d without rounding", r.RatString(), scale)
	}
	return unscaled, nil
}

// checkDecimalPrecision rejects unscaled values whose decimal-digit count
// exceeds precision. Per Avro 1.12 spec: precision is "the (maximum)
// precision of decimals stored in this type". Java's Conversions.DecimalConversion,
// fastavro, and hamba/avro all enforce this on encode.
func checkDecimalPrecision(unscaled *big.Int, precision int) error {
	if precision <= 0 {
		return nil
	}
	digits := decimalDigitCount(unscaled)
	if digits > precision {
		return fmt.Errorf("decimal value has %d digits, exceeds schema precision %d", digits, precision)
	}
	return nil
}

// decimalDigitCount returns the number of decimal digits in |i|.
// Uses big.Int.BitLen as a fast lower-bound check, then String() for the
// exact count when ambiguous.
func decimalDigitCount(i *big.Int) int {
	if i.Sign() == 0 {
		return 1
	}
	abs := new(big.Int).Abs(i)
	return len(abs.String())
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
func serFixedUUIDReflect(dst []byte, v reflect.Value, depth int) ([]byte, error) {
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
	return (&serSize{16}).ser(dst, v, depth)
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

func serUUID(dst []byte, v reflect.Value, depth int) ([]byte, error) {
	v, err := indirect(v)
	if err != nil {
		return nil, err
	}
	if isUUIDType(v.Type()) {
		var u [16]byte
		reflect.Copy(reflect.ValueOf(&u).Elem(), v)
		return doSerString(dst, uuidToString(u)), nil
	}
	return serString(dst, v, depth)
}
