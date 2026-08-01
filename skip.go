package avro

import (
	"fmt"
	"reflect"
	"sync"
)

// skipfn advances past an encoded Avro value without storing it. It threads
// the slab through purely to share the recursion-depth bound with deserfn
// (sl.depth); skips otherwise don't need it.
type skipfn func(src []byte, sl *slab) ([]byte, error)

func skipNull(src []byte, _ *slab) ([]byte, error) {
	return src, nil
}

// needLen reports a ShortBufferError when src is too short to read n bytes.
// Single constructor of the fixed-size {Type + Need + Have} short-buffer
// shape so every fixed-length read/skip agrees on it (skip paths via
// skipBytesN; decode paths — deserFixed, deserDuration, deserFixedDecimal,
// deserFixedUUIDReflect, udDuration, readFixedUUID — call it directly).
func needLen(src []byte, n int, typ string) error {
	if len(src) < n {
		return &ShortBufferError{Type: typ, Need: n, Have: len(src)}
	}
	return nil
}

// skipBytesN advances past n bytes (boolean/float/double/fixed). One
// helper for every fixed-size skip so they agree on the ShortBufferError
// shape (Type + Need + Have).
func skipBytesN(src []byte, n int, typ string) ([]byte, error) {
	if err := needLen(src, n, typ); err != nil {
		return nil, err
	}
	return src[n:], nil
}

func skipBoolean(src []byte, _ *slab) ([]byte, error) { return skipBytesN(src, 1, "boolean") }
func skipFloat(src []byte, _ *slab) ([]byte, error)   { return skipBytesN(src, 4, "float") }
func skipDouble(src []byte, _ *slab) ([]byte, error)  { return skipBytesN(src, 8, "double") }

func skipInt(src []byte, _ *slab) ([]byte, error) {
	_, src, err := readVarint(src)
	return src, err
}

func skipLong(src []byte, _ *slab) ([]byte, error) {
	_, src, err := readVarlong(src)
	return src, err
}

func skipBytes(src []byte, _ *slab) ([]byte, error) {
	n, src, err := readLength(src, "bytes")
	if err != nil {
		return nil, err
	}
	return src[n:], nil
}

func skipString(src []byte, sl *slab) ([]byte, error) {
	return skipBytes(src, sl)
}

func skipFixed(size int) skipfn {
	return func(src []byte, _ *slab) ([]byte, error) { return skipBytesN(src, size, "fixed") }
}

func skipEnum(src []byte, sl *slab) ([]byte, error) {
	return skipInt(src, sl)
}

type skipRecordFields struct {
	once   sync.Once
	fields []skipfn
	node   *schemaNode
}

func skipRecord(w *schemaNode) skipfn {
	s := &skipRecordFields{node: w}
	return func(src []byte, sl *slab) ([]byte, error) {
		if sl.depth >= maxDepth {
			return nil, errTooDeep
		}
		sl.depth++
		defer func() { sl.depth-- }()
		s.once.Do(func() {
			// One walk for this record's whole field set: the fields can point
			// many containers at one shared subtree, and compiling them here
			// (once per record, guarded by once) with a fresh walk per field
			// would recompute that subtree per field. Sharing bounds the record's
			// compile cost the way finalize and resolve bound theirs; the cost
			// across DIFFERENT records is bounded separately because each
			// record's once.Do fires only when the wire actually reaches it.
			mbw := newMinBytesWalk()
			s.fields = make([]skipfn, len(s.node.fields))
			for i := range s.node.fields {
				s.fields[i] = buildSkip(s.node.fields[i].node, mbw)
			}
		})
		var err error
		for _, f := range s.fields {
			if src, err = f(src, sl); err != nil {
				return nil, err
			}
		}
		return src, nil
	}
}

// skipBlocks iterates Avro block-framed data, invoking inner once per
// item/entry. blockType labels readBlockHeader errors ("array" / "map",
// the same labels the value-path block walkers pass, so the two paths
// report identical text for identical corruption — readBlockHeader's
// formats append their own "block"). When negative-count byte-size
// framing is present the
// block is fast-skipped via byteSize; otherwise totalGuard (when non-nil)
// gates the per-block count, and inner is called for each item/entry.
// Shared by skipArray and skipMap.
func skipBlocks(src []byte, sl *slab, blockType string,
	totalGuard func(count, total int64, srcLen int) error,
	inner func(src []byte, sl *slab) ([]byte, error),
) ([]byte, error) {
	if sl.depth >= maxDepth {
		return nil, errTooDeep
	}
	sl.depth++
	defer func() { sl.depth-- }()
	var total int64
	for {
		count, byteSize, rest, end, err := readBlockHeader(src, blockType, true)
		if err != nil {
			return nil, err
		}
		src = rest
		if end {
			return src, nil
		}
		if byteSize > 0 {
			// Negative-count framing: skip the whole block by byte-size.
			src = src[byteSize:]
			continue
		}
		if totalGuard != nil {
			if err := totalGuard(count, total, len(src)); err != nil {
				return nil, err
			}
			total += count
		}
		for range int(count) {
			if src, err = inner(src, sl); err != nil {
				return nil, err
			}
		}
	}
}

func skipArray(w *schemaNode, mbw *minBytesWalk) skipfn {
	itemSkip := buildSkip(w.items, mbw)
	minItemBytes := mbw.minBytesOf(w.items)
	return func(src []byte, sl *slab) ([]byte, error) {
		return skipBlocks(src, sl, "array",
			func(count, total int64, srcLen int) error {
				return checkArrayBlockBounds(count, total, srcLen, minItemBytes)
			},
			itemSkip)
	}
}

func skipMap(w *schemaNode, mbw *minBytesWalk) skipfn {
	valueSkip := buildSkip(w.values, mbw)
	// minEntryBytes = 1 (key length varint, ≥1 byte for an empty key) +
	// the value's minimum wire bytes — identical to deserMap.minEntryBytes.
	minEntryBytes := 1 + mbw.minBytesOf(w.values)
	return func(src []byte, sl *slab) ([]byte, error) {
		return skipBlocks(src, sl, "map",
			// Bound the block count against the remaining buffer, matching
			// deserMap (deser.go) and skipArray's checkArrayBlockBounds.
			// Without it, skipBlocks' `for range int(count)` loop truncates
			// a count above 2^31 on a 32-bit build, mis-framing the skip of
			// subsequent bytes; on 64-bit the loop is buffer-bounded already,
			// but this keeps deserMap, skipArray, and skipMap on one rule.
			// minEntryBytes ≥ 1, so this is always the buffer-relative bound
			// and never false-rejects a legitimate block (each entry occupies
			// at least minEntryBytes wire bytes).
			func(count, _ int64, srcLen int) error {
				return checkMapBlockBounds(count, srcLen, minEntryBytes)
			},
			func(src []byte, sl *slab) ([]byte, error) {
				// Skip key (string), then value.
				if src, err := skipString(src, sl); err != nil {
					return nil, err
				} else if src, err = valueSkip(src, sl); err != nil {
					return nil, err
				} else {
					return src, nil
				}
			})
	}
}

func skipUnion(w *schemaNode, mbw *minBytesWalk) skipfn {
	branchSkips := make([]skipfn, len(w.branches))
	for i, br := range w.branches {
		branchSkips[i] = buildSkip(br, mbw)
	}
	return func(src []byte, sl *slab) ([]byte, error) {
		if sl.depth >= maxDepth {
			return nil, errTooDeep
		}
		sl.depth++
		defer func() { sl.depth-- }()
		idx, src, err := readVarint(src)
		if err != nil {
			return nil, err
		}
		if idx < 0 || int(idx) >= len(branchSkips) {
			return nil, fmt.Errorf("union index %d out of range [0, %d)", idx, len(branchSkips))
		}
		return branchSkips[idx](src, sl)
	}
}

var primitiveSkips = map[string]skipfn{
	"null":    skipNull,
	"boolean": skipBoolean,
	"int":     skipInt,
	"long":    skipLong,
	"float":   skipFloat,
	"double":  skipDouble,
	"bytes":   skipBytes,
	"string":  skipString,
}

// buildSkip compiles a skipper for writer node w. mbw is the min-bytes walk
// shared across the containers reached WITHOUT crossing a record boundary — the
// array/map/union arms consult it and pass it down, so a record-free chain of N
// nested containers over one shared subtree computes that subtree once. A record
// breaks the chain: skipRecord compiles its fields lazily and starts its own
// walk (see there), so mbw does not thread into it.
func buildSkip(w *schemaNode, mbw *minBytesWalk) skipfn {
	if f, ok := primitiveSkips[w.kind]; ok {
		return f
	}
	switch w.kind {
	case "record":
		return skipRecord(w)
	case "enum":
		return skipEnum
	case "array":
		return skipArray(w, mbw)
	case "map":
		return skipMap(w, mbw)
	case "union":
		return skipUnion(w, mbw)
	case "fixed":
		return skipFixed(w.size)
	default:
		return func(src []byte, _ *slab) ([]byte, error) {
			return nil, fmt.Errorf("cannot skip unknown type %q", w.kind)
		}
	}
}

// skipToDeser wraps a skipfn as a deserfn that ignores the reflect.Value.
func skipToDeser(skip skipfn) deserfn {
	return func(src []byte, _ reflect.Value, sl *slab) ([]byte, error) {
		return skip(src, sl)
	}
}
