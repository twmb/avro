package avro

import (
	"errors"
	"fmt"
	"sync"
)

// skipfn advances past an encoded Avro value without storing it. We thread the
// slab through purely to share the recursion-depth bound with deserfn
// (sl.depth); skips otherwise don't need it.
type skipfn func(src []byte, sl *slab) ([]byte, error)

func skipNull(src []byte, _ *slab) ([]byte, error) {
	return src, nil
}

// needLen reports a ShortBufferError when src is too short to read n bytes.
// It is the only constructor of the fixed-size short-buffer error, so every
// fixed-length read and skip agrees on the shape.
func needLen(src []byte, n int, typ string) error {
	if len(src) < n {
		return &ShortBufferError{Type: typ, Need: n, Have: len(src)}
	}
	return nil
}

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

func skipFixed(size int) skipfn {
	return func(src []byte, _ *slab) ([]byte, error) { return skipBytesN(src, size, "fixed") }
}

type skipRecordFields struct {
	once   sync.Once
	fields []skipfn
	node   *schemaNode
	mbw    *minBytesWalk
}

func skipRecord(w *schemaNode, mbw *minBytesWalk) skipfn {
	s := &skipRecordFields{node: w, mbw: mbw}
	return func(src []byte, sl *slab) ([]byte, error) {
		if sl.depth >= maxDepth {
			return nil, errTooDeep
		}
		sl.depth++
		defer func() { sl.depth-- }()
		s.once.Do(func() {
			// We use the operation's walk, threaded down from Resolve, not a
			// fresh one: the schema chooses how many records a dropped
			// subtree references, and a per-record walk would multiply the
			// per-walk allowance by that count. This runs at decode time on
			// a shared walk, so minBytesOf locks; see minBytesWalk.mu.
			s.fields = make([]skipfn, len(s.node.fields))
			for i := range s.node.fields {
				s.fields[i] = buildSkip(s.node.fields[i].node, s.mbw)
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

// skipBlocks iterates Avro block-framed data, calling inner once per item or
// entry. Shared by skipArray and skipMap.
//
// blockType labels readBlockHeader errors ("array" or "map"). The value-path
// block walkers pass the same labels, so both paths report identical text for
// identical corruption; readBlockHeader's formats append their own "block".
// Negative-count byte-size framing fast-skips the whole block via byteSize.
// Otherwise totalGuard (when non-nil) gates the per-block count, and inner
// runs for each item or entry.
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
	// minEntryBytes = 1 (key length varint, >=1 byte for an empty key) plus
	// the value's minimum wire bytes, identical to deserMap.minEntryBytes.
	minEntryBytes := mapEntryMinBytes(mbw.minBytesOf(w.values))
	return func(src []byte, sl *slab) ([]byte, error) {
		return skipBlocks(src, sl, "map",
			// Bound the block count against the remaining buffer, as deserMap
			// and skipArray do. Without it the `for range int(count)` loop
			// truncates a count above 2^31 on a 32-bit build and mis-frames
			// what follows. minEntryBytes is at least 1, so a legitimate
			// block is never rejected.
			func(count, _ int64, srcLen int) error {
				return checkMapBlockBounds(count, srcLen, minEntryBytes)
			},
			func(src []byte, sl *slab) ([]byte, error) {
				src, err := skipBytes(src, sl) // the entry key
				if err != nil {
					return nil, err
				}
				return valueSkip(src, sl)
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
	// A string is a length-prefixed byte run on the wire, so skipping one
	// *is* skipping bytes; the shared reader already labels its
	// short-buffer error "bytes" for both.
	"string": skipBytes,
}

// buildSkip compiles a skipper for writer node w. mbw is the one min-bytes
// walk of the operation that reached here, and every arm passes it down,
// including the record arm, whose compile is deferred to decode time. The
// schema picks how many containers, branches and records there are, so a
// per-unit allowance would be the per-walk bound times a number the schema
// author chooses.
func buildSkip(w *schemaNode, mbw *minBytesWalk) skipfn {
	if f, ok := primitiveSkips[w.kind]; ok {
		return f
	}
	switch w.kind {
	case "record":
		return skipRecord(w, mbw)
	case "enum":
		return skipInt // an enum is its symbol index, encoded as an int
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

// skipUnbuildable stands in where a field's skipper could not be compiled
// because the node behind it is missing. Reaching it is a wiring bug, not bad
// input, so it errors rather than dereferencing nil.
func skipUnbuildable(_ []byte, _ *slab) ([]byte, error) {
	return nil, errors.New("avro: internal: record field has no skipper")
}
