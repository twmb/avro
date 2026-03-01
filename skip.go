package avro

import (
	"fmt"
	"reflect"
	"sync"
)

// skipfn advances past an encoded Avro value without storing it.
type skipfn func(src []byte) ([]byte, error)

func skipNull(src []byte) ([]byte, error) {
	return src, nil
}

func skipBoolean(src []byte) ([]byte, error) {
	if len(src) < 1 {
		return nil, &ShortBufferError{Type: "boolean"}
	}
	return src[1:], nil
}

func skipInt(src []byte) ([]byte, error) {
	_, src, err := readVarint(src)
	return src, err
}

func skipLong(src []byte) ([]byte, error) {
	_, src, err := readVarlong(src)
	return src, err
}

func skipFloat(src []byte) ([]byte, error) {
	if len(src) < 4 {
		return nil, &ShortBufferError{Type: "float", Need: 4, Have: len(src)}
	}
	return src[4:], nil
}

func skipDouble(src []byte) ([]byte, error) {
	if len(src) < 8 {
		return nil, &ShortBufferError{Type: "double", Need: 8, Have: len(src)}
	}
	return src[8:], nil
}

func skipBytes(src []byte) ([]byte, error) {
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
	return src[n:], nil
}

func skipString(src []byte) ([]byte, error) {
	return skipBytes(src)
}

func skipFixed(size int) skipfn {
	return func(src []byte) ([]byte, error) {
		if len(src) < size {
			return nil, &ShortBufferError{Type: "fixed", Need: size, Have: len(src)}
		}
		return src[size:], nil
	}
}

func skipEnum(src []byte) ([]byte, error) {
	return skipInt(src)
}

type skipRecordFields struct {
	once   sync.Once
	fields []skipfn
	node   *schemaNode
}

func skipRecord(w *schemaNode) skipfn {
	s := &skipRecordFields{node: w}
	return func(src []byte) ([]byte, error) {
		s.once.Do(func() {
			s.fields = make([]skipfn, len(s.node.fields))
			for i := range s.node.fields {
				s.fields[i] = buildSkip(s.node.fields[i].node)
			}
		})
		var err error
		for _, f := range s.fields {
			if src, err = f(src); err != nil {
				return nil, err
			}
		}
		return src, nil
	}
}

func skipArray(w *schemaNode) skipfn {
	itemSkip := buildSkip(w.items)
	return func(src []byte) ([]byte, error) {
		var err error
		for {
			var count int64
			count, src, err = readVarlong(src)
			if err != nil {
				return nil, err
			}
			if count == 0 {
				return src, nil
			}
			if count < 0 {
				count = -count
				if count < 0 {
					return nil, fmt.Errorf("invalid array block count")
				}
				// Negative count means the block has a byte size we can skip.
				var byteSize int64
				byteSize, src, err = readVarlong(src)
				if err != nil {
					return nil, err
				}
				if int(byteSize) > len(src) {
					return nil, &ShortBufferError{Type: "array block", Need: int(byteSize), Have: len(src)}
				}
				src = src[byteSize:]
				continue
			}
			for range int(count) {
				if src, err = itemSkip(src); err != nil {
					return nil, err
				}
			}
		}
	}
}

func skipMap(w *schemaNode) skipfn {
	valueSkip := buildSkip(w.values)
	return func(src []byte) ([]byte, error) {
		var err error
		for {
			var count int64
			count, src, err = readVarlong(src)
			if err != nil {
				return nil, err
			}
			if count == 0 {
				return src, nil
			}
			if count < 0 {
				count = -count
				if count < 0 {
					return nil, fmt.Errorf("invalid map block count")
				}
				var byteSize int64
				byteSize, src, err = readVarlong(src)
				if err != nil {
					return nil, err
				}
				if int(byteSize) > len(src) {
					return nil, &ShortBufferError{Type: "map block", Need: int(byteSize), Have: len(src)}
				}
				src = src[byteSize:]
				continue
			}
			for range int(count) {
				// Skip key (string).
				if src, err = skipString(src); err != nil {
					return nil, err
				}
				// Skip value.
				if src, err = valueSkip(src); err != nil {
					return nil, err
				}
			}
		}
	}
}

func skipUnion(w *schemaNode) skipfn {
	branchSkips := make([]skipfn, len(w.branches))
	for i, br := range w.branches {
		branchSkips[i] = buildSkip(br)
	}
	return func(src []byte) ([]byte, error) {
		idx, src, err := readVarint(src)
		if err != nil {
			return nil, err
		}
		if idx < 0 || int(idx) >= len(branchSkips) {
			return nil, fmt.Errorf("union index %d out of range [0, %d)", idx, len(branchSkips))
		}
		return branchSkips[idx](src)
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

func buildSkip(w *schemaNode) skipfn {
	if f, ok := primitiveSkips[w.kind]; ok {
		return f
	}
	switch w.kind {
	case "record":
		return skipRecord(w)
	case "enum":
		return skipEnum
	case "array":
		return skipArray(w)
	case "map":
		return skipMap(w)
	case "union":
		return skipUnion(w)
	case "fixed":
		return skipFixed(w.size)
	default:
		return func(src []byte) ([]byte, error) {
			return nil, fmt.Errorf("cannot skip unknown type %q", w.kind)
		}
	}
}

// skipToDeser wraps a skipfn as a deserfn that ignores the reflect.Value.
func skipToDeser(skip skipfn) deserfn {
	return func(src []byte, _ reflect.Value) ([]byte, error) {
		return skip(src)
	}
}
