package avro

import (
	"errors"
	"math/bits"
	"unsafe"
)

func appendUint32(dst []byte, u uint32) []byte {
	return append(dst, byte(u), byte(u>>8), byte(u>>16), byte(u>>24))
}

func appendUint64(dst []byte, u uint64) []byte {
	return append(dst, byte(u), byte(u>>8), byte(u>>16), byte(u>>24),
		byte(u>>32), byte(u>>40), byte(u>>48), byte(u>>56))
}

// uvarintLens maps bits.Len32's result to the uvarint byte length. Only
// indices 0..32 are read; the table is 256 long so the compiler can prove
// byte(bits.Len32(u)) is in bounds and drop the check. The padding past 32
// is not a valid 64-bit extension (the entries are written as decimal
// digits, so index 64 holds 16, not 10), and appendVarlong does its own
// magnitude switch.
const uvarintLens = "\x01\x01\x01\x01\x01\x01\x01\x01\x02\x02\x02\x02\x02\x02\x02\x03\x03\x03\x03\x03\x03\x03\x04\x04\x04\x04\x04\x04\x04\x05\x05\x05\x05\x05\x05\x05\x06\x06\x06\x06\x06\x06\x06\x07\x07\x07\x07\x07\x07\x07\x08\x08\x08\x08\x08\x08\x08\x09\x09\x09\x09\x09\x09\x09\x10\x10\x10\x10\x10\x10\x10\x11\x11\x11\x11\x11\x11\x11\x12\x12\x12\x12\x12\x12\x12\x13\x13\x13\x13\x13\x13\x13\x14\x14\x14\x14\x14\x14\x14\x15\x15\x15\x15\x15\x15\x15\x16\x16\x16\x16\x16\x16\x16\x17\x17\x17\x17\x17\x17\x17\x18\x18\x18\x18\x18\x18\x18\x19\x19\x19\x19\x19\x19\x19\x20\x20\x20\x20\x20\x20\x20\x21\x21\x21\x21\x21\x21\x21\x22\x22\x22\x22\x22\x22\x22\x23\x23\x23\x23\x23\x23\x23\x24\x24\x24\x24\x24\x24\x24\x25\x25\x25\x25\x25\x25\x25\x26\x26\x26\x26\x26\x26\x26\x27\x27\x27\x27\x27\x27\x27\x28\x28\x28\x28\x28\x28\x28\x29\x29\x29\x29\x29\x29\x29\x30\x30\x30\x30\x30\x30\x30\x31\x31\x31\x31\x31\x31\x31\x32\x32\x32\x32\x32\x32\x32\x33\x33\x33\x33\x33\x33\x33\x34\x34\x34\x34\x34\x34\x34\x35\x35\x35\x35\x35\x35\x35\x36\x36\x36\x36\x36\x36\x36\x37\x37\x37"

func uvarintLen(u uint32) int {
	return int(uvarintLens[byte(bits.Len32(u))])
}

func appendVarint(dst []byte, i int32) []byte {
	return appendUvarint(dst, uint32(i)<<1^uint32(i>>31))
}

func appendUvarint(dst []byte, u uint32) []byte {
	switch uvarintLen(u) {
	case 5:
		return append(dst,
			byte(u&0x7f|0x80),
			byte((u>>7)&0x7f|0x80),
			byte((u>>14)&0x7f|0x80),
			byte((u>>21)&0x7f|0x80),
			byte(u>>28))
	case 4:
		return append(dst,
			byte(u&0x7f|0x80),
			byte((u>>7)&0x7f|0x80),
			byte((u>>14)&0x7f|0x80),
			byte(u>>21))
	case 3:
		return append(dst,
			byte(u&0x7f|0x80),
			byte((u>>7)&0x7f|0x80),
			byte(u>>14))
	case 2:
		return append(dst,
			byte(u&0x7f|0x80),
			byte(u>>7))
	default:
		return append(dst, byte(u))
	}
}

// appendVarlong writes i as a zigzag-encoded varlong (1-10 bytes). We
// mirror appendUvarint's length-keyed switch: benchmarks put the switch
// 30-50% ahead of the generic loop at every byte length.
func appendVarlong(dst []byte, i int64) []byte {
	u := uint64(i)<<1 ^ uint64(i>>63)
	switch {
	case u < 1<<7:
		return append(dst, byte(u))
	case u < 1<<14:
		return append(dst, byte(u)|0x80, byte(u>>7))
	case u < 1<<21:
		return append(dst, byte(u)|0x80, byte(u>>7)|0x80, byte(u>>14))
	case u < 1<<28:
		return append(dst, byte(u)|0x80, byte(u>>7)|0x80, byte(u>>14)|0x80, byte(u>>21))
	case u < 1<<35:
		return append(dst, byte(u)|0x80, byte(u>>7)|0x80, byte(u>>14)|0x80, byte(u>>21)|0x80, byte(u>>28))
	case u < 1<<42:
		return append(dst, byte(u)|0x80, byte(u>>7)|0x80, byte(u>>14)|0x80, byte(u>>21)|0x80, byte(u>>28)|0x80, byte(u>>35))
	case u < 1<<49:
		return append(dst, byte(u)|0x80, byte(u>>7)|0x80, byte(u>>14)|0x80, byte(u>>21)|0x80, byte(u>>28)|0x80, byte(u>>35)|0x80, byte(u>>42))
	case u < 1<<56:
		return append(dst, byte(u)|0x80, byte(u>>7)|0x80, byte(u>>14)|0x80, byte(u>>21)|0x80, byte(u>>28)|0x80, byte(u>>35)|0x80, byte(u>>42)|0x80, byte(u>>49))
	case u < 1<<63:
		return append(dst, byte(u)|0x80, byte(u>>7)|0x80, byte(u>>14)|0x80, byte(u>>21)|0x80, byte(u>>28)|0x80, byte(u>>35)|0x80, byte(u>>42)|0x80, byte(u>>49)|0x80, byte(u>>56))
	default:
		return append(dst, byte(u)|0x80, byte(u>>7)|0x80, byte(u>>14)|0x80, byte(u>>21)|0x80, byte(u>>28)|0x80, byte(u>>35)|0x80, byte(u>>42)|0x80, byte(u>>49)|0x80, byte(u>>56)|0x80, byte(u>>63))
	}
}

// readUvar reads an unsigned varint of U's width: at most five bytes for
// uint32, ten for uint64. The width picks the loop bound and the last byte's
// overflow mask, and both fold to constants per instantiation, so each width
// compiles to the direct loop the two hand-written readers had. The body
// calls nothing on its hot path: a call inside a generic body is not
// inlined, so a helper here would cost a call per varint.
func readUvar[U uint32 | uint64](src []byte) (U, []byte, error) {
	width := int(unsafe.Sizeof(U(0))) * 8
	last := width / 7 // index of the byte that cannot carry a continuation
	name, overflow := "uvarint", "uvarint overflows 32 bits"
	if width == 64 {
		name, overflow = "uvarlong", "uvarlong overflows 64 bits"
	}
	var u U
	for i := range last {
		if i >= len(src) {
			return 0, nil, &ShortBufferError{Type: name}
		}
		b := src[i]
		u |= U(b&0x7f) << (7 * i)
		if b&0x80 == 0 {
			return u, src[i+1:], nil
		}
	}
	if len(src) <= last {
		return 0, nil, &ShortBufferError{Type: name}
	}
	b := src[last]
	if b > 1<<(width-7*last)-1 {
		return 0, nil, errors.New(overflow)
	}
	u |= U(b) << (7 * last)
	return u, src[last+1:], nil
}

func readUvarint(src []byte) (uint32, []byte, error)  { return readUvar[uint32](src) }
func readUvarlong(src []byte) (uint64, []byte, error) { return readUvar[uint64](src) }

// readVarint and readVarlong stay two plain functions: each inlines its
// readUvar wrapper and so calls the instantiation directly on the multi-byte
// path, where a shared generic body would add a call.
func readVarint(src []byte) (int32, []byte, error) {
	if len(src) > 0 && src[0] < 0x80 {
		u := uint32(src[0])
		return int32(u>>1) ^ -int32(u&1), src[1:], nil
	}
	u, src, err := readUvarint(src)
	if err != nil {
		return 0, nil, err
	}
	return int32(u>>1) ^ -int32(u&1), src, nil
}

func readVarlong(src []byte) (int64, []byte, error) {
	if len(src) > 0 && src[0] < 0x80 {
		u := uint64(src[0])
		return int64(u>>1) ^ -int64(u&1), src[1:], nil
	}
	u, src, err := readUvarlong(src)
	if err != nil {
		return 0, nil, err
	}
	return int64(u>>1) ^ -int64(u&1), src, nil
}

func readUint32(src []byte) (uint32, []byte, error) {
	if len(src) < 4 {
		return 0, nil, &ShortBufferError{Type: "uint32", Need: 4, Have: len(src)}
	}
	u := uint32(src[0]) | uint32(src[1])<<8 | uint32(src[2])<<16 | uint32(src[3])<<24
	return u, src[4:], nil
}

func readUint64(src []byte) (uint64, []byte, error) {
	if len(src) < 8 {
		return 0, nil, &ShortBufferError{Type: "uint64", Need: 8, Have: len(src)}
	}
	u := uint64(src[0]) | uint64(src[1])<<8 | uint64(src[2])<<16 | uint64(src[3])<<24 |
		uint64(src[4])<<32 | uint64(src[5])<<40 | uint64(src[6])<<48 | uint64(src[7])<<56
	return u, src[8:], nil
}
