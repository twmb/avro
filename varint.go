package main

import "math/bits"

func appendUint32(dst []byte, u uint32) []byte {
	return append(dst, byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

func appendUint64(dst []byte, u uint64) []byte {
	return append(dst, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32),
		byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

// uvarintLens could only be length 65, but using 256 allows bounds check
// elimination on lookup.
const uvarintLens = "\x01\x01\x01\x01\x01\x01\x01\x01\x02\x02\x02\x02\x02\x02\x02\x03\x03\x03\x03\x03\x03\x03\x04\x04\x04\x04\x04\x04\x04\x05\x05\x05\x05\x05\x05\x05\x06\x06\x06\x06\x06\x06\x06\x07\x07\x07\x07\x07\x07\x07\x08\x08\x08\x08\x08\x08\x08\x09\x09\x09\x09\x09\x09\x09\x10\x10\x10\x10\x10\x10\x10\x11\x11\x11\x11\x11\x11\x11\x12\x12\x12\x12\x12\x12\x12\x13\x13\x13\x13\x13\x13\x13\x14\x14\x14\x14\x14\x14\x14\x15\x15\x15\x15\x15\x15\x15\x16\x16\x16\x16\x16\x16\x16\x17\x17\x17\x17\x17\x17\x17\x18\x18\x18\x18\x18\x18\x18\x19\x19\x19\x19\x19\x19\x19\x20\x20\x20\x20\x20\x20\x20\x21\x21\x21\x21\x21\x21\x21\x22\x22\x22\x22\x22\x22\x22\x23\x23\x23\x23\x23\x23\x23\x24\x24\x24\x24\x24\x24\x24\x25\x25\x25\x25\x25\x25\x25\x26\x26\x26\x26\x26\x26\x26\x27\x27\x27\x27\x27\x27\x27\x28\x28\x28\x28\x28\x28\x28\x29\x29\x29\x29\x29\x29\x29\x30\x30\x30\x30\x30\x30\x30\x31\x31\x31\x31\x31\x31\x31\x32\x32\x32\x32\x32\x32\x32\x33\x33\x33\x33\x33\x33\x33\x34\x34\x34\x34\x34\x34\x34\x35\x35\x35\x35\x35\x35\x35\x36\x36\x36\x36\x36\x36\x36\x37\x37\x37"

// varintLen returns how long i would be if it were varint encoded.
func varintLen(i int32) int {
	u := uint32(i)<<1 ^ uint32(i>>31)
	return uvarintLen(u)
}

// uvarintLen returns how long u would be if it were uvarint encoded.
func uvarintLen(u uint32) int {
	return int(uvarintLens[byte(bits.Len32(u))])
}

// appendVarint appends a varint encoded i to dst.
func appendVarint(dst []byte, i int32) []byte {
	return appendUvarint(dst, uint32(i)<<1^uint32(i>>31))
}

// appendUvarint appends a uvarint encoded u to dst.
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
	case 1:
		return append(dst, byte(u))
	}
	return dst
}

// appendVarlong appends a varint encoded i to dst.
func appendVarlong(dst []byte, i int64) []byte {
	u := uint64(i)<<1 ^ uint64(i>>63)
	for u&0x7f != u {
		dst = append(dst, byte(u&0x7f|0x80))
		u >>= 7
	}
	return append(dst, byte(u))
}
