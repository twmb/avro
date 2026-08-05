package avro

import (
	"encoding/binary"
	"math/bits"
	"testing"
)

// Candidate implementations of varlong (signed zigzag) for benchmarking.
//
//   - appendVarlongLoop: current production shape (simple loop).
//   - appendVarlongSwitch: hand-unrolled switch keyed on bits.Len64.
//   - binary.AppendVarint: encoding/binary stdlib reference (same loop,
//     different package).
//
// Avro and encoding/binary use the same zigzag-varlong wire format
// (PutVarint and twmb's appendVarlong both flip via ^(x<<1) on x<0).

// appendVarlongLoop matches the current appendVarlong.
func appendVarlongLoop(dst []byte, i int64) []byte {
	u := uint64(i)<<1 ^ uint64(i>>63)
	for u&0x7f != u {
		dst = append(dst, byte(u&0x7f|0x80))
		u >>= 7
	}
	return append(dst, byte(u))
}

// appendVarlongSwitch unrolls the varint write into a switch on the
// encoded byte length. Length = ceil(bits.Len64(u)/7), clamped to 1
// (for u == 0).
func appendVarlongSwitch(dst []byte, i int64) []byte {
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

// Avoid bench tear-down dominating the measurement: write into a reused
// buffer that's reset each iteration.

func benchVarlongOver(b *testing.B, samples []int64, fn func(dst []byte, x int64) []byte) {
	var buf [16]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn(buf[:0], samples[i%len(samples)])
	}
}

// Representative magnitude buckets, one per varlong byte length.
var varlongSamples = []int64{
	0, 1, 63, 64, // 1-2 bytes
	1 << 10,                       // 3 bytes
	1 << 17,                       // 4 bytes
	1 << 24,                       // 5 bytes
	1 << 31,                       // 6 bytes
	1 << 38,                       // 7 bytes
	1 << 45,                       // 8 bytes
	1 << 52,                       // 9 bytes
	1 << 60,                       // 10 bytes
	-1, -64, -1 << 31, -(1 << 62), // negatives across magnitudes
}

func BenchmarkVarlong_Loop(b *testing.B)    { benchVarlongOver(b, varlongSamples, appendVarlongLoop) }
func BenchmarkVarlong_Switch(b *testing.B)  { benchVarlongOver(b, varlongSamples, appendVarlongSwitch) }
func BenchmarkVarlong_StdLib(b *testing.B)  { benchVarlongOver(b, varlongSamples, binary.AppendVarint) }
func BenchmarkVarlong_Current(b *testing.B) { benchVarlongOver(b, varlongSamples, appendVarlong) }

// Per-length micro-benchmarks let us see whether the switch's advantage
// is mostly on small values (where the loop's branch predicts poorly)
// or applies uniformly. Each sub-benchmark uses a single sample of the
// given length to make the loop perfectly predictable for both impls.

var varlongPerLength = []int64{
	0,       // 1 byte
	1 << 7,  // 2 bytes
	1 << 14, // 3 bytes
	1 << 21, // 4 bytes
	1 << 28, // 5 bytes
	1 << 35, // 6 bytes
	1 << 42, // 7 bytes
	1 << 49, // 8 bytes
	1 << 56, // 9 bytes
	1 << 62, // 10 bytes (max with sign bit)
}

func benchPerLen(b *testing.B, fn func(dst []byte, x int64) []byte) {
	var buf [16]byte
	for _, x := range varlongPerLength {
		n := bytesForVarlong(x)
		b.Run(itoaPad(n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fn(buf[:0], x)
			}
		})
	}
}

func BenchmarkVarlongPerLen_Loop(b *testing.B)   { benchPerLen(b, appendVarlongLoop) }
func BenchmarkVarlongPerLen_Switch(b *testing.B) { benchPerLen(b, appendVarlongSwitch) }
func BenchmarkVarlongPerLen_StdLib(b *testing.B) { benchPerLen(b, binary.AppendVarint) }

// bytesForVarlong returns the number of bytes the zigzag varlong
// encoding of x will occupy (1..10). Used by the per-length labels.
func bytesForVarlong(x int64) int {
	u := uint64(x)<<1 ^ uint64(x>>63)
	if u == 0 {
		return 1
	}
	return (bits.Len64(u) + 6) / 7
}

func itoaPad(n int) string {
	if n < 10 {
		return string([]byte{byte('0' + n)})
	}
	return string([]byte{byte('0' + n/10), byte('0' + n%10)})
}

// Correctness check across all 10 byte-length buckets.
func TestVarlongShapesAgree(t *testing.T) {
	cases := append([]int64{}, varlongSamples...)
	cases = append(cases, varlongPerLength...)
	cases = append(cases, -(1 << 62), 1<<62, -1<<63, 1<<63-1)
	for _, x := range cases {
		want := appendVarlongLoop(nil, x)
		gotSwitch := appendVarlongSwitch(nil, x)
		gotStd := binary.AppendVarint(nil, x)
		if string(want) != string(gotSwitch) {
			t.Fatalf("switch differs from loop for %d: loop=%x switch=%x", x, want, gotSwitch)
		}
		if string(want) != string(gotStd) {
			t.Fatalf("stdlib differs from loop for %d: loop=%x stdlib=%x", x, want, gotStd)
		}
	}
}
