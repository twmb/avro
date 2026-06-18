package ocf

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/twmb/avro"
)

// ocfWith assembles an OCF: header (schema + codec) + one block (count, size,
// compressed payload, sync).
func ocfWith(schemaJSON, codec string, count int64, compressed []byte) []byte {
	var sync [16]byte
	var b []byte
	b = append(b, 'O', 'b', 'j', 1)
	b = binary.AppendVarint(b, 2) // 2 metadata entries
	put := func(s string) { b = binary.AppendVarint(b, int64(len(s))); b = append(b, s...) }
	put("avro.schema")
	put(schemaJSON)
	put("avro.codec")
	put(codec)
	b = binary.AppendVarint(b, 0) // metadata terminator
	b = append(b, sync[:]...)
	b = binary.AppendVarint(b, count)
	b = binary.AppendVarint(b, int64(len(compressed)))
	b = append(b, compressed...)
	b = append(b, sync[:]...)
	return b
}

// A block whose DECOMPRESSED size exceeds the per-block decompression limit is
// rejected — across deflate (unbounded io.ReadAll), snappy (pre-allocates from
// a declared length) and zstd (library default permits multi-GiB) — and the
// null-codec count loop is bounded by the same limit. Without this, a tiny
// compressed block inflates to a huge allocation / decode loop: an OCF
// decompression-amplification DoS. The compressed-side cap (WithMaxBlockBytes)
// does not bound the decompressed size; WithMaxDecompressedBlockBytes does.
func TestRegression_OCFDecompressionAmplificationBounded(t *testing.T) {
	const limit = 1 << 20   // 1 MiB configured limit (small => tiny test allocations)
	const bombLen = 4 << 20 // 4 MiB decompressed: over the limit
	zeros := make([]byte, bombLen)

	// snappy frame declaring 4 MiB (CRC trailer appended, per the codec).
	snap := snappy.Encode(nil, zeros)
	snap = binary.BigEndian.AppendUint32(snap, 0) // CRC slot (rejected before CRC check)

	// deflate stream that inflates to 4 MiB.
	var defBuf bytes.Buffer
	dw, _ := flate.NewWriter(&defBuf, flate.DefaultCompression)
	dw.Write(zeros)
	dw.Close()

	// zstd stream that inflates to 4 MiB.
	zenc, _ := zstd.NewWriter(nil)
	zst := zenc.EncodeAll(zeros, nil)
	zenc.Close()

	cases := []struct {
		name, codec string
		payload     []byte
	}{
		{"deflate", "deflate", defBuf.Bytes()},
		{"snappy", "snappy", snap},
		{"zstd", "zstandard", zst},
	}
	for _, c := range cases {
		t.Run(c.name+"/rejected-at-limit", func(t *testing.T) {
			data := ocfWith(`"null"`, c.codec, 1, c.payload)
			r, err := NewReader(bytes.NewReader(data), WithMaxDecompressedBlockBytes(limit))
			if err != nil {
				return // header/codec rejection is also a safe outcome
			}
			// Require the limit-specific rejection ("exceeds"): all three
			// codecs report it (snappy/deflate via the in-codec cap, zstd via
			// WithDecoderMaxMemory's "decompressed size exceeds configured
			// limit"). Demanding this token — not merely "some error" —
			// matters because the null schema would ALSO error on trailing
			// bytes if the bomb were allowed to inflate, which would mask a
			// missing cap. The error must come from the limit, not the decode.
			var v any
			err = r.Decode(&v)
			if err == nil || !strings.Contains(err.Error(), "exceeds") {
				t.Errorf("%s block inflating to %d bytes under a %d-byte limit: want an over-limit rejection, got %v", c.codec, bombLen, limit, err)
			}
			r.Close()
		})
		t.Run(c.name+"/accepted-when-raised", func(t *testing.T) {
			// With the limit raised above the decompressed size, the block
			// decompresses fine (decode then fails on the null-schema trailing
			// bytes, which is a normal decode error, not a limit rejection).
			data := ocfWith(`"null"`, c.codec, 1, c.payload)
			r, err := NewReader(bytes.NewReader(data), WithMaxDecompressedBlockBytes(8<<20))
			if err != nil {
				t.Fatalf("raised-limit NewReader: %v", err)
			}
			var v any
			err = r.Decode(&v)
			if err != nil && strings.Contains(err.Error(), "exceeds") {
				t.Errorf("%s block within the raised limit was still rejected as over-limit: %v", c.codec, err)
			}
			r.Close()
		})
	}

	// null codec: DecompressBounded rejects an over-cap raw block (the
	// "decompressed" size IS the input size), which also bounds the count loop.
	// A 2 MiB raw block (no compression) over a 1 MiB limit is rejected before
	// the decode loop runs.
	t.Run("null-count-loop-bounded", func(t *testing.T) {
		raw := make([]byte, 2<<20)
		data := ocfWith(`"null"`, "null", 1, raw)
		r, err := NewReader(bytes.NewReader(data), WithMaxDecompressedBlockBytes(limit))
		if err != nil {
			t.Fatal(err)
		}
		var v any
		if err := r.Decode(&v); err == nil || !strings.Contains(err.Error(), "exceeds") {
			t.Errorf("2 MiB null block over a 1 MiB limit: want over-limit rejection, got %v", err)
		}
		r.Close()
	})

	// A legitimate small file round-trips under the (large) default limit.
	t.Run("legit-roundtrip-default-limit", func(t *testing.T) {
		s := avro.MustParse(`"string"`)
		var buf bytes.Buffer
		w, err := NewWriter(&buf, s, WithCodec(DeflateCodec(1)))
		if err != nil {
			t.Fatal(err)
		}
		w.Encode("hello")
		w.Close()
		r, err := NewReader(bytes.NewReader(buf.Bytes())) // default 64 MiB limit
		if err != nil {
			t.Fatal(err)
		}
		var got string
		if err := r.Decode(&got); err != nil || got != "hello" {
			t.Errorf("legit round-trip under default limit failed: got %q err %v", got, err)
		}
		r.Close()
	})
}

// A user expressing "no practical decompressed-size limit" as math.MaxInt64
// (rather than the documented 0) must still read a valid deflate-compressed
// OCF. deflateCodec.DecompressBounded reads io.LimitReader(r, max+1) to detect
// over-limit without materializing the bomb; at max==MaxInt64 the +1
// overflows to MinInt64, LimitReader returns 0 bytes, the block decodes as
// empty, and a valid file fails to read. The bound must not invert at its own
// extreme value. The default-limit and limit==0 (unlimited) paths are the
// boundary-1 controls that must keep working.
func TestRegression_OCFDeflateDecompressLimitMaxInt(t *testing.T) {
	s := avro.MustParse(`"string"`)
	payload := strings.Repeat("hello world ", 2000) // ~24 KiB, compresses well
	mk := func() []byte {
		var buf bytes.Buffer
		w, err := NewWriter(&buf, s, WithCodec(DeflateCodec(1)))
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Encode(payload); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}
	// Reader auto-selects the built-in deflate codec from the header; the cap is
	// passed to its DecompressBounded from WithMaxDecompressedBlockBytes.
	for _, tc := range []struct {
		name  string
		limit int64
	}{
		{"max-int64", math.MaxInt64}, // the overflow boundary
		{"unlimited-zero", 0},        // documented "unlimited" control
		{"generous", 64 << 20},       // ordinary large control
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := NewReader(bytes.NewReader(mk()), WithMaxDecompressedBlockBytes(tc.limit))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			var got string
			if err := r.Decode(&got); err != nil {
				t.Fatalf("limit=%d: Decode of a valid deflate file failed: %v", tc.limit, err)
			}
			if got != payload {
				t.Fatalf("limit=%d: round-trip mismatch: got %d bytes, want %d", tc.limit, len(got), len(payload))
			}
		})
	}
}

// A zstd codec supplied as an INSTANCE via WithCodec is bounded by the reader's
// WithMaxDecompressedBlockBytes, the same as a name-resolved zstd codec: the
// decoder is built lazily with zstd.WithDecoderMaxMemory from the cap. A frame
// inflating past the cap is rejected; the same frame under a raised cap decodes.
func TestRegression_OCFSuppliedZstdInstanceBounded(t *testing.T) {
	const limit = 1 << 20
	const bombLen = 4 << 20
	zeros := make([]byte, bombLen)
	zenc, _ := zstd.NewWriter(nil)
	zst := zenc.EncodeAll(zeros, nil)
	zenc.Close()
	data := ocfWith(`"null"`, "zstandard", 1, zst)

	for _, sc := range []struct {
		name  string
		codec func() Codec
	}{
		{"instance", func() Codec { c, _ := ZstdCodec(nil, nil); return c }},
	} {
		t.Run(sc.name+"/rejected-at-limit", func(t *testing.T) {
			r, err := NewReader(bytes.NewReader(data), WithCodec(sc.codec()), WithMaxDecompressedBlockBytes(limit))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			var v any
			if err := r.Decode(&v); err == nil || !strings.Contains(err.Error(), "exceeds") {
				t.Errorf("supplied zstd %s: 4 MiB frame under a 1 MiB cap: want over-limit rejection, got %v", sc.name, err)
			}
		})
		t.Run(sc.name+"/accepted-when-raised", func(t *testing.T) {
			r, err := NewReader(bytes.NewReader(data), WithCodec(sc.codec()), WithMaxDecompressedBlockBytes(8<<20))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			var v any
			if err := r.Decode(&v); err != nil && strings.Contains(err.Error(), "exceeds") {
				t.Errorf("supplied zstd %s under a raised cap was still rejected as over-limit: %v", sc.name, err)
			}
		})
	}
}

// rawCodec is a no-op "compression" codec: the stored block IS the raw bytes.
type rawCodec struct{ name string }

func (c rawCodec) Name() string                        { return c.name }
func (rawCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (rawCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (rawCodec) Close() error                          { return nil }

// boundedRawCodec adds the BoundedDecompressor capability to rawCodec.
type boundedRawCodec struct{ rawCodec }

func (boundedRawCodec) DecompressBounded(src []byte, max int64) ([]byte, error) {
	if max > 0 && int64(len(src)) > max {
		return nil, fmt.Errorf("rawcodec: %d bytes exceeds limit of %d", len(src), max)
	}
	return src, nil
}

// A custom codec implementing BoundedDecompressor is bounded by the reader's
// WithMaxDecompressedBlockBytes; a custom codec that does NOT implement it is
// honestly unbounded — the reader adds no post-decompression backstop (false
// comfort once the block is allocated). This pins the capability contract that
// replaced the type-asserted "is this a built-in instance" recognition.
func TestRegression_OCFCustomCodecBoundedDecompressorContract(t *testing.T) {
	const limit = 1 << 20
	raw := make([]byte, 4<<20) // 4 MiB "compressed" block == 4 MiB decompressed

	t.Run("implements-bounded/rejected", func(t *testing.T) {
		data := ocfWith(`"null"`, "bnd", 1, raw)
		r, err := NewReader(bytes.NewReader(data), WithCodec(boundedRawCodec{rawCodec{"bnd"}}), WithMaxDecompressedBlockBytes(limit))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		var v any
		if err := r.Decode(&v); err == nil || !strings.Contains(err.Error(), "exceeds") {
			t.Errorf("bounded custom codec: 4 MiB block over a 1 MiB cap: want rejection, got %v", err)
		}
	})
	t.Run("plain/unbounded", func(t *testing.T) {
		data := ocfWith(`"null"`, "unb", 1, raw)
		r, err := NewReader(bytes.NewReader(data), WithCodec(rawCodec{"unb"}), WithMaxDecompressedBlockBytes(limit))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		// No BoundedDecompressor => the cap does not apply. The decode fails on
		// the null schema's trailing bytes, NOT with an over-limit rejection.
		var v any
		if err := r.Decode(&v); err != nil && strings.Contains(err.Error(), "exceeds") {
			t.Errorf("plain custom codec must be unbounded (no over-limit reject), got %v", err)
		}
	})
}
