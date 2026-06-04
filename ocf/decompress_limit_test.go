package ocf

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
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

	// null codec: the post-decompress backstop bounds the count loop. A 2 MiB
	// raw block (no compression) over a 1 MiB limit is rejected before the
	// decode loop runs.
	t.Run("null-count-loop-backstop", func(t *testing.T) {
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
