package ocf_test

import (
	"bytes"
	"runtime"
	"strings"
	"testing"

	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

// A reader configured with a codec instance via WithCodec must enforce
// WithMaxDecompressedBlockBytes the same way a name-resolved codec does: by
// PREVENTING the over-cap allocation, not by decompressing the whole block and
// rejecting after. The reader passes its cap to the codec's DecompressBounded
// (the BoundedDecompressor capability) at decode time, so the bound reaches a
// supplied instance — AND a NopCloser-wrapped instance, which forwards the
// capability — exactly like the name-resolved built-in. Without this, deflate
// decompresses via an unbounded io.ReadAll: a tiny deflate bomb materializes in
// full (OOM on a real bomb) before any rejection.
//
// The pin is the ALLOCATION: a block declaring far more decompressed bytes than
// the cap must be rejected having allocated only on the order of the cap, not
// the full decompressed size. Reaching the assertion without materializing the
// whole datum is the property; an over-cap allocation would show as a TotalAlloc
// delta near the decompressed size. The NopCloser rows pin that wrapping a
// built-in for sharing does not silently drop its bounding.
func TestRegression_OCFUserBuiltinCodecBoundsDecompression(t *testing.T) {
	const datumSize = 8 << 20 // 8 MiB decompressed (highly compressible -> tiny compressed)
	const cap = 256 << 10     // 256 KiB decompressed cap
	s := avro.MustParse(`"bytes"`)

	mkFile := func(codec ocf.Codec) []byte {
		var buf bytes.Buffer
		w, err := ocf.NewWriter(&buf, s, ocf.WithCodec(codec))
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		if err := w.Encode(make([]byte, datumSize)); err != nil {
			t.Fatalf("encode: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		return buf.Bytes()
	}

	decodeAlloc := func(file []byte, codec ocf.Codec) (uint64, error) {
		r, err := ocf.NewReader(bytes.NewReader(file),
			ocf.WithCodec(codec),
			ocf.WithMaxDecompressedBlockBytes(cap))
		if err != nil {
			return 0, err
		}
		var m0, m1 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m0)
		var v []byte
		derr := r.Decode(&v)
		runtime.ReadMemStats(&m1)
		return m1.TotalAlloc - m0.TotalAlloc, derr
	}

	for _, tc := range []struct {
		name  string
		write ocf.Codec
		read  ocf.Codec
	}{
		{"deflate", ocf.DeflateCodec(9), ocf.DeflateCodec(9)},
		{"snappy", ocf.SnappyCodec(), ocf.SnappyCodec()},
		// NopCloser-wrapped built-ins (the realistic shared-codec form): the
		// wrapper forwards BoundedDecompressor, so the bound still applies.
		{"deflate_nopcloser", ocf.DeflateCodec(9), ocf.NopCloser(ocf.DeflateCodec(9))},
		{"snappy_nopcloser", ocf.SnappyCodec(), ocf.NopCloser(ocf.SnappyCodec())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := mkFile(tc.write)
			alloc, err := decodeAlloc(file, tc.read)
			if err == nil {
				t.Fatalf("expected the over-cap block to be rejected, got nil")
			}
			if !strings.Contains(err.Error(), "exceeds limit") {
				t.Fatalf("unexpected error (want a decompression-limit reject): %v", err)
			}
			// The bounded codec stops near the cap; the unbounded codec would
			// materialize all datumSize bytes (8 MiB). A threshold of half the
			// decompressed size cleanly separates "prevented" from "materialized
			// then rejected".
			if alloc >= datumSize/2 {
				t.Fatalf("codec materialized the over-cap block before rejecting: allocated %d bytes for a %d-byte cap (decompressed size %d)", alloc, cap, datumSize)
			}
		})
	}
}
