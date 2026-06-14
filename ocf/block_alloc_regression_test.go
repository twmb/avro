package ocf_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

// TestRegression_OCFRaisedBlockCapDoesNotEagerAllocate pins that a reader with a
// raised WithMaxBlockBytes does not eagerly allocate an attacker-declared block
// size before reading the payload.
//
// A block frame declares its compressed size, which the reader bounds by
// WithMaxBlockBytes. A caller who raises that cap to a very large value (the
// natural way to express "accept big blocks", mirroring the decompressed side's
// MaxInt64 "effectively unlimited" sentinel) used to expose readBlock's
// make([]byte, declaredSize): a tiny hostile file declaring a 256 TiB block
// with no payload behind it drove that allocation to an unrecoverable
// "fatal error: out of memory" — a runtime.throw a caller cannot recover() from.
//
// The reader now reads the block incrementally once the declared size exceeds
// the eager-allocation window, so the buffer grows only to the bytes actually
// present and a declared-but-absent size fails with an ordinary error instead.
// Reaching the assertion without the process dying IS the pin; the boundary-1
// case (a legitimately large block reading back under a raised cap) is held by
// TestRegression_OCFLargeDatumReaderCap, which exercises the same incremental
// path with real payload bytes.
func TestRegression_OCFRaisedBlockCapDoesNotEagerAllocate(t *testing.T) {
	// A valid header for "long", reused for its embedded 16-byte sync marker.
	var hb bytes.Buffer
	w, err := ocf.NewWriter(&hb, avro.MustParse(`"long"`))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	hdr := hb.Bytes()
	sync := hdr[len(hdr)-16:]

	// One hostile block: count=1, a 256 TiB declared compressed size, and NO
	// payload bytes (the file ends right after the size + sync framing).
	var file bytes.Buffer
	file.Write(hdr)
	file.Write(binary.AppendVarint(nil, 1))     // count
	file.Write(binary.AppendVarint(nil, 1<<48)) // declared size = 256 TiB
	file.Write(sync)

	// Cap raised ABOVE the declared size, so the size>maxBlockBytes guard does
	// not fire and the read path itself must stay bounded.
	r, err := ocf.NewReader(bytes.NewReader(file.Bytes()), ocf.WithMaxBlockBytes(1<<50))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	var v int64
	if err := r.Decode(&v); err == nil {
		t.Fatal("expected an error for a 256 TiB declared-size block with no payload, got nil")
	}
}
