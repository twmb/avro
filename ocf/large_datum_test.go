package ocf_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

// TestRegression_OCFLargeDatumReaderCap documents, via test, the OCF block-size
// contract: the writer writes freely (no producer-side cap, matching Java's
// DataFileWriter and fastavro), while the reader caps block size for DoS safety
// (defaults 64 MiB). A single Avro datum cannot be split across blocks, so a
// value larger than the reader default forms one block a DEFAULT reader refuses
// — but with an ACTIONABLE error naming the option to raise — and it reads back
// once the reader's caps are raised to match. (We deliberately do NOT enforce a
// producer-side cap; the reader is where the DoS knob lives.)
func TestRegression_OCFLargeDatumReaderCap(t *testing.T) {
	s := avro.MustParse(`"bytes"`)
	const n = 80 << 20 // 80 MiB > the 64 MiB reader default
	blob := make([]byte, n)
	blob[0], blob[n-1] = 0xAB, 0xCD // sentinels for an integrity spot-check

	// The writer accepts a large datum freely — no producer-side cap.
	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(blob); err != nil {
		t.Fatalf("writer must accept a large datum freely: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A DEFAULT reader refuses the oversized block — with an error that names
	// the option to raise (not a silent failure, and not a cryptic one).
	rDefault, err := ocf.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader (default): %v", err)
	}
	var got []byte
	derr := rDefault.Decode(&got)
	if derr == nil {
		t.Fatal("default reader should refuse a block over its cap")
	}
	if !strings.Contains(derr.Error(), "WithMaxBlockBytes") &&
		!strings.Contains(derr.Error(), "WithMaxDecompressedBlockBytes") {
		t.Fatalf("reader error must name the option to raise, got: %v", derr)
	}

	// Raising the reader's caps to match reads the same file back.
	rRaised, err := ocf.NewReader(bytes.NewReader(buf.Bytes()),
		ocf.WithMaxBlockBytes(128<<20), ocf.WithMaxDecompressedBlockBytes(128<<20))
	if err != nil {
		t.Fatalf("NewReader (raised): %v", err)
	}
	var got2 []byte
	if err := rRaised.Decode(&got2); err != nil {
		t.Fatalf("raised reader must read the file back: %v", err)
	}
	if len(got2) != n || got2[0] != 0xAB || got2[n-1] != 0xCD {
		t.Fatalf("round-trip mismatch: len=%d sentinels=%x,%x", len(got2), got2[0], got2[n-1])
	}
}
