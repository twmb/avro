package ocf

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Block-FRAMING contract: the writer's block boundaries are an operational
// contract (sync-point density for splittable processing, the WithBlockCount
// and WithBlockBytes knobs), and the no-empty-block invariant keeps
// twmb-written files fully readable everywhere: this package's Reader and
// fastavro SKIP a validated count-0 block, but Java's DataFileStream
// for-each stops at one (silently truncating everything after it for Java
// consumers), avro-rs stops, and goavro errors — so the writer must never
// emit one. Round-trip tests can't see any of this (every split is a valid
// file), so the framing itself is parsed and asserted here.
// ---------------------------------------------------------------------------

// parseBlockCounts parses an OCF's raw container framing and returns the
// per-block datum counts, validating each block's sync marker.
func parseBlockCounts(t *testing.T, data []byte) []int64 {
	t.Helper()
	br := bufio.NewReader(bytes.NewReader(data))
	_, _, sync, err := readHeader(br, nil)
	if err != nil {
		t.Fatalf("parse header: %v", err)
	}
	var counts []int64
	for {
		count, err := binary.ReadVarint(br)
		if err == io.EOF {
			return counts
		}
		if err != nil {
			t.Fatalf("block %d count: %v", len(counts), err)
		}
		size, err := binary.ReadVarint(br)
		if err != nil {
			t.Fatalf("block %d size: %v", len(counts), err)
		}
		if _, err := io.CopyN(io.Discard, br, size); err != nil {
			t.Fatalf("block %d data: %v", len(counts), err)
		}
		var s2 [16]byte
		if _, err := io.ReadFull(br, s2[:]); err != nil {
			t.Fatalf("block %d sync: %v", len(counts), err)
		}
		if s2 != sync {
			t.Fatalf("block %d sync mismatch", len(counts))
		}
		counts = append(counts, count)
	}
}

// fiveByteDatum encodes to exactly 5 wire bytes (length varint 0x08 + 4).
const fiveByteDatum = "abcd"

func framingFile(t *testing.T, opts []WriterOpt, ops func(w *Writer)) []byte {
	t.Helper()
	s := avro.MustParse(`"string"`)
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, opts...)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	ops(w)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

func encodeN(t *testing.T, w *Writer, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := w.Encode(fiveByteDatum); err != nil {
			t.Fatalf("Encode #%d: %v", i, err)
		}
	}
}

func wantCounts(t *testing.T, data []byte, want ...int64) {
	t.Helper()
	got := parseBlockCounts(t, data)
	if len(got) != len(want) {
		t.Fatalf("block counts: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("block counts: got %v want %v", got, want)
		}
	}
	for _, c := range got {
		if c <= 0 {
			t.Fatalf("EMPTY BLOCK in framing (%v): Java's for-each reader stops at a count-0 block, truncating twmb-written files for Java consumers", got)
		}
	}
}

func TestWriterBlockFramingContract(t *testing.T) {
	t.Run("count-option-seals-at-exactly-n", func(t *testing.T) {
		data := framingFile(t, []WriterOpt{WithBlockCount(2)}, func(w *Writer) { encodeN(t, w, 5) })
		wantCounts(t, data, 2, 2, 1)
	})
	t.Run("byte-threshold-seals-at-crossing", func(t *testing.T) {
		// 5-byte datums against an 8-byte threshold: the buffer reaches the
		// threshold at the second datum (10 >= 8), sealing pairs.
		data := framingFile(t, []WriterOpt{WithBlockBytes(8)}, func(w *Writer) { encodeN(t, w, 4) })
		wantCounts(t, data, 2, 2)
	})
	t.Run("flush-on-empty-writes-nothing", func(t *testing.T) {
		data := framingFile(t, nil, func(w *Writer) {
			if err := w.Flush(); err != nil { // before anything
				t.Fatalf("empty Flush: %v", err)
			}
			encodeN(t, w, 1)
			if err := w.Flush(); err != nil { // seals the pending datum
				t.Fatalf("Flush: %v", err)
			}
			if err := w.Flush(); err != nil { // now empty again: must no-op
				t.Fatalf("second empty Flush: %v", err)
			}
			encodeN(t, w, 2)
		})
		wantCounts(t, data, 1, 2)
	})
	t.Run("flush-seals-exactly-the-pending-count", func(t *testing.T) {
		data := framingFile(t, nil, func(w *Writer) {
			encodeN(t, w, 3)
			if err := w.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			encodeN(t, w, 1)
		})
		wantCounts(t, data, 3, 1)
	})
	t.Run("reset-on-empty-emits-no-block", func(t *testing.T) {
		s := avro.MustParse(`"string"`)
		var first, second bytes.Buffer
		w, err := NewWriter(&first, s)
		if err != nil {
			t.Fatal(err)
		}
		encodeN(t, w, 2)
		if err := w.Flush(); err != nil { // first now holds one sealed block
			t.Fatal(err)
		}
		if err := w.Reset(&second); err != nil { // EMPTY at reset: no extra block
			t.Fatalf("Reset: %v", err)
		}
		encodeN(t, w, 1)
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		wantCounts(t, first.Bytes(), 2)
	})
	t.Run("negative-count-option-means-unlimited", func(t *testing.T) {
		data := framingFile(t, []WriterOpt{WithBlockCount(-3)}, func(w *Writer) { encodeN(t, w, 4) })
		wantCounts(t, data, 4) // byte-driven only: one block under the default cap
	})
	t.Run("zero-bytes-option-means-default", func(t *testing.T) {
		data := framingFile(t, []WriterOpt{WithBlockBytes(0)}, func(w *Writer) { encodeN(t, w, 4) })
		wantCounts(t, data, 4) // default 64KiB cap, not seal-per-datum
	})
}

// The append writer honors the same sizing options and normalizations for
// the blocks IT writes.
func TestAppendWriterBlockFramingContract(t *testing.T) {
	mk := func(t *testing.T, opts ...WriterOpt) []byte {
		t.Helper()
		s := avro.MustParse(`"string"`)
		var buf bytes.Buffer
		w, err := NewWriter(&buf, s, WithBlockCount(2))
		if err != nil {
			t.Fatal(err)
		}
		encodeN(t, w, 2)
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		f := newMemFile(buf.Bytes())
		aw, err := NewAppendWriter(f, opts...)
		if err != nil {
			t.Fatalf("NewAppendWriter: %v", err)
		}
		encodeN(t, aw, 5)
		if err := aw.Close(); err != nil {
			t.Fatal(err)
		}
		return f.data
	}
	t.Run("append-count-option", func(t *testing.T) {
		wantCounts(t, mk(t, WithBlockCount(2)), 2, 2, 2, 1)
	})
	t.Run("append-byte-threshold", func(t *testing.T) {
		wantCounts(t, mk(t, WithBlockBytes(8)), 2, 2, 2, 1)
	})
	t.Run("append-defaults-normalize", func(t *testing.T) {
		wantCounts(t, mk(t, WithBlockCount(-1), WithBlockBytes(0)), 2, 5)
	})
}

// newMemFile gives NewAppendWriter the ReadWriteSeeker it needs, in memory.
type memFile struct {
	data []byte
	pos  int64
}

func newMemFile(b []byte) *memFile { return &memFile{data: append([]byte{}, b...)} }

func (m *memFile) Read(p []byte) (int, error) {
	if m.pos >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[m.pos:])
	m.pos += int64(n)
	return n, nil
}

func (m *memFile) Write(p []byte) (int, error) {
	need := m.pos + int64(len(p))
	for int64(len(m.data)) < need {
		m.data = append(m.data, 0)
	}
	copy(m.data[m.pos:], p)
	m.pos = need
	return len(p), nil
}

func (m *memFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		m.pos = offset
	case io.SeekCurrent:
		m.pos += offset
	case io.SeekEnd:
		m.pos = int64(len(m.data)) + offset
	}
	return m.pos, nil
}
