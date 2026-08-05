package ocf

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/twmb/avro"
)

// io.EOF is Decode's end-of-stream sentinel: it must be returned only when
// the stream ends cleanly at a block boundary. A block-header truncation is
// an error, and that error must NOT satisfy errors.Is(err, io.EOF) —
// otherwise the idiomatic errors.Is termination check reads a truncated
// stream as a clean, complete one and silently drops the promised tail.
//
// The hazard is specific to the zero-bytes-available cuts: io.ReadFull and
// binary.ReadVarint return bare io.EOF when no bytes remain (partial reads
// already yield io.ErrUnexpectedEOF), so exactly these cuts would leak the
// sentinel through a %w wrap. fastavro errors at every one of these cuts
// (EOFError / "expected sync marker not found"); the spec makes all four
// block parts (count, size, objects, sync) mandatory.
func TestRegression_TruncatedBlockHeaderNotEOF(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	// One complete block with one record.
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(int32(7)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	full := buf.Bytes()

	cuts := []struct {
		name  string
		bytes func() []byte
	}{
		{"after-count-varint", func() []byte {
			// A complete count varint promising a second block, then EOF
			// before the size varint.
			return binary.AppendVarint(append([]byte{}, full...), 1)
		}},
		{"after-size-varint", func() []byte {
			// Count and size complete, zero data bytes present.
			d := binary.AppendVarint(append([]byte{}, full...), 1)
			return binary.AppendVarint(d, 100)
		}},
		{"at-sync-start", func() []byte {
			// Data complete, zero sync bytes present.
			d := binary.AppendVarint(append([]byte{}, full...), 1)
			d = binary.AppendVarint(d, 1)
			return append(d, 0x02)
		}},
	}
	for _, c := range cuts {
		t.Run(c.name, func(t *testing.T) {
			r, err := NewReader(bytes.NewReader(c.bytes()))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			var v int32
			if err := r.Decode(&v); err != nil || v != 7 {
				t.Fatalf("first record: v=%d err=%v", v, err)
			}
			err = r.Decode(&v)
			if err == nil {
				t.Fatal("expected error for truncated second block")
			}
			if errors.Is(err, io.EOF) {
				t.Fatalf("truncation error satisfies errors.Is(err, io.EOF): %v", err)
			}
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("truncation error should match io.ErrUnexpectedEOF, got: %v", err)
			}
		})
	}

	// Control: a true end-of-stream is the bare io.EOF sentinel.
	r, err := NewReader(bytes.NewReader(full))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var v int32
	if err := r.Decode(&v); err != nil {
		t.Fatal(err)
	}
	if err := r.Decode(&v); err != io.EOF {
		t.Fatalf("clean end must be bare io.EOF, got %v", err)
	}
}

// The same sentinel contract through the incremental (io.CopyN) block-data
// arm, reached when the declared size exceeds the eager-allocation window
// and the reader cap is raised to allow it. io.CopyN returns bare io.EOF on
// ANY shortfall — zero bytes available AND a partial copy — so both cells
// are pinned (unlike io.ReadFull, whose partial reads already return
// io.ErrUnexpectedEOF).
func TestRegression_TruncatedLargeBlockDataNotEOF(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(int32(7)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	full := buf.Bytes()

	for _, c := range []struct {
		name    string
		partial []byte // data bytes present before the cut
	}{
		{"zero-data-bytes", nil},
		{"partial-data-bytes", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
	} {
		t.Run(c.name, func(t *testing.T) {
			data := binary.AppendVarint(append([]byte{}, full...), 1) // count
			data = binary.AppendVarint(data, (64<<20)+1)              // size past the eager window
			data = append(data, c.partial...)
			r, err := NewReader(bytes.NewReader(data), WithMaxBlockBytes(128<<20))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			var v int32
			if err := r.Decode(&v); err != nil || v != 7 {
				t.Fatalf("first record: v=%d err=%v", v, err)
			}
			err = r.Decode(&v)
			if err == nil {
				t.Fatal("expected error for truncated block data")
			}
			if errors.Is(err, io.EOF) {
				t.Fatalf("truncation error satisfies errors.Is(err, io.EOF): %v", err)
			}
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("truncation error should match io.ErrUnexpectedEOF, got: %v", err)
			}
		})
	}
}
