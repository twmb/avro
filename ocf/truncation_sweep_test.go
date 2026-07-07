package ocf

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/twmb/avro"
)

// TestMatrix_TruncationTerminalErrorIdentity sweeps a multi-block file cut
// at EVERY byte offset from end-of-header to one byte short of the full
// file, across codecs, pinning the terminal-error identity contract as a
// class:
//
//   - a cut exactly at a block boundary (end of header, end of a record
//     block, end of an empty block) is a clean end of stream: Decode's
//     terminal error is BARE io.EOF, with exactly the records of the
//     complete blocks before the cut;
//   - every other cut is truncation: the terminal error is non-nil and
//     does NOT satisfy errors.Is(err, io.EOF), so the idiomatic errors.Is
//     termination check can never read a truncated stream as complete;
//   - no cut ever yields records beyond the blocks complete before it
//     (blocks are consumed wholesale, so a partial block contributes zero).
//
// The spliced count-0 block puts the skip arm's reads (count, size, sync of
// a block that yields no records) inside the sweep: cuts inside it must
// error, and the cut exactly after it is a clean boundary that still
// reports only the prior blocks' records. Counts of 100 and 70 make the
// block-header count varints multi-byte, so mid-varint cuts participate.
// Both codecs share the invariant; they differ in the data-read arms the
// sweep traverses (stored vs compressed payloads).
func TestMatrix_TruncationTerminalErrorIdentity(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	for _, codec := range []struct {
		name string
		opts []WriterOpt
	}{
		{"null", nil},
		{"deflate", []WriterOpt{WithCodec(DeflateCodec(6))}},
	} {
		t.Run(codec.name, func(t *testing.T) {
			var buf bytes.Buffer
			w, err := NewWriter(&buf, s, codec.opts...)
			if err != nil {
				t.Fatal(err)
			}
			headerEnd := buf.Len()
			for i := range 100 {
				if err := w.Encode(int32(i)); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
			block1End := buf.Len()
			for i := range 70 {
				if err := w.Encode(int32(1000 + i)); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			raw := buf.Bytes()
			if headerEnd == 0 || block1End <= headerEnd || len(raw) <= block1End {
				t.Fatalf("layout snapshots out of order: header=%d block1=%d len=%d", headerEnd, block1End, len(raw))
			}

			// Splice a count-0 block (count 0, size 0, sync) between the two
			// record blocks, using the file's own sync marker.
			sync := raw[block1End-16 : block1End]
			var file []byte
			file = append(file, raw[:block1End]...)
			file = append(file, 0x00, 0x00)
			file = append(file, sync...)
			emptyEnd := block1End + 2 + 16
			file = append(file, raw[block1End:]...)

			// recordsAt maps each clean boundary to the records readable at it.
			recordsAt := map[int]int{
				headerEnd: 0,
				block1End: 100,
				emptyEnd:  100,
				len(file): 170,
			}

			readAll := func(prefix []byte) (n int, sum int64, term error) {
				r, err := NewReader(bytes.NewReader(prefix))
				if err != nil {
					return 0, 0, err
				}
				defer r.Close()
				for {
					var v int32
					err := r.Decode(&v)
					if err != nil {
						return n, sum, err
					}
					n++
					sum += int64(v)
				}
			}

			// Baseline: the spliced file reads fully with the empty block
			// skipped.
			if n, _, term := readAll(file); n != 170 || term != io.EOF {
				t.Fatalf("spliced baseline: n=%d term=%v", n, term)
			}

			for L := headerEnd; L <= len(file); L++ {
				n, _, term := readAll(file[:L])
				want, boundary := recordsAt[L]
				if !boundary {
					// Records never exceed the complete blocks before the cut.
					switch {
					case L < block1End && n != 0, L >= block1End && n != 100:
						t.Fatalf("L=%d: %d records beyond the blocks complete before the cut", L, n)
					}
					if term == nil {
						t.Fatalf("L=%d: truncated stream read as complete (%d records, no error)", L, n)
					}
					if errors.Is(term, io.EOF) {
						t.Fatalf("L=%d: truncation error satisfies errors.Is(err, io.EOF): %v", L, term)
					}
					continue
				}
				if n != want {
					t.Fatalf("boundary L=%d: read %d records, want %d", L, n, want)
				}
				if term != io.EOF {
					t.Fatalf("boundary L=%d: terminal error must be bare io.EOF, got %v", L, term)
				}
			}
		})
	}
}
