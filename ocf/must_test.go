package ocf

import (
	"io"
	"testing"

	"github.com/twmb/avro"
)

// The must* helpers below each run one library call and fail the test if it
// errors. A cell that only needs the successful result then reads as one line,
// not four. Every one calls t.Helper(), so a failure lands on the caller's
// line, and every message names the operation. Where a site's own message says
// more than that (which cell, which input, which axis), we leave its error
// handling alone and do not fold it in.

func mustNewWriter(t testing.TB, w io.Writer, s *avro.Schema, opts ...WriterOpt) *Writer {
	t.Helper()
	ow, err := NewWriter(w, s, opts...)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return ow
}

func mustNewAppendWriter(t testing.TB, rws io.ReadWriteSeeker, opts ...WriterOpt) *Writer {
	t.Helper()
	w, err := NewAppendWriter(rws, opts...)
	if err != nil {
		t.Fatalf("NewAppendWriter: %v", err)
	}
	return w
}

func mustNewReader(t testing.TB, r io.Reader, opts ...ReaderOpt) *Reader {
	t.Helper()
	or, err := NewReader(r, opts...)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return or
}

func mustFlush(t testing.TB, w *Writer) {
	t.Helper()
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func mustClose(t testing.TB, c io.Closer) {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// drainAll decodes every remaining record in r into a slice, stopping at EOF.
// A cell that wants to say what the reader returned at a particular record
// index reads the loop itself instead.
func drainAll[T any](t testing.TB, r *Reader) []T {
	t.Helper()
	var out []T
	for {
		var v T
		if err := r.Decode(&v); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("Decode: %v", err)
		}
		out = append(out, v)
	}
	return out
}
