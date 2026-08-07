package ocf

import (
	"io"
	"testing"

	"github.com/twmb/avro"
)

// The must* helpers below each run one library call and fail the test if it
// errors. They exist so a cell that only needs the successful result reads as
// one line instead of four; every one calls t.Helper(), so a failure is
// reported at the caller's line, and every message names the operation. A site
// whose own failure message says more than the operation name — which cell,
// which input, which axis — keeps its own error handling and is not folded
// into these.

func mustParse(t testing.TB, schema string, opts ...avro.SchemaOpt) *avro.Schema {
	t.Helper()
	s, err := avro.Parse(schema, opts...)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return s
}

func mustAppendEncode(t testing.TB, s *avro.Schema, dst []byte, v any, opts ...avro.Opt) []byte {
	t.Helper()
	b, err := s.AppendEncode(dst, v, opts...)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	return b
}

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
