package ocf_test

import (
	"io"
	"testing"

	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

// The must* helpers below each run one library call and fail the test if it
// errors. A cell that only needs the successful result then reads as one line,
// not four. Every one calls t.Helper(), so a failure lands on the caller's
// line.

func mustNewWriter(t testing.TB, w io.Writer, s *avro.Schema, opts ...ocf.WriterOpt) *ocf.Writer {
	t.Helper()
	ow, err := ocf.NewWriter(w, s, opts...)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return ow
}

func mustNewReader(t testing.TB, r io.Reader, opts ...ocf.ReaderOpt) *ocf.Reader {
	t.Helper()
	or, err := ocf.NewReader(r, opts...)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return or
}

func mustClose(t testing.TB, c io.Closer) {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
