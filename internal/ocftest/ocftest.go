// Package ocftest holds the ocf must helpers the external test packages
// share. The package ocf tests cannot import this without an import cycle,
// so ocf/must_test.go keeps its own copies.
package ocftest

import (
	"io"
	"testing"

	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

func MustNewWriter(t testing.TB, w io.Writer, s *avro.Schema, opts ...ocf.WriterOpt) *ocf.Writer {
	t.Helper()
	ow, err := ocf.NewWriter(w, s, opts...)
	if err != nil {
		t.Fatalf("ocf.NewWriter: %v", err)
	}
	return ow
}

func MustNewReader(t testing.TB, r io.Reader, opts ...ocf.ReaderOpt) *ocf.Reader {
	t.Helper()
	or, err := ocf.NewReader(r, opts...)
	if err != nil {
		t.Fatalf("ocf.NewReader: %v", err)
	}
	return or
}
