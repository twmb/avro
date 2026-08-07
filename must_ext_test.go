package avro_test

import (
	"encoding/json"
	"io"
	"testing"

	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
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

func mustCacheParse(t testing.TB, c *avro.SchemaCache, schema string, opts ...avro.SchemaOpt) *avro.Schema {
	t.Helper()
	s, err := c.Parse(schema, opts...)
	if err != nil {
		t.Fatalf("SchemaCache.Parse: %v", err)
	}
	return s
}

func mustResolve(t testing.TB, writer, reader *avro.Schema) *avro.Schema {
	t.Helper()
	s, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	return s
}

func mustNodeSchema(t testing.TB, n *avro.SchemaNode, opts ...avro.SchemaOpt) *avro.Schema {
	t.Helper()
	s, err := n.Schema(opts...)
	if err != nil {
		t.Fatalf("SchemaNode.Schema: %v", err)
	}
	return s
}

func mustEncode(t testing.TB, s *avro.Schema, v any, opts ...avro.Opt) []byte {
	t.Helper()
	b, err := s.Encode(v, opts...)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return b
}

func mustAppendEncode(t testing.TB, s *avro.Schema, dst []byte, v any, opts ...avro.Opt) []byte {
	t.Helper()
	b, err := s.AppendEncode(dst, v, opts...)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	return b
}

func mustDecode(t testing.TB, s *avro.Schema, src []byte, v any, opts ...avro.Opt) []byte {
	t.Helper()
	rest, err := s.Decode(src, v, opts...)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return rest
}

func mustEncodeJSON(t testing.TB, s *avro.Schema, v any, opts ...avro.Opt) []byte {
	t.Helper()
	b, err := s.EncodeJSON(v, opts...)
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	return b
}

func mustAppendEncodeJSON(t testing.TB, s *avro.Schema, dst []byte, v any, opts ...avro.Opt) []byte {
	t.Helper()
	b, err := s.AppendEncodeJSON(dst, v, opts...)
	if err != nil {
		t.Fatalf("AppendEncodeJSON: %v", err)
	}
	return b
}

func mustDecodeJSON(t testing.TB, s *avro.Schema, src []byte, v any, opts ...avro.Opt) {
	t.Helper()
	if err := s.DecodeJSON(src, v, opts...); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
}

func mustAppendSingleObject(t testing.TB, s *avro.Schema, dst []byte, v any, opts ...avro.Opt) []byte {
	t.Helper()
	b, err := s.AppendSingleObject(dst, v, opts...)
	if err != nil {
		t.Fatalf("AppendSingleObject: %v", err)
	}
	return b
}

func mustDecodeSingleObject(t testing.TB, s *avro.Schema, src []byte, v any, opts ...avro.Opt) []byte {
	t.Helper()
	rest, err := s.DecodeSingleObject(src, v, opts...)
	if err != nil {
		t.Fatalf("DecodeSingleObject: %v", err)
	}
	return rest
}

func mustNewWriter(t testing.TB, w io.Writer, s *avro.Schema, opts ...ocf.WriterOpt) *ocf.Writer {
	t.Helper()
	ow, err := ocf.NewWriter(w, s, opts...)
	if err != nil {
		t.Fatalf("ocf.NewWriter: %v", err)
	}
	return ow
}

func mustNewReader(t testing.TB, r io.Reader, opts ...ocf.ReaderOpt) *ocf.Reader {
	t.Helper()
	or, err := ocf.NewReader(r, opts...)
	if err != nil {
		t.Fatalf("ocf.NewReader: %v", err)
	}
	return or
}

func mustClose(t testing.TB, c io.Closer) {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func mustUnmarshal(t testing.TB, data []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
}
