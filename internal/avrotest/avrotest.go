// Package avrotest holds the must helpers the external test packages share.
// Each runs one library call and fails the test if it errors, so a test that
// only needs the successful result reads as one line. The package avro tests
// cannot import this without an import cycle, so must_test.go keeps its own
// copies.
package avrotest

import (
	"encoding/json"
	"io"
	"testing"

	"github.com/twmb/avro"
)

func MustParse(t testing.TB, schema string, opts ...avro.SchemaOpt) *avro.Schema {
	t.Helper()
	s, err := avro.Parse(schema, opts...)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return s
}

func MustCacheParse(t testing.TB, c *avro.SchemaCache, schema string, opts ...avro.SchemaOpt) *avro.Schema {
	t.Helper()
	s, err := c.Parse(schema, opts...)
	if err != nil {
		t.Fatalf("SchemaCache.Parse: %v", err)
	}
	return s
}

func MustResolve(t testing.TB, writer, reader *avro.Schema) *avro.Schema {
	t.Helper()
	s, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	return s
}

func MustNodeSchema(t testing.TB, n *avro.SchemaNode, opts ...avro.SchemaOpt) *avro.Schema {
	t.Helper()
	s, err := n.Schema(opts...)
	if err != nil {
		t.Fatalf("SchemaNode.Schema: %v", err)
	}
	return s
}

func MustEncode(t testing.TB, s *avro.Schema, v any, opts ...avro.Opt) []byte {
	t.Helper()
	b, err := s.Encode(v, opts...)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return b
}

func MustAppendEncode(t testing.TB, s *avro.Schema, dst []byte, v any, opts ...avro.Opt) []byte {
	t.Helper()
	b, err := s.AppendEncode(dst, v, opts...)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	return b
}

func MustDecode(t testing.TB, s *avro.Schema, src []byte, v any, opts ...avro.Opt) []byte {
	t.Helper()
	rest, err := s.Decode(src, v, opts...)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return rest
}

func MustEncodeJSON(t testing.TB, s *avro.Schema, v any, opts ...avro.Opt) []byte {
	t.Helper()
	b, err := s.EncodeJSON(v, opts...)
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	return b
}

func MustAppendEncodeJSON(t testing.TB, s *avro.Schema, dst []byte, v any, opts ...avro.Opt) []byte {
	t.Helper()
	b, err := s.AppendEncodeJSON(dst, v, opts...)
	if err != nil {
		t.Fatalf("AppendEncodeJSON: %v", err)
	}
	return b
}

func MustDecodeJSON(t testing.TB, s *avro.Schema, src []byte, v any, opts ...avro.Opt) {
	t.Helper()
	if err := s.DecodeJSON(src, v, opts...); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
}

func MustAppendSingleObject(t testing.TB, s *avro.Schema, dst []byte, v any, opts ...avro.Opt) []byte {
	t.Helper()
	b, err := s.AppendSingleObject(dst, v, opts...)
	if err != nil {
		t.Fatalf("AppendSingleObject: %v", err)
	}
	return b
}

func MustDecodeSingleObject(t testing.TB, s *avro.Schema, src []byte, v any, opts ...avro.Opt) []byte {
	t.Helper()
	rest, err := s.DecodeSingleObject(src, v, opts...)
	if err != nil {
		t.Fatalf("DecodeSingleObject: %v", err)
	}
	return rest
}

func MustClose(t testing.TB, c io.Closer) {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func MustUnmarshal(t testing.TB, data []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
}
