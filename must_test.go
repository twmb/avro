package avro

import (
	"encoding/json"
	"testing"
)

// The must* helpers below each run one library call and fail the test if it
// errors. They exist so a cell that only needs the successful result reads as
// one line instead of four; every one calls t.Helper(), so a failure is
// reported at the caller's line, and every message names the operation. A site
// whose own failure message says more than the operation name — which cell,
// which input, which axis — keeps its own error handling and is not folded
// into these.

func mustParse(t testing.TB, schema string, opts ...SchemaOpt) *Schema {
	t.Helper()
	s, err := Parse(schema, opts...)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return s
}

func mustCacheParse(t testing.TB, c *SchemaCache, schema string, opts ...SchemaOpt) *Schema {
	t.Helper()
	s, err := c.Parse(schema, opts...)
	if err != nil {
		t.Fatalf("SchemaCache.Parse: %v", err)
	}
	return s
}

func mustSchemaFor[T any](t testing.TB, opts ...SchemaOpt) *Schema {
	t.Helper()
	s, err := SchemaFor[T](opts...)
	if err != nil {
		t.Fatalf("SchemaFor: %v", err)
	}
	return s
}

func mustResolve(t testing.TB, writer, reader *Schema) *Schema {
	t.Helper()
	s, err := Resolve(writer, reader)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	return s
}

func mustNodeSchema(t testing.TB, n *SchemaNode, opts ...SchemaOpt) *Schema {
	t.Helper()
	s, err := n.Schema(opts...)
	if err != nil {
		t.Fatalf("SchemaNode.Schema: %v", err)
	}
	return s
}

func mustEncode(t testing.TB, s *Schema, v any, opts ...Opt) []byte {
	t.Helper()
	b, err := s.Encode(v, opts...)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return b
}

func mustAppendEncode(t testing.TB, s *Schema, dst []byte, v any, opts ...Opt) []byte {
	t.Helper()
	b, err := s.AppendEncode(dst, v, opts...)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	return b
}

func mustDecode(t testing.TB, s *Schema, src []byte, v any, opts ...Opt) []byte {
	t.Helper()
	rest, err := s.Decode(src, v, opts...)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return rest
}

func mustEncodeJSON(t testing.TB, s *Schema, v any, opts ...Opt) []byte {
	t.Helper()
	b, err := s.EncodeJSON(v, opts...)
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	return b
}

func mustAppendEncodeJSON(t testing.TB, s *Schema, dst []byte, v any, opts ...Opt) []byte {
	t.Helper()
	b, err := s.AppendEncodeJSON(dst, v, opts...)
	if err != nil {
		t.Fatalf("AppendEncodeJSON: %v", err)
	}
	return b
}

func mustDecodeJSON(t testing.TB, s *Schema, src []byte, v any, opts ...Opt) {
	t.Helper()
	if err := s.DecodeJSON(src, v, opts...); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
}

func mustAppendSingleObject(t testing.TB, s *Schema, dst []byte, v any, opts ...Opt) []byte {
	t.Helper()
	b, err := s.AppendSingleObject(dst, v, opts...)
	if err != nil {
		t.Fatalf("AppendSingleObject: %v", err)
	}
	return b
}

func mustDecodeSingleObject(t testing.TB, s *Schema, src []byte, v any, opts ...Opt) []byte {
	t.Helper()
	rest, err := s.DecodeSingleObject(src, v, opts...)
	if err != nil {
		t.Fatalf("DecodeSingleObject: %v", err)
	}
	return rest
}

func mustMarshal(t testing.TB, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return b
}

func mustUnmarshal(t testing.TB, data []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
}
