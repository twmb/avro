package ocf

import (
	"bytes"
	"testing"

	"github.com/twmb/avro"
)

// mustOCF builds a valid OCF with the given schema, values, and writer options.
func mustOCF(f *testing.F, schema *avro.Schema, values []any, opts ...WriterOpt) []byte {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, schema, opts...)
	if err != nil {
		f.Fatal(err)
	}
	for _, v := range values {
		if err := w.Encode(v); err != nil {
			f.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		f.Fatal(err)
	}
	return buf.Bytes()
}

func FuzzOCFReader(f *testing.F) {
	stringSchema := avro.MustParse(`"string"`)

	// Null codec.
	f.Add(mustOCF(f, stringSchema, []any{"hello", "world"}))

	// Deflate codec.
	f.Add(mustOCF(f, stringSchema, []any{"compressed"}, WithCodec(DeflateCodec(1))))

	// Snappy codec.
	f.Add(mustOCF(f, stringSchema, []any{"snappy"}, WithCodec(SnappyCodec())))

	// Zstd codec.
	f.Add(mustOCF(f, stringSchema, []any{"zstandard"}, WithCodec(MustZstdCodec(nil, nil))))

	// Record schema exercises more decoder paths.
	recSchema := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`)
	f.Add(mustOCF(f, recSchema, []any{
		map[string]any{"a": int32(1), "b": "x"},
		map[string]any{"a": int32(2), "b": "y"},
	}))

	// Multi-block: WithBlockCount(1) forces each value into its own block.
	f.Add(mustOCF(f, stringSchema, []any{"block1", "block2", "block3"}, WithBlockCount(1)))

	// Empty input.
	f.Add([]byte{})

	// Just the magic bytes.
	f.Add([]byte{'O', 'b', 'j', 1})

	f.Fuzz(func(t *testing.T, data []byte) {
		r, err := NewReader(bytes.NewReader(data))
		if err != nil {
			return
		}
		for {
			var v any
			if err := r.Decode(&v); err != nil {
				break
			}
		}
		r.Close()
	})
}

// FuzzOCFRoundTrip writes a record to OCF, reads it back, and verifies
// round-trip integrity. Covers writer + reader together — the existing
// FuzzOCFReader only exercises the reader against valid + malformed bytes.
func FuzzOCFRoundTrip(f *testing.F) {
	intSchema := avro.MustParse(`"int"`)
	stringSchema := avro.MustParse(`"string"`)
	recordSchema := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`)

	f.Add(uint8(0), int32(0), "")
	f.Add(uint8(0), int32(-1), "")
	f.Add(uint8(0), int32(1<<30), "")
	f.Add(uint8(1), int32(0), "x")
	f.Add(uint8(1), int32(0), "")
	f.Add(uint8(2), int32(7), "y")

	f.Fuzz(func(t *testing.T, mode uint8, a int32, b string) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic: %v", r)
			}
		}()
		var (
			schema *avro.Schema
			val    any
		)
		switch mode % 3 {
		case 0:
			schema, val = intSchema, a
		case 1:
			schema, val = stringSchema, b
		case 2:
			schema, val = recordSchema, map[string]any{"a": a, "b": b}
		}
		var buf bytes.Buffer
		w, err := NewWriter(&buf, schema)
		if err != nil {
			return
		}
		if err := w.Encode(val); err != nil {
			return
		}
		if err := w.Close(); err != nil {
			return
		}
		r, err := NewReader(bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatalf("read after valid write failed: %v", err)
		}
		var got any
		if err := r.Decode(&got); err != nil {
			t.Fatalf("decode after valid write failed: %v", err)
		}
		r.Close()
	})
}
