package ocf

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// OCF matrix: schema fragments × every built-in codec × multi-block files ×
// append mode × reader-schema evolution. The container layer (blocks, sync
// markers, codecs, header schema text) is plumbing the value-level matrix
// never touches.
// ---------------------------------------------------------------------------

type ocfFrag struct {
	label  string
	schema string
	values []any
}

func ocfFrags() []ocfFrag {
	return []ocfFrag{
		{"int", `"int"`, []any{int32(1), int32(-2), int32(3)}},
		{"string", `"string"`, []any{"a", "", "ccc"}},
		{"record", `{"type":"record","name":"OR","fields":[
			{"name":"a","type":"int"},{"name":"b","type":["null","string"],"default":null}]}`,
			[]any{
				map[string]any{"a": int32(1), "b": "x"},
				map[string]any{"a": int32(2), "b": nil},
			}},
		{"fixed0", `{"type":"fixed","name":"OF0","size":0}`, []any{[]byte{}, []byte{}}},
		{"decimal", `{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}`,
			[]any{[]byte{0x30, 0x39}, []byte{0x01}}},
		{"timestamp", `{"type":"long","logicalType":"timestamp-millis"}`,
			[]any{int64(1717243496789), int64(0)}},
		{"recursive", `{"type":"record","name":"ON","fields":[
			{"name":"v","type":"int"},{"name":"next","type":["null","ON"],"default":null}]}`,
			[]any{
				map[string]any{"v": int32(1), "next": map[string]any{"v": int32(2), "next": nil}},
				map[string]any{"v": int32(3), "next": nil},
			}},
		{"nullunion", `["null","long"]`, []any{int64(5), nil, int64(-9)}},
	}
}

// ocfCodecMakers returns fresh-codec factories: Writer.Close closes its
// codec (zstd holds goroutines whose lifetime must be bounded), so a codec
// instance must never be shared between a writer and a reader.
func ocfCodecMakers() []struct {
	name string
	mk   func(t *testing.T) Codec
} {
	return []struct {
		name string
		mk   func(t *testing.T) Codec
	}{
		{"deflate", func(*testing.T) Codec { return DeflateCodec(5) }},
		{"snappy", func(*testing.T) Codec { return SnappyCodec() }},
		{"zstandard", func(t *testing.T) Codec {
			z, err := ZstdCodec(nil, nil)
			if err != nil {
				t.Fatalf("ZstdCodec: %v", err)
			}
			return z
		}},
	}
}

func TestMatrixOCF_CodecsAndBlocks(t *testing.T) {
	const rounds = 7 // × values per fragment, split across several blocks
	for _, fr := range ocfFrags() {
		schema := avro.MustParse(fr.schema)
		// Canonical decoded forms, calibrated through the schema itself.
		var want []any
		for r := 0; r < rounds; r++ {
			for _, v := range fr.values {
				w, err := schema.AppendEncode(nil, v)
				if err != nil {
					t.Fatalf("%s: encode: %v", fr.label, err)
				}
				var a any
				if _, err := schema.Decode(w, &a); err != nil {
					t.Fatalf("%s: decode: %v", fr.label, err)
				}
				want = append(want, a)
			}
		}
		for _, cm := range ocfCodecMakers() {
			t.Run(fr.label+"/"+cm.name, func(t *testing.T) {
				var buf bytes.Buffer
				// Tiny block count forces multiple blocks per file.
				w, err := NewWriter(&buf, schema, WithCodec(cm.mk(t)), WithBlockCount(2))
				if err != nil {
					t.Fatalf("NewWriter: %v", err)
				}
				for r := 0; r < rounds; r++ {
					for _, v := range fr.values {
						if err := w.Encode(v); err != nil {
							t.Fatalf("Encode: %v", err)
						}
					}
				}
				if err := w.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}

				r, err := NewReader(bytes.NewReader(buf.Bytes()), WithCodec(cm.mk(t)))
				if err != nil {
					t.Fatalf("NewReader: %v", err)
				}
				defer r.Close()
				var got []any
				for {
					var v any
					err := r.Decode(&v)
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						t.Fatalf("Decode #%d: %v", len(got), err)
					}
					got = append(got, v)
				}
				if len(got) != len(want) {
					t.Fatalf("read %d of %d", len(got), len(want))
				}
				for i := range want {
					if !reflect.DeepEqual(got[i], want[i]) {
						t.Fatalf("datum %d: got %#v want %#v", i, got[i], want[i])
					}
				}
			})
		}
	}
}

// Append mode: NewAppendWriter must continue a file written by NewWriter —
// same schema, same codec recovered from the header, sync preserved — and
// the combined stream must read back in order.
func TestMatrixOCF_AppendWriter(t *testing.T) {
	for _, fr := range ocfFrags() {
		for _, cm := range ocfCodecMakers() {
			t.Run(fr.label+"/"+cm.name, func(t *testing.T) {
				schema := avro.MustParse(fr.schema)
				path := filepath.Join(t.TempDir(), "f.avro")
				f, err := os.Create(path)
				if err != nil {
					t.Fatal(err)
				}
				w, err := NewWriter(f, schema, WithCodec(cm.mk(t)), WithBlockCount(2))
				if err != nil {
					t.Fatalf("NewWriter: %v", err)
				}
				for _, v := range fr.values {
					if err := w.Encode(v); err != nil {
						t.Fatalf("Encode: %v", err)
					}
				}
				if err := w.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
				f.Close()

				f2, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				aw, err := NewAppendWriter(f2, WithCodec(cm.mk(t)), WithBlockCount(2))
				if err != nil {
					t.Fatalf("NewAppendWriter: %v", err)
				}
				for _, v := range fr.values {
					if err := aw.Encode(v); err != nil {
						t.Fatalf("append Encode: %v", err)
					}
				}
				if err := aw.Close(); err != nil {
					t.Fatalf("append Close: %v", err)
				}
				f2.Close()

				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				r, err := NewReader(bytes.NewReader(data), WithCodec(cm.mk(t)))
				if err != nil {
					t.Fatalf("NewReader: %v", err)
				}
				defer r.Close()
				var n int
				for {
					var v any
					err := r.Decode(&v)
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						t.Fatalf("Decode #%d: %v", n, err)
					}
					n++
				}
				if n != 2*len(fr.values) {
					t.Fatalf("read %d of %d after append", n, 2*len(fr.values))
				}
			})
		}
	}
}

// Reader-schema evolution through the OCF header: promotion and a defaulted
// added field, via both WithReaderSchema and WithReaderSchemaFunc.
func TestMatrixOCF_ReaderSchemaEvolution(t *testing.T) {
	wSchema := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	rSchema := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"long"},
		{"name":"b","type":"string","default":"dflt"}]}`)
	var buf bytes.Buffer
	w, err := NewWriter(&buf, wSchema)
	if err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < 5; i++ {
		if err := w.Encode(map[string]any{"a": i}); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	check := func(t *testing.T, r *Reader) {
		t.Helper()
		defer r.Close()
		var i int64
		for {
			var v map[string]any
			err := r.Decode(&v)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if v["a"] != i || v["b"] != "dflt" {
				t.Fatalf("datum %d: %#v", i, v)
			}
			i++
		}
		if i != 5 {
			t.Fatalf("read %d", i)
		}
	}
	r1, err := NewReader(bytes.NewReader(buf.Bytes()), WithReaderSchema(rSchema))
	if err != nil {
		t.Fatalf("WithReaderSchema: %v", err)
	}
	check(t, r1)

	r2, err := NewReader(bytes.NewReader(buf.Bytes()), WithReaderSchemaFunc(func(r *Reader) (*avro.Schema, error) {
		return rSchema, nil
	}))
	if err != nil {
		t.Fatalf("WithReaderSchemaFunc: %v", err)
	}
	check(t, r2)
}
