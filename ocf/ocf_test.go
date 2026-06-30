package ocf

import (
	"bufio"
	"bytes"
	"compress/flate"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/twmb/avro"
)

const recordSchema = `{"type":"record","name":"person","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`

type person struct {
	Name string `avro:"name"`
	Age  int32  `avro:"age"`
}

func TestRoundTrip(t *testing.T) {
	s, err := avro.Parse(recordSchema)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	in := []person{
		{"Alice", 30},
		{"Bob", 25},
	}
	for _, p := range in {
		if err := w.Encode(&p); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var out []person
	for {
		var p person
		if err := r.Decode(&p); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		out = append(out, p)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("got %v, want %v", out, in)
	}
}

func TestDeflate(t *testing.T) {
	s, err := avro.Parse(recordSchema)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithCodec(DeflateCodec(flate.DefaultCompression)))
	if err != nil {
		t.Fatal(err)
	}
	in := []person{
		{"Alice", 30},
		{"Bob", 25},
		{"Charlie", 40},
	}
	for _, p := range in {
		if err := w.Encode(&p); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var out []person
	for {
		var p person
		if err := r.Decode(&p); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		out = append(out, p)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("got %v, want %v", out, in)
	}
}

func TestMultipleBlocks(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	const n = 250
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s) // default block length 100
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		v := int32(i)
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		var v int32
		if err := r.Decode(&v); err != nil {
			t.Fatalf("item %d: %v", i, err)
		}
		if v != int32(i) {
			t.Fatalf("item %d: got %d, want %d", i, v, i)
		}
	}
	var v int32
	if err := r.Decode(&v); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestCustomBlockCount(t *testing.T) {
	s, err := avro.Parse(`"string"`)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithBlockCount(2))
	if err != nil {
		t.Fatal(err)
	}
	strs := []string{"a", "b", "c", "d", "e"}
	for _, v := range strs {
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for {
		var v string
		if err := r.Decode(&v); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if !reflect.DeepEqual(strs, got) {
		t.Fatalf("got %v, want %v", got, strs)
	}
}

func TestMetadata(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s,
		WithMetadata(map[string][]byte{
			"my.key":  []byte("my.value"),
			"another": []byte("data"),
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	meta := r.Metadata()
	if got := string(meta["my.key"]); got != "my.value" {
		t.Fatalf("my.key: got %q, want %q", got, "my.value")
	}
	if got := string(meta["another"]); got != "data" {
		t.Fatalf("another: got %q, want %q", got, "data")
	}
}

func TestReaderSchema(t *testing.T) {
	s, err := avro.Parse(recordSchema)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	p := person{"Alice", 30}
	if err := w.Encode(&p); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	// Use the reader's schema to decode.
	rs := r.Schema()
	if rs == nil {
		t.Fatal("schema is nil")
	}
	var out person
	if err := r.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != p {
		t.Fatalf("got %v, want %v", out, p)
	}
}

func TestEmpty(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var v int32
	if err := r.Decode(&v); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

type xorCodec struct{ key byte }

func (x xorCodec) Name() string { return "xor" }
func (x xorCodec) Close() error { return nil }
func (x xorCodec) Compress(src []byte) ([]byte, error) {
	dst := make([]byte, len(src))
	for i, b := range src {
		dst[i] = b ^ x.key
	}
	return dst, nil
}

func (x xorCodec) Decompress(src []byte) ([]byte, error) {
	return x.Compress(src) // xor is its own inverse
}

func TestCustomCodec(t *testing.T) {
	s, err := avro.Parse(`"long"`)
	if err != nil {
		t.Fatal(err)
	}
	codec := xorCodec{0xAB}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithCodec(codec))
	if err != nil {
		t.Fatal(err)
	}
	in := []int64{1, 2, 3, 100, -50}
	for _, v := range in {
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf, WithCodec(codec))
	if err != nil {
		t.Fatal(err)
	}
	var out []int64
	for {
		var v int64
		if err := r.Decode(&v); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		out = append(out, v)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("got %v, want %v", out, in)
	}
}

func TestBadMagic(t *testing.T) {
	_, err := NewReader(bytes.NewReader([]byte("garbage data here")))
	if err == nil {
		t.Fatal("expected error for bad magic")
	}
}

func TestUnknownCodec(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	codec := xorCodec{0x42}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithCodec(codec))
	if err != nil {
		t.Fatal(err)
	}
	v := int32(1)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Read without registering the codec.
	_, err = NewReader(bytes.NewReader(buf.Bytes()))
	if err == nil {
		t.Fatal("expected error for unknown codec")
	}
}

func TestBadSync(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithBlockCount(1))
	if err != nil {
		t.Fatal(err)
	}
	v := int32(42)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	// Encode flushes automatically at block length 1, so the block is
	// already written. Write a second item to get another block.
	v = 99
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the sync marker in the second block.
	data := buf.Bytes()
	// The sync marker is the last 16 bytes of the second block, which
	// ends at the last 16 bytes of the file. Corrupt the one before that.
	// Find the second sync marker: it's at len(data) - 16 (last block's sync).
	// The first block's sync ends at some point before that. We corrupt
	// a byte in the second-to-last sync marker area.
	//
	// With block length 1, layout is:
	//   header (magic + metadata + sync)
	//   block1 (count + size + data + sync)
	//   block2 (count + size + data + sync)
	//
	// We want to corrupt block2's sync. That's the last 16 bytes.
	if len(data) < 32 {
		t.Fatal("data too short")
	}
	data[len(data)-1] ^= 0xFF // flip bits in last sync byte

	r, err := NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	// First block should decode fine.
	if err := r.Decode(&v); err != nil {
		t.Fatal(err)
	}
	// Second block should fail with sync mismatch.
	err = r.Decode(&v)
	if err == nil {
		t.Fatal("expected sync mismatch error")
	}
}

func TestPrimitiveSchema(t *testing.T) {
	s, err := avro.Parse(`"string"`)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	in := []string{"hello", "world", ""}
	for _, v := range in {
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for {
		var v string
		if err := r.Decode(&v); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		out = append(out, v)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("got %v, want %v", out, in)
	}
}

func TestBlockCountZeroOrNegative(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	// Block count 0 with no block bytes defaults to 100.
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithBlockCount(0))
	if err != nil {
		t.Fatal(err)
	}
	v := int32(7)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var out int32
	if err := r.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != 7 {
		t.Fatalf("got %d, want 7", out)
	}
}

func TestCloseIdempotent(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	// Close with no items.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Close again — no error expected.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseFlushError(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	ew := &errAfterN{max: 4096}
	w, err := NewWriter(ew, s, WithBlockCount(1000)) // large block, no auto-flush
	if err != nil {
		t.Fatal(err)
	}
	v := int32(1)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	// Items are buffered. Make the writer fail so Close's flush fails.
	ew.max = 0
	if err := w.Close(); err == nil {
		t.Fatal("expected error from flush during Close")
	}
}

func TestStickyWriteError(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	// Use a writer that accepts the header but fails on block writes.
	ew := &errAfterN{max: 4096}
	w, err := NewWriter(ew, s, WithBlockCount(1))
	if err != nil {
		t.Fatal(err)
	}
	// Now make subsequent writes fail.
	ew.max = 0
	v := int32(1)
	err = w.Encode(&v)
	if err == nil {
		t.Fatal("expected error")
	}
	// Subsequent calls should return the sticky error.
	if err2 := w.Encode(&v); err2 == nil {
		t.Fatal("expected sticky error on second encode")
	}
	if err3 := w.Close(); err3 == nil {
		t.Fatal("expected sticky error on close")
	}
}

type errAfterN struct {
	written int
	max     int
}

func (e *errAfterN) Write(p []byte) (int, error) {
	if e.written+len(p) > e.max {
		return 0, io.ErrClosedPipe
	}
	e.written += len(p)
	return len(p), nil
}

// failFirstWriteSink fails its first Write with err, then accepts every
// subsequent write into buf. It models a sink whose state is "not knowable"
// after a failed header write during Reset (the first write to the new sink):
// an un-poisoned Writer would go on to emit a headerless block here, so the
// poison contract is what stops the silent corruption.
type failFirstWriteSink struct {
	buf    bytes.Buffer
	err    error
	failed bool
}

func (f *failFirstWriteSink) Write(p []byte) (int, error) {
	if !f.failed {
		f.failed = true
		return 0, f.err
	}
	return f.buf.Write(p)
}

func TestShortHeader(t *testing.T) {
	// Only 3 bytes — not enough for magic.
	_, err := NewReader(bytes.NewReader([]byte{0x4f, 0x62, 0x6a}))
	if err == nil {
		t.Fatal("expected error for short header")
	}
}

func TestDeflateRoundTripLarge(t *testing.T) {
	s, err := avro.Parse(`"string"`)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s,
		WithCodec(DeflateCodec(flate.BestSpeed)),
		WithBlockCount(10),
	)
	if err != nil {
		t.Fatal(err)
	}
	var in []string
	for i := range 100 {
		v := fmt.Sprintf("value-%d", i)
		in = append(in, v)
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for {
		var v string
		if err := r.Decode(&v); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		out = append(out, v)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatal("large deflate round trip mismatch")
	}
}

func TestEncodeError(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithBlockCount(100))
	if err != nil {
		t.Fatal(err)
	}
	// Encode a string into an int schema — should fail.
	v := "not an int"
	err = w.Encode(&v)
	if err == nil {
		t.Fatal("expected error encoding bad type")
	}
	// A value error discards only the failed datum; the Writer remains
	// usable (see TestRegression_OCFWriterValueErrorRecovers for the
	// full accepted-datums-survive contract). Only I/O and compression
	// errors poison the Writer (TestCompressError).
	n := int32(1)
	if err := w.Encode(&n); err != nil {
		t.Fatalf("encode after value error: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

type failCompressCodec struct{}

func (failCompressCodec) Name() string { return "failcompress" }
func (failCompressCodec) Close() error { return nil }
func (failCompressCodec) Compress([]byte) ([]byte, error) {
	return nil, errors.New("compress failed")
}
func (failCompressCodec) Decompress(src []byte) ([]byte, error) { return src, nil }

func TestCompressError(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithCodec(failCompressCodec{}), WithBlockCount(1))
	if err != nil {
		t.Fatal(err)
	}
	v := int32(1)
	err = w.Encode(&v)
	if err == nil {
		t.Fatal("expected compress error")
	}
	if !strings.Contains(err.Error(), "compress failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBadSchemaInFile(t *testing.T) {
	// Build a valid OCF header but with an invalid schema.
	var hdr []byte
	hdr = append(hdr, 'O', 'b', 'j', 0x01)
	hdr = encodeMap(hdr, []kv{
		{"avro.schema", []byte(`"invalid_type"`)},
	})
	hdr = append(hdr, make([]byte, 16)...) // sync marker
	_, err := NewReader(bytes.NewReader(hdr))
	if err == nil {
		t.Fatal("expected error for bad schema")
	}
}

func TestMissingSchemaInFile(t *testing.T) {
	var hdr []byte
	hdr = append(hdr, 'O', 'b', 'j', 0x01)
	hdr = encodeMap(hdr, []kv{
		{"some.other.key", []byte("value")},
	})
	hdr = append(hdr, make([]byte, 16)...)
	_, err := NewReader(bytes.NewReader(hdr))
	if err == nil {
		t.Fatal("expected error for missing schema")
	}
}

func TestTruncatedBlockCount(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Append a partial varlong (continuation byte with no termination)
	// after the valid empty file to trigger a non-EOF readBlock error.
	data := buf.Bytes()
	data = append(data, 0x80) // continuation byte, then EOF
	r, err := NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var v int32
	err = r.Decode(&v)
	if err == nil || err == io.EOF {
		t.Fatalf("expected non-EOF read error, got %v", err)
	}
}

func TestTruncatedBlockSize(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Write a valid block count (1) but truncate before the size.
	data := buf.Bytes()
	data = binary.AppendVarint(data, 1) // count = 1, then EOF before size
	r, err := NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var v int32
	err = r.Decode(&v)
	if err == nil {
		t.Fatal("expected error for truncated block size")
	}
}

func TestTruncatedBlockData(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	data = binary.AppendVarint(data, 1)   // count
	data = binary.AppendVarint(data, 100) // size = 100 bytes, but EOF
	data = append(data, 0x01)             // only 1 byte of data
	r, err := NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var v int32
	err = r.Decode(&v)
	if err == nil {
		t.Fatal("expected error for truncated block data")
	}
}

func TestTruncatedBlockSyncMarker(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	data = binary.AppendVarint(data, 1) // count
	data = binary.AppendVarint(data, 1) // size = 1
	data = append(data, 0x02)           // 1 byte of data
	data = append(data, 0x00)           // only 1 byte of sync marker, need 16
	r, err := NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var v int32
	err = r.Decode(&v)
	if err == nil {
		t.Fatal("expected error for truncated sync marker")
	}
}

func TestNegativeBlockSize(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	data = binary.AppendVarint(data, 1)  // count
	data = binary.AppendVarint(data, -1) // negative size
	r, err := NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var v int32
	err = r.Decode(&v)
	if err == nil {
		t.Fatal("expected error for negative block size")
	}
	if !strings.Contains(err.Error(), "negative block size") {
		t.Fatalf("unexpected error: %v", err)
	}
}

type failDecompressCodec struct{}

func (failDecompressCodec) Name() string                        { return "faildecompress" }
func (failDecompressCodec) Close() error                        { return nil }
func (failDecompressCodec) Compress(src []byte) ([]byte, error) { return src, nil }
func (failDecompressCodec) Decompress([]byte) ([]byte, error) {
	return nil, errors.New("decompress failed")
}

func TestDecompressError(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	codec := failDecompressCodec{}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithCodec(codec), WithBlockCount(1))
	if err != nil {
		t.Fatal(err)
	}
	v := int32(42)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), WithCodec(codec))
	if err != nil {
		t.Fatal(err)
	}
	err = r.Decode(&v)
	if err == nil {
		t.Fatal("expected decompress error")
	}
	if !strings.Contains(err.Error(), "decompress") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDecodeError(t *testing.T) {
	// Write a string value, then try to decode as int.
	s, err := avro.Parse(`"string"`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithBlockCount(1))
	if err != nil {
		t.Fatal(err)
	}
	v := "hello"
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	var n int32
	err = r.Decode(&n)
	if err == nil {
		t.Fatal("expected decode error")
	}
}

func TestTrailingBytesInBlock(t *testing.T) {
	// Construct a block where item count is 1 but the data has extra bytes.
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Manually construct a block with count=1 but data for 2 ints.
	hdr := buf.Bytes()
	sync := make([]byte, 16)
	copy(sync, hdr[len(hdr)-16:]) // extract sync from header

	var block []byte
	block = binary.AppendVarint(block, 1) // count = 1
	// Data: two varints (each is 1 byte for small values).
	itemData := binary.AppendVarint(nil, 10)
	itemData = binary.AppendVarint(itemData, 20) // extra data
	block = binary.AppendVarint(block, int64(len(itemData)))
	block = append(block, itemData...)
	block = append(block, sync...)

	data := append(hdr, block...)

	r, err := NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var v int32
	err = r.Decode(&v)
	if err == nil {
		t.Fatal("expected trailing bytes error")
	}
	if !strings.Contains(err.Error(), "trailing bytes") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestVarlongOverflow(t *testing.T) {
	// Feed 10 continuation bytes to readVarlongFrom to trigger overflow.
	data := bytes.Repeat([]byte{0x80}, 11)
	r := bufio.NewReader(bytes.NewReader(data))
	_, err := binary.ReadVarint(r)
	if err == nil {
		t.Fatal("expected overflow error")
	}
	if !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEncodeMapEmpty(t *testing.T) {
	// Verify empty map encodes as a single zero byte and decodes back.
	data := encodeMap(nil, nil)
	if len(data) != 1 || data[0] != 0 {
		t.Fatalf("expected [0], got %v", data)
	}
	r := bufio.NewReader(bytes.NewReader(data))
	m, err := decodeMap(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 0 {
		t.Fatalf("expected empty map, got %v", m)
	}
}

func TestTruncatedHeaderSyncMarker(t *testing.T) {
	// Valid magic + valid metadata + truncated sync marker.
	var hdr []byte
	hdr = append(hdr, 'O', 'b', 'j', 0x01)
	hdr = encodeMap(hdr, []kv{
		{"avro.schema", []byte(`"int"`)},
	})
	hdr = append(hdr, 0x00, 0x01) // only 2 bytes of sync, need 16
	_, err := NewReader(bytes.NewReader(hdr))
	if err == nil {
		t.Fatal("expected error for truncated header sync")
	}
}

func TestTruncatedMetadata(t *testing.T) {
	// Valid magic, then truncated metadata (just a continuation byte).
	var data []byte
	data = append(data, 'O', 'b', 'j', 0x01)
	data = append(data, 0x80) // continuation byte, incomplete varlong
	_, err := NewReader(bytes.NewReader(data))
	if err == nil {
		t.Fatal("expected error for truncated metadata")
	}
}

func TestDeflateInvalidLevel(t *testing.T) {
	c := DeflateCodec(999) // invalid flate level
	_, err := c.Compress([]byte("data"))
	if err == nil {
		t.Fatal("expected error for invalid deflate level")
	}
}

func TestDecodeMapNegativeCount(t *testing.T) {
	// Avro spec: negative count means abs(count) entries followed by block byte-size.
	// Build a map with negative count encoding.
	var data []byte
	count := int64(-2) // 2 entries
	data = binary.AppendVarint(data, count)
	// Build the entries first to know the byte size.
	var entries []byte
	// Entry 1: key="a", value="b"
	entries = binary.AppendVarint(entries, 1) // key len
	entries = append(entries, 'a')
	entries = binary.AppendVarint(entries, 1) // val len
	entries = append(entries, 'b')
	// Entry 2: key="c", value="d"
	entries = binary.AppendVarint(entries, 1)
	entries = append(entries, 'c')
	entries = binary.AppendVarint(entries, 1)
	entries = append(entries, 'd')
	data = binary.AppendVarint(data, int64(len(entries))) // byte size
	data = append(data, entries...)
	data = append(data, 0) // terminating zero-count block

	r := bufio.NewReader(bytes.NewReader(data))
	m, err := decodeMap(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(m["a"]) != "b" || string(m["c"]) != "d" {
		t.Fatalf("unexpected map: %v", m)
	}
}

func TestOptMarkerMethods(t *testing.T) {
	// Cover the unexported interface marker methods.
	var wo WriterOpt
	wo = WithCodec(nullCodec{})
	wo.(Opt).writerOpt()
	wo = WithBlockCount(1)
	wo.(optBlockCount).writerOpt()
	wo = WithBlockBytes(1)
	wo.(optBlockBytes).writerOpt()
	wo = WithMetadata(map[string][]byte{"k": nil})
	wo.(optMetadata).writerOpt()
	wo = WithSyncMarker([16]byte{})
	wo.(optSyncMarker).writerOpt()
	wo = WithSchema("")
	wo.(optSchema).writerOpt()
	var ro ReaderOpt
	ro = WithCodec(nullCodec{})
	ro.(Opt).readerOpt()
}

func TestHeaderWriteError(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewWriter(&errAfterN{max: 0}, s)
	if err == nil {
		t.Fatal("expected error writing header")
	}
}

func TestDecodeMapTruncatedNegCountSize(t *testing.T) {
	// Negative count followed by EOF where byte-size should be.
	var data []byte
	data = binary.AppendVarint(data, -2) // negative count
	// No byte-size follows — truncated.
	r := bufio.NewReader(bytes.NewReader(data))
	_, err := decodeMap(r)
	if err == nil {
		t.Fatal("expected error for truncated negative count byte-size")
	}
}

func TestDecodeMapTruncatedKeyLen(t *testing.T) {
	// Count = 1, then truncated key length.
	var data []byte
	data = binary.AppendVarint(data, 1) // 1 entry
	// No key length — truncated.
	r := bufio.NewReader(bytes.NewReader(data))
	_, err := decodeMap(r)
	if err == nil {
		t.Fatal("expected error for truncated key length")
	}
}

func TestDecodeMapTruncatedKeyData(t *testing.T) {
	var data []byte
	data = binary.AppendVarint(data, 1)  // 1 entry
	data = binary.AppendVarint(data, 10) // key length = 10
	data = append(data, 'a')             // only 1 byte of key, need 10
	r := bufio.NewReader(bytes.NewReader(data))
	_, err := decodeMap(r)
	if err == nil {
		t.Fatal("expected error for truncated key data")
	}
}

func TestDecodeMapTruncatedValLen(t *testing.T) {
	var data []byte
	data = binary.AppendVarint(data, 1) // 1 entry
	data = binary.AppendVarint(data, 1) // key length = 1
	data = append(data, 'k')            // key
	// No value length — truncated.
	r := bufio.NewReader(bytes.NewReader(data))
	_, err := decodeMap(r)
	if err == nil {
		t.Fatal("expected error for truncated value length")
	}
}

func TestDecodeMapTruncatedValData(t *testing.T) {
	var data []byte
	data = binary.AppendVarint(data, 1)  // 1 entry
	data = binary.AppendVarint(data, 1)  // key length = 1
	data = append(data, 'k')             // key
	data = binary.AppendVarint(data, 10) // val length = 10
	data = append(data, 'v')             // only 1 byte of val, need 10
	r := bufio.NewReader(bytes.NewReader(data))
	_, err := decodeMap(r)
	if err == nil {
		t.Fatal("expected error for truncated value data")
	}
}

func TestRandReadError(t *testing.T) {
	orig := randRead
	randRead = func(b []byte) (int, error) { return 0, errors.New("rand failed") }
	defer func() { randRead = orig }()

	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewWriter(&bytes.Buffer{}, s)
	if err == nil {
		t.Fatal("expected error from failing rand")
	}
	if !strings.Contains(err.Error(), "rand failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBlockCountNegative(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithBlockCount(-5))
	if err != nil {
		t.Fatal(err)
	}
	// Negative count defaults to 100; single item flushed on Close.
	v := int32(99)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var out int32
	if err := r.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != 99 {
		t.Fatalf("got %d, want 99", out)
	}
}

// ---------- seekBuf: io.ReadWriteSeeker for tests ----------

type seekBuf struct {
	data []byte
	pos  int
}

func (s *seekBuf) Read(p []byte) (int, error) {
	if s.pos >= len(s.data) {
		return 0, io.EOF
	}
	n := copy(p, s.data[s.pos:])
	s.pos += n
	return n, nil
}

func (s *seekBuf) Write(p []byte) (int, error) {
	end := s.pos + len(p)
	if end > len(s.data) {
		s.data = append(s.data[:s.pos], p...)
	} else {
		copy(s.data[s.pos:], p)
	}
	s.pos = end
	return len(p), nil
}

func (s *seekBuf) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = int64(s.pos) + offset
	case io.SeekEnd:
		abs = int64(len(s.data)) + offset
	default:
		return 0, errors.New("invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("negative position")
	}
	s.pos = int(abs)
	return abs, nil
}

// ---------- New feature tests ----------

func TestFlush(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithBlockCount(1000)) // large block, won't auto-flush
	if err != nil {
		t.Fatal(err)
	}

	// Encode two items, then flush.
	v := int32(1)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	v = 2
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Encode more after flush.
	v = 3
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Read all three items back.
	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var got []int32
	for {
		var x int32
		if err := r.Decode(&x); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		got = append(got, x)
	}
	want := []int32{1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestFlushEmpty(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	// Flush with nothing buffered — should be a no-op.
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var v int32
	if err := r.Decode(&v); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestFlushAfterError(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	ew := &errAfterN{max: 4096}
	w, err := NewWriter(ew, s, WithBlockCount(1))
	if err != nil {
		t.Fatal(err)
	}
	ew.max = 0
	v := int32(1)
	if err := w.Encode(&v); err == nil {
		t.Fatal("expected error")
	}
	// Flush should return the sticky error.
	if err := w.Flush(); err == nil {
		t.Fatal("expected sticky error on flush")
	}
}

func TestWithSyncMarker(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	var marker [16]byte
	for i := range marker {
		marker[i] = byte(i + 0xA0)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithSyncMarker(marker))
	if err != nil {
		t.Fatal(err)
	}
	v := int32(42)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// The sync marker should appear in the raw output.
	data := buf.Bytes()
	if !bytes.Contains(data, marker[:]) {
		t.Fatal("sync marker not found in output")
	}

	// Verify we can still read it back.
	r, err := NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var out int32
	if err := r.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != 42 {
		t.Fatalf("got %d, want 42", out)
	}
}

func TestWithBlockBytes(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	// Each int encodes as 1 byte (zigzag for small values).
	// Set maxBytes=3 so that after 3 items, the block is flushed.
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithBlockCount(0), WithBlockBytes(3))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 7 {
		v := int32(i)
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var got []int32
	for {
		var v int32
		if err := r.Decode(&v); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		got = append(got, v)
	}
	want := []int32{0, 1, 2, 3, 4, 5, 6}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestWithBlockBytesAndBlockCount(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	// Block count 2, block bytes very large — count triggers first.
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithBlockCount(2), WithBlockBytes(100000))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		v := int32(i)
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var got []int32
	for {
		var v int32
		if err := r.Decode(&v); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		got = append(got, v)
	}
	want := []int32{0, 1, 2, 3, 4}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestWithBlockCountZero(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	// Block count 0 + block bytes 2: only bytes triggers flush.
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithBlockCount(0), WithBlockBytes(2))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		v := int32(i)
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var got []int32
	for {
		var v int32
		if err := r.Decode(&v); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		got = append(got, v)
	}
	want := []int32{0, 1, 2, 3, 4}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestReset(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	var buf1 bytes.Buffer
	w, err := NewWriter(&buf1, s)
	if err != nil {
		t.Fatal(err)
	}
	v := int32(1)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}

	// Reset to a second buffer.
	var buf2 bytes.Buffer
	if err := w.Reset(&buf2); err != nil {
		t.Fatal(err)
	}
	v = 2
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// buf1 should contain item 1.
	r1, err := NewReader(&buf1)
	if err != nil {
		t.Fatal(err)
	}
	var out int32
	if err := r1.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != 1 {
		t.Fatalf("buf1: got %d, want 1", out)
	}

	// buf2 should contain item 2.
	r2, err := NewReader(&buf2)
	if err != nil {
		t.Fatal(err)
	}
	if err := r2.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != 2 {
		t.Fatalf("buf2: got %d, want 2", out)
	}
}

func TestResetClearsError(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	ew := &errAfterN{max: 4096}
	w, err := NewWriter(ew, s, WithBlockCount(1))
	if err != nil {
		t.Fatal(err)
	}
	ew.max = 0
	v := int32(1)
	// This encode triggers a flush which fails.
	if err := w.Encode(&v); err == nil {
		t.Fatal("expected error")
	}

	// Reset to a working writer clears the error.
	var buf bytes.Buffer
	if err := w.Reset(&buf); err != nil {
		t.Fatal(err)
	}
	v = 2
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var out int32
	if err := r.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != 2 {
		t.Fatalf("got %d, want 2", out)
	}
}

func TestResetFlushError(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	ew := &errAfterN{max: 4096}
	w, err := NewWriter(ew, s, WithBlockCount(1000))
	if err != nil {
		t.Fatal(err)
	}
	v := int32(1)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	// Items are buffered. Now make the writer fail so the flush during
	// Reset fails.
	ew.max = 0
	var buf bytes.Buffer
	if err := w.Reset(&buf); err == nil {
		t.Fatal("expected error from flush during reset")
	}
}

func TestResetRandError(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	v := int32(9)

	// LIVE writer: NewWriter generates the initial sync before the override is
	// installed. (Pre-fix this closed the writer first, so Reset returned
	// errClosed and the randRead override below never ran — the test pinned
	// nothing.)
	var first bytes.Buffer
	w, err := NewWriter(&first, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}

	// Fail sync-marker generation across the Reset.
	orig := randRead
	boom := errors.New("rand boom")
	randRead = func(b []byte) (int, error) { return 0, boom }
	defer func() { randRead = orig }()

	var second bytes.Buffer
	rerr := w.Reset(&second)
	if rerr == nil {
		t.Fatal("Reset should return the sync-generation error")
	}
	if !errors.Is(rerr, boom) {
		t.Fatalf("Reset error should wrap the rand error, got %v", rerr)
	}
	// The sink was repointed before sync generation failed, so the Writer is
	// poisoned: every subsequent call returns the sticky error.
	if err := w.Encode(&v); !errors.Is(err, boom) {
		t.Fatalf("Encode after failed Reset: want sticky %v, got %v", boom, err)
	}
	if err := w.Flush(); !errors.Is(err, boom) {
		t.Fatalf("Flush after failed Reset: want sticky %v, got %v", boom, err)
	}
	if err := w.Close(); !errors.Is(err, boom) {
		t.Fatalf("Close after failed Reset: want sticky %v, got %v", boom, err)
	}
	// Sync generation fails before any header write, so the new sink is
	// untouched — no partial/headerless stream.
	if second.Len() != 0 {
		t.Fatalf("new sink must be untouched after a sync-gen failure, got %d bytes", second.Len())
	}
}

func TestAppendWriter(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	// Write initial items.
	sb := &seekBuf{}
	w, err := NewWriter(sb, s)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		v := int32(i)
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Append more items.
	sb.pos = 0
	aw, err := NewAppendWriter(sb)
	if err != nil {
		t.Fatal(err)
	}
	for i := 3; i < 6; i++ {
		v := int32(i)
		if err := aw.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	if err := aw.Close(); err != nil {
		t.Fatal(err)
	}

	// Read all items.
	sb.pos = 0
	r, err := NewReader(sb)
	if err != nil {
		t.Fatal(err)
	}
	var got []int32
	for {
		var v int32
		if err := r.Decode(&v); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		got = append(got, v)
	}
	want := []int32{0, 1, 2, 3, 4, 5}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestAppendWriterBadHeader(t *testing.T) {
	sb := &seekBuf{data: []byte("garbage data here")}
	_, err := NewAppendWriter(sb)
	if err == nil {
		t.Fatal("expected error for bad header")
	}
}

func TestAppendWriterCustomCodec(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	codec := xorCodec{0xAB}

	// Write initial items with custom codec.
	sb := &seekBuf{}
	w, err := NewWriter(sb, s, WithCodec(codec), WithBlockCount(2))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		v := int32(i)
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Append with the same codec.
	sb.pos = 0
	aw, err := NewAppendWriter(sb, WithCodec(codec))
	if err != nil {
		t.Fatal(err)
	}
	for i := 3; i < 5; i++ {
		v := int32(i)
		if err := aw.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	if err := aw.Close(); err != nil {
		t.Fatal(err)
	}

	// Read all back.
	sb.pos = 0
	r, err := NewReader(sb, WithCodec(codec))
	if err != nil {
		t.Fatal(err)
	}
	var got []int32
	for {
		var v int32
		if err := r.Decode(&v); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		got = append(got, v)
	}
	want := []int32{0, 1, 2, 3, 4}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestAppendWriterSeekError(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	// Write a valid OCF file first.
	sb := &seekBuf{}
	w, err := NewWriter(sb, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Wrap in a type that fails on Seek.
	fsb := &failSeekRWS{data: sb.data}
	_, err = NewAppendWriter(fsb)
	if err == nil {
		t.Fatal("expected error from seek failure")
	}
}

type failSeekRWS struct {
	data []byte
	pos  int
}

func (f *failSeekRWS) Read(p []byte) (int, error) {
	if f.pos >= len(f.data) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.pos:])
	f.pos += n
	return n, nil
}

func (f *failSeekRWS) Write(p []byte) (int, error) {
	end := f.pos + len(p)
	if end > len(f.data) {
		f.data = append(f.data[:f.pos], p...)
	} else {
		copy(f.data[f.pos:], p)
	}
	f.pos = end
	return len(p), nil
}

func (f *failSeekRWS) Seek(int64, int) (int64, error) {
	return 0, errors.New("seek failed")
}

func TestAppendWriterUnknownCodec(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	// Write with a custom codec.
	codec := xorCodec{0x42}
	sb := &seekBuf{}
	w, err := NewWriter(sb, s, WithCodec(codec))
	if err != nil {
		t.Fatal(err)
	}
	v := int32(1)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Try to append without providing the codec.
	sb.pos = 0
	_, err = NewAppendWriter(sb)
	if err == nil {
		t.Fatal("expected error for unknown codec")
	}
}

func TestWithBlockBytesNegative(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	// Negative block bytes is clamped to 0; both zero → defaults to count 100.
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithBlockBytes(-1))
	if err != nil {
		t.Fatal(err)
	}
	v := int32(42)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var out int32
	if err := r.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != 42 {
		t.Fatalf("got %d, want 42", out)
	}
}

func TestAppendWriterBlockOpts(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	// Write initial items.
	sb := &seekBuf{}
	w, err := NewWriter(sb, s)
	if err != nil {
		t.Fatal(err)
	}
	v := int32(1)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Append with block count and block bytes opts (including negative
	// values to exercise clamping).
	sb.pos = 0
	aw, err := NewAppendWriter(sb, WithBlockCount(-1), WithBlockBytes(-1))
	if err != nil {
		t.Fatal(err)
	}
	v = 2
	if err := aw.Encode(&v); err != nil {
		t.Fatal(err)
	}
	if err := aw.Close(); err != nil {
		t.Fatal(err)
	}

	// Read all items back.
	sb.pos = 0
	r, err := NewReader(sb)
	if err != nil {
		t.Fatal(err)
	}
	var got []int32
	for {
		var x int32
		if err := r.Decode(&x); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		got = append(got, x)
	}
	want := []int32{1, 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestResetHeaderWriteError(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	v := int32(7)

	// A failed Reset header write must POISON a LIVE writer: the sink was
	// already repointed, so a subsequent Encode/Flush/Close must return the
	// sticky error rather than silently emit a headerless block onto the new
	// sink. (Pre-fix this used Close-before-Reset, so Reset returned errClosed
	// and the header-write path was never exercised.)
	t.Run("poisons", func(t *testing.T) {
		var first bytes.Buffer
		w, err := NewWriter(&first, s)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}

		boom := errors.New("header write boom")
		bad := &failFirstWriteSink{err: boom}
		rerr := w.Reset(bad)
		if rerr == nil {
			t.Fatal("Reset should return the header-write error")
		}
		if !errors.Is(rerr, boom) {
			t.Fatalf("Reset error should wrap the sink error, got %v", rerr)
		}
		// Sticky on every subsequent call.
		if err := w.Encode(&v); !errors.Is(err, boom) {
			t.Fatalf("Encode after failed Reset: want sticky %v, got %v", boom, err)
		}
		if err := w.Flush(); !errors.Is(err, boom) {
			t.Fatalf("Flush after failed Reset: want sticky %v, got %v", boom, err)
		}
		if err := w.Close(); !errors.Is(err, boom) {
			t.Fatalf("Close after failed Reset: want sticky %v, got %v", boom, err)
		}
		// No readable OCF leaked onto the new sink (un-poisoned, the post-Reset
		// Encode/Flush would have written a headerless block here).
		if _, err := NewReader(bytes.NewReader(bad.buf.Bytes())); err == nil {
			t.Fatalf("new sink must not hold a readable OCF; %d bytes parsed", bad.buf.Len())
		}
	})

	// A later successful Reset clears the poison and recovers, matching the
	// flush arm.
	t.Run("recovers", func(t *testing.T) {
		var first bytes.Buffer
		w, err := NewWriter(&first, s)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
		if err := w.Reset(&failFirstWriteSink{err: errors.New("boom")}); err == nil {
			t.Fatal("Reset to a failing sink should error")
		}
		if err := w.Encode(&v); err == nil {
			t.Fatal("writer should be poisoned after a failed Reset")
		}

		var good bytes.Buffer
		if err := w.Reset(&good); err != nil {
			t.Fatalf("Reset to a good sink should recover, got %v", err)
		}
		if err := w.Encode(&v); err != nil {
			t.Fatalf("Encode after recovery: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close after recovery: %v", err)
		}
		r, err := NewReader(&good)
		if err != nil {
			t.Fatalf("recovered OCF should be readable: %v", err)
		}
		var got int32
		if err := r.Decode(&got); err != nil {
			t.Fatalf("decode recovered datum: %v", err)
		}
		if got != v {
			t.Fatalf("recovered datum = %d, want %d", got, v)
		}
	})
}

// ---------- Write (pre-encoded bytes) ----------

func TestWrite(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	// Pre-encode a value.
	var encoded []byte
	v := int32(42)
	encoded, err = s.AppendEncode(encoded, &v)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	n, err := w.Write(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(encoded) {
		t.Fatalf("Write returned %d, want %d", n, len(encoded))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var out int32
	if err := r.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != 42 {
		t.Fatalf("got %d, want 42", out)
	}
}

func TestWriteAfterError(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	ew := &errAfterN{max: 4096}
	w, err := NewWriter(ew, s, WithBlockCount(1))
	if err != nil {
		t.Fatal(err)
	}
	ew.max = 0
	v := int32(1)
	if err := w.Encode(&v); err == nil {
		t.Fatal("expected error")
	}
	// Write should return the sticky error.
	_, err = w.Write([]byte{0x02})
	if err == nil {
		t.Fatal("expected sticky error on Write")
	}
}

func TestWriteAutoFlush(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithBlockCount(2))
	if err != nil {
		t.Fatal(err)
	}

	// Pre-encode values.
	for i := range 5 {
		var encoded []byte
		v := int32(i)
		encoded, err = s.AppendEncode(encoded, &v)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(encoded); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var got []int32
	for {
		var v int32
		if err := r.Decode(&v); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		got = append(got, v)
	}
	want := []int32{0, 1, 2, 3, 4}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestWriteFlushError(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	ew := &errAfterN{max: 4096}
	w, err := NewWriter(ew, s, WithBlockCount(1))
	if err != nil {
		t.Fatal(err)
	}
	ew.max = 0
	// Write triggers auto-flush which fails.
	_, err = w.Write([]byte{0x02})
	if err == nil {
		t.Fatal("expected error from flush during Write")
	}
}

// ---------- Snappy codec ----------

func TestSnappy(t *testing.T) {
	s, err := avro.Parse(recordSchema)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithCodec(SnappyCodec()))
	if err != nil {
		t.Fatal(err)
	}
	in := []person{
		{"Alice", 30},
		{"Bob", 25},
		{"Charlie", 40},
	}
	for _, p := range in {
		if err := w.Encode(&p); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Reader auto-resolves snappy codec.
	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var out []person
	for {
		var p person
		if err := r.Decode(&p); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		out = append(out, p)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("got %v, want %v", out, in)
	}
}

func TestSnappyDecompressTooShort(t *testing.T) {
	_, err := snappyCodec{}.Decompress([]byte{0x01, 0x02})
	if err == nil {
		t.Fatal("expected error for data too short")
	}
	if !strings.Contains(err.Error(), "too short") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSnappyDecompressBadCRC(t *testing.T) {
	compressed, err := snappyCodec{}.Compress([]byte("test data"))
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt the CRC (last 4 bytes).
	compressed[len(compressed)-1] ^= 0xFF
	_, err = snappyCodec{}.Decompress(compressed)
	if err == nil {
		t.Fatal("expected CRC mismatch error")
	}
	if !strings.Contains(err.Error(), "CRC mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSnappyDecompressBadData(t *testing.T) {
	// Valid length for CRC (>= 4) but invalid snappy content.
	data := []byte{0xFF, 0xFE, 0x00, 0x00, 0x00, 0x00}
	_, err := snappyCodec{}.Decompress(data)
	if err == nil {
		t.Fatal("expected snappy decode error")
	}
}

// ---------- Zstd codec ----------

func TestZstd(t *testing.T) {
	s, err := avro.Parse(recordSchema)
	if err != nil {
		t.Fatal(err)
	}

	codec, err := ZstdCodec(nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithCodec(codec))
	if err != nil {
		t.Fatal(err)
	}
	in := []person{
		{"Alice", 30},
		{"Bob", 25},
		{"Charlie", 40},
	}
	for _, p := range in {
		if err := w.Encode(&p); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Reader auto-resolves zstandard codec.
	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var out []person
	for {
		var p person
		if err := r.Decode(&p); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		out = append(out, p)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("got %v, want %v", out, in)
	}
}

// ---------- Codec close ----------

func TestWriterClosesCodec(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	codec := &trackCloseCodec{}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithCodec(codec))
	if err != nil {
		t.Fatal(err)
	}
	v := int32(1)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if !codec.closed {
		t.Fatal("expected codec to be closed by Writer")
	}
}

type trackCloseCodec struct {
	nullCodec
	closed bool
}

func (c *trackCloseCodec) Close() error {
	c.closed = true
	return nil
}

func TestWriterCloseCodecError(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithCodec(&failCloseCodec{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err == nil {
		t.Fatal("expected error from codec close")
	}
}

type failCloseCodec struct{ nullCodec }

func (failCloseCodec) Close() error { return errors.New("close failed") }

func TestReaderClose(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	// Null codec — Close is a no-op.
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReaderCloseZstd(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	codec, err := ZstdCodec(nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithCodec(codec))
	if err != nil {
		t.Fatal(err)
	}
	v := int32(1)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Reader creates its own zstd codec via resolveCodec.
	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	var out int32
	if err := r.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != 1 {
		t.Fatalf("got %d, want 1", out)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestZstdCodecEncoderOpts(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	codec, err := ZstdCodec([]zstd.EOption{zstd.WithEncoderLevel(zstd.SpeedBestCompression)}, nil)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithCodec(codec))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(int32(42)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var out int32
	if err := r.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != 42 {
		t.Fatalf("got %d, want 42", out)
	}
}

func TestZstdCodecConcurrencyOverride(t *testing.T) {
	// Verify that the default concurrency(1) can be overridden.
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	codec, err := ZstdCodec(
		[]zstd.EOption{zstd.WithEncoderConcurrency(2)},
		[]zstd.DOption{zstd.WithDecoderConcurrency(2)},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer codec.Close()
	shared := NopCloser(codec)

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithCodec(shared))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(int32(99)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf, WithCodec(shared))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var out int32
	if err := r.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != 99 {
		t.Fatalf("got %d, want 99", out)
	}
}

func TestZstdCodecShared(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	// Create a zstd codec and wrap it with NopCloser so that individual
	// Writer/Reader Close calls don't release the shared resources.
	codec, err := ZstdCodec(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer codec.Close()
	shared := NopCloser(codec)

	// Write two separate files with the same shared codec.
	var buf1, buf2 bytes.Buffer
	for i, buf := range []*bytes.Buffer{&buf1, &buf2} {
		w, err := NewWriter(buf, s, WithCodec(shared))
		if err != nil {
			t.Fatalf("file %d: %v", i, err)
		}
		if err := w.Encode(int32(i + 1)); err != nil {
			t.Fatalf("file %d: %v", i, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("file %d: %v", i, err)
		}
	}

	// Read both back using the same shared codec.
	for i, buf := range []*bytes.Buffer{&buf1, &buf2} {
		r, err := NewReader(buf, WithCodec(shared))
		if err != nil {
			t.Fatalf("file %d: %v", i, err)
		}
		var out int32
		if err := r.Decode(&out); err != nil {
			t.Fatalf("file %d: %v", i, err)
		}
		if out != int32(i+1) {
			t.Fatalf("file %d: got %d, want %d", i, out, i+1)
		}
		if err := r.Close(); err != nil {
			t.Fatalf("file %d: %v", i, err)
		}
	}
}

func TestWithSchema(t *testing.T) {
	fullSchema := `{"type":"record","name":"person","fields":[{"name":"name","type":"string","doc":"The name"},{"name":"age","type":"int"}]}`
	s, err := avro.Parse(fullSchema)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithSchema(fullSchema))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(&person{"Alice", 30}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// The header should contain the full schema, not the canonical form.
	got := string(r.Metadata()["avro.schema"])
	if got != fullSchema {
		t.Fatalf("schema in header:\n  got  %s\n  want %s", got, fullSchema)
	}

	// Data should still decode correctly.
	var out person
	if err := r.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != (person{"Alice", 30}) {
		t.Fatalf("got %v, want {Alice 30}", out)
	}
}

func TestResolveCodecCustomOverridesBuiltin(t *testing.T) {
	// A custom codec with a built-in name should override the built-in.
	custom := &testCodec{name: "zstandard"}
	codec, err := resolveCodec("zstandard", []Codec{custom})
	if err != nil {
		t.Fatal(err)
	}
	if codec != Codec(custom) {
		t.Fatal("expected custom codec to override built-in zstandard")
	}
}

type testCodec struct {
	name string
}

func (c *testCodec) Name() string                          { return c.name }
func (c *testCodec) Close() error                          { return nil }
func (c *testCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (c *testCodec) Decompress(src []byte) ([]byte, error) { return src, nil }

// ---------- golden file tests ----------

type weather struct {
	Station string `avro:"station"`
	Time    int64  `avro:"time"`
	Temp    int32  `avro:"temp"`
}

var wantWeather = []weather{
	{"011990-99999", -619524000000, 0},
	{"011990-99999", -619506000000, 22},
	{"011990-99999", -619484400000, -11},
	{"012650-99999", -655531200000, 111},
	{"012650-99999", -655509600000, 78},
}

func TestGoldenWeather(t *testing.T) {
	tests := []struct {
		file  string
		codec string
	}{
		{"weather.avro", "null"},
		{"weather-deflate.avro", "deflate"},
		{"weather-snappy.avro", "snappy"},
		{"weather-zstd.avro", "zstandard"},
	}
	for _, tt := range tests {
		t.Run(tt.codec, func(t *testing.T) {
			f, err := os.Open(filepath.Join("testdata", tt.file))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			r, err := NewReader(f)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()

			// Verify codec metadata.
			if tt.codec != "null" {
				if got := string(r.Metadata()["avro.codec"]); got != tt.codec {
					t.Fatalf("codec = %q, want %q", got, tt.codec)
				}
			}

			var got []weather
			for {
				var w weather
				if err := r.Decode(&w); err != nil {
					if err == io.EOF {
						break
					}
					t.Fatal(err)
				}
				got = append(got, w)
			}
			if !reflect.DeepEqual(got, wantWeather) {
				t.Fatalf("got %v, want %v", got, wantWeather)
			}
		})
	}
}

func TestZstdCodecBadEncoderOption(t *testing.T) {
	// Invalid window size (too small) causes ZstdCodec to fail.
	_, err := ZstdCodec([]zstd.EOption{zstd.WithWindowSize(1)}, nil)
	if err == nil {
		t.Fatal("expected error for invalid zstd encoder option")
	}
	if !strings.Contains(err.Error(), "zstd") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestZstdCodecBadDecoderOption(t *testing.T) {
	// The decoder is built lazily on first decompress (so the reader's per-block
	// cap can be folded in), so an invalid decoder option surfaces there rather
	// than at construction — and a write-only codec with a bad decoder option
	// never builds the decoder, so it stays usable.
	c, err := ZstdCodec(nil, []zstd.DOption{zstd.WithDecoderConcurrency(-1)})
	if err != nil {
		t.Fatalf("ZstdCodec should defer decoder construction, got %v", err)
	}
	defer c.Close()
	if _, err := c.Decompress([]byte("whatever")); err == nil {
		t.Fatal("expected error for invalid zstd decoder option on first decompress")
	} else if !strings.Contains(err.Error(), "zstd") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMustZstdCodecPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from MustZstdCodec with bad options")
		}
	}()
	MustZstdCodec([]zstd.EOption{zstd.WithWindowSize(1)}, nil)
}

func TestReadBlockOversizedBlock(t *testing.T) {
	// Construct a valid OCF header followed by a block whose size
	// exceeds the 64 MiB safety limit.
	var data []byte
	data = append(data, magic[:]...)
	data = encodeMap(data, []kv{{"avro.schema", []byte(`"null"`)}})
	var sync [16]byte
	data = append(data, sync[:]...)
	data = binary.AppendVarint(data, 1)       // block count
	data = binary.AppendVarint(data, 1<<26+1) // block size > 64 MiB

	r, err := NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var v any
	err = r.Decode(&v)
	if err == nil || !strings.Contains(err.Error(), "exceeds safety limit") {
		t.Fatalf("expected safety limit error, got %v", err)
	}
}

func TestWithMaxBlockBytes(t *testing.T) {
	s, err := avro.Parse(`"string"`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode("hello world, this is a test string"); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Reading with a very small max should fail.
	r, err := NewReader(bytes.NewReader(buf.Bytes()), WithMaxBlockBytes(1))
	if err != nil {
		t.Fatal(err)
	}
	var v any
	err = r.Decode(&v)
	if err == nil || !strings.Contains(err.Error(), "exceeds safety limit") {
		t.Fatalf("expected safety limit error, got %v", err)
	}

	// Reading with a large enough max should succeed.
	r, err = NewReader(bytes.NewReader(buf.Bytes()), WithMaxBlockBytes(1024))
	if err != nil {
		t.Fatal(err)
	}
	err = r.Decode(&v)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDecodeMapOversizedCount(t *testing.T) {
	var data []byte
	data = binary.AppendVarint(data, 1<<20+1) // count exceeds limit
	_, err := decodeMap(bufio.NewReader(bytes.NewReader(data)))
	if err == nil || !strings.Contains(err.Error(), "exceeds safety limit") {
		t.Fatalf("expected safety limit error, got %v", err)
	}
}

func TestDecodeMapOversizedKeyLen(t *testing.T) {
	var data []byte
	data = binary.AppendVarint(data, 1)       // 1 entry
	data = binary.AppendVarint(data, 1<<20+1) // key length too large
	_, err := decodeMap(bufio.NewReader(bytes.NewReader(data)))
	if err == nil || !strings.Contains(err.Error(), "key length") {
		t.Fatalf("expected key length error, got %v", err)
	}
}

func TestDecodeMapOversizedValLen(t *testing.T) {
	var data []byte
	data = binary.AppendVarint(data, 1)       // 1 entry
	data = binary.AppendVarint(data, 1)       // key length = 1
	data = append(data, 'k')                  // key
	data = binary.AppendVarint(data, 1<<20+1) // value length too large
	_, err := decodeMap(bufio.NewReader(bytes.NewReader(data)))
	if err == nil || !strings.Contains(err.Error(), "value length") {
		t.Fatalf("expected value length error, got %v", err)
	}
}

func TestGoldenCorruptData(t *testing.T) {
	tests := []struct {
		file    string
		wantErr string
	}{
		{"deflate-invalid-data.avro", "decompressing"},
		{"snappy-invalid-crc.avro", "CRC mismatch"},
		{"snappy-invalid-data.avro", "decompressing"},
		{"snappy-short-crc.avro", "too short"},
		{"zstd-invalid-data.avro", "decompressing"},
	}
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			f, err := os.Open(filepath.Join("testdata", tt.file))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			r, err := NewReader(f)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()

			// The header is valid; the first Decode should fail
			// because the block data is corrupt.
			var v any
			err = r.Decode(&v)
			if err == nil {
				t.Fatal("expected error decoding corrupt data")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestWithReaderSchema(t *testing.T) {
	// Writer schema has name+age.
	writerSchema := avro.MustParse(recordSchema)

	// Reader schema adds a new field "email" with a default.
	readerSchemaStr := `{"type":"record","name":"person","fields":[
		{"name":"name","type":"string"},
		{"name":"age","type":"int"},
		{"name":"email","type":"string","default":"unknown"}
	]}`
	readerSchema := avro.MustParse(readerSchemaStr)

	// Write some data.
	var buf bytes.Buffer
	w, err := NewWriter(&buf, writerSchema)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(&person{Name: "Alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Read with evolved schema.
	r, err := NewReader(&buf, WithReaderSchema(readerSchema))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	type personV2 struct {
		Name  string `avro:"name"`
		Age   int32  `avro:"age"`
		Email string `avro:"email"`
	}
	var p personV2
	if err := r.Decode(&p); err != nil {
		t.Fatal(err)
	}
	if p.Name != "Alice" || p.Age != 30 || p.Email != "unknown" {
		t.Fatalf("unexpected: %+v", p)
	}
}

func TestWithReaderSchemaIncompatible(t *testing.T) {
	writerSchema := avro.MustParse(recordSchema)

	// Incompatible reader schema: missing field "name" with no default.
	readerSchemaStr := `{"type":"record","name":"person","fields":[
		{"name":"age","type":"int"},
		{"name":"newfield","type":"string"}
	]}`
	readerSchema := avro.MustParse(readerSchemaStr)

	var buf bytes.Buffer
	w, err := NewWriter(&buf, writerSchema)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(&person{Name: "Alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = NewReader(&buf, WithReaderSchema(readerSchema))
	if err == nil {
		t.Fatal("expected error for incompatible schemas")
	}
}

func TestReservedMetadataKey(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	_, err = NewWriter(&buf, s, WithMetadata(map[string][]byte{
		"avro.custom": []byte("value"),
	}))
	if err == nil {
		t.Fatal("expected error for avro.* metadata key")
	}
}

func TestOptReaderSchemaMarker(t *testing.T) {
	// Cover optReaderSchema.readerOpt marker method.
	s := avro.MustParse(`"int"`)
	var ro ReaderOpt = WithReaderSchema(s)
	ro.(optReaderSchema).readerOpt()
}

// TestWithReaderSchemaFunc verifies the callback variant is invoked after the
// header is parsed, can inspect rd.Schema() and rd.Metadata() to choose a
// reader schema, and that its returned schema is used for resolution.
func TestWithReaderSchemaFunc(t *testing.T) {
	writerSchema := avro.MustParse(recordSchema)

	readerSchemaStr := `{"type":"record","name":"person","fields":[
		{"name":"name","type":"string"},
		{"name":"age","type":"int"},
		{"name":"email","type":"string","default":"unknown"}
	]}`
	readerSchema := avro.MustParse(readerSchemaStr)

	var buf bytes.Buffer
	w, err := NewWriter(&buf, writerSchema,
		WithMetadata(map[string][]byte{"format-version": []byte("2")}))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(&person{Name: "Alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	callbackInvoked := false
	r, err := NewReader(&buf, WithReaderSchemaFunc(func(rd *Reader) (*avro.Schema, error) {
		callbackInvoked = true
		// Verify the callback sees the parsed header state.
		if rd.Schema() == nil {
			t.Error("rd.Schema() was nil in callback")
		}
		if got := string(rd.Metadata()["format-version"]); got != "2" {
			t.Errorf("rd.Metadata()[format-version] = %q, want %q", got, "2")
		}
		// Choose reader schema based on inspected metadata.
		return readerSchema, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if !callbackInvoked {
		t.Fatal("reader schema callback was not invoked")
	}

	type personV2 struct {
		Name  string `avro:"name"`
		Age   int32  `avro:"age"`
		Email string `avro:"email"`
	}
	var p personV2
	if err := r.Decode(&p); err != nil {
		t.Fatal(err)
	}
	if p.Name != "Alice" || p.Age != 30 || p.Email != "unknown" {
		t.Fatalf("unexpected: %+v", p)
	}
}

// TestWithReaderSchemaFuncReturnsNil verifies that returning (nil, nil) from
// the callback disables resolution — records decode against the writer schema.
func TestWithReaderSchemaFuncReturnsNil(t *testing.T) {
	writerSchema := avro.MustParse(recordSchema)

	var buf bytes.Buffer
	w, err := NewWriter(&buf, writerSchema)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(&person{Name: "Alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(&buf, WithReaderSchemaFunc(func(rd *Reader) (*avro.Schema, error) {
		return nil, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var p person
	if err := r.Decode(&p); err != nil {
		t.Fatal(err)
	}
	if p.Name != "Alice" || p.Age != 30 {
		t.Fatalf("unexpected: %+v", p)
	}
}

// TestWithReaderSchemaFuncReturnsError verifies that an error from the
// callback is surfaced from NewReader.
func TestWithReaderSchemaFuncReturnsError(t *testing.T) {
	writerSchema := avro.MustParse(recordSchema)

	var buf bytes.Buffer
	w, err := NewWriter(&buf, writerSchema)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(&person{Name: "Alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	sentinel := errors.New("picky callback")
	_, err = NewReader(&buf, WithReaderSchemaFunc(func(rd *Reader) (*avro.Schema, error) {
		return nil, sentinel
	}))
	if err == nil {
		t.Fatal("expected error from callback to surface")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want wrapping %v", err, sentinel)
	}
}

// TestReaderSchemaOptionsAreExclusive verifies that WithReaderSchema and
// WithReaderSchemaFunc cannot both be used in the same NewReader call.
func TestReaderSchemaOptionsAreExclusive(t *testing.T) {
	writerSchema := avro.MustParse(recordSchema)

	var buf bytes.Buffer
	w, err := NewWriter(&buf, writerSchema)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(&person{Name: "Alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	s := avro.MustParse(recordSchema)
	_, err = NewReader(&buf,
		WithReaderSchema(s),
		WithReaderSchemaFunc(func(*Reader) (*avro.Schema, error) { return s, nil }),
	)
	if err == nil {
		t.Fatal("expected error when both options are provided")
	}
}

func TestNegativeBlockCountRead(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Append a block with negative count after the valid (empty) file.
	data := buf.Bytes()
	data = binary.AppendVarint(data, -1)     // negative count
	data = binary.AppendVarint(data, 0)      // size
	data = append(data, make([]byte, 16)...) // sync marker
	r, err := NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var v int32
	err = r.Decode(&v)
	if err == nil {
		t.Fatal("expected error for negative block count")
	}
	if !strings.Contains(err.Error(), "negative block count") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWriterSchema(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`)
	var buf bytes.Buffer
	w, err := NewWriter(&buf, schema)
	if err != nil {
		t.Fatal(err)
	}
	if got := w.Schema(); got != schema {
		t.Fatal("Writer.Schema() did not return the original schema")
	}
	w.Close()
}

// TestWithSchemaOptsCustomType verifies that [WithSchemaOpts] passes
// [avro.CustomType] through to the reader-schema parse so the registered
// callback fires on the matching logical-typed field during Decode.
//
// Pre-fix the OCF writer wrote [avro.Schema.Canonical] (PCF) to the
// header — PCF strips logicalType — so the reader's parsed-from-header
// schema had no logical type to dispatch on, the CustomType handler
// never fired, and the built-in date-on-int default kicked in
// producing an int32 instead of a time.Time. The old assertion
// `out["d"].(int32)` was tautological: it succeeded whether or not
// the CustomType fired, because the default behavior absent the
// logical type was also int32. Post-fix the OCF writer preserves
// logicalType in the header (matching Java + fastavro), so the
// CustomType handler actually fires; this test now tracks via a
// captured bool that Decode was invoked, AND asserts the returned
// type matches what the handler produced (a string "tag") rather
// than the default int32 or built-in time.Time.
func TestWithSchemaOptsCustomType(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"d","type":{"type":"int","logicalType":"date"}}
	]}`)

	var buf bytes.Buffer
	w, err := NewWriter(&buf, schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(map[string]any{"d": int32(18262)}); err != nil {
		t.Fatal(err)
	}
	w.Close()

	called := false
	ct := avro.CustomType{
		LogicalType: "date",
		AvroType:    "int",
		Decode: func(v any, _ *avro.SchemaNode) (any, error) {
			called = true
			// Return a value whose Go type differs from the int32
			// default-decode and the built-in time.Time logical-decode,
			// so a successful test cannot be satisfied by either of
			// those fallbacks.
			return fmt.Sprintf("date-tag-%v", v), nil
		},
	}
	r, err := NewReader(&buf, WithSchemaOpts(ct))
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := r.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("WithSchemaOpts CustomType.Decode was never called — OCF header likely strips logicalType")
	}
	got, ok := out["d"].(string)
	if !ok {
		t.Fatalf("expected string from custom Decode, got %T(%v)", out["d"], out["d"])
	}
	if want := "date-tag-18262"; got != want {
		t.Fatalf("custom Decode result: got %q, want %q", got, want)
	}
}

// TestRegression_AppendWriterSchemaOpts verifies that [WithSchemaOpts]
// reaches the header-schema parse in [NewAppendWriter]. The append writer
// recovers its schema by re-parsing the file header, so a SchemaOpt the
// schema needs in order to PARSE (most importantly [avro.WithLaxNames])
// must be threadable. Without it, a file this package writes (NewWriter
// accepts any parsed schema) and reads (NewReader takes WithSchemaOpts)
// can never be reopened for append — the open fails on header-schema name
// validation with no way around it short of rewriting the file.
func TestRegression_AppendWriterSchemaOpts(t *testing.T) {
	schema, err := avro.Parse(`{"type":"record","name":"my-rec","fields":[
		{"name":"f","type":"int"}]}`, avro.WithLaxNames(nil))
	if err != nil {
		t.Fatal(err)
	}
	f := &seekBuf{}
	w, err := NewWriter(f, schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(map[string]any{"f": int32(1)}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Without the option the header parse must still reject the lax name:
	// the option is the explicit opt-in, exactly as it is for NewReader.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAppendWriter(f); err == nil {
		t.Fatal("NewAppendWriter without WithSchemaOpts accepted a lax-named header schema")
	}

	// With the same option NewReader takes, append must work.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	aw, err := NewAppendWriter(f, WithSchemaOpts(avro.WithLaxNames(nil)))
	if err != nil {
		t.Fatalf("NewAppendWriter with WithSchemaOpts: %v", err)
	}
	if err := aw.Encode(map[string]any{"f": int32(2)}); err != nil {
		t.Fatal(err)
	}
	if err := aw.Close(); err != nil {
		t.Fatal(err)
	}

	// Both the original and the appended record read back.
	r, err := NewReader(bytes.NewReader(f.data), WithSchemaOpts(avro.WithLaxNames(nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for i, want := range []int32{1, 2} {
		var out map[string]any
		if err := r.Decode(&out); err != nil {
			t.Fatalf("decode record %d: %v", i, err)
		}
		if got := out["f"].(int32); got != want {
			t.Fatalf("record %d: got %d, want %d", i, got, want)
		}
	}
	var out map[string]any
	if err := r.Decode(&out); err != io.EOF {
		t.Fatalf("expected EOF after 2 records, got %v", err)
	}
}

// TestRegression_BlockCountZeroTerminatesStream verifies that an OCF
// block with count==0 is treated as end-of-stream — AFTER reading and
// validating the block's size and sync marker per spec. Java's
// DataFileStream.nextRawBlock and fastavro both read the full block
// envelope (count + size + data + sync) before signalling EOF.
func TestRegression_BlockCountZeroTerminatesStream(t *testing.T) {
	sync := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var buf bytes.Buffer
	s, err := avro.Parse(recordSchema)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWriter(&buf, s, WithSyncMarker(sync))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(&person{Name: "alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Append a count=0, size=0 block with the REAL sync. The reader
	// reads size and sync first, validates the sync, THEN sees count==0
	// and returns io.EOF.
	zeroBlock := append([]byte{}, 0x00, 0x00)
	zeroBlock = append(zeroBlock, sync[:]...)
	buf.Write(zeroBlock)

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var p person
	if err := r.Decode(&p); err != nil {
		t.Fatalf("decode first record: %v", err)
	}
	if err := r.Decode(&p); err != io.EOF {
		t.Fatalf("expected io.EOF on count=0 block (after sync validation), got %v", err)
	}
}

// TestRegression_BlockCountZeroValidatesSync locks the sibling
// invariant: a count=0 block with a CORRUPT sync must be rejected as
// a sync-mismatch error, not silently accepted as clean EOF. Pre-fix,
// readBlock returned io.EOF immediately on count==0 without reading
// size + data + sync — meaning a tail-truncated file whose count byte
// happened to read as 0 was accepted as a clean stream end, losing
// the corruption-detection invariant the spec requires.
func TestRegression_BlockCountZeroValidatesSync(t *testing.T) {
	sync := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var buf bytes.Buffer
	s, err := avro.Parse(recordSchema)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWriter(&buf, s, WithSyncMarker(sync))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(&person{Name: "alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// count=0, size=0, but corrupt sync (all 0xFF).
	zeroBlock := append([]byte{}, 0x00, 0x00)
	zeroBlock = append(zeroBlock, bytes.Repeat([]byte{0xFF}, 16)...)
	buf.Write(zeroBlock)

	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var p person
	if err := r.Decode(&p); err != nil {
		t.Fatalf("decode first record: %v", err)
	}
	err = r.Decode(&p)
	if err == nil || err == io.EOF {
		t.Fatalf("expected sync-mismatch error on count=0 block with corrupt sync, got %v", err)
	}
	if !strings.Contains(err.Error(), "sync marker mismatch") {
		t.Fatalf("expected sync marker mismatch error, got: %v", err)
	}
}

// TestRegression_OCFBigDecimalJavaInterop verifies that a Java-generated
// big-decimal OCF file decodes to the correct *big.Rat value through
// the twmb reader. testdata/bigdec.avro is the same vendored file
// avro-rs uses in test_avro_3779_from_java_file (avro/tests/bigdec.avro);
// it contains a single record {field_name: 2.24}. The wire-format byte
// match for 2.24 is also pinned at the binary-payload level in
// TestSpecBigDecimalWireFormat/java_ground_truth_2.24 — this test
// extends the interop guarantee end-to-end through the OCF framing.
// leakDetectCodec is a Codec that records whether Close() has been
// called. Used by the Writer/NewReader/NewAppendWriter leak-on-error
// regression tests below.
type leakDetectCodec struct {
	name   string
	closed bool
}

func (c *leakDetectCodec) Name() string                          { return c.name }
func (c *leakDetectCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (c *leakDetectCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (c *leakDetectCodec) Close() error                          { c.closed = true; return nil }

// failAfterNWrites is an io.Writer that succeeds for the first N
// Write calls and then returns an error. Used to drive a Writer past
// header writing into the poisoned state on a subsequent flush.
type failAfterNWrites struct {
	n int
}

func (f *failAfterNWrites) Write(p []byte) (int, error) {
	if f.n <= 0 {
		return 0, errors.New("synthetic write failure")
	}
	f.n--
	return len(p), nil
}

// TestRegression_OCFWriterCloseClosesCodecWhenPoisoned locks the
// invariant that Writer.Close() always closes the codec, even when
// the writer is in an error state from a prior flush failure. Pre-
// fix Close() short-circuited on w.err != nil and never called
// codec.Close() — zstd and similar codecs leak goroutines/buffers
// in that path. Mirrors Java's DataFileWriter.close's try/finally.
func TestRegression_OCFWriterCloseClosesCodecWhenPoisoned(t *testing.T) {
	codec := &leakDetectCodec{name: "null"}
	// One successful write for the header; subsequent writes fail.
	// The next flush poisons w.err without aborting construction.
	w, err := NewWriter(&failAfterNWrites{n: 1}, avro.MustParse(`"long"`), WithCodec(codec), WithBlockCount(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	// Encode → buffered. BlockCount=1 triggers an immediate flush
	// which writes to the now-failing writer and poisons w.err.
	_ = w.Encode(int64(1))
	_ = w.Close()
	if !codec.closed {
		t.Fatal("Writer.Close() did not call codec.Close() after the writer was poisoned")
	}
}

// TestRegression_OCFNewReaderClosesCodecOnReaderSchemaFnError locks
// that NewReader closes the codec on any error path after
// resolveCodec succeeds. Pre-fix readerSchemaFn / Resolve errors
// returned nil + err without closing the already-resolved codec.
func TestRegression_OCFNewReaderClosesCodecOnReaderSchemaFnError(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, avro.MustParse(`"long"`))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Encode(int64(1)); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	codec := &leakDetectCodec{name: "null"}
	_, err = NewReader(
		bytes.NewReader(buf.Bytes()),
		WithCodec(codec),
		WithReaderSchemaFunc(func(*Reader) (*avro.Schema, error) {
			return nil, errors.New("synthetic error")
		}),
	)
	if err == nil {
		t.Fatal("expected error from reader-schema func")
	}
	if !codec.closed {
		t.Fatal("NewReader returned error without closing the codec")
	}
}

// TestRegression_OCFNewReaderClosesCodecOnResolveError locks the
// same invariant for the avro.Resolve error path.
func TestRegression_OCFNewReaderClosesCodecOnResolveError(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, avro.MustParse(`"long"`))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Encode(int64(1)); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reader schema is incompatible with the writer (long → string),
	// forcing avro.Resolve to error.
	codec := &leakDetectCodec{name: "null"}
	_, err = NewReader(
		bytes.NewReader(buf.Bytes()),
		WithCodec(codec),
		WithReaderSchema(avro.MustParse(`"string"`)),
	)
	if err == nil {
		t.Fatal("expected Resolve error")
	}
	if !codec.closed {
		t.Fatal("NewReader returned Resolve error without closing the codec")
	}
}

// TestRegression_OCFBlockEnvelopeInvariant locks the spec invariant
// that EVERY block consists of (count + size + data + sync) and any
// path that signals EOF or surfaces a value must have fully consumed
// (or rejected) the envelope first. Sibling cases to the count=0
// sync-validation finding: negative count, negative size, and size
// exceeding the safety limit must all error loudly without
// fall-through. Pre-finding 3 the count=0 path bailed before reading
// size/sync; future spec-bypass regressions here fail loudly.
func TestRegression_OCFBlockEnvelopeInvariant(t *testing.T) {
	sync := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	makeOCFWithBadBlock := func(t *testing.T, suffix []byte) []byte {
		t.Helper()
		var buf bytes.Buffer
		s, err := avro.Parse(`"long"`)
		if err != nil {
			t.Fatal(err)
		}
		w, err := NewWriter(&buf, s, WithSyncMarker(sync))
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Encode(int64(1)); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		buf.Write(suffix)
		return buf.Bytes()
	}

	cases := []struct {
		name   string
		suffix []byte
		want   string // substring expected in error
	}{
		{
			name:   "negative count",
			suffix: []byte{0x01, 0x00}, // varint(-1), varint(0)
			want:   "negative block count",
		},
		{
			name:   "negative size",
			suffix: []byte{0x02, 0x01}, // varint(1), varint(-1)
			want:   "negative block size",
		},
		{
			name: "size exceeds safety limit",
			suffix: func() []byte {
				out := binary.AppendVarint(nil, 1)
				// 256 MiB — past the default 64 MiB safety limit.
				out = binary.AppendVarint(out, 256*1024*1024)
				return out
			}(),
			want: "exceeds safety limit",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bs := makeOCFWithBadBlock(t, c.suffix)
			r, err := NewReader(bytes.NewReader(bs))
			if err != nil {
				t.Fatal(err)
			}
			var v int64
			if err := r.Decode(&v); err != nil { // first valid record
				t.Fatalf("decode first: %v", err)
			}
			err = r.Decode(&v) // bad block
			if err == nil || err == io.EOF {
				t.Fatalf("expected loud error containing %q, got %v", c.want, err)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("expected error containing %q, got: %v", c.want, err)
			}
		})
	}
}

// TestRegression_OCFBlockCountCap locks in the DoS-resistance cap on
// the OCF block count, which was previously uncapped. For zero-byte
// record schemas (EmptyRecord, records of all-null-typed fields) every
// record encodes to 0 wire bytes; without the cap an attacker could
// claim ~10^9 records in a 5-byte zigzag-varint count, forcing the
// user's `for rd.Decode(&v) == nil` loop to iterate that many times
// (each call advancing rd.block by 0 bytes) from a tiny attacker input
// — ~10^9 CPU amplification.
//
// The cap (readBlock in ocf.go) bounds count by
//
//	len(decompressed block) + maxOCFZeroByteSlack
//
// where the slack matches deser.go:558's maxZeroByteItems philosophy
// for Avro array<null>/array<EmptyRecord> block-counts. Legitimate
// zero-byte schemas split into multiple blocks when the per-block
// record count exceeds the slack.
//
// Java's DataFileStream (lang/java/avro/src/main/java/org/apache/avro/
// file/DataFileStream.java:303) and fastavro's _iter_avro_records
// (_read_py.py:807) leave this uncapped — this cap is twmb's
// defense-in-depth extension matching its existing array/map caps.
func TestRegression_OCFBlockCountCap(t *testing.T) {
	zigzag := func(n int64) []byte {
		u := uint64(n<<1) ^ uint64(n>>63)
		var buf []byte
		for u >= 0x80 {
			buf = append(buf, byte(u)|0x80)
			u >>= 7
		}
		return append(buf, byte(u))
	}

	// Build a header-only OCF for a schema and extract the sync marker.
	headerAndSync := func(t *testing.T, schemaJSON string) ([]byte, []byte) {
		t.Helper()
		s, err := avro.Parse(schemaJSON)
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		w, err := NewWriter(&buf, s)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		hdr := buf.Bytes()
		return hdr, hdr[len(hdr)-16:]
	}

	t.Run("zero-byte schema, count > slack", func(t *testing.T) {
		hdr, sync := headerAndSync(t, `{"type":"record","name":"E","fields":[]}`)
		var mb []byte
		mb = append(mb, hdr...)
		mb = append(mb, zigzag(1_000_000_000)...) // count = 10^9
		mb = append(mb, zigzag(0)...)             // size = 0 (empty block payload)
		mb = append(mb, sync...)

		rd, err := NewReader(bytes.NewReader(mb))
		if err != nil {
			t.Fatal(err)
		}
		defer rd.Close()

		var v map[string]any
		err = rd.Decode(&v)
		if err == nil || err == io.EOF {
			t.Fatalf("expected error rejecting huge block count, got %v", err)
		}
		if !strings.Contains(err.Error(), "zero-byte slack") {
			t.Fatalf("expected error containing %q, got: %v", "zero-byte slack", err)
		}
	})

	t.Run("zero-byte schema, count at slack boundary", func(t *testing.T) {
		// count == maxOCFZeroByteSlack (4096) should be accepted; the
		// reader iterates 4096 times without complaint. This pins that
		// the cap is the documented slack, not a tighter bound.
		const slack = 4 << 10
		hdr, sync := headerAndSync(t, `{"type":"record","name":"E","fields":[]}`)
		var mb []byte
		mb = append(mb, hdr...)
		mb = append(mb, zigzag(slack)...)
		mb = append(mb, zigzag(0)...)
		mb = append(mb, sync...)

		rd, err := NewReader(bytes.NewReader(mb))
		if err != nil {
			t.Fatal(err)
		}
		defer rd.Close()

		iter := 0
		for {
			var v map[string]any
			err := rd.Decode(&v)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("unexpected error at iter %d: %v", iter, err)
			}
			iter++
			if iter > slack+10 {
				t.Fatalf("reader iterated past slack boundary (%d > %d)", iter, slack)
			}
		}
		if iter != slack {
			t.Errorf("expected %d iterations (at slack), got %d", slack, iter)
		}
	})

	t.Run("non-zero-byte schema, count exceeds block size", func(t *testing.T) {
		// Schema with minItemBytes >= 1 per record. Attacker claims
		// 10^9 records but block size is small (here, empty after the
		// initial valid record block). The cap rejects since
		// count > len(block) + slack with len(block) ~ 0.
		hdr, sync := headerAndSync(t, `"long"`)
		var mb []byte
		mb = append(mb, hdr...)
		mb = append(mb, zigzag(1_000_000_000)...)
		mb = append(mb, zigzag(0)...)
		mb = append(mb, sync...)

		rd, err := NewReader(bytes.NewReader(mb))
		if err != nil {
			t.Fatal(err)
		}
		defer rd.Close()

		var v int64
		err = rd.Decode(&v)
		if err == nil || err == io.EOF {
			t.Fatalf("expected error rejecting huge count for empty block, got %v", err)
		}
		if !strings.Contains(err.Error(), "zero-byte slack") {
			t.Fatalf("expected error containing %q, got: %v", "zero-byte slack", err)
		}
	})

	t.Run("legitimate non-zero-byte schema unaffected", func(t *testing.T) {
		// Write a legitimate OCF with 5000 long records — count well
		// past the 4096 zero-byte slack, but len(block) >= count so
		// the check accepts.
		s, err := avro.Parse(`"long"`)
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		w, err := NewWriter(&buf, s, WithBlockCount(10_000))
		if err != nil {
			t.Fatal(err)
		}
		const n = 5000
		for i := range n {
			if err := w.Encode(int64(i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		rd, err := NewReader(bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		defer rd.Close()
		iter := 0
		for {
			var v int64
			err := rd.Decode(&v)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("unexpected error at iter %d: %v", iter, err)
			}
			iter++
		}
		if iter != n {
			t.Errorf("expected %d records, got %d", n, iter)
		}
	})
}

func TestRegression_OCFBigDecimalJavaInterop(t *testing.T) {
	data, err := os.ReadFile("testdata/bigdec.avro")
	if err != nil {
		t.Fatalf("read bigdec.avro: %v", err)
	}
	r, err := NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	var got map[string]any
	if err := r.Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	field, ok := got["field_name"]
	if !ok {
		t.Fatalf("decoded record missing field_name: %#v", got)
	}
	rat, ok := field.(*big.Rat)
	if !ok {
		t.Fatalf("field_name: got %T %#v, want *big.Rat", field, field)
	}
	want := new(big.Rat).SetFrac64(224, 100)
	if rat.Cmp(want) != 0 {
		t.Fatalf("field_name: got %s, want %s", rat.RatString(), want.RatString())
	}
	// Second Decode hits EOF (single-record file).
	if err := r.Decode(&got); err != io.EOF {
		t.Fatalf("expected io.EOF after single record, got %v", err)
	}
}

// TestRegression_OCFCodecMatrix pins round-trip behavior for every
// supported codec (null, deflate, snappy, zstd) at writer and reader
// sides. Snappy specifically uses a trailing CRC32 that the reader
// verifies (fastavro skips this verification — see AUDIT.md known
// divergences); the matrix asserts CRC mismatches are detected.
func TestRegression_OCFCodecMatrix(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"v","type":"long"}]}`)
	records := []map[string]any{
		{"v": int64(1)},
		{"v": int64(2)},
		{"v": int64(3)},
		{"v": int64(1 << 40)}, // larger value to give compression something to chew on
	}

	type codecCase struct {
		desc  string
		codec Codec
	}
	zstdC, err := ZstdCodec(nil, nil)
	if err != nil {
		t.Fatalf("ZstdCodec: %v", err)
	}
	codecs := []codecCase{
		{"null", nil}, // explicit nil = default null codec
		{"deflate", DeflateCodec(flate.DefaultCompression)},
		{"snappy", SnappyCodec()},
		{"zstd", zstdC},
	}

	for _, c := range codecs {
		t.Run(c.desc, func(t *testing.T) {
			var buf bytes.Buffer
			var opts []WriterOpt
			if c.codec != nil {
				opts = append(opts, WithCodec(c.codec))
			}
			w, err := NewWriter(&buf, schema, opts...)
			if err != nil {
				t.Fatalf("NewWriter: %v", err)
			}
			for _, r := range records {
				if err := w.Encode(r); err != nil {
					t.Fatalf("Encode: %v", err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			// Read back.
			r, err := NewReader(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			for i, want := range records {
				var got map[string]any
				if err := r.Decode(&got); err != nil {
					t.Fatalf("Decode #%d: %v", i, err)
				}
				if got["v"] != want["v"] {
					t.Errorf("record %d: got %v, want %v", i, got, want)
				}
			}
			// EOF after last record.
			var dummy any
			if err := r.Decode(&dummy); err != io.EOF {
				t.Errorf("expected EOF after %d records, got %v", len(records), err)
			}
		})
	}
}

// TestRegression_OCFEmptyFile pins behavior for an OCF file with zero
// records (header + sync, no data blocks). Reader should produce EOF
// on first Decode without error.
func TestRegression_OCFEmptyFile(t *testing.T) {
	schema := avro.MustParse(`"long"`)
	var buf bytes.Buffer
	w, err := NewWriter(&buf, schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	var v int64
	if err := r.Decode(&v); err != io.EOF {
		t.Errorf("expected EOF on empty file, got %v", err)
	}
}

// TestRegression_OCFTruncatedFile pins behavior on truncation: header-
// only, header + partial block, etc. Reader should error gracefully
// rather than panic or return wrong data.
func TestRegression_OCFTruncatedFile(t *testing.T) {
	schema := avro.MustParse(`"long"`)
	var buf bytes.Buffer
	w, err := NewWriter(&buf, schema)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		if err := w.Encode(int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	full := buf.Bytes()

	// Try truncating at every byte position; reader should not panic.
	for cut := 0; cut < len(full); cut += 7 {
		cut := cut
		t.Run(fmt.Sprintf("cut@%d", cut), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic on truncated input at offset %d: %v", cut, r)
				}
			}()
			r, err := NewReader(bytes.NewReader(full[:cut]))
			if err != nil {
				return // header parse failure is fine
			}
			for range 100 {
				var v int64
				if err := r.Decode(&v); err != nil {
					break // any error (EOF or truncation) is fine
				}
			}
		})
	}
}

// TestRegression_OCFReaderSchemaPromotion pins reader-schema promotion
// (writer schema embedded in file, reader supplies a separate schema
// via WithReaderSchema). int file decoded with long reader → long
// values.
func TestRegression_OCFReaderSchemaPromotion(t *testing.T) {
	wschema := avro.MustParse(`"int"`)
	rschema := avro.MustParse(`"long"`)

	var buf bytes.Buffer
	w, err := NewWriter(&buf, wschema)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(int32(42)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()), WithReaderSchema(rschema))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	var got int64
	if err := r.Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Errorf("got %d, want 42", got)
	}
}

// TestRegression_OCFUnknownCodecErrorBounded pins that an unknown
// avro.codec metadata value from a hostile OCF file produces a bounded
// error message. The per-entry metadata cap is ocfMetadataSafetyLimit
// (1 MiB), so without truncation in the unknown-codec error path a
// hostile producer could emit a 1 MiB codec name and the parse error
// would echo all of it — 1:1 DoS amplification through logs and RPC
// error trailers.
//
// Boundary symmetry: a short bad codec name still produces an
// informative error message.
func TestRegression_OCFUnknownCodecErrorBounded(t *testing.T) {
	const maxErrLen = 4096

	mkOCF := func(codecName string) []byte {
		var buf bytes.Buffer
		buf.Write([]byte{'O', 'b', 'j', 0x01})
		schemaJSON := `"long"`
		mb := make([]byte, 0)
		mb = binary.AppendVarint(mb, 2)
		mb = binary.AppendVarint(mb, int64(len("avro.schema")))
		mb = append(mb, "avro.schema"...)
		mb = binary.AppendVarint(mb, int64(len(schemaJSON)))
		mb = append(mb, schemaJSON...)
		mb = binary.AppendVarint(mb, int64(len("avro.codec")))
		mb = append(mb, "avro.codec"...)
		mb = binary.AppendVarint(mb, int64(len(codecName)))
		mb = append(mb, codecName...)
		mb = binary.AppendVarint(mb, 0)
		buf.Write(mb)
		buf.Write(make([]byte, 16)) // sync marker
		return buf.Bytes()
	}

	// Hostile 1 MiB codec name.
	hostile := strings.Repeat("X", 1<<20)
	_, err := NewReader(bytes.NewReader(mkOCF(hostile)))
	if err == nil {
		t.Fatal("expected error for hostile codec name")
	}
	if got := len(err.Error()); got > maxErrLen {
		t.Errorf("hostile codec name: error length %d exceeds %d-byte cap", got, maxErrLen)
	}

	// Short bad name — informative content preserved.
	_, err = NewReader(bytes.NewReader(mkOCF("bogus")))
	if err == nil {
		t.Fatal("expected error for bogus codec name")
	}
	if !strings.Contains(err.Error(), "bogus") {
		t.Errorf("short codec name error %q lost diagnostic content", err.Error())
	}
}

// closeCountCodec is a null-passthrough codec that counts Close calls. A custom
// codec that frees pooled buffers or decrements a refcount in Close must be
// closed exactly once even when Writer.Close/Reader.Close are called
// repeatedly.
type closeCountCodec struct{ closes *int }

func (closeCountCodec) Name() string                        { return "null" }
func (closeCountCodec) Compress(src []byte) ([]byte, error) { return append([]byte(nil), src...), nil }
func (closeCountCodec) Decompress(src []byte) ([]byte, error) {
	return append([]byte(nil), src...), nil
}
func (c closeCountCodec) Close() error { *c.closes++; return nil }

// assertOneInt reads buf as an OCF and asserts it contains exactly one int
// datum equal to want — the second Decode must be a clean io.EOF. This catches
// a file silently extended past its logical EOF.
func assertOneInt(t *testing.T, buf *bytes.Buffer, want int32) {
	t.Helper()
	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var got int32
	if err := r.Decode(&got); err != nil {
		t.Fatalf("decoding first datum: %v", err)
	}
	if got != want {
		t.Fatalf("first datum = %d, want %d", got, want)
	}
	if err := r.Decode(&got); err != io.EOF {
		t.Fatalf("second Decode = %v (value %d), want io.EOF — file extended past logical EOF", err, got)
	}
}

// After Close, the Writer must reject every mutator rather than silently
// extending the file. A closed codec cannot be relied on to catch the misuse
// (klauspost's zstd encoder silently re-initializes after Close), so the Writer
// tracks its own closed state. Mirrors Java DataFileWriter.assertOpen.
func TestRegression_WriterRejectsMutatorsAfterClose(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	build := func() (*Writer, *bytes.Buffer) {
		var buf bytes.Buffer
		w, err := NewWriter(&buf, s)
		if err != nil {
			t.Fatal(err)
		}
		v := int32(1)
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return w, &buf
	}

	t.Run("Encode", func(t *testing.T) {
		w, buf := build()
		v := int32(2)
		if err := w.Encode(&v); err == nil {
			t.Error("Encode after Close returned nil; want error")
		}
		assertOneInt(t, buf, 1)
	})
	t.Run("Write", func(t *testing.T) {
		w, buf := build()
		if _, err := w.Write([]byte{0x04}); err == nil {
			t.Error("Write after Close returned nil; want error")
		}
		assertOneInt(t, buf, 1)
	})
	t.Run("Flush", func(t *testing.T) {
		w, buf := build()
		if err := w.Flush(); err == nil {
			t.Error("Flush after Close returned nil; want error")
		}
		assertOneInt(t, buf, 1)
	})
	t.Run("Reset", func(t *testing.T) {
		w, _ := build()
		var buf2 bytes.Buffer
		if err := w.Reset(&buf2); err == nil {
			t.Error("Reset after Close returned nil; want error")
		}
	})
}

// Writer.Close is idempotent toward the codec: repeated Close calls close the
// underlying codec exactly once.
func TestRegression_WriterCloseClosesCodecOnce(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	closes := 0
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithCodec(closeCountCodec{&closes}))
	if err != nil {
		t.Fatal(err)
	}
	v := int32(7)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if err := w.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i+1, err)
		}
	}
	if closes != 1 {
		t.Errorf("codec.Close called %d times across 3 Writer.Close calls; want 1", closes)
	}
}

// Reader.Close is idempotent toward the codec, and Decode after Close errors
// rather than returning data or a clean io.EOF.
func TestRegression_ReaderRejectsUseAfterCloseAndClosesCodecOnce(t *testing.T) {
	s, err := avro.Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	// Two records in one block, so the second is buffered after the first
	// Decode — a use-after-Close that returns buffered data would leak it.
	for _, v := range []int32{7, 9} {
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	closes := 0
	r, err := NewReader(bytes.NewReader(buf.Bytes()), WithCodec(closeCountCodec{&closes}))
	if err != nil {
		t.Fatal(err)
	}
	var got int32
	if err := r.Decode(&got); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if err := r.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i+1, err)
		}
	}
	if closes != 1 {
		t.Errorf("codec.Close called %d times across 2 Reader.Close calls; want 1", closes)
	}
	// The second record is still buffered; Decode after Close must error
	// rather than leak it.
	if err := r.Decode(&got); err == nil {
		t.Errorf("Decode after Close returned nil (value %d); want error", got)
	}
}

// A VALUE error from Encode (the datum doesn't fit the schema) must not
// poison the Writer: the failed datum's partial bytes are discarded by
// restoring the buffer snapshot, previously-accepted datums survive, and
// the Writer remains usable — mirroring Java DataFileWriter.append, which
// truncates its buffer to the pre-append position and rethrows. Only
// IO/compression/flush errors (where the sink state is unknowable) poison.
func TestRegression_OCFWriterValueErrorRecovers(t *testing.T) {
	s := avro.MustParse(`"int"`)
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for i := range 5 {
		if err := w.Encode(int32(i)); err != nil {
			t.Fatalf("encode %d: %v", i, err)
		}
	}
	// 3.5 is not a whole number: a pure value error.
	if err := w.Encode(3.5); err == nil {
		t.Fatal("bad-value Encode returned nil; want error")
	}
	for i := 5; i < 7; i++ {
		if err := w.Encode(int32(i)); err != nil {
			t.Fatalf("encode %d after value error: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	var got []int32
	for {
		var v int32
		if err := r.Decode(&v); err != nil {
			break
		}
		got = append(got, v)
	}
	want := []int32{0, 1, 2, 3, 4, 5, 6}
	if len(got) != len(want) {
		t.Fatalf("file datums: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("file datums: got %v, want %v", got, want)
		}
	}
}
