package ocf

import (
	"bufio"
	"bytes"
	"compress/flate"
	"errors"
	"fmt"
	"io"
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
	s, err := avro.NewSchema(recordSchema)
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
	s, err := avro.NewSchema(recordSchema)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"string"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(recordSchema)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"long"`)
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

	r, err := NewReader(&buf, WithReaderCodec(codec))
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"string"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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

func TestShortHeader(t *testing.T) {
	// Only 3 bytes — not enough for magic.
	_, err := NewReader(bytes.NewReader([]byte{0x4f, 0x62, 0x6a}))
	if err == nil {
		t.Fatal("expected error for short header")
	}
}

func TestDeflateRoundTripLarge(t *testing.T) {
	s, err := avro.NewSchema(`"string"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	// Sticky: subsequent encode should also fail.
	n := int32(1)
	if err := w.Encode(&n); err == nil {
		t.Fatal("expected sticky error")
	}
}

type failCompressCodec struct{}

func (failCompressCodec) Name() string { return "failcompress" }
func (failCompressCodec) Compress([]byte) ([]byte, error) {
	return nil, errors.New("compress failed")
}
func (failCompressCodec) Decompress(src []byte) ([]byte, error) { return src, nil }

func TestCompressError(t *testing.T) {
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	data = appendVarlong(data, 1) // count = 1, then EOF before size
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
	s, err := avro.NewSchema(`"int"`)
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
	data = appendVarlong(data, 1)   // count
	data = appendVarlong(data, 100) // size = 100 bytes, but EOF
	data = append(data, 0x01)       // only 1 byte of data
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
	s, err := avro.NewSchema(`"int"`)
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
	data = appendVarlong(data, 1) // count
	data = appendVarlong(data, 1) // size = 1
	data = append(data, 0x02)    // 1 byte of data
	data = append(data, 0x00)    // only 1 byte of sync marker, need 16
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
	s, err := avro.NewSchema(`"int"`)
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
	data = appendVarlong(data, 1)  // count
	data = appendVarlong(data, -1) // negative size
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

func (failDecompressCodec) Name() string                         { return "faildecompress" }
func (failDecompressCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (failDecompressCodec) Decompress([]byte) ([]byte, error) {
	return nil, errors.New("decompress failed")
}

func TestDecompressError(t *testing.T) {
	s, err := avro.NewSchema(`"int"`)
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

	r, err := NewReader(bytes.NewReader(buf.Bytes()), WithReaderCodec(codec))
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
	s, err := avro.NewSchema(`"string"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	block = appendVarlong(block, 1) // count = 1
	// Data: two varints (each is 1 byte for small values).
	itemData := appendVarlong(nil, 10)
	itemData = appendVarlong(itemData, 20) // extra data
	block = appendVarlong(block, int64(len(itemData)))
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
	_, err := readVarlongFrom(r)
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
	data = appendVarlong(data, count)
	// Build the entries first to know the byte size.
	var entries []byte
	// Entry 1: key="a", value="b"
	entries = appendVarlong(entries, 1) // key len
	entries = append(entries, 'a')
	entries = appendVarlong(entries, 1) // val len
	entries = append(entries, 'b')
	// Entry 2: key="c", value="d"
	entries = appendVarlong(entries, 1)
	entries = append(entries, 'c')
	entries = appendVarlong(entries, 1)
	entries = append(entries, 'd')
	data = appendVarlong(data, int64(len(entries))) // byte size
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
	wo.(optCodec).writerOpt()
	wo = WithBlockCount(1)
	wo.(optBlockCount).writerOpt()
	wo = WithBlockBytes(1)
	wo.(optBlockBytes).writerOpt()
	wo = WithMetadata(map[string][]byte{"k": nil})
	wo.(optMetadata).writerOpt()
	wo = WithSyncMarker([16]byte{})
	wo.(optSyncMarker).writerOpt()
	var ro ReaderOpt
	ro = WithReaderCodec(nullCodec{})
	ro.(optReaderCodec).readerOpt()
}

func TestHeaderWriteError(t *testing.T) {
	s, err := avro.NewSchema(`"int"`)
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
	data = appendVarlong(data, -2) // negative count
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
	data = appendVarlong(data, 1) // 1 entry
	// No key length — truncated.
	r := bufio.NewReader(bytes.NewReader(data))
	_, err := decodeMap(r)
	if err == nil {
		t.Fatal("expected error for truncated key length")
	}
}

func TestDecodeMapTruncatedKeyData(t *testing.T) {
	var data []byte
	data = appendVarlong(data, 1)  // 1 entry
	data = appendVarlong(data, 10) // key length = 10
	data = append(data, 'a')      // only 1 byte of key, need 10
	r := bufio.NewReader(bytes.NewReader(data))
	_, err := decodeMap(r)
	if err == nil {
		t.Fatal("expected error for truncated key data")
	}
}

func TestDecodeMapTruncatedValLen(t *testing.T) {
	var data []byte
	data = appendVarlong(data, 1) // 1 entry
	data = appendVarlong(data, 1) // key length = 1
	data = append(data, 'k')     // key
	// No value length — truncated.
	r := bufio.NewReader(bytes.NewReader(data))
	_, err := decodeMap(r)
	if err == nil {
		t.Fatal("expected error for truncated value length")
	}
}

func TestDecodeMapTruncatedValData(t *testing.T) {
	var data []byte
	data = appendVarlong(data, 1)  // 1 entry
	data = appendVarlong(data, 1)  // key length = 1
	data = append(data, 'k')      // key
	data = appendVarlong(data, 10) // val length = 10
	data = append(data, 'v')      // only 1 byte of val, need 10
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

	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	var buf1 bytes.Buffer
	w, err := NewWriter(&buf1, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	orig := randRead
	randRead = func(b []byte) (int, error) { return 0, errors.New("rand failed") }
	defer func() { randRead = orig }()

	var buf2 bytes.Buffer
	if err := w.Reset(&buf2); err == nil {
		t.Fatal("expected error from rand during reset")
	}
}

func TestAppendWriter(t *testing.T) {
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	r, err := NewReader(sb, WithReaderCodec(codec))
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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

	// Reset to a writer that fails.
	if err := w.Reset(&errAfterN{max: 0}); err == nil {
		t.Fatal("expected error writing header during reset")
	}
}

// ---------- Write (pre-encoded bytes) ----------

func TestWrite(t *testing.T) {
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(recordSchema)
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
	s, err := avro.NewSchema(recordSchema)
	if err != nil {
		t.Fatal(err)
	}

	codec, err := ZstdCodec()
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

func TestWriterCloseCodec(t *testing.T) {
	s, err := avro.NewSchema(`"int"`)
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
		t.Fatal("expected codec to be closed")
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
	s, err := avro.NewSchema(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, WithCodec(&failCloseCodec{}))
	if err != nil {
		t.Fatal(err)
	}
	err = w.Close()
	if err == nil {
		t.Fatal("expected error from codec close")
	}
}

type failCloseCodec struct{ nullCodec }

func (failCloseCodec) Close() error { return errors.New("close failed") }

func TestReaderClose(t *testing.T) {
	s, err := avro.NewSchema(`"int"`)
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
	s, err := avro.NewSchema(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	codec, err := ZstdCodec()
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
	s, err := avro.NewSchema(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	codec, err := ZstdCodec(zstd.WithEncoderLevel(zstd.SpeedBestCompression))
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

func TestZstdCodecFromShared(t *testing.T) {
	s, err := avro.NewSchema(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()

	codec := ZstdCodecFrom(enc, dec)

	// Write two separate files with the same shared codec.
	var buf1, buf2 bytes.Buffer
	for i, buf := range []*bytes.Buffer{&buf1, &buf2} {
		w, err := NewWriter(buf, s, WithCodec(codec))
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
		r, err := NewReader(buf, WithReaderCodec(codec))
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

func TestZstdCodecFromNilEncoder(t *testing.T) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()

	codec := ZstdCodecFrom(nil, dec)
	if _, err := codec.Compress([]byte("data")); err == nil {
		t.Fatal("expected error compressing with nil encoder")
	}
	// Decompress should work.
	enc2, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer enc2.Close()
	compressed := enc2.EncodeAll([]byte{0x02}, nil) // zigzag 1
	got, err := codec.Decompress(compressed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte{0x02}) {
		t.Fatalf("got %x, want 02", got)
	}
}

func TestZstdCodecFromNilDecoder(t *testing.T) {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()

	codec := ZstdCodecFrom(enc, nil)
	if _, err := codec.Decompress([]byte("data")); err == nil {
		t.Fatal("expected error decompressing with nil decoder")
	}
	// Compress should work.
	got, err := codec.Compress([]byte{0x02})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("expected non-empty compressed output")
	}
}

func TestWithSchema(t *testing.T) {
	fullSchema := `{"type":"record","name":"person","fields":[{"name":"name","type":"string","doc":"The name"},{"name":"age","type":"int"}]}`
	s, err := avro.NewSchema(fullSchema)
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
	if codec != custom {
		t.Fatal("expected custom codec to override built-in zstandard")
	}
}

type testCodec struct {
	name string
}

func (c *testCodec) Name() string                         { return c.name }
func (c *testCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (c *testCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
