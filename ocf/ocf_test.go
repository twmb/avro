package ocf

import (
	"bufio"
	"bytes"
	"compress/flate"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"math"
	"math/big"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/twmb/avro"
)

// ---------- ocf_test.go ----------

const recordSchema = `{"type":"record","name":"person","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`

type person struct {
	Name string `avro:"name"`
	Age  int32  `avro:"age"`
}

func TestRoundTrip(t *testing.T) {
	s := mustParse(t, recordSchema)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	in := []person{
		{"Alice", 30},
		{"Bob", 25},
	}
	for _, p := range in {
		if err := w.Encode(&p); err != nil {
			t.Fatal(err)
		}
	}
	mustClose(t, w)

	r := mustNewReader(t, &buf)
	out := drainAll[person](t, r)
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("got %v, want %v", out, in)
	}
}

func TestDeflate(t *testing.T) {
	s := mustParse(t, recordSchema)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithCodec(DeflateCodec(flate.DefaultCompression)))
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
	mustClose(t, w)

	r := mustNewReader(t, &buf)
	out := drainAll[person](t, r)
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("got %v, want %v", out, in)
	}
}

func TestMultipleBlocks(t *testing.T) {
	s := mustParse(t, `"int"`)

	const n = 250
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s) // default block length 100
	writeInts(t, w, n)
	mustClose(t, w)

	r := mustNewReader(t, &buf)
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
	s := mustParse(t, `"string"`)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithBlockCount(2))
	strs := []string{"a", "b", "c", "d", "e"}
	for _, v := range strs {
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	mustClose(t, w)

	r := mustNewReader(t, &buf)
	got := drainAll[string](t, r)
	if !reflect.DeepEqual(strs, got) {
		t.Fatalf("got %v, want %v", got, strs)
	}
}

func TestMetadata(t *testing.T) {
	s := mustParse(t, `"int"`)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithMetadata(map[string][]byte{
		"my.key":  []byte("my.value"),
		"another": []byte("data"),
	}))
	mustClose(t, w)

	r := mustNewReader(t, &buf)
	meta := r.Metadata()
	if got := string(meta["my.key"]); got != "my.value" {
		t.Fatalf("my.key: got %q, want %q", got, "my.value")
	}
	if got := string(meta["another"]); got != "data" {
		t.Fatalf("another: got %q, want %q", got, "data")
	}
}

func TestReaderSchema(t *testing.T) {
	s := mustParse(t, recordSchema)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	p := person{"Alice", 30}
	if err := w.Encode(&p); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)

	r := mustNewReader(t, &buf)
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
	s := mustParse(t, `"int"`)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	mustClose(t, w)

	r := mustNewReader(t, &buf)
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
	s := mustParse(t, `"long"`)
	codec := xorCodec{0xAB}

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithCodec(codec))
	in := []int64{1, 2, 3, 100, -50}
	for _, v := range in {
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	mustClose(t, w)

	r := mustNewReader(t, &buf, WithCodec(codec))
	out := drainAll[int64](t, r)
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
	s := mustParse(t, `"int"`)
	codec := xorCodec{0x42}

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithCodec(codec))
	v := int32(1)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)

	// Read without registering the codec.
	_, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err == nil {
		t.Fatal("expected error for unknown codec")
	}
}

func TestBadSync(t *testing.T) {
	s := mustParse(t, `"int"`)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithBlockCount(1))
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
	mustClose(t, w)

	// Corrupt the sync marker in the second block.
	data := buf.Bytes()
	// With block length 1 the layout is header, block1, block2, each block being
	// count + size + data + sync. We corrupt block2's sync, the last 16 bytes.
	if len(data) < 32 {
		t.Fatal("data too short")
	}
	data[len(data)-1] ^= 0xFF // flip bits in last sync byte

	r := mustNewReader(t, bytes.NewReader(data))
	// First block should decode fine.
	if err := r.Decode(&v); err != nil {
		t.Fatal(err)
	}
	// Second block should fail with sync mismatch.
	err := r.Decode(&v)
	if err == nil {
		t.Fatal("expected sync mismatch error")
	}
}

func TestPrimitiveSchema(t *testing.T) {
	s := mustParse(t, `"string"`)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	in := []string{"hello", "world", ""}
	for _, v := range in {
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	mustClose(t, w)

	r := mustNewReader(t, &buf)
	out := drainAll[string](t, r)
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("got %v, want %v", out, in)
	}
}

func TestBlockCountZeroOrNegative(t *testing.T) {
	s := mustParse(t, `"int"`)
	// Block count 0 with no block bytes defaults to 100.
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithBlockCount(0))
	v := int32(7)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)
	r := mustNewReader(t, &buf)
	var out int32
	if err := r.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != 7 {
		t.Fatalf("got %d, want 7", out)
	}
}

func TestCloseIdempotent(t *testing.T) {
	s := mustParse(t, `"int"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	// Close with no items.
	mustClose(t, w)
	// Close again — no error expected.
	mustClose(t, w)
}

func TestCloseFlushError(t *testing.T) {
	s := mustParse(t, `"int"`)
	ew := &errAfterN{max: 4096}
	w := mustNewWriter(t, ew, s, WithBlockCount(1000)) // large block, no auto-flush
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
	s := mustParse(t, `"int"`)
	// Use a writer that accepts the header but fails on block writes.
	ew := &errAfterN{max: 4096}
	w := mustNewWriter(t, ew, s, WithBlockCount(1))
	// Now make subsequent writes fail.
	ew.max = 0
	v := int32(1)
	err := w.Encode(&v)
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
	s := mustParse(t, `"string"`)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithCodec(DeflateCodec(flate.BestSpeed)), WithBlockCount(10))
	var in []string
	for i := range 100 {
		v := fmt.Sprintf("value-%d", i)
		in = append(in, v)
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	mustClose(t, w)

	r := mustNewReader(t, &buf)
	out := drainAll[string](t, r)
	if !reflect.DeepEqual(in, out) {
		t.Fatal("large deflate round trip mismatch")
	}
}

func TestEncodeError(t *testing.T) {
	s := mustParse(t, `"int"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithBlockCount(100))
	// Encode a string into an int schema — should fail.
	v := "not an int"
	err := w.Encode(&v)
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
	mustClose(t, w)
}

type failCompressCodec struct{}

func (failCompressCodec) Name() string { return "failcompress" }
func (failCompressCodec) Close() error { return nil }
func (failCompressCodec) Compress([]byte) ([]byte, error) {
	return nil, errors.New("compress failed")
}
func (failCompressCodec) Decompress(src []byte) ([]byte, error) { return src, nil }

func TestCompressError(t *testing.T) {
	s := mustParse(t, `"int"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithCodec(failCompressCodec{}), WithBlockCount(1))
	v := int32(1)
	err := w.Encode(&v)
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
	s := mustParse(t, `"int"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	mustClose(t, w)
	// Append a partial varlong (continuation byte with no termination)
	// after the valid empty file to trigger a non-EOF readBlock error.
	data := buf.Bytes()
	data = append(data, 0x80) // continuation byte, then EOF
	r := mustNewReader(t, bytes.NewReader(data))
	var v int32
	err := r.Decode(&v)
	if err == nil || err == io.EOF {
		t.Fatalf("expected non-EOF read error, got %v", err)
	}
}

func TestTruncatedBlockSize(t *testing.T) {
	s := mustParse(t, `"int"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	mustClose(t, w)
	// Write a valid block count (1) but truncate before the size.
	data := buf.Bytes()
	data = binary.AppendVarint(data, 1) // count = 1, then EOF before size
	r := mustNewReader(t, bytes.NewReader(data))
	var v int32
	err := r.Decode(&v)
	if err == nil {
		t.Fatal("expected error for truncated block size")
	}
	// The count varint promised a block, so this cut is truncation, not the
	// end-of-stream sentinel: the error must be unexpected-EOF-shaped, never
	// io.EOF-shaped (io.EOF is reserved for a clean end at a block boundary).
	if errors.Is(err, io.EOF) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated block size must match io.ErrUnexpectedEOF and not io.EOF, got: %v", err)
	}
}

func TestTruncatedBlockData(t *testing.T) {
	s := mustParse(t, `"int"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	mustClose(t, w)
	data := buf.Bytes()
	data = binary.AppendVarint(data, 1)   // count
	data = binary.AppendVarint(data, 100) // size = 100 bytes, but EOF
	data = append(data, 0x01)             // only 1 byte of data
	r := mustNewReader(t, bytes.NewReader(data))
	var v int32
	err := r.Decode(&v)
	if err == nil {
		t.Fatal("expected error for truncated block data")
	}
	if errors.Is(err, io.EOF) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated block data must match io.ErrUnexpectedEOF and not io.EOF, got: %v", err)
	}
}

func TestTruncatedBlockSyncMarker(t *testing.T) {
	s := mustParse(t, `"int"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	mustClose(t, w)
	data := buf.Bytes()
	data = binary.AppendVarint(data, 1) // count
	data = binary.AppendVarint(data, 1) // size = 1
	data = append(data, 0x02)           // 1 byte of data
	data = append(data, 0x00)           // only 1 byte of sync marker, need 16
	r := mustNewReader(t, bytes.NewReader(data))
	var v int32
	err := r.Decode(&v)
	if err == nil {
		t.Fatal("expected error for truncated sync marker")
	}
	if errors.Is(err, io.EOF) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated sync marker must match io.ErrUnexpectedEOF and not io.EOF, got: %v", err)
	}
}

func TestNegativeBlockSize(t *testing.T) {
	s := mustParse(t, `"int"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	mustClose(t, w)
	data := buf.Bytes()
	data = binary.AppendVarint(data, 1)  // count
	data = binary.AppendVarint(data, -1) // negative size
	r := mustNewReader(t, bytes.NewReader(data))
	var v int32
	err := r.Decode(&v)
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
	s := mustParse(t, `"int"`)
	codec := failDecompressCodec{}
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithCodec(codec), WithBlockCount(1))
	v := int32(42)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)

	r := mustNewReader(t, bytes.NewReader(buf.Bytes()), WithCodec(codec))
	err := r.Decode(&v)
	if err == nil {
		t.Fatal("expected decompress error")
	}
	if !strings.Contains(err.Error(), "decompress") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDecodeError(t *testing.T) {
	// Write a string value, then try to decode as int.
	s := mustParse(t, `"string"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithBlockCount(1))
	v := "hello"
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)

	r := mustNewReader(t, bytes.NewReader(buf.Bytes()))
	var n int32
	err := r.Decode(&n)
	if err == nil {
		t.Fatal("expected decode error")
	}
}

func TestTrailingBytesInBlock(t *testing.T) {
	// Construct a block where item count is 1 but the data has extra bytes.
	s := mustParse(t, `"int"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	mustClose(t, w)

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

	r := mustNewReader(t, bytes.NewReader(data))
	var v int32
	err := r.Decode(&v)
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
	s := mustParse(t, `"int"`)
	_, err := NewWriter(&errAfterN{max: 0}, s)
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

	s := mustParse(t, `"int"`)
	_, err := NewWriter(&bytes.Buffer{}, s)
	if err == nil {
		t.Fatal("expected error from failing rand")
	}
	if !strings.Contains(err.Error(), "rand failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBlockCountNegative(t *testing.T) {
	s := mustParse(t, `"int"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithBlockCount(-5))
	// Negative count defaults to 100; single item flushed on Close.
	v := int32(99)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)
	r := mustNewReader(t, &buf)
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
	s := mustParse(t, `"int"`)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithBlockCount(1000)) // large block, won't auto-flush

	// Encode two items, then flush.
	v := int32(1)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	v = 2
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	mustFlush(t, w)

	// Encode more after flush.
	v = 3
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)

	// Read all three items back.
	r := mustNewReader(t, &buf)
	got := drainAll[int32](t, r)
	want := []int32{1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestFlushEmpty(t *testing.T) {
	s := mustParse(t, `"int"`)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	// Flush with nothing buffered — should be a no-op.
	mustFlush(t, w)
	mustClose(t, w)

	r := mustNewReader(t, &buf)
	var v int32
	if err := r.Decode(&v); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestFlushAfterError(t *testing.T) {
	s := mustParse(t, `"int"`)

	ew := &errAfterN{max: 4096}
	w := mustNewWriter(t, ew, s, WithBlockCount(1))
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
	s := mustParse(t, `"int"`)

	var marker [16]byte
	for i := range marker {
		marker[i] = byte(i + 0xA0)
	}

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithSyncMarker(marker))
	v := int32(42)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)

	// The sync marker should appear in the raw output.
	data := buf.Bytes()
	if !bytes.Contains(data, marker[:]) {
		t.Fatal("sync marker not found in output")
	}

	// Verify we can still read it back.
	r := mustNewReader(t, bytes.NewReader(data))
	var out int32
	if err := r.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != 42 {
		t.Fatalf("got %d, want 42", out)
	}
}

func TestWithBlockBytes(t *testing.T) {
	s := mustParse(t, `"int"`)

	// Each int encodes as 1 byte (zigzag for small values).
	// Set maxBytes=3 so that after 3 items, the block is flushed.
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithBlockCount(0), WithBlockBytes(3))
	writeInts(t, w, 7)
	mustClose(t, w)

	r := mustNewReader(t, &buf)
	got := drainAll[int32](t, r)
	want := []int32{0, 1, 2, 3, 4, 5, 6}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestWithBlockBytesAndBlockCount(t *testing.T) {
	s := mustParse(t, `"int"`)

	// Block count 2, block bytes very large — count triggers first.
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithBlockCount(2), WithBlockBytes(100000))
	writeInts(t, w, 5)
	mustClose(t, w)

	r := mustNewReader(t, &buf)
	got := drainAll[int32](t, r)
	want := []int32{0, 1, 2, 3, 4}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestWithBlockCountZero(t *testing.T) {
	s := mustParse(t, `"int"`)

	// Block count 0 + block bytes 2: only bytes triggers flush.
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithBlockCount(0), WithBlockBytes(2))
	writeInts(t, w, 5)
	mustClose(t, w)

	r := mustNewReader(t, &buf)
	got := drainAll[int32](t, r)
	want := []int32{0, 1, 2, 3, 4}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestReset(t *testing.T) {
	s := mustParse(t, `"int"`)

	var buf1 bytes.Buffer
	w := mustNewWriter(t, &buf1, s)
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
	mustClose(t, w)

	// buf1 should contain item 1.
	r1 := mustNewReader(t, &buf1)
	var out int32
	if err := r1.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != 1 {
		t.Fatalf("buf1: got %d, want 1", out)
	}

	// buf2 should contain item 2.
	r2 := mustNewReader(t, &buf2)
	if err := r2.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != 2 {
		t.Fatalf("buf2: got %d, want 2", out)
	}
}

func TestResetClearsError(t *testing.T) {
	s := mustParse(t, `"int"`)

	ew := &errAfterN{max: 4096}
	w := mustNewWriter(t, ew, s, WithBlockCount(1))
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
	mustClose(t, w)

	r := mustNewReader(t, &buf)
	var out int32
	if err := r.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != 2 {
		t.Fatalf("got %d, want 2", out)
	}
}

func TestResetFlushError(t *testing.T) {
	s := mustParse(t, `"int"`)

	ew := &errAfterN{max: 4096}
	w := mustNewWriter(t, ew, s, WithBlockCount(1000))
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
	s := mustParse(t, `"int"`)
	v := int32(9)

	// LIVE writer: NewWriter generates the initial sync before the override is
	// installed. (Pre-fix this closed the writer first, so Reset returned
	// errClosed and the randRead override below never ran — the test pinned
	// nothing.)
	var first bytes.Buffer
	w := mustNewWriter(t, &first, s)
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
	s := mustParse(t, `"int"`)

	// Write initial items.
	sb := &seekBuf{}
	w := mustNewWriter(t, sb, s)
	writeInts(t, w, 3)
	mustClose(t, w)

	// Append more items.
	sb.pos = 0
	aw := mustNewAppendWriter(t, sb)
	for i := 3; i < 6; i++ {
		v := int32(i)
		if err := aw.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	mustClose(t, aw)

	// Read all items.
	sb.pos = 0
	r := mustNewReader(t, sb)
	got := drainAll[int32](t, r)
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
	s := mustParse(t, `"int"`)

	codec := xorCodec{0xAB}

	// Write initial items with custom codec.
	sb := &seekBuf{}
	w := mustNewWriter(t, sb, s, WithCodec(codec), WithBlockCount(2))
	writeInts(t, w, 3)
	mustClose(t, w)

	// Append with the same codec.
	sb.pos = 0
	aw := mustNewAppendWriter(t, sb, WithCodec(codec))
	for i := 3; i < 5; i++ {
		v := int32(i)
		if err := aw.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	mustClose(t, aw)

	// Read all back.
	sb.pos = 0
	r := mustNewReader(t, sb, WithCodec(codec))
	got := drainAll[int32](t, r)
	want := []int32{0, 1, 2, 3, 4}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestAppendWriterSeekError(t *testing.T) {
	s := mustParse(t, `"int"`)

	// Write a valid OCF file first.
	sb := &seekBuf{}
	w := mustNewWriter(t, sb, s)
	mustClose(t, w)

	// Wrap in a type that fails on Seek.
	fsb := &failSeekRWS{data: sb.data}
	_, err := NewAppendWriter(fsb)
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
	s := mustParse(t, `"int"`)

	// Write with a custom codec.
	codec := xorCodec{0x42}
	sb := &seekBuf{}
	w := mustNewWriter(t, sb, s, WithCodec(codec))
	v := int32(1)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)

	// Try to append without providing the codec.
	sb.pos = 0
	_, err := NewAppendWriter(sb)
	if err == nil {
		t.Fatal("expected error for unknown codec")
	}
}

func TestWithBlockBytesNegative(t *testing.T) {
	s := mustParse(t, `"int"`)
	// Negative block bytes is clamped to 0; both zero → defaults to count 100.
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithBlockBytes(-1))
	v := int32(42)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)
	r := mustNewReader(t, &buf)
	var out int32
	if err := r.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != 42 {
		t.Fatalf("got %d, want 42", out)
	}
}

func TestAppendWriterBlockOpts(t *testing.T) {
	s := mustParse(t, `"int"`)

	// Write initial items.
	sb := &seekBuf{}
	w := mustNewWriter(t, sb, s)
	v := int32(1)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)

	// Append with block count and block bytes opts (including negative
	// values to exercise clamping).
	sb.pos = 0
	aw := mustNewAppendWriter(t, sb, WithBlockCount(-1), WithBlockBytes(-1))
	v = 2
	if err := aw.Encode(&v); err != nil {
		t.Fatal(err)
	}
	mustClose(t, aw)

	// Read all items back.
	sb.pos = 0
	r := mustNewReader(t, sb)
	got := drainAll[int32](t, r)
	want := []int32{1, 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestResetHeaderWriteError(t *testing.T) {
	s := mustParse(t, `"int"`)
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
	mustClose(t, w)

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
	s := mustParse(t, `"int"`)

	ew := &errAfterN{max: 4096}
	w := mustNewWriter(t, ew, s, WithBlockCount(1))
	ew.max = 0
	v := int32(1)
	if err := w.Encode(&v); err == nil {
		t.Fatal("expected error")
	}
	// Write should return the sticky error.
	_, err := w.Write([]byte{0x02})
	if err == nil {
		t.Fatal("expected sticky error on Write")
	}
}

func TestWriteAutoFlush(t *testing.T) {
	s := mustParse(t, `"int"`)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithBlockCount(2))

	// Pre-encode values.
	for i := range 5 {
		var encoded []byte
		v := int32(i)
		encoded = mustAppendEncode(t, s, encoded, &v)
		if _, err := w.Write(encoded); err != nil {
			t.Fatal(err)
		}
	}
	mustClose(t, w)

	r := mustNewReader(t, &buf)
	got := drainAll[int32](t, r)
	want := []int32{0, 1, 2, 3, 4}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestWriteFlushError(t *testing.T) {
	s := mustParse(t, `"int"`)
	ew := &errAfterN{max: 4096}
	w := mustNewWriter(t, ew, s, WithBlockCount(1))
	ew.max = 0
	// Write triggers auto-flush which fails.
	_, err := w.Write([]byte{0x02})
	if err == nil {
		t.Fatal("expected error from flush during Write")
	}
}

// ---------- Snappy codec ----------

func TestSnappy(t *testing.T) {
	s := mustParse(t, recordSchema)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithCodec(SnappyCodec()))
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
	mustClose(t, w)

	// Reader auto-resolves snappy codec.
	r := mustNewReader(t, &buf)
	out := drainAll[person](t, r)
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
	mustClose(t, w)

	// Reader auto-resolves zstandard codec.
	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	out := drainAll[person](t, r)
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("got %v, want %v", out, in)
	}
}

// ---------- Codec close ----------

func TestWriterClosesCodec(t *testing.T) {
	s := mustParse(t, `"int"`)

	codec := &trackCloseCodec{}
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithCodec(codec))
	v := int32(1)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)
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
	s := mustParse(t, `"int"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithCodec(&failCloseCodec{}))
	if err := w.Close(); err == nil {
		t.Fatal("expected error from codec close")
	}
}

type failCloseCodec struct{ nullCodec }

func (failCloseCodec) Close() error { return errors.New("close failed") }

func TestReaderClose(t *testing.T) {
	s := mustParse(t, `"int"`)

	// Null codec — Close is a no-op.
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	mustClose(t, w)
	r := mustNewReader(t, &buf)
	mustClose(t, r)
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
	mustClose(t, w)

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
	mustClose(t, r)
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
	mustClose(t, w)

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
	mustClose(t, w)

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
	s := mustParse(t, fullSchema)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithSchema(fullSchema))
	if err := w.Encode(&person{"Alice", 30}); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)

	r := mustNewReader(t, &buf)
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
	codec, adopted, err := resolveCodec("zstandard", []Codec{custom})
	if err != nil {
		t.Fatal(err)
	}
	if codec != Codec(custom) {
		t.Fatal("expected custom codec to override built-in zstandard")
	}
	// The index identifies WHICH offer was taken, so the caller can release the
	// ones that were not; a built-in resolved by name reports -1.
	if adopted != 0 {
		t.Fatalf("adopted index = %d, want 0 (the sole supplied codec)", adopted)
	}
	if _, adopted, err := resolveCodec("deflate", []Codec{custom}); err != nil || adopted != -1 {
		t.Fatalf("built-in resolution: adopted = %d (want -1), err = %v", adopted, err)
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

			got := drainAll[weather](t, r)
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

	r := mustNewReader(t, bytes.NewReader(data))
	var v any
	err := r.Decode(&v)
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
	mustClose(t, w)

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
	w := mustNewWriter(t, &buf, writerSchema)
	if err := w.Encode(&person{Name: "Alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)

	// Read with evolved schema.
	r := mustNewReader(t, &buf, WithReaderSchema(readerSchema))
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
	w := mustNewWriter(t, &buf, writerSchema)
	if err := w.Encode(&person{Name: "Alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)

	_, err := NewReader(&buf, WithReaderSchema(readerSchema))
	if err == nil {
		t.Fatal("expected error for incompatible schemas")
	}
}

func TestReservedMetadataKey(t *testing.T) {
	s := mustParse(t, `"int"`)
	var buf bytes.Buffer
	_, err := NewWriter(&buf, s, WithMetadata(map[string][]byte{
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
	mustClose(t, w)

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
	w := mustNewWriter(t, &buf, writerSchema)
	if err := w.Encode(&person{Name: "Alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)

	r := mustNewReader(t, &buf, WithReaderSchemaFunc(func(rd *Reader) (*avro.Schema, error) {
		return nil, nil
	}))
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
	w := mustNewWriter(t, &buf, writerSchema)
	if err := w.Encode(&person{Name: "Alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)

	sentinel := errors.New("picky callback")
	_, err := NewReader(&buf, WithReaderSchemaFunc(func(rd *Reader) (*avro.Schema, error) {
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
	w := mustNewWriter(t, &buf, writerSchema)
	if err := w.Encode(&person{Name: "Alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)

	s := avro.MustParse(recordSchema)
	_, err := NewReader(&buf,
		WithReaderSchema(s),
		WithReaderSchemaFunc(func(*Reader) (*avro.Schema, error) { return s, nil }),
	)
	if err == nil {
		t.Fatal("expected error when both options are provided")
	}
}

func TestNegativeBlockCountRead(t *testing.T) {
	s := mustParse(t, `"int"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	mustClose(t, w)
	// Append a block with negative count after the valid (empty) file.
	data := buf.Bytes()
	data = binary.AppendVarint(data, -1)     // negative count
	data = binary.AppendVarint(data, 0)      // size
	data = append(data, make([]byte, 16)...) // sync marker
	r := mustNewReader(t, bytes.NewReader(data))
	var v int32
	err := r.Decode(&v)
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
	w := mustNewWriter(t, &buf, schema)
	if got := w.Schema(); got != schema {
		t.Fatal("Writer.Schema() did not return the original schema")
	}
	w.Close()
}

// TestWithSchemaOptsCustomType verifies that [WithSchemaOpts] passes
// [avro.CustomType] through to the reader-schema parse so the registered callback
// fires on the matching logical-typed field during Decode.
//
// Pre-fix the OCF writer wrote PCF to the header, which strips logicalType, so the
// reader's parsed-from-header schema had no logical type to dispatch on and the
// built-in date-on-int default produced an int32. The old assertion
// `out["d"].(int32)` was tautological, succeeding whether or not the CustomType
// fired; this tracks via a captured bool AND asserts the handler's return type.
func TestWithSchemaOptsCustomType(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"d","type":{"type":"int","logicalType":"date"}}
	]}`)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, schema)
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
	r := mustNewReader(t, &buf, WithSchemaOpts(ct))
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
	mustClose(t, w)

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
	mustClose(t, aw)

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

// TestRegression_TrailingEmptyBlockIsCleanEOF verifies that a count=0
// block at the very end of a file yields a clean io.EOF: the block's
// size, payload, and sync marker are read and validated first, the empty
// block is skipped, and the next block-count read hits the true end of
// stream. (A validated count-0 block is skipped wherever it appears —
// see TestRegression_EmptyBlockMidStreamSkipped for the mid-stream case
// and the cross-implementation notes.) Reading the full envelope first
// means a tail-truncated file whose count byte happens to read as 0 is
// never mistaken for a clean end — see
// TestRegression_BlockCountZeroValidatesSync.
func TestRegression_TrailingEmptyBlockIsCleanEOF(t *testing.T) {
	sync := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var buf bytes.Buffer
	s := mustParse(t, recordSchema)
	w := mustNewWriter(t, &buf, s, WithSyncMarker(sync))
	if err := w.Encode(&person{Name: "alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)
	// Append a count=0, size=0 block with the REAL sync. The reader
	// reads size and sync first, validates the sync, THEN sees count==0
	// and returns io.EOF.
	zeroBlock := append([]byte{}, 0x00, 0x00)
	zeroBlock = append(zeroBlock, sync[:]...)
	buf.Write(zeroBlock)

	r := mustNewReader(t, &buf)
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
	s := mustParse(t, recordSchema)
	w := mustNewWriter(t, &buf, s, WithSyncMarker(sync))
	if err := w.Encode(&person{Name: "alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)
	// count=0, size=0, but corrupt sync (all 0xFF).
	zeroBlock := append([]byte{}, 0x00, 0x00)
	zeroBlock = append(zeroBlock, bytes.Repeat([]byte{0xFF}, 16)...)
	buf.Write(zeroBlock)

	r := mustNewReader(t, &buf)
	var p person
	if err := r.Decode(&p); err != nil {
		t.Fatalf("decode first record: %v", err)
	}
	err := r.Decode(&p)
	if err == nil || err == io.EOF {
		t.Fatalf("expected sync-mismatch error on count=0 block with corrupt sync, got %v", err)
	}
	if !strings.Contains(err.Error(), "sync marker mismatch") {
		t.Fatalf("expected sync marker mismatch error, got: %v", err)
	}
}

// TestRegression_EmptyBlockMidStreamSkipped verifies that a count=0 block whose
// sync marker validates is SKIPPED rather than treated as end-of-stream. The spec
// places no constraint on a block's object count: unlike Avro arrays and maps,
// file blocks have no terminator, so a zero-count block is valid framing, and
// fastavro's record iterator reads straight past one — treating it as EOF silently
// dropped every record after it. Java never emits the shape and its for-each
// reader stops at one, while goavro errors and avro-rs stops; skipping reads
// everything a foreign writer put in the file and loses nothing.
func TestRegression_EmptyBlockMidStreamSkipped(t *testing.T) {
	sync := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	s := avro.MustParse(`"string"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithSyncMarker(sync))
	if err := w.Encode("first"); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil { // seals block 1: [count=1]["first"][sync]
		t.Fatal(err)
	}

	// Block 2: count=0, size=0, valid sync — spec-valid empty framing.
	buf.Write(binary.AppendVarint(nil, 0))
	buf.Write(binary.AppendVarint(nil, 0))
	buf.Write(sync[:])

	// Block 3: count=1, one "second" datum, valid sync.
	datum := mustAppendEncode(t, s, nil, "second")
	buf.Write(binary.AppendVarint(nil, 1))
	buf.Write(binary.AppendVarint(nil, int64(len(datum))))
	buf.Write(datum)
	buf.Write(sync[:])

	r := mustNewReader(t, bytes.NewReader(buf.Bytes()))
	var got []string
	for {
		var v string
		err := r.Decode(&v)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		got = append(got, v)
	}
	if len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Fatalf("records across a mid-stream empty block: got %v, want [first second]", got)
	}
}

// TestRegression_OCFBigDecimalJavaInterop verifies that a Java-generated
// big-decimal OCF file decodes to the correct *big.Rat through the twmb reader.
// testdata/bigdec.avro is the same vendored file avro-rs uses in
// test_avro_3779_from_java_file; it holds one record {field_name: 2.24}. The
// wire-format byte match for 2.24 is pinned at the payload level in
// TestSpecBigDecimalWireFormat; this extends the guarantee end-to-end through the
// OCF framing.
//
// leakDetectCodec records whether Close() has been called, for the tests below.
type leakDetectCodec struct {
	name   string
	closes int
}

func (c *leakDetectCodec) Name() string                          { return c.name }
func (c *leakDetectCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (c *leakDetectCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (c *leakDetectCodec) Close() error                          { c.closes++; return nil }

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
// in that path. (Deliberately more careful than Java, whose
// DataFileWriter.close is flush-then-close with no finally — a
// failing flush skips its close, DataFileWriter.java:483-489.)
func TestRegression_OCFWriterCloseClosesCodecWhenPoisoned(t *testing.T) {
	codec := &leakDetectCodec{name: "null"}
	// One successful write for the header; subsequent writes fail.
	// The next flush poisons w.err without aborting construction.
	w := mustNewWriter(t, &failAfterNWrites{n: 1}, avro.MustParse(`"long"`), WithCodec(codec), WithBlockCount(1))
	// Encode → buffered. BlockCount=1 triggers an immediate flush
	// which writes to the now-failing writer and poisons w.err.
	_ = w.Encode(int64(1))
	_ = w.Close()
	if codec.closes == 0 {
		t.Fatal("Writer.Close() did not call codec.Close() after the writer was poisoned")
	}
}

// TestRegression_OCFNewReaderClosesCodecOnReaderSchemaFnError locks
// that NewReader closes the codec on any error path after
// resolveCodec succeeds. Pre-fix readerSchemaFn / Resolve errors
// returned nil + err without closing the already-resolved codec.
func TestRegression_OCFNewReaderClosesCodecOnReaderSchemaFnError(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, avro.MustParse(`"long"`))
	if err := w.Encode(int64(1)); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	mustClose(t, w)

	codec := &leakDetectCodec{name: "null"}
	_, err := NewReader(
		bytes.NewReader(buf.Bytes()),
		WithCodec(codec),
		WithReaderSchemaFunc(func(*Reader) (*avro.Schema, error) {
			return nil, errors.New("synthetic error")
		}),
	)
	if err == nil {
		t.Fatal("expected error from reader-schema func")
	}
	if codec.closes == 0 {
		t.Fatal("NewReader returned error without closing the codec")
	}
}

// TestRegression_OCFNewReaderClosesCodecOnResolveError locks the
// same invariant for the avro.Resolve error path.
func TestRegression_OCFNewReaderClosesCodecOnResolveError(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, avro.MustParse(`"long"`))
	if err := w.Encode(int64(1)); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	mustClose(t, w)

	// Reader schema is incompatible with the writer (long → string),
	// forcing avro.Resolve to error.
	codec := &leakDetectCodec{name: "null"}
	_, err := NewReader(
		bytes.NewReader(buf.Bytes()),
		WithCodec(codec),
		WithReaderSchema(avro.MustParse(`"string"`)),
	)
	if err == nil {
		t.Fatal("expected Resolve error")
	}
	if codec.closes == 0 {
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
		s := mustParse(t, `"long"`)
		w := mustNewWriter(t, &buf, s, WithSyncMarker(sync))
		if err := w.Encode(int64(1)); err != nil {
			t.Fatal(err)
		}
		mustClose(t, w)
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
			r := mustNewReader(t, bytes.NewReader(bs))
			var v int64
			if err := r.Decode(&v); err != nil { // first valid record
				t.Fatalf("decode first: %v", err)
			}
			err := r.Decode(&v) // bad block
			if err == nil || err == io.EOF {
				t.Fatalf("expected loud error containing %q, got %v", c.want, err)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("expected error containing %q, got: %v", c.want, err)
			}
		})
	}
}

// TestRegression_OCFBlockCountCap locks the DoS-resistance cap on the OCF block
// count, previously uncapped. For zero-byte record schemas every record encodes
// to 0 wire bytes, so without the cap an attacker could claim ~10^9 records in a
// 5-byte varint, forcing the user's decode loop to iterate that many times from
// a tiny input. The cap bounds count by len(decompressed block) +
// maxOCFZeroByteSlack, the slack matching deser.go's maxZeroByteItems philosophy;
// legitimate zero-byte schemas split into multiple blocks. Java's DataFileStream
// and fastavro's _iter_avro_records leave this uncapped — the cap is twmb's
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
		s := mustParse(t, schemaJSON)
		var buf bytes.Buffer
		w := mustNewWriter(t, &buf, s)
		mustClose(t, w)
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

		rd := mustNewReader(t, bytes.NewReader(mb))
		defer rd.Close()

		var v map[string]any
		err := rd.Decode(&v)
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

		rd := mustNewReader(t, bytes.NewReader(mb))
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

		rd := mustNewReader(t, bytes.NewReader(mb))
		defer rd.Close()

		var v int64
		err := rd.Decode(&v)
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
		s := mustParse(t, `"long"`)
		var buf bytes.Buffer
		w := mustNewWriter(t, &buf, s, WithBlockCount(10_000))
		const n = 5000
		for i := range n {
			if err := w.Encode(int64(i)); err != nil {
				t.Fatal(err)
			}
		}
		mustClose(t, w)

		rd := mustNewReader(t, bytes.NewReader(buf.Bytes()))
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
// verifies, as Java's SnappyCodec does; fastavro reads the 4 CRC bytes
// but never compares them (_read_py.py snappy_read_block — see
// NOT_BUGS.md #20); the matrix asserts CRC mismatches are detected.
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
			w := mustNewWriter(t, &buf, schema, opts...)
			for _, r := range records {
				if err := w.Encode(r); err != nil {
					t.Fatalf("Encode: %v", err)
				}
			}
			mustClose(t, w)

			// Read back.
			r := mustNewReader(t, bytes.NewReader(buf.Bytes()))
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
	w := mustNewWriter(t, &buf, schema)
	mustClose(t, w)

	r := mustNewReader(t, bytes.NewReader(buf.Bytes()))
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
	w := mustNewWriter(t, &buf, schema)
	for i := range 5 {
		if err := w.Encode(int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	mustClose(t, w)
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
	w := mustNewWriter(t, &buf, wschema)
	if err := w.Encode(int32(42)); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)

	r := mustNewReader(t, bytes.NewReader(buf.Bytes()), WithReaderSchema(rschema))
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
	r := mustNewReader(t, bytes.NewReader(buf.Bytes()))
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
	s := mustParse(t, `"int"`)
	build := func() (*Writer, *bytes.Buffer) {
		var buf bytes.Buffer
		w := mustNewWriter(t, &buf, s)
		v := int32(1)
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
		mustClose(t, w)
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
	s := mustParse(t, `"int"`)
	closes := 0
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithCodec(closeCountCodec{&closes}))
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
	s := mustParse(t, `"int"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	// Two records in one block, so the second is buffered after the first
	// Decode — a use-after-Close that returns buffered data would leak it.
	for _, v := range []int32{7, 9} {
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
	}
	mustClose(t, w)

	closes := 0
	r := mustNewReader(t, bytes.NewReader(buf.Bytes()), WithCodec(closeCountCodec{&closes}))
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
	w := mustNewWriter(t, &buf, s)
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
	mustClose(t, w)

	r := mustNewReader(t, bytes.NewReader(buf.Bytes()))
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

// NewAppendWriter never rewrites the existing file's header: the schema, sync
// marker, and metadata always come from the file, so WithSchema, WithSyncMarker,
// and WithMetadata are accepted-and-ignored on append. The references behave the
// same — Java's appendTo copies schema/sync/meta from the file and its setMeta
// throws once appending, and fastavro's append mode silently drops its metadata
// kwarg (executed against 1.12.2). This pin locks the ignore, so a future change
// that honors or rejects these options is a deliberate flip; the appended records
// decoding cleanly is the sync-marker assertion.
func TestAppendWriterIgnoresHeaderOptions(t *testing.T) {
	schema := `{"type":"record","name":"R","fields":[{"name":"f","type":"string"}]}`
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, avro.MustParse(schema), WithMetadata(map[string][]byte{"orig": []byte("yes")}))
	if err := w.Encode(map[string]any{"f": "one"}); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	mustClose(t, w)

	f := newMemFile(buf.Bytes())
	aw := mustNewAppendWriter(t, f, WithSchema(`{"type":"record","name":"Other","fields":[{"name":"f","type":"string"}]}`), WithSyncMarker([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}), WithMetadata(map[string][]byte{"added": []byte("later")}))
	if err := aw.Encode(map[string]any{"f": "two"}); err != nil {
		t.Fatalf("append Encode: %v", err)
	}
	if err := aw.Close(); err != nil {
		t.Fatalf("append Close: %v", err)
	}

	rd := mustNewReader(t, bytes.NewReader(f.data))
	meta := rd.Metadata()
	if v, ok := meta["added"]; ok {
		t.Errorf("append-time WithMetadata landed in the header: added=%q", v)
	}
	if string(meta["orig"]) != "yes" {
		t.Errorf("original metadata lost: orig=%q", meta["orig"])
	}
	if !bytes.Equal(rd.Schema().Canonical(), avro.MustParse(schema).Canonical()) {
		t.Errorf("append-time WithSchema replaced the header schema: %s", rd.Schema().String())
	}
	var got []string
	for {
		var v map[string]any
		if err := rd.Decode(&v); err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("Decode: %v", err)
			}
			break
		}
		got = append(got, v["f"].(string))
	}
	if len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Errorf("file datums: got %v, want [one two]", got)
	}
}

// ---------- block_framing_test.go ----------

// ---------------------------------------------------------------------------
// Block-FRAMING contract: the writer's block boundaries are an operational
// contract (sync-point density for splittable processing, the WithBlockCount
// and WithBlockBytes knobs), and the no-empty-block invariant keeps
// twmb-written files fully readable everywhere: this package's Reader and
// fastavro SKIP a validated count-0 block, but Java's DataFileStream
// for-each stops at one (silently truncating everything after it for Java
// consumers), avro-rs stops, and goavro errors — so the writer must never
// emit one. Round-trip tests can't see any of this (every split is a valid
// file), so the framing itself is parsed and asserted here.
// ---------------------------------------------------------------------------

// parseBlockCounts parses an OCF's raw container framing and returns the
// per-block datum counts, validating each block's sync marker.
func parseBlockCounts(t *testing.T, data []byte) []int64 {
	t.Helper()
	br := bufio.NewReader(bytes.NewReader(data))
	_, _, sync, err := readHeader(br, nil)
	if err != nil {
		t.Fatalf("parse header: %v", err)
	}
	var counts []int64
	for {
		count, err := binary.ReadVarint(br)
		if err == io.EOF {
			return counts
		}
		if err != nil {
			t.Fatalf("block %d count: %v", len(counts), err)
		}
		size, err := binary.ReadVarint(br)
		if err != nil {
			t.Fatalf("block %d size: %v", len(counts), err)
		}
		if _, err := io.CopyN(io.Discard, br, size); err != nil {
			t.Fatalf("block %d data: %v", len(counts), err)
		}
		var s2 [16]byte
		if _, err := io.ReadFull(br, s2[:]); err != nil {
			t.Fatalf("block %d sync: %v", len(counts), err)
		}
		if s2 != sync {
			t.Fatalf("block %d sync mismatch", len(counts))
		}
		counts = append(counts, count)
	}
}

// fiveByteDatum encodes to exactly 5 wire bytes (length varint 0x08 + 4).
const fiveByteDatum = "abcd"

func framingFile(t *testing.T, opts []WriterOpt, ops func(w *Writer)) []byte {
	t.Helper()
	s := avro.MustParse(`"string"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, opts...)
	ops(w)
	mustClose(t, w)
	return buf.Bytes()
}

func encodeN(t *testing.T, w *Writer, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := w.Encode(fiveByteDatum); err != nil {
			t.Fatalf("Encode #%d: %v", i, err)
		}
	}
}

func wantCounts(t *testing.T, data []byte, want ...int64) {
	t.Helper()
	got := parseBlockCounts(t, data)
	if len(got) != len(want) {
		t.Fatalf("block counts: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("block counts: got %v want %v", got, want)
		}
	}
	for _, c := range got {
		if c <= 0 {
			t.Fatalf("EMPTY BLOCK in framing (%v): Java's for-each reader stops at a count-0 block, truncating twmb-written files for Java consumers", got)
		}
	}
}

func TestWriterBlockFramingContract(t *testing.T) {
	t.Run("count-option-seals-at-exactly-n", func(t *testing.T) {
		data := framingFile(t, []WriterOpt{WithBlockCount(2)}, func(w *Writer) { encodeN(t, w, 5) })
		wantCounts(t, data, 2, 2, 1)
	})
	t.Run("byte-threshold-seals-at-crossing", func(t *testing.T) {
		// 5-byte datums against an 8-byte threshold: the buffer reaches the
		// threshold at the second datum (10 >= 8), sealing pairs.
		data := framingFile(t, []WriterOpt{WithBlockBytes(8)}, func(w *Writer) { encodeN(t, w, 4) })
		wantCounts(t, data, 2, 2)
	})
	t.Run("flush-on-empty-writes-nothing", func(t *testing.T) {
		data := framingFile(t, nil, func(w *Writer) {
			if err := w.Flush(); err != nil { // before anything
				t.Fatalf("empty Flush: %v", err)
			}
			encodeN(t, w, 1)
			if err := w.Flush(); err != nil { // seals the pending datum
				t.Fatalf("Flush: %v", err)
			}
			if err := w.Flush(); err != nil { // now empty again: must no-op
				t.Fatalf("second empty Flush: %v", err)
			}
			encodeN(t, w, 2)
		})
		wantCounts(t, data, 1, 2)
	})
	t.Run("flush-seals-exactly-the-pending-count", func(t *testing.T) {
		data := framingFile(t, nil, func(w *Writer) {
			encodeN(t, w, 3)
			mustFlush(t, w)
			encodeN(t, w, 1)
		})
		wantCounts(t, data, 3, 1)
	})
	t.Run("reset-on-empty-emits-no-block", func(t *testing.T) {
		s := avro.MustParse(`"string"`)
		var first, second bytes.Buffer
		w := mustNewWriter(t, &first, s)
		encodeN(t, w, 2)
		if err := w.Flush(); err != nil { // first now holds one sealed block
			t.Fatal(err)
		}
		if err := w.Reset(&second); err != nil { // EMPTY at reset: no extra block
			t.Fatalf("Reset: %v", err)
		}
		encodeN(t, w, 1)
		mustClose(t, w)
		wantCounts(t, first.Bytes(), 2)
	})
	t.Run("negative-count-option-means-unlimited", func(t *testing.T) {
		data := framingFile(t, []WriterOpt{WithBlockCount(-3)}, func(w *Writer) { encodeN(t, w, 4) })
		wantCounts(t, data, 4) // byte-driven only: one block under the default cap
	})
	t.Run("zero-bytes-option-means-default", func(t *testing.T) {
		data := framingFile(t, []WriterOpt{WithBlockBytes(0)}, func(w *Writer) { encodeN(t, w, 4) })
		wantCounts(t, data, 4) // default 64KiB cap, not seal-per-datum
	})
}

// The append writer honors the same sizing options and normalizations for
// the blocks IT writes.
func TestAppendWriterBlockFramingContract(t *testing.T) {
	mk := func(t *testing.T, opts ...WriterOpt) []byte {
		t.Helper()
		s := avro.MustParse(`"string"`)
		var buf bytes.Buffer
		w := mustNewWriter(t, &buf, s, WithBlockCount(2))
		encodeN(t, w, 2)
		mustClose(t, w)
		f := newMemFile(buf.Bytes())
		aw := mustNewAppendWriter(t, f, opts...)
		encodeN(t, aw, 5)
		mustClose(t, aw)
		return f.data
	}
	t.Run("append-count-option", func(t *testing.T) {
		wantCounts(t, mk(t, WithBlockCount(2)), 2, 2, 2, 1)
	})
	t.Run("append-byte-threshold", func(t *testing.T) {
		wantCounts(t, mk(t, WithBlockBytes(8)), 2, 2, 2, 1)
	})
	t.Run("append-defaults-normalize", func(t *testing.T) {
		wantCounts(t, mk(t, WithBlockCount(-1), WithBlockBytes(0)), 2, 5)
	})
}

// newMemFile gives NewAppendWriter the ReadWriteSeeker it needs, in memory.
type memFile struct {
	data []byte
	pos  int64
}

func newMemFile(b []byte) *memFile { return &memFile{data: append([]byte{}, b...)} }

func (m *memFile) Read(p []byte) (int, error) {
	if m.pos >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[m.pos:])
	m.pos += int64(n)
	return n, nil
}

func (m *memFile) Write(p []byte) (int, error) {
	need := m.pos + int64(len(p))
	for int64(len(m.data)) < need {
		m.data = append(m.data, 0)
	}
	copy(m.data[m.pos:], p)
	m.pos = need
	return len(p), nil
}

func (m *memFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		m.pos = offset
	case io.SeekCurrent:
		m.pos += offset
	case io.SeekEnd:
		m.pos = int64(len(m.data)) + offset
	}
	return m.pos, nil
}

// ---------- callback_contract_matrix_test.go ----------

// User-supplied callback contract matrix: every point where this package
// does arithmetic, slicing, or a state transition on a value returned by
// USER code (Codec.Compress / Codec.Decompress / BoundedDecompressor,
// io.Reader, io.Writer, WithReaderSchemaFunc). The invariant pinned per
// cell: a contract violation NEVER panics through the public API and
// NEVER silently corrupts sibling data — detectable violations yield
// named errors; undetectable ones corrupt only the violating user's own
// stream and are pinned here as documented behavior.

var contractSchema = avro.MustParse(`{"type":"record","name":"CR","fields":[{"name":"x","type":"long"}]}`)

// contractCodec is an identity codec whose Compress/Decompress return
// shapes are selectable per violation class.
type contractCodec struct {
	cmpMode string
	decMode string
	closed  *bool
}

func (e *contractCodec) Name() string { return "contract-test" }
func (e *contractCodec) Close() error {
	if e.closed != nil {
		*e.closed = true
	}
	return nil
}

func (e *contractCodec) Compress(src []byte) ([]byte, error) {
	switch e.cmpMode {
	case "", "identity":
		return append([]byte(nil), src...), nil
	case "nil-nil":
		return nil, nil
	case "garbage":
		return []byte("garbagegarbage"), nil
	case "error":
		// A non-nil error alongside a usable-looking value: the error must
		// win and the value must never reach the file.
		return []byte("usable"), errors.New("cmp boom")
	}
	panic("bad cmpMode " + e.cmpMode)
}

func (e *contractCodec) Decompress(src []byte) ([]byte, error) {
	switch e.decMode {
	case "", "identity":
		return append([]byte(nil), src...), nil
	case "alias":
		// Returning the input slice itself is legal: the reader owns the
		// compressed buffer and hands the block to the decoder before the
		// next block read; decoded []byte/string targets copy out.
		return src, nil
	case "nil-nil":
		return nil, nil
	case "short":
		return src[:1], nil
	case "pad":
		return append(append([]byte(nil), src...), make([]byte, 1024)...), nil
	case "error":
		return []byte("usable"), errors.New("dec boom")
	}
	panic("bad decMode " + e.decMode)
}

func writeContractOCF(t *testing.T, c Codec, vals ...int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, contractSchema, WithCodec(c))
	for _, v := range vals {
		if err := w.Encode(map[string]any{"x": v}); err != nil {
			t.Fatal(err)
		}
	}
	mustClose(t, w)
	return buf.Bytes()
}

// TestMatrix_CodecCompressReturnShapes: the writer computes the block
// length itself from the returned slice (a codec cannot lie about it),
// copies the returned bytes into its own block buffer (no aliasing
// hazard), and treats a returned error as fatal-and-poisoning per the
// Writer's I/O-error discipline — the accompanying value is discarded.
// A codec that returns wrong bytes with a nil error corrupts only its
// own stream: the write side cannot detect it, the read side surfaces
// named decode errors, and no shape panics.
func TestMatrix_CodecCompressReturnShapes(t *testing.T) {
	t.Run("nil-nil", func(t *testing.T) {
		file := writeContractOCF(t, &contractCodec{cmpMode: "nil-nil"}, 1, 2)
		r, err := NewReader(bytes.NewReader(file), WithCodec(&contractCodec{}))
		if err != nil {
			t.Logf("NewReader: %v", err)
			return
		}
		var v map[string]any
		for {
			if err := r.Decode(&v); err != nil {
				if err == io.EOF {
					t.Fatal("nil-compressed blocks with count>0 read back as clean EOF")
				}
				t.Logf("named: %v", err)
				return
			}
		}
	})
	t.Run("garbage", func(t *testing.T) {
		file := writeContractOCF(t, &contractCodec{cmpMode: "garbage"}, 1)
		r, err := NewReader(bytes.NewReader(file), WithCodec(&contractCodec{}))
		if err != nil {
			return
		}
		var v map[string]any
		var derr error
		for derr = r.Decode(&v); derr == nil; derr = r.Decode(&v) {
		}
		if derr == io.EOF {
			t.Fatal("garbage-compressed block read back cleanly")
		}
	})
	t.Run("error-poisons-and-codec-still-closes", func(t *testing.T) {
		closed := false
		c := &contractCodec{cmpMode: "error", closed: &closed}
		var buf bytes.Buffer
		w := mustNewWriter(t, &buf, contractSchema, WithCodec(c))
		if err := w.Encode(map[string]any{"x": int64(1)}); err != nil {
			t.Fatalf("buffered encode must not compress yet: %v", err)
		}
		ferr := w.Flush()
		if ferr == nil || !strings.Contains(ferr.Error(), "cmp boom") {
			t.Fatalf("Compress error not surfaced with identity: %v", ferr)
		}
		if buf.Len() != 0 && bytes.Contains(buf.Bytes(), []byte("usable")) {
			t.Error("value returned alongside the error reached the file")
		}
		if err := w.Encode(map[string]any{"x": int64(2)}); err == nil {
			t.Error("writer not poisoned after Compress error")
		}
		if cerr := w.Close(); cerr == nil {
			t.Error("Close cleared the compression poison")
		}
		if !closed {
			t.Error("codec.Close skipped on poisoned Close")
		}
	})
}

// TestMatrix_CodecDecompressReturnShapes: the reader uses the RETURNED
// slice's real length for its count bound and trailing-bytes check, so a
// codec cannot lie about length; wrong content surfaces as named decode
// errors; a returned error is surfaced (with the accompanying value
// discarded) and the reader advances to the next block on the following
// call rather than wedging.
func TestMatrix_CodecDecompressReturnShapes(t *testing.T) {
	file := writeContractOCF(t, &contractCodec{}, 1, 2, 3)

	run := func(mode string) (decoded []int64, derr error) {
		r, err := NewReader(bytes.NewReader(file), WithCodec(&contractCodec{decMode: mode}))
		if err != nil {
			return nil, err
		}
		for range 10 {
			var v map[string]any
			err := r.Decode(&v)
			if err == io.EOF {
				return decoded, nil
			}
			if err != nil {
				return decoded, err
			}
			decoded = append(decoded, v["x"].(int64))
		}
		return decoded, nil
	}

	t.Run("identity", func(t *testing.T) {
		d, err := run("identity")
		if err != nil || len(d) != 3 {
			t.Fatalf("control: %v %v", d, err)
		}
	})
	t.Run("alias-input", func(t *testing.T) {
		d, err := run("alias")
		if err != nil || len(d) != 3 {
			t.Fatalf("aliasing the input slice must be legal: %v %v", d, err)
		}
	})
	t.Run("nil-nil", func(t *testing.T) {
		d, err := run("nil-nil")
		if err == nil {
			t.Fatalf("nil block against count>0 silently decoded: %v", d)
		}
	})
	t.Run("short", func(t *testing.T) {
		_, err := run("short")
		if err == nil {
			t.Fatal("short block silently decoded")
		}
	})
	t.Run("pad", func(t *testing.T) {
		d, err := run("pad")
		if err == nil {
			t.Fatalf("padded block's trailing bytes silently ignored: %v", d)
		}
		if !strings.Contains(err.Error(), "trailing bytes") {
			t.Errorf("padding surfaced as %v, want the trailing-bytes reject", err)
		}
	})
	t.Run("error-surfaces-then-reader-advances", func(t *testing.T) {
		r := mustNewReader(t, bytes.NewReader(file), WithCodec(&contractCodec{decMode: "error"}))
		var v map[string]any
		if err := r.Decode(&v); err == nil || !strings.Contains(err.Error(), "dec boom") {
			t.Fatalf("Decompress error not surfaced with identity: %v", err)
		}
		// The failed block's bytes were consumed; the reader moves on
		// (a single-block file ends cleanly).
		if err := r.Decode(&v); err != io.EOF {
			t.Fatalf("post-error Decode = %v, want io.EOF", err)
		}
	})
}

// invalidCountReader returns Read counts outside [0, len(p)] with a nil
// error — the io.Reader contract violation classes. Handing such a
// reader to bufio unguarded panics (negative count trips bufio's own
// panic; an over-count drives the buffer slice out of range), so the
// reader must be wrapped before bufio ever sees it.
type invalidCountReader struct{ n func(lenP int) int }

func (r invalidCountReader) Read(p []byte) (int, error) { return r.n(len(p)), nil }

// TestRegression_ReaderInvalidReadCountNamedError pins that an io.Reader
// returning a count outside [0, len(p)] with a nil error surfaces as a
// named error from NewReader / NewAppendWriter — never a panic. The
// stdlib norm is a panic (bufio panics on negative counts by design, and
// encoding/json's Decoder slice-panics on both shapes); the named error
// here is deliberately more defensive because the count feeds buffer
// arithmetic the caller cannot recover from.
func TestRegression_ReaderInvalidReadCountNamedError(t *testing.T) {
	neg := invalidCountReader{func(int) int { return -1 }}
	over := invalidCountReader{func(lenP int) int { return lenP + 8 }}

	t.Run("negative-count/NewReader", func(t *testing.T) {
		_, err := NewReader(neg)
		if err == nil {
			t.Fatal("negative Read count accepted")
		}
		if !strings.Contains(err.Error(), "invalid count") {
			t.Errorf("want the invalid-count reject, got: %v", err)
		}
	})
	t.Run("over-count/NewReader", func(t *testing.T) {
		_, err := NewReader(over)
		if err == nil {
			t.Fatal("over-length Read count accepted")
		}
		if !strings.Contains(err.Error(), "invalid count") {
			t.Errorf("want the invalid-count reject, got: %v", err)
		}
	})
	t.Run("negative-count/NewAppendWriter", func(t *testing.T) {
		_, err := NewAppendWriter(lyingRWS{invalidCountReader{func(int) int { return -1 }}})
		if err == nil {
			t.Fatal("negative Read count accepted")
		}
		if !strings.Contains(err.Error(), "invalid count") {
			t.Errorf("want the invalid-count reject, got: %v", err)
		}
	})
	// A contract-abiding reader is untouched by the guard: a normal file
	// round-trips through the same construction path.
	t.Run("contract-abiding-control", func(t *testing.T) {
		file := writeContractOCF(t, &contractCodec{}, 9)
		r := mustNewReader(t, bytes.NewReader(file), WithCodec(&contractCodec{}))
		var v map[string]any
		if err := r.Decode(&v); err != nil || v["x"].(int64) != 9 {
			t.Fatalf("control decode: %v %v", err, v)
		}
	})
}

// prefixThenZeroReader serves a byte prefix, then returns (0, nil)
// forever — the io.Reader shape the docs discourage but permit.
type prefixThenZeroReader struct {
	data []byte
	pos  int
}

func (z *prefixThenZeroReader) Read(p []byte) (int, error) {
	if z.pos < len(z.data) {
		n := copy(p, z.data[z.pos:])
		z.pos += n
		return n, nil
	}
	return 0, nil
}

// TestRegression_ZeroCountReaderNoLivelock pins that a reader stuck
// returning (0, nil) surfaces io.ErrNoProgress instead of spinning
// forever. bufio's fill guards its own small-read path after 100 empty
// reads, but its large-read path hands the underlying Read result
// through verbatim — and the block-data io.ReadFull loops on (0, nil)
// indefinitely without the wrapper applying the same discipline
// uniformly. The mid-header cut (bufio's small-read path) and the
// mid-block cut (the direct path) must both end with the named error.
func TestRegression_ZeroCountReaderNoLivelock(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, contractSchema)
	// A block bigger than bufio's buffer so the block read takes the
	// large-read direct path.
	if err := w.Encode(map[string]any{"x": int64(1)}); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)
	full := buf.Bytes()

	for _, cut := range []struct {
		name string
		n    int
	}{
		{"mid-header", 5},
		{"mid-block", len(full) - 3},
	} {
		t.Run(cut.name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() {
				r, err := NewReader(&prefixThenZeroReader{data: full[:cut.n]})
				if err != nil {
					done <- err
					return
				}
				var v map[string]any
				done <- r.Decode(&v)
			}()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("truncated-then-zero stream read cleanly")
				}
				if !errors.Is(err, io.ErrNoProgress) {
					t.Errorf("want io.ErrNoProgress in the chain, got: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("(0, nil) reader livelocks: still spinning after 5s")
			}
		})
	}
}

// eofErrCodec's Decompress fails with bare io.EOF — the sentinel
// Reader.Decode reserves for a clean end of file.
type eofErrCodec struct{ contractCodec }

func (e *eofErrCodec) Decompress(src []byte) ([]byte, error) { return nil, io.EOF }

// TestRegression_UserErrorEOFNeverCleanEnd pins Reader.Decode's
// documented io.EOF exclusivity against USER-originated errors: a
// codec Decompress error of bare io.EOF, or a CustomType decode
// callback returning bare io.EOF, must NOT surface as an error
// matching io.EOF (a `for rd.Decode(&v) != io.EOF`-style loop would
// treat the failure as a clean end and silently drop the rest of the
// file). Both normalize to a chain matching io.ErrUnexpectedEOF, the
// same normalization every truncation path applies.
func TestRegression_UserErrorEOFNeverCleanEnd(t *testing.T) {
	t.Run("codec-decompress-eof", func(t *testing.T) {
		file := writeContractOCF(t, &contractCodec{}, 1, 2)
		r := mustNewReader(t, bytes.NewReader(file), WithCodec(&eofErrCodec{}))
		var v map[string]any
		derr := r.Decode(&v)
		if derr == nil {
			t.Fatal("failing codec read cleanly")
		}
		if errors.Is(derr, io.EOF) {
			t.Errorf("user codec's io.EOF surfaced as a clean-end match: %v", derr)
		}
		if !errors.Is(derr, io.ErrUnexpectedEOF) {
			t.Errorf("want the ErrUnexpectedEOF normalization, got: %v", derr)
		}
	})
	t.Run("custom-decode-eof", func(t *testing.T) {
		ct := avro.CustomType{AvroType: "long", Decode: func(v any, sn *avro.SchemaNode) (any, error) {
			return nil, io.EOF
		}}
		s := mustParse(t, `{"type":"record","name":"CR2","fields":[{"name":"x","type":"long"}]}`, ct)
		var buf bytes.Buffer
		w := mustNewWriter(t, &buf, s)
		if err := w.Encode(map[string]any{"x": int64(1)}); err != nil {
			t.Fatal(err)
		}
		mustClose(t, w)
		r := mustNewReader(t, bytes.NewReader(buf.Bytes()), WithSchemaOpts(ct))
		var v map[string]any
		derr := r.Decode(&v)
		if derr == nil {
			t.Fatal("failing custom decoder read cleanly")
		}
		if errors.Is(derr, io.EOF) {
			t.Errorf("custom decoder's io.EOF surfaced as a clean-end match: %v", derr)
		}
		if !errors.Is(derr, io.ErrUnexpectedEOF) {
			t.Errorf("want the ErrUnexpectedEOF normalization, got: %v", derr)
		}
	})
}

// TestRegression_AliasingCodecOwnedValues pins that decoded []byte and
// string values own their storage: a codec whose Decompress returns
// its own REUSED buffer must not let block N+1's decompression rewrite
// values decoded from block N (setBytesValue / the slab copy out of
// the wire window).
func TestRegression_AliasingCodecOwnedValues(t *testing.T) {
	s := avro.MustParse(`{"type":"record","name":"BR","fields":[
		{"name":"b","type":"bytes"},{"name":"s","type":"string"}]}`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	if err := w.Encode(map[string]any{"b": []byte("first!"), "s": "FIRST"}); err != nil {
		t.Fatal(err)
	}
	mustFlush(t, w)
	if err := w.Encode(map[string]any{"b": []byte("second"), "s": "SECON"}); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)
	type target struct {
		B []byte `avro:"b"`
		S string `avro:"s"`
	}
	r := mustNewReader(t, bytes.NewReader(buf.Bytes()), WithCodec(&reusedBufferCodec{}))
	var one, two target
	if err := r.Decode(&one); err != nil {
		t.Fatal(err)
	}
	if err := r.Decode(&two); err != nil {
		t.Fatal(err)
	}
	if string(one.B) != "first!" || one.S != "FIRST" {
		t.Errorf("block-2 decompression into the codec's reused buffer corrupted block-1 values: b=%q s=%q", one.B, one.S)
	}
	if string(two.B) != "second" || two.S != "SECON" {
		t.Errorf("block-2 values wrong: b=%q s=%q", two.B, two.S)
	}
}

// reusedBufferCodec claims the "null" codec name and decompresses into
// one buffer it reuses across calls — legal, and the sharpest aliasing
// shape a codec can hand back.
type reusedBufferCodec struct {
	contractCodec
	buf []byte
}

func (c *reusedBufferCodec) Name() string { return "null" }
func (c *reusedBufferCodec) Decompress(src []byte) ([]byte, error) {
	c.buf = append(c.buf[:0], src...)
	return c.buf, nil
}

type lyingRWS struct{ r io.Reader }

func (l lyingRWS) Read(p []byte) (int, error)                 { return l.r.Read(p) }
func (lyingRWS) Write(p []byte) (int, error)                  { return len(p), nil }
func (lyingRWS) Seek(offset int64, whence int) (int64, error) { return 0, nil }

// shortCountWriter drops the final byte of every write and reports
// success — the io.Writer contract violation (n < len(p) with nil err).
type shortCountWriter struct{ buf *bytes.Buffer }

func (w shortCountWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	n, _ := w.buf.Write(p[:len(p)-1])
	return n, nil
}

// overCountWriter reports having written more bytes than it was given.
type overCountWriter struct{ buf *bytes.Buffer }

func (w overCountWriter) Write(p []byte) (int, error) {
	n, _ := w.buf.Write(p)
	return n + 3, nil
}

// TestRegression_WriterInvalidWriteCountNamedError pins that an
// io.Writer violating its contract (returning n != len(p) with a nil
// error) yields a named error instead of a silently truncated file. A
// short count maps to io.ErrShortWrite — the same discipline io.Copy
// and bufio.Writer apply — and a count outside [0, len(p)] is named as
// invalid; encoding/json's Encoder by contrast trusts the writer and
// silently drops the shortfall.
func TestRegression_WriterInvalidWriteCountNamedError(t *testing.T) {
	t.Run("short-count-header", func(t *testing.T) {
		var buf bytes.Buffer
		_, err := NewWriter(shortCountWriter{&buf}, contractSchema)
		if err == nil {
			t.Fatal("short-count header write reported success; the file is silently truncated")
		}
		if !errors.Is(err, io.ErrShortWrite) {
			t.Errorf("want io.ErrShortWrite, got: %v", err)
		}
	})
	t.Run("short-count-block", func(t *testing.T) {
		var hdr bytes.Buffer
		w := mustNewWriter(t, &hdr, contractSchema)
		if err := w.Encode(map[string]any{"x": int64(1)}); err != nil {
			t.Fatal(err)
		}
		// Repoint the flush at the violating writer: Reset flushes the
		// buffered block to the OLD destination first, so encode again
		// after repointing and flush into the violator.
		var buf bytes.Buffer
		if err := w.Reset(shortCountWriter{&buf}); err == nil {
			t.Fatal("short-count header write on Reset reported success")
		} else if !errors.Is(err, io.ErrShortWrite) {
			t.Errorf("want io.ErrShortWrite, got: %v", err)
		}
	})
	t.Run("over-count", func(t *testing.T) {
		var buf bytes.Buffer
		_, err := NewWriter(overCountWriter{&buf}, contractSchema)
		if err == nil {
			t.Fatal("over-count write reported success")
		}
		if errors.Is(err, io.ErrShortWrite) {
			t.Errorf("over-count is not a short write: %v", err)
		}
		if !strings.Contains(err.Error(), "invalid count") {
			t.Errorf("want the invalid-count reject, got: %v", err)
		}
	})
}

// TestMatrix_ReaderSchemaFuncReturnShapes: a (nil, nil) return means "no
// reader schema" — the file's writer schema is used as-is; a returned
// error aborts NewReader with the error's identity preserved, and any
// schema returned alongside it is discarded.
func TestMatrix_ReaderSchemaFuncReturnShapes(t *testing.T) {
	file := writeContractOCF(t, &contractCodec{}, 7)
	t.Run("nil-nil", func(t *testing.T) {
		r, err := NewReader(bytes.NewReader(file), WithCodec(&contractCodec{}),
			WithReaderSchemaFunc(func(rd *Reader) (*avro.Schema, error) { return nil, nil }))
		if err != nil {
			t.Fatalf("nil-nil schema func rejected: %v", err)
		}
		var v map[string]any
		if err := r.Decode(&v); err != nil || v["x"].(int64) != 7 {
			t.Fatalf("writer-schema decode after nil-nil: %v %v", err, v)
		}
	})
	t.Run("error-with-usable-schema", func(t *testing.T) {
		_, err := NewReader(bytes.NewReader(file), WithCodec(&contractCodec{}),
			WithReaderSchemaFunc(func(rd *Reader) (*avro.Schema, error) {
				return avro.MustParse(`"long"`), errors.New("fn boom")
			}))
		if err == nil || !strings.Contains(err.Error(), "fn boom") {
			t.Fatalf("schema-func error not surfaced with identity: %v", err)
		}
	})
}

// ---------- codec_nil_spelling_test.go ----------

// ---------------------------------------------------------------------------
// Nil has two spellings, and an option is only "supported" if BOTH of them
// behave the same on EVERY constructor.
//
// WithCodec takes an interface, and a caller writes nil two ways without meaning
// anything different: WithCodec(nil), and WithCodec(newMyCodec()) where the
// constructor's concrete return type yields a non-nil interface holding a nil
// *myCodec, which passes c != nil.
//
// A method call on either is a crash, so the library must not make one. The
// severity a CALLER sees is not uniform: a Close with a pointer receiver dies on
// a nil receiver, while a nil-safe Close returns cleanly and the wrong call is
// INVISIBLE — so a matrix built only from dereferencing codecs would measure
// "does it crash" and pass vacuously. The nilSafe rows make the mistake
// observable WITHOUT a crash: they count the calls the library had no business
// making.
// ---------------------------------------------------------------------------

// nilSafeCloses counts Close calls that arrived on a NIL *nilSafeCodec. A nil
// receiver has no field to record into, so the counter is package level. This is
// the whole point of the type: it converts "the library called Close on a codec
// it should have ignored" from a segfault into a number, so the cell reports the
// defect on its own instead of depending on the caller's Close being fragile.
var nilSafeCloses int

type nilSafeCodec struct {
	name   string
	closes *int
}

func (c *nilSafeCodec) Name() string {
	if c == nil {
		return "nil-safe"
	}
	return c.name
}
func (c *nilSafeCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (c *nilSafeCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (c *nilSafeCodec) Close() error {
	if c == nil {
		nilSafeCloses++
		return nil
	}
	*c.closes++
	return nil
}

// derefCodec is the common shape: a pointer receiver whose Close touches the
// receiver. Calling Close on a nil one is a segfault, which is what a caller
// hitting the typed-nil trap actually experiences.
type derefCodec struct {
	name   string
	closes *int
}

func (c *derefCodec) Name() string                          { return c.name }
func (c *derefCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (c *derefCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (c *derefCodec) Close() error                          { *c.closes++; return nil }

// mapKindCodec is nil in a kind that is neither an interface nor a pointer, and
// is additionally UNCOMPARABLE — the two properties the release path's repeat
// bookkeeping branches on, since an uncomparable codec cannot be a map key and
// is therefore tracked by TYPE instead. That makes a nil and a REAL codec of
// this one type the combination where mis-recording the nil is destructive: the
// nil poisons the seen-type list and the real codec of the same type then reads
// as a repeat and is silently never closed.
//
// Close counts go to package-level counters because a nil receiver has no field
// to record into, and because the nil and non-nil values here are the same type.
type mapKindCodec map[string]string

var mapKindCloses, mapKindNilCloses int

func (c mapKindCodec) Name() string {
	if c == nil {
		return "nil-map"
	}
	return c["name"]
}
func (c mapKindCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (c mapKindCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (c mapKindCodec) Close() error {
	if c == nil {
		mapKindNilCloses++
	} else {
		mapKindCloses++
	}
	return nil
}

// nilSpelling is one way of writing a nil codec, plus the REAL codec the cell
// pairs it with and how to read that real codec's close count.
type nilSpelling struct {
	label string
	make  func() Codec
	// observable is true when an unwanted Close on the NIL is COUNTABLE rather
	// than fatal. Those rows carry the extra assertion; the fatal ones can only
	// assert that no crash happened.
	observable bool
	// nilCloses reads the count of Close calls that landed on the nil. Only
	// meaningful when observable.
	nilCloses func() int
	// makeReal builds the non-nil codec in the SAME TYPE FAMILY as the nil.
	// Pairing them is what reaches the uncomparable-type bookkeeping: a cell
	// whose real codec is always a comparable pointer cannot tell whether the
	// nil was wrongly recorded as a seen TYPE, because no real codec shares
	// that type. realCloses reads its count.
	makeReal   func(name string) Codec
	realCloses func() int
	reset      func()
}

func nilSpellings() []nilSpelling {
	derefCloses := 0
	realDeref := func(name string) Codec { return &derefCodec{name: name, closes: &derefCloses} }
	derefCount := func() int { return derefCloses }
	resetDeref := func() { derefCloses = 0 }

	return []nilSpelling{
		// The spelling the package already pinned as working, on the one
		// constructor that happened to choose by position.
		{label: "untypedNil", make: func() Codec { return nil },
			makeReal: realDeref, realCloses: derefCount, reset: resetDeref},
		// The typed-nil trap, in the receiver shape that crashes.
		{label: "typedNilDerefClose", make: func() Codec { return (*derefCodec)(nil) },
			makeReal: realDeref, realCloses: derefCount, reset: resetDeref},
		// The same trap with a nil-safe Close: no crash either way, so this
		// row's verdict rests on the nil's own count, not on survival.
		{label: "typedNilSafeClose", make: func() Codec { return (*nilSafeCodec)(nil) },
			observable: true, nilCloses: func() int { return nilSafeCloses },
			makeReal: func(name string) Codec {
				return &nilSafeCodec{name: name, closes: &nilSafeRealCloses}
			},
			realCloses: func() int { return nilSafeRealCloses },
			reset:      func() { nilSafeCloses, nilSafeRealCloses = 0, 0 }},
		// Nil in a non-pointer, uncomparable kind, paired with a REAL codec of
		// that same uncomparable type.
		{label: "typedNilMapKind", make: func() Codec { return mapKindCodec(nil) },
			observable: true, nilCloses: func() int { return mapKindNilCloses },
			makeReal:   func(name string) Codec { return mapKindCodec{"name": name} },
			realCloses: func() int { return mapKindCloses },
			reset:      func() { mapKindCloses, mapKindNilCloses = 0, 0 }},
	}
}

var nilSafeRealCloses int

// offerLayout places the nil offer among the real ones. Position is a real axis
// here: the writer chooses by POSITION (last wins) and the readers choose by
// NAME, so the same layout puts the nil in a different role on each.
type offerLayout struct {
	label string
	// nilAt lists the offer positions that get the nil spelling; every other
	// position gets the real codec.
	nilAt []int
	total int
}

func offerLayouts() []offerLayout {
	return []offerLayout{
		{label: "nilOnly", nilAt: []int{0}, total: 1},
		{label: "nilTwice", nilAt: []int{0, 1}, total: 2},
		{label: "nilFirstRealLast", nilAt: []int{0}, total: 2},
		{label: "realFirstNilLast", nilAt: []int{1}, total: 2},
		{label: "nilBetweenReals", nilAt: []int{1}, total: 3},
	}
}

// ctorRunner builds one constructor's inputs and runs it. headerCodec is the
// name written into the file the reader-side constructors open, which is what
// decides whether the real offer is adopted or declined there.
type ctorRunner struct {
	name string
	// usesHeader is true when the file's avro.codec decides adoption. The two
	// reader-side constructors read it; NewWriter chooses by position and never
	// sees one, which is why the same offer layout means different things to
	// them and why the layout axis is not redundant with the spelling axis.
	usesHeader bool
	run        func(t *testing.T, headerCodec string, opts []Opt) (io.Closer, error)
}

// TestMatrix_NilCodecOfferIgnoredEverySpelling is the class eliminator, over
// spelling x constructor x offer layout x reader-side adoption.
//
// spelling: the axis the defect turned on and the one the report was NOT written
// in — the reported instance was a typed nil, and the untyped nil was already
// pinned as working, on one constructor.
// constructor: DERIVED, not listed — the set comes from
// codecOwningConstructors' go/ast walk, and the cross-check at the end fails if
// the source grows a constructor this matrix does not drive.
// offer layout: where the nil sits. The writer adopts by position and the
// readers adopt by name, so "the nil is last" is a superseded offer on one and
// an ordinary declined offer on the others; an all-nil layout must fall through
// to the built-in on all three.
// reader-side adoption: the header names the real codec or it does not.
//
// The oracle is not read off current behavior. WithCodec documents that a nil
// offer behaves as though it were not written, and Codec.Close is documented to
// release the codec's resources — so the two asserted facts are that the
// constructor produces a usable object and that the library never calls Close on
// a codec the caller did not effectively supply. The real codec's own count is
// the CONTROL: it must still be closed exactly once, so a fix that ignored
// everything would fail here.
func TestMatrix_NilCodecOfferIgnoredEverySpelling(t *testing.T) {
	var drivenCtors []string

	for _, sp := range nilSpellings() {
		for _, layout := range offerLayouts() {
			for _, headerAdopts := range []bool{true, false} {
				for _, ctor := range nilMatrixConstructors() {
					if !slices.Contains(drivenCtors, ctor.name) {
						drivenCtors = append(drivenCtors, ctor.name)
					}
					cell := fmt.Sprintf("%s/%s/%s/headerAdopts=%v",
						sp.label, ctor.name, layout.label, headerAdopts)
					t.Run(cell, func(t *testing.T) {
						runNilSpellingCell(t, sp, layout, headerAdopts, ctor)
					})
				}
			}
		}
	}

	// The constructor axis is the derived set, or this matrix is a list.
	derived := codecOwningConstructors(t)
	slices.Sort(drivenCtors)
	if !slices.Equal(derived, drivenCtors) {
		t.Errorf("matrix drives %v but the source derives %v; a codec-owning constructor "+
			"is exempt from the nil-spelling rule", drivenCtors, derived)
	}
}

func runNilSpellingCell(t *testing.T, sp nilSpelling, layout offerLayout, headerAdopts bool, ctor ctorRunner) {
	t.Helper()

	const realName = "real-codec"
	headerCodec := realName
	if !headerAdopts {
		headerCodec = "null"
	}

	sp.reset()
	real := sp.makeReal(realName)

	var opts []Opt
	nilCount := 0
	for i := range layout.total {
		if slices.Contains(layout.nilAt, i) {
			opts = append(opts, WithCodec(sp.make()))
			nilCount++
			continue
		}
		opts = append(opts, WithCodec(real))
	}
	allNil := nilCount == layout.total

	// The defect is a segfault, so the cell must catch it and REPORT rather
	// than let it kill the binary: a panicking test process cannot be told
	// apart from another panicking test process, and every neuter below has to
	// produce a red set that names its own mechanism.
	var obj io.Closer
	var err error
	func() {
		defer func() {
			if p := recover(); p != nil {
				t.Fatalf("%s panicked on a nil codec offer: %v", ctor.name, p)
			}
		}()
		obj, err = ctor.run(t, headerCodec, opts)
	}()

	// One arm legitimately fails, and it is not a nil-handling failure: every
	// offer is nil AND the file names a codec no built-in provides, so after
	// ignoring the nils there is nothing left that can decompress it. Ignoring a
	// nil offer means behaving as though it were not written, and not writing
	// WithCodec at all against such a file is an unknown-codec error. The nil
	// must not turn that diagnosis into a crash or into a silent success.
	if allNil && ctor.usesHeader && headerAdopts {
		if err == nil {
			t.Fatalf("%s accepted a file naming %q with no codec supplying it", ctor.name, realName)
		}
		if !strings.Contains(err.Error(), "unknown codec") {
			t.Errorf("%s: error %q does not name the unknown codec", ctor.name, err)
		}
		if sp.observable && sp.nilCloses() != 0 {
			t.Errorf("Close called %d time(s) on a nil codec on the error path", sp.nilCloses())
		}
		return
	}

	if err != nil {
		t.Fatalf("%s returned an error for an offer set whose only defect is a nil: %v", ctor.name, err)
	}
	if obj == nil {
		t.Fatalf("%s returned no object and no error", ctor.name)
	}

	// The library must never have called Close on the nil. Only the nil-safe
	// spelling can observe this without dying, which is exactly why it is here.
	if sp.observable && sp.nilCloses() != 0 {
		t.Errorf("Close called %d time(s) on a nil codec the caller never effectively supplied",
			sp.nilCloses())
	}

	// CONTROL: a real offer alongside a nil is still governed by the ordinary
	// rule — closed exactly once by the time the caller is done. Without this a
	// fix that simply skipped every codec would pass every cell above.
	if !allNil {
		mustClose(t, obj)
		if got := sp.realCloses(); got != 1 {
			t.Errorf("real codec closed %d times, want exactly 1 "+
				"(the nil offer must not change the real one's fate)", got)
		}
		return
	}

	// All offers nil: the constructor must behave as though WithCodec were
	// never written, which means falling through to the built-in the header
	// names rather than adopting nothing and failing.
	if err := obj.Close(); err != nil {
		t.Fatalf("Close after an all-nil offer set: %v", err)
	}
	if sp.observable && sp.nilCloses() != 0 {
		t.Errorf("Close called %d time(s) on a nil codec after the object was closed",
			sp.nilCloses())
	}
}

// TestRegression_NilCodecOfferUnknownCodecRatherThanPanic pins the one arm where
// a nil offer still changes the outcome: it is the ONLY offer, and the file
// names a codec no built-in provides. Nothing can decompress that file, so the
// constructor must say so — an unknown-codec error, not a crash and not a
// silent read of nothing.
func TestRegression_NilCodecOfferUnknownCodecRatherThanPanic(t *testing.T) {
	for _, sp := range nilSpellings() {
		t.Run(sp.label, func(t *testing.T) {
			data := ocfWithHeaderCodec(t, "no-such-codec")
			var err error
			func() {
				defer func() {
					if p := recover(); p != nil {
						t.Fatalf("NewReader panicked on a nil offer: %v", p)
					}
				}()
				_, err = NewReader(bytes.NewReader(data), WithCodec(sp.make()))
			}()
			if err == nil {
				t.Fatal("NewReader accepted a file whose codec nothing supplies")
			}
			if !strings.Contains(err.Error(), "unknown codec") {
				t.Errorf("error %q does not name the unknown codec", err)
			}
		})
	}
}

// TestInvariant_NilCodecAskedThroughOnePredicate is the source-level half: the
// behavioral matrix proves the constructors agree TODAY, this keeps them
// agreeing by construction. Derived, not listed: every non-test function that
// REACHES INTO a []Codec — ranging over one or indexing one — is handling
// caller-supplied offers and must consult isNilCodec, and the set comes from the
// declared TYPE, so a function added later is caught by taking offers rather
// than by being remembered.
//
// Indexing counts, not just ranging: the site whose missing check split the
// constructors was NewWriter's adoption, which walks the offers backwards by
// index, so a derivation seeing only `range` would have reported full coverage
// of the exact class it exists to catch. Functions that merely APPEND to such a
// slice and hand it on are correctly outside. Scope is decided by the TYPE, not
// by a name, so a function handed a single Codec is outside it.
func TestInvariant_NilCodecAskedThroughOnePredicate(t *testing.T) {
	files, names := parsePackageFiles(t, false)

	var ranging []string
	for fi, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			// Identifiers in this function declared as []Codec, whether as a
			// parameter or as a local var.
			slices := map[string]bool{}
			collect := func(fl *ast.FieldList) {
				if fl == nil {
					return
				}
				for _, fld := range fl.List {
					if isCodecSliceType(fld.Type) {
						for _, n := range fld.Names {
							slices[n.Name] = true
						}
					}
				}
			}
			collect(fd.Type.Params)
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				vs, ok := n.(*ast.ValueSpec)
				if ok && isCodecSliceType(vs.Type) {
					for _, id := range vs.Names {
						slices[id.Name] = true
					}
				}
				return true
			})
			if len(slices) == 0 {
				continue
			}

			var reachesIntoOffers bool
			var asksPredicate bool
			var comparesToNil []string
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.Ident:
					if x.Name == "isNilCodec" {
						asksPredicate = true
					}
				case *ast.RangeStmt:
					if id, ok := x.X.(*ast.Ident); ok && slices[id.Name] {
						reachesIntoOffers = true
						if v, ok := x.Value.(*ast.Ident); ok && v.Name != "_" {
							comparesToNil = append(comparesToNil,
								nilComparisons(x.Body, v.Name)...)
						}
					}
				case *ast.IndexExpr:
					if id, ok := x.X.(*ast.Ident); ok && slices[id.Name] {
						reachesIntoOffers = true
					}
				}
				return true
			})
			if !reachesIntoOffers {
				continue
			}
			ranging = append(ranging, fd.Name.Name)
			if !asksPredicate {
				t.Errorf("%s (%s) reaches into caller-supplied codecs without asking isNilCodec",
					fd.Name.Name, names[fi])
			}
			for _, c := range comparesToNil {
				t.Errorf("%s (%s) tests a supplied codec with %s; that reads only the "+
					"interface spelling of nil — ask isNilCodec", fd.Name.Name, names[fi], c)
			}
		}
	}

	// Fails the other way too: if the derivation stops finding the sites, the
	// guard has gone blind rather than the package having gotten simpler.
	slices2 := append([]string(nil), ranging...)
	slices.Sort(slices2)
	want := []string{"NewWriter", "releaseUnadopted", "resolveCodec"}
	if !slices.Equal(slices2, want) {
		t.Errorf("derivation found %v reaching into caller-supplied codecs, want %v; "+
			"a site was added or the walk stopped seeing them", slices2, want)
	}
}

// chanCodec and funcCodec exist so the predicate's nilable-kind list is driven
// rather than read. A Codec is any type with the four methods, and Go lets that
// be a channel or a func as readily as a pointer; each is nil-able and each
// would crash the same way.
type chanCodec chan int

func (c chanCodec) Name() string                          { return "chan" }
func (c chanCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (c chanCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (c chanCodec) Close() error                          { return nil }

type funcCodec func()

func (c funcCodec) Name() string                          { return "func" }
func (c funcCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (c funcCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (c funcCodec) Close() error                          { return nil }

type sliceCodec []byte

func (c sliceCodec) Name() string                          { return "slice" }
func (c sliceCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (c sliceCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (c sliceCodec) Close() error                          { return nil }

// TestIsNilCodecAnswersEveryNilableKind drives the predicate directly across
// every reflect kind a Codec implementation can have. The switch inside it is a
// list of kinds, and a list is only as good as the cases someone thought of, so
// the nil and non-nil value of each kind are both asked here — which is what
// makes a missing case fail rather than merely be absent. It also EXECUTES the
// claim the predicate's comment makes about reflect.Interface: reflect.ValueOf
// takes an any and resolves it to the dynamic value, so a Codec interface value
// never presents as Kind Interface.
func TestIsNilCodecAnswersEveryNilableKind(t *testing.T) {
	nilCases := []struct {
		kind string
		c    Codec
	}{
		{"untyped interface", nil},
		{"pointer", (*derefCodec)(nil)},
		{"map", mapKindCodec(nil)},
		{"chan", chanCodec(nil)},
		{"func", funcCodec(nil)},
		{"slice", sliceCodec(nil)},
	}
	for _, tc := range nilCases {
		if !isNilCodec(tc.c) {
			t.Errorf("isNilCodec(nil %s) = false; a method call on it would crash", tc.kind)
		}
	}

	nonNil := []struct {
		kind string
		c    Codec
	}{
		{"pointer", &derefCodec{name: "p", closes: new(int)}},
		{"map", mapKindCodec{"name": "m"}},
		{"chan", chanCodec(make(chan int))},
		{"func", funcCodec(func() {})},
		{"slice", sliceCodec{}},
		{"struct (never nilable)", nullCodec{}},
		{"struct with fields", deflateCodec{level: 1}},
	}
	for _, tc := range nonNil {
		if isNilCodec(tc.c) {
			t.Errorf("isNilCodec(non-nil %s) = true; a usable codec would be silently ignored", tc.kind)
		}
	}

	// The omitted-kind claim, executed.
	for _, tc := range append(nilCases[1:], nonNil...) {
		if k := reflect.ValueOf(tc.c).Kind(); k == reflect.Interface {
			t.Errorf("a Codec holding %s presented as Kind Interface; the predicate's "+
				"switch omits that case on the grounds it cannot happen", tc.kind)
		}
	}
}

func isCodecSliceType(e ast.Expr) bool {
	at, ok := e.(*ast.ArrayType)
	if !ok || at.Len != nil {
		return false
	}
	id, ok := at.Elt.(*ast.Ident)
	return ok && id.Name == "Codec"
}

// nilComparisons returns the source of every `name == nil` / `name != nil` in
// body, which is the exact test that misses a typed nil.
func nilComparisons(body *ast.BlockStmt, name string) []string {
	var out []string
	ast.Inspect(body, func(n ast.Node) bool {
		be, ok := n.(*ast.BinaryExpr)
		if !ok {
			return true
		}
		op := be.Op.String()
		if op != "==" && op != "!=" {
			return true
		}
		x, xok := be.X.(*ast.Ident)
		y, yok := be.Y.(*ast.Ident)
		if xok && yok && x.Name == name && y.Name == "nil" {
			out = append(out, name+" "+op+" nil")
		}
		return true
	})
	return out
}

// ocfWithHeaderCodec writes a one-datum OCF whose avro.codec metadata names
// codec, without needing an implementation of it: the file is produced with the
// null codec and the header rewritten, which is what a foreign producer's file
// looks like to this package.
func ocfWithHeaderCodec(t *testing.T, codec string) []byte {
	t.Helper()
	var buf bytes.Buffer
	c := &derefCodec{name: codec, closes: new(int)}
	w := mustNewWriter(t, &buf, avro.MustParse(`"long"`), WithCodec(c))
	if err := w.Encode(int64(1)); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	mustClose(t, w)
	return buf.Bytes()
}

func nilMatrixConstructors() []ctorRunner {
	schema := avro.MustParse(`"long"`)
	return []ctorRunner{
		{
			name: "NewWriter",
			run: func(t *testing.T, _ string, opts []Opt) (io.Closer, error) {
				var buf bytes.Buffer
				wopts := make([]WriterOpt, len(opts))
				for i, o := range opts {
					wopts[i] = o.(WriterOpt)
				}
				return NewWriter(&buf, schema, wopts...)
			},
		},
		{
			name:       "NewReader",
			usesHeader: true,
			run: func(t *testing.T, headerCodec string, opts []Opt) (io.Closer, error) {
				data := ocfWithHeaderCodec(t, headerCodec)
				ropts := make([]ReaderOpt, len(opts))
				for i, o := range opts {
					ropts[i] = o.(ReaderOpt)
				}
				return NewReader(bytes.NewReader(data), ropts...)
			},
		},
		{
			name:       "NewAppendWriter",
			usesHeader: true,
			run: func(t *testing.T, headerCodec string, opts []Opt) (io.Closer, error) {
				data := ocfWithHeaderCodec(t, headerCodec)
				f, err := os.CreateTemp(t.TempDir(), "ocf")
				if err != nil {
					t.Fatalf("temp file: %v", err)
				}
				t.Cleanup(func() { f.Close() })
				if _, err := f.Write(data); err != nil {
					t.Fatalf("writing fixture: %v", err)
				}
				if _, err := f.Seek(0, 0); err != nil {
					t.Fatalf("seek: %v", err)
				}
				wopts := make([]WriterOpt, len(opts))
				for i, o := range opts {
					wopts[i] = o.(WriterOpt)
				}
				return NewAppendWriter(f, wopts...)
			},
		},
	}
}

// ---------- codec_ownership_test.go ----------

// A constructor that adopts the caller's Codec owns it from that point on: a
// failure hands back no Writer or Reader, so there is nothing left for the
// caller to Close, and when the codec was built inline in the call — the form
// the doc example uses — the caller has no handle either.
//
// The rule has two halves, and a codec reaches a constructor without an owner
// under either. The codec a constructor ADOPTS and then fails on is the first.
// The codec it is OFFERED and declines is the second: at most one offer is
// taken, and the constructor then SUCCEEDS, so nothing signals the caller that
// their codec went unused. Both are the same argument about ownership, so a row
// covering one and not the other leaves a member unguarded. The set of such
// constructors is DERIVED from source rather than listed.
type codecOwnerRow struct {
	// ctor is the constructor as declared in the package source.
	ctor string
	// coveredBy names the tests that assert release of the ADOPTED codec on
	// that constructor's error arms. A row whose test no longer exists fails
	// the guard.
	coveredBy []string
	// declinedCoveredBy names the tests that assert release of a supplied
	// codec the constructor did NOT adopt — the success-path half, which no
	// error-arm test can reach.
	declinedCoveredBy []string
}

var codecOwnerRows = []codecOwnerRow{
	{
		ctor:      "NewWriter",
		coveredBy: []string{"TestConstructorErrorReleasesCodec"},
		declinedCoveredBy: []string{
			"TestMatrix_SuppliedCodecClosedExactlyOnce",
			"TestRegression_OCFNewWriterReleasesSupersededCodec",
			"TestRegression_OCFNilCodecOfferIsNeverClosed",
			"TestRegression_OCFUncomparableCodecOfferReleasedOnce",
		},
	},
	{
		ctor:      "NewAppendWriter",
		coveredBy: []string{"TestConstructorErrorReleasesCodec"},
		declinedCoveredBy: []string{
			"TestMatrix_SuppliedCodecClosedExactlyOnce",
			"TestRegression_OCFAppendWriterReleasesUnmatchedCodec",
		},
	},
	{
		ctor: "NewReader",
		coveredBy: []string{
			"TestRegression_OCFNewReaderClosesCodecOnReaderSchemaFnError",
			"TestRegression_OCFNewReaderClosesCodecOnResolveError",
		},
		declinedCoveredBy: []string{
			"TestMatrix_SuppliedCodecClosedExactlyOnce",
			"TestRegression_OCFNewReaderReleasesUnmatchedCodec",
		},
	},
}

// codecOwningConstructors derives the constructor set from the package source in
// two steps, neither of which reads a name: a struct with a field of the Codec
// interface type is codec-owning, and a top-level function returning a pointer
// to such a struct alongside an error is a constructor that can fail after
// adopting one. Asking go/ast for the shape rather than matching a "New" prefix
// keeps the derivation independent of how a future constructor is spelled.
// Scope: the package's own non-test .go files — a codec-owning constructor in
// another package, or one that hands the codec to a struct built by a helper, is
// outside it.
func codecOwningConstructors(t *testing.T) []string {
	t.Helper()
	files, _ := parsePackageFiles(t, false)

	owners := map[string]bool{}
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok {
				return true
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok || st.Fields == nil {
				return true
			}
			for _, fld := range st.Fields.List {
				// Named field or embedded — both hold the codec.
				if id, ok := fld.Type.(*ast.Ident); ok && id.Name == "Codec" {
					owners[ts.Name.Name] = true
				}
			}
			return true
		})
	}
	if len(owners) == 0 {
		t.Fatal("derivation found no struct holding a Codec; the walk is broken, not the package")
	}

	var ctors []string
	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Recv != nil || fd.Type.Results == nil {
				continue
			}
			var ownsResult, errResult bool
			for _, res := range fd.Type.Results.List {
				switch rt := res.Type.(type) {
				case *ast.StarExpr:
					if id, ok := rt.X.(*ast.Ident); ok && owners[id.Name] {
						ownsResult = true
					}
				case *ast.Ident:
					if rt.Name == "error" {
						errResult = true
					}
				}
			}
			if ownsResult && errResult {
				ctors = append(ctors, fd.Name.Name)
			}
		}
	}
	slices.Sort(ctors)
	return ctors
}

// parsePackageFiles parses the package's .go files, test files or not.
func parsePackageFiles(t *testing.T, tests bool) ([]*ast.File, []string) {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading package dir: %v", err)
	}
	var files []*ast.File
	var names []string
	fset := token.NewFileSet()
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") != tests {
			continue
		}
		f, err := parser.ParseFile(fset, n, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", n, err)
		}
		files = append(files, f)
		names = append(names, n)
	}
	if len(files) == 0 {
		t.Fatalf("no source files found (tests=%v)", tests)
	}
	return files, names
}

// TestCodecOwningConstructorsAreRowed fails in both directions: a constructor
// the source grows with no row, and a row naming a constructor or a covering
// test that no longer exists. Either way the release rule would be unguarded
// for some member of the set.
func TestCodecOwningConstructorsAreRowed(t *testing.T) {
	derived := codecOwningConstructors(t)

	rowed := make(map[string]codecOwnerRow, len(codecOwnerRows))
	for _, r := range codecOwnerRows {
		if _, dup := rowed[r.ctor]; dup {
			t.Errorf("duplicate row for %s", r.ctor)
		}
		rowed[r.ctor] = r
	}

	for _, c := range derived {
		if _, ok := rowed[c]; !ok {
			t.Errorf("%s returns a codec-owning value and an error but has no row: "+
				"it must release the adopted codec on every error return, and a test must assert it", c)
		}
	}
	for _, r := range codecOwnerRows {
		if !slices.Contains(derived, r.ctor) {
			t.Errorf("row names %s, which the source no longer declares as a codec-owning constructor", r.ctor)
		}
	}

	// Every covering test must exist, so deleting a pin surfaces here rather
	// than silently leaving a member undriven.
	testFiles, _ := parsePackageFiles(t, true)
	declared := map[string]bool{}
	for _, f := range testFiles {
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Recv == nil {
				declared[fd.Name.Name] = true
			}
		}
	}
	for _, r := range codecOwnerRows {
		if len(r.coveredBy) == 0 {
			t.Errorf("row %s names no test for the adopted-codec release", r.ctor)
		}
		if len(r.declinedCoveredBy) == 0 {
			t.Errorf("row %s names no test for the declined-codec release; a constructor that "+
				"succeeds without adopting a supplied codec must still close it", r.ctor)
		}
		for _, name := range slices.Concat(r.coveredBy, r.declinedCoveredBy) {
			if !declared[name] {
				t.Errorf("row %s names covering test %s, which is not declared in this package", r.ctor, name)
			}
		}
	}
}

// TestEveryCodecOfferingConstructorReleasesUnadopted is the half of the rule a
// table cannot hold: whether the SOURCE actually routes each constructor through
// the shared release. Rows record which tests drive a constructor; this asks the
// package which constructors take codec options and which of those call
// releaseUnadopted, and reds on any that takes offers without releasing what it
// declines. Deriving it this way is what makes the guard survive a constructor
// added later. Scope it cannot see: a release extracted into a helper not named
// here, a constructor in another package, and one that hands its options to a
// collector rather than switching on them itself.
func TestEveryCodecOfferingConstructorReleasesUnadopted(t *testing.T) {
	files, _ := parsePackageFiles(t, false)
	derived := codecOwningConstructors(t)

	type facts struct{ offers, releases bool }
	got := map[string]facts{}
	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Recv != nil || !slices.Contains(derived, fd.Name.Name) {
				continue
			}
			var fa facts
			ast.Inspect(fd, func(n ast.Node) bool {
				id, ok := n.(*ast.Ident)
				if !ok {
					return true
				}
				switch id.Name {
				case "optCodec":
					fa.offers = true
				case "releaseUnadopted":
					fa.releases = true
				}
				return true
			})
			got[fd.Name.Name] = fa
		}
	}

	var offering int
	for _, c := range derived {
		fa, ok := got[c]
		if !ok {
			t.Errorf("derived constructor %s was not found again when reading bodies; the walk is broken", c)
			continue
		}
		if !fa.offers {
			continue
		}
		offering++
		if !fa.releases {
			t.Errorf("%s accepts WithCodec but never calls releaseUnadopted: a supplied codec it "+
				"declines is dropped with no owner, and the constructor succeeds so nothing tells "+
				"the caller", c)
		}
	}
	// A derivation that matched nothing would pass silently, which is the way
	// this kind of guard usually fails.
	if offering == 0 {
		t.Fatal("no derived constructor was found to accept codec options; the derivation is broken, not the package")
	}
}

// TestConstructorErrorReleasesCodec crosses constructor x error arm x option
// order. The expectation is not read off the code: a caller handed an error owns
// no closable object, so "closed exactly once" is the only state in which the
// codec's Close contract has been honored — and the success cells pin the other
// side, that a constructor returning a usable Writer must NOT have closed the
// codec it is about to use.
//
// The option-order axis is the one the arms behave differently on: a
// reserved-key rejection raised while the option loop is still running fires
// before or after WithCodec depending on where the caller wrote it, so the codec
// is adopted in one spelling and not the other. Validating after the loop makes
// both adopt, which is what makes the release uniform and observable at all.
func TestConstructorErrorReleasesCodec(t *testing.T) {
	intSchema := avro.MustParse(`"int"`)
	reserved := map[string][]byte{"avro.reserved": []byte("x")}

	// A complete null-codec OCF: the append-writer arms need a header to read,
	// and its absent avro.codec key resolves to the name the observer answers.
	var valid bytes.Buffer
	vw, err := NewWriter(&valid, intSchema)
	if err != nil {
		t.Fatalf("building fixture: %v", err)
	}
	if err := vw.Encode(int32(1)); err != nil {
		t.Fatalf("building fixture: %v", err)
	}
	if err := vw.Close(); err != nil {
		t.Fatalf("building fixture: %v", err)
	}

	// failWrites(0) fails every write, so the header write fails.
	failWrites := func(n int) *failAfterNWrites { return &failAfterNWrites{n: n} }

	cells := []struct {
		ctor string
		arm  string
		// order describes where WithCodec sits among the options.
		order string
		// wantErr is false for the success cells (the boundary that must
		// still pass), true for the failure cells.
		wantErr bool
		run     func(t *testing.T, c *leakDetectCodec) (*Writer, error)
	}{
		{
			ctor: "NewWriter", arm: "header-write", order: "codec-first", wantErr: true,
			run: func(t *testing.T, c *leakDetectCodec) (*Writer, error) {
				return NewWriter(failWrites(0), intSchema, WithCodec(c))
			},
		},
		{
			ctor: "NewWriter", arm: "sync-marker", order: "codec-only", wantErr: true,
			run: func(t *testing.T, c *leakDetectCodec) (*Writer, error) {
				orig := randRead
				randRead = func([]byte) (int, error) { return 0, errors.New("synthetic rand failure") }
				defer func() { randRead = orig }()
				return NewWriter(&bytes.Buffer{}, intSchema, WithCodec(c))
			},
		},
		{
			ctor: "NewWriter", arm: "reserved-metadata-key", order: "codec-first", wantErr: true,
			run: func(t *testing.T, c *leakDetectCodec) (*Writer, error) {
				return NewWriter(&bytes.Buffer{}, intSchema, WithCodec(c), WithMetadata(reserved))
			},
		},
		{
			// The order-swapped twin of the cell above. Rejecting inside the
			// option loop leaves this spelling with an un-adopted codec, which
			// looks identical to a leak from outside; rejecting after the loop
			// gives both spellings the same release.
			ctor: "NewWriter", arm: "reserved-metadata-key", order: "codec-last", wantErr: true,
			run: func(t *testing.T, c *leakDetectCodec) (*Writer, error) {
				return NewWriter(&bytes.Buffer{}, intSchema, WithMetadata(reserved), WithCodec(c))
			},
		},
		{
			ctor: "NewAppendWriter", arm: "seek", order: "codec-only", wantErr: true,
			run: func(t *testing.T, c *leakDetectCodec) (*Writer, error) {
				return NewAppendWriter(&failSeekRWS{data: slices.Clone(valid.Bytes())}, WithCodec(c))
			},
		},
		{
			// Boundary that must still pass: a constructor that succeeds hands
			// back a usable Writer, so the codec it is about to compress with
			// must be open.
			ctor: "NewWriter", arm: "success", order: "codec-first", wantErr: false,
			run: func(t *testing.T, c *leakDetectCodec) (*Writer, error) {
				return NewWriter(&bytes.Buffer{}, intSchema, WithCodec(c))
			},
		},
		{
			ctor: "NewAppendWriter", arm: "success", order: "codec-only", wantErr: false,
			run: func(t *testing.T, c *leakDetectCodec) (*Writer, error) {
				return NewAppendWriter(&seekBuf{data: slices.Clone(valid.Bytes())}, WithCodec(c))
			},
		},
	}

	seen := map[string]bool{}
	for _, c := range cells {
		seen[c.ctor] = true
		t.Run(c.ctor+"/"+c.arm+"/"+c.order, func(t *testing.T) {
			codec := &leakDetectCodec{name: "null"}
			w, err := c.run(t, codec)
			if c.wantErr {
				if err == nil {
					t.Fatalf("%s: expected the constructor to fail", c.ctor)
				}
				if codec.closes != 1 {
					t.Fatalf("%s failed with %v but closed the adopted codec %d times, want exactly 1: "+
						"the caller has no Writer to Close and, for an inline codec, no handle at all",
						c.ctor, err, codec.closes)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", c.ctor, err)
			}
			if codec.closes != 0 {
				t.Fatalf("%s succeeded but already closed the codec %d times; the Writer still needs it",
					c.ctor, codec.closes)
			}
			mustClose(t, w)
			if codec.closes != 1 {
				t.Fatalf("%s: Writer.Close closed the codec %d times, want exactly 1", c.ctor, codec.closes)
			}
		})
	}

	// Keep the cell set and the derived constructor set from drifting apart:
	// every constructor this test claims to cover must still be one.
	derived := codecOwningConstructors(t)
	for ctor := range seen {
		if !slices.Contains(derived, ctor) {
			t.Errorf("cells drive %s, which is no longer a codec-owning constructor", ctor)
		}
	}
}

// suppliedCodec is one WithCodec argument in a matrix cell, paired with whether
// the constructor is expected to take the offer.
type suppliedCodec struct {
	name string
	// adopted is the disposition under test: true when this codec's Name
	// matches the file header's avro.codec (reader side) or it is the last
	// WithCodec written (writer side), false when the constructor declines it.
	adopted bool
	// alias, when non-zero, means this entry is the SAME codec object as the
	// entry that many positions later: one codec offered from two positions,
	// rather than two codecs that merely answer the same name. Position and
	// identity disagree here, which is the whole point of the cell.
	alias int
	// wrapNopCloser offers the codec through NopCloser — the form WithCodec's
	// doc points a caller sharing one codec at.
	wrapNopCloser bool
	codec         *leakDetectCodec
}

// TestMatrix_SuppliedCodecClosedExactlyOnce crosses the axes a supplied codec's
// fate can turn on: constructor x disposition x outcome x offer count and
// position.
//
// constructor: every member of the derived codec-owning set, asserted against
// that derivation at the end so the two cannot drift.
// disposition: adopted vs declined — the axis the defect turned on, and the one
// no error-arm test reaches, since declining happens on the SUCCESS path.
// outcome: the constructor succeeds, fails after choosing a codec, or fails
// before choosing one; the three differ in how many codecs are unowned at
// return.
// offer count and position: one offer, and two with the adopted one first vs
// last, so "release everything except index k" is driven with k at both ends and
// absent entirely.
//
// The expectation is not read off the code. Codec.Close is documented to release
// the codec's resources, and a codec passed to a constructor has exactly one
// moment where that can happen — so "closed exactly once by the time the caller
// is done with what the constructor returned" is the only state honoring the
// contract. Twice is a defect of its own, which is why the count is asserted
// rather than a boolean. The adopted cells are the control: they must NOT be
// closed while the returned Writer or Reader is still using them.
func TestMatrix_SuppliedCodecClosedExactlyOnce(t *testing.T) {
	longSchema := avro.MustParse(`"long"`)

	// A complete OCF with no avro.codec key, so the header names "null": a
	// supplied codec named "null" is adopted, any other name is declined.
	nullFile := func(t *testing.T) []byte {
		t.Helper()
		var buf bytes.Buffer
		w, err := NewWriter(&buf, longSchema)
		if err != nil {
			t.Fatalf("building fixture: %v", err)
		}
		if err := w.Encode(int64(1)); err != nil {
			t.Fatalf("building fixture: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("building fixture: %v", err)
		}
		return buf.Bytes()
	}

	type cell struct {
		ctor string
		// desc names the disposition/outcome combination under test.
		desc string
		// supplied is in the order the options are written.
		supplied []suppliedCodec
		wantErr  bool
		// run receives the WithCodec options already built from supplied.
		run func(t *testing.T, file []byte, opts []Opt) (io.Closer, error)
	}

	// Both product types are closed through io.Closer so one assertion block
	// covers Writer and Reader without knowing which it holds.
	newWriter := func(t *testing.T, _ []byte, opts []Opt) (io.Closer, error) {
		wopts := make([]WriterOpt, len(opts))
		for i, o := range opts {
			wopts[i] = o.(WriterOpt)
		}
		return NewWriter(&bytes.Buffer{}, longSchema, wopts...)
	}

	cells := []cell{
		// ---- NewReader: header says "null" ----
		{
			ctor: "NewReader", desc: "declined/success",
			supplied: []suppliedCodec{{name: "zippy"}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith(file, opts)
			},
		},
		{
			ctor: "NewReader", desc: "adopted/success (control)",
			supplied: []suppliedCodec{{name: "null", adopted: true}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith(file, opts)
			},
		},
		{
			ctor: "NewReader", desc: "two offers, adopted first/success",
			supplied: []suppliedCodec{{name: "null", adopted: true}, {name: "zippy"}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith(file, opts)
			},
		},
		{
			ctor: "NewReader", desc: "two offers, adopted last/success",
			supplied: []suppliedCodec{{name: "zippy"}, {name: "null", adopted: true}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith(file, opts)
			},
		},
		{
			// Fails AFTER the codec is chosen: the adopted one is released by
			// the error defer, the declined one by the sweep.
			ctor: "NewReader", desc: "adopted+declined/failure after choice", wantErr: true,
			supplied: []suppliedCodec{{name: "null", adopted: true}, {name: "zippy"}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith(file, opts, WithReaderSchemaFunc(func(*Reader) (*avro.Schema, error) {
					return nil, errors.New("synthetic reader-schema failure")
				}))
			},
		},
		{
			// Fails BEFORE any codec is chosen (the mutually-exclusive
			// reader-schema options are rejected ahead of the header read), so
			// nothing is adopted and every offer must be released.
			ctor: "NewReader", desc: "none adopted/failure before choice", wantErr: true,
			supplied: []suppliedCodec{{name: "null"}, {name: "zippy"}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith(file, opts,
					WithReaderSchema(longSchema),
					WithReaderSchemaFunc(func(*Reader) (*avro.Schema, error) { return nil, nil }))
			},
		},
		{
			// A header that cannot be read fails before the choice too, on a
			// different arm.
			ctor: "NewReader", desc: "none adopted/failure on header read", wantErr: true,
			supplied: []suppliedCodec{{name: "null"}},
			run: func(t *testing.T, _ []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith([]byte("not an avro file"), opts)
			},
		},

		// ---- NewAppendWriter ----
		{
			// The same codec on both sides of the name match: the reader adopts
			// index 0 and must not release index 1, which is the same object.
			ctor: "NewReader", desc: "same codec offered twice/success",
			supplied: []suppliedCodec{{name: "null", adopted: true, alias: 1}, {name: "null", adopted: true}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith(file, opts)
			},
		},
		{
			// NopCloser is what WithCodec's doc points a sharing caller at, so
			// the declined path must leave a wrapped codec untouched — the
			// wrapper's Close is a no-op, and Name is promoted through it, so
			// the wrapped codec is still matchable afterwards.
			ctor: "NewReader", desc: "declined NopCloser/success",
			supplied: []suppliedCodec{{name: "zippy", wrapNopCloser: true}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith(file, opts)
			},
		},
		{
			// The adopted twin of the cell above, which is what proves Name is
			// promoted through the wrapper: if it were not, this codec would be
			// declined instead of adopted and the cell would be the previous one
			// over again rather than its opposite.
			ctor: "NewReader", desc: "adopted NopCloser/success",
			supplied: []suppliedCodec{{name: "null", adopted: true, wrapNopCloser: true}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith(file, opts)
			},
		},

		// ---- NewAppendWriter ----
		{
			ctor: "NewAppendWriter", desc: "declined/success",
			supplied: []suppliedCodec{{name: "zippy"}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newAppendWith(&seekBuf{data: slices.Clone(file)}, opts)
			},
		},
		{
			ctor: "NewAppendWriter", desc: "adopted/success (control)",
			supplied: []suppliedCodec{{name: "null", adopted: true}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newAppendWith(&seekBuf{data: slices.Clone(file)}, opts)
			},
		},
		{
			ctor: "NewAppendWriter", desc: "adopted+declined/failure after choice", wantErr: true,
			supplied: []suppliedCodec{{name: "zippy"}, {name: "null", adopted: true}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newAppendWith(&failSeekRWS{data: slices.Clone(file)}, opts)
			},
		},
		{
			ctor: "NewAppendWriter", desc: "none adopted/failure on header read", wantErr: true,
			supplied: []suppliedCodec{{name: "null"}, {name: "zippy"}},
			run: func(t *testing.T, _ []byte, opts []Opt) (io.Closer, error) {
				return newAppendWith(&seekBuf{data: []byte("not an avro file")}, opts)
			},
		},

		// ---- NewWriter: the last WithCodec written is adopted ----
		{
			ctor: "NewWriter", desc: "adopted/success (control)",
			supplied: []suppliedCodec{{name: "null", adopted: true}},
			run:      newWriter,
		},
		{
			ctor: "NewWriter", desc: "superseded+adopted/success",
			supplied: []suppliedCodec{{name: "first"}, {name: "null", adopted: true}},
			run:      newWriter,
		},
		{
			ctor: "NewWriter", desc: "three offers, last adopted/success",
			supplied: []suppliedCodec{{name: "a"}, {name: "b"}, {name: "null", adopted: true}},
			run:      newWriter,
		},
		{
			// One codec offered twice. Position alone says index 0 is unadopted,
			// but closing it would release the very codec the returned Writer is
			// about to compress with, so identity has to beat position.
			ctor: "NewWriter", desc: "same codec offered twice/success",
			supplied: []suppliedCodec{{name: "null", adopted: true, alias: 1}, {name: "null", adopted: true}},
			run:      newWriter,
		},
		{
			// Three offers of one codec with a different codec adopted last: the
			// repeats must collapse to a single Close, not one per position.
			ctor: "NewWriter", desc: "declined codec offered twice/success",
			supplied: []suppliedCodec{{name: "a", alias: 1}, {name: "a"}, {name: "null", adopted: true}},
			run:      newWriter,
		},
		{
			ctor: "NewWriter", desc: "superseded+adopted/failure on header write", wantErr: true,
			supplied: []suppliedCodec{{name: "first"}, {name: "null", adopted: true}},
			run: func(t *testing.T, _ []byte, opts []Opt) (io.Closer, error) {
				wopts := []WriterOpt{}
				for _, o := range opts {
					wopts = append(wopts, o.(WriterOpt))
				}
				return NewWriter(&failAfterNWrites{n: 0}, longSchema, wopts...)
			},
		},
		{
			// The reserved-key arm returns after the option loop, so both the
			// superseded and the adopted codec have been collected by then.
			ctor: "NewWriter", desc: "superseded+adopted/failure on reserved key", wantErr: true,
			supplied: []suppliedCodec{{name: "first"}, {name: "null", adopted: true}},
			run: func(t *testing.T, _ []byte, opts []Opt) (io.Closer, error) {
				wopts := []WriterOpt{}
				for _, o := range opts {
					wopts = append(wopts, o.(WriterOpt))
				}
				wopts = append(wopts, WithMetadata(map[string][]byte{"avro.reserved": []byte("x")}))
				return NewWriter(&bytes.Buffer{}, longSchema, wopts...)
			},
		},
	}

	file := nullFile(t)
	seen := map[string]bool{}
	for _, c := range cells {
		seen[c.ctor] = true
		t.Run(c.ctor+"/"+c.desc, func(t *testing.T) {
			supplied := slices.Clone(c.supplied)
			// Two passes so an aliased entry can point at an object built for a
			// later position.
			for i := range supplied {
				if supplied[i].alias == 0 {
					supplied[i].codec = &leakDetectCodec{name: supplied[i].name}
				}
			}
			for i := range supplied {
				if k := supplied[i].alias; k != 0 {
					supplied[i].codec = supplied[i+k].codec
				}
			}
			opts := make([]Opt, 0, len(supplied))
			for _, s := range supplied {
				var c Codec = s.codec
				if s.wrapNopCloser {
					c = NopCloser(c)
				}
				opts = append(opts, WithCodec(c))
			}

			// Expectations are per DISTINCT codec object, not per offer: one
			// codec offered from several positions is closed once or not at
			// all, never once per position. An object counts as adopted if any
			// of its positions was.
			type objExp struct {
				name              string
				adopted, shielded bool
			}
			objs := map[*leakDetectCodec]*objExp{}
			var order []*leakDetectCodec
			for _, s := range supplied {
				e, ok := objs[s.codec]
				if !ok {
					e = &objExp{name: s.name}
					objs[s.codec] = e
					order = append(order, s.codec)
				}
				e.adopted = e.adopted || s.adopted
				// A NopCloser at ANY position shields the object: the wrapper
				// absorbs the Close instead of forwarding it.
				e.shielded = e.shielded || s.wrapNopCloser
			}
			check := func(when string, want func(*objExp) int) {
				t.Helper()
				for _, obj := range order {
					e := objs[obj]
					if got := obj.closes; got != want(e) {
						t.Errorf("%s codec %q closed %d times %s, want %d",
							dispositionOf(e.adopted, e.shielded), e.name, got, when, want(e))
					}
				}
			}

			product, err := c.run(t, file, opts)

			if c.wantErr {
				if err == nil {
					t.Fatalf("expected the constructor to fail")
				}
				// The caller was handed no closable object, so every codec it
				// passed must already be released — shielded ones excepted,
				// since NopCloser is exactly a refusal to be released.
				check("after a failed constructor", func(e *objExp) int {
					if e.shielded {
						return 0
					}
					return 1
				})
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Before Close: a declined codec is already released (nothing will
			// ever use it), an adopted one must still be open.
			check("right after a successful constructor", func(e *objExp) int {
				if e.adopted || e.shielded {
					return 0
				}
				return 1
			})

			mustClose(t, product)
			check("after Close", func(e *objExp) int {
				if e.shielded {
					return 0
				}
				return 1
			})
		})
	}

	derived := codecOwningConstructors(t)
	for ctor := range seen {
		if !slices.Contains(derived, ctor) {
			t.Errorf("cells drive %s, which is no longer a codec-owning constructor", ctor)
		}
	}
	for _, c := range derived {
		if !seen[c] {
			t.Errorf("%s is a codec-owning constructor with no cell in this matrix", c)
		}
	}
}

func dispositionOf(adopted, shielded bool) string {
	switch {
	case shielded:
		return "NopCloser-shielded"
	case adopted:
		return "adopted"
	}
	return "declined"
}

// newReaderWith turns the cell's WithCodec options (which satisfy both option
// interfaces) into reader options and appends any reader-only ones the cell adds
// to drive a particular error arm.
func newReaderWith(file []byte, opts []Opt, extra ...ReaderOpt) (io.Closer, error) {
	ropts := make([]ReaderOpt, 0, len(opts)+len(extra))
	for _, o := range opts {
		ropts = append(ropts, o.(ReaderOpt))
	}
	ropts = append(ropts, extra...)
	return NewReader(bytes.NewReader(file), ropts...)
}

func newAppendWith(rws io.ReadWriteSeeker, opts []Opt) (io.Closer, error) {
	wopts := make([]WriterOpt, len(opts))
	for i, o := range opts {
		wopts[i] = o.(WriterOpt)
	}
	return NewAppendWriter(rws, wopts...)
}

// The three reported instances, pinned individually so each reads on its own
// next to the matrix that decides its whole class. Each is a constructor that
// SUCCEEDS: the caller gets a usable Writer or Reader and no indication that the
// codec they passed was never used, so the leak is invisible from the call site.

// TestRegression_OCFNewReaderReleasesUnmatchedCodec: the file header names
// "null" and the supplied codec answers a different name, so the reader resolves
// the built-in and the supplied one is never used by anything.
func TestRegression_OCFNewReaderReleasesUnmatchedCodec(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, avro.MustParse(`"long"`))
	if err := w.Encode(int64(7)); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	mustClose(t, w)

	codec := &leakDetectCodec{name: "zippy"}
	rd := mustNewReader(t, bytes.NewReader(buf.Bytes()), WithCodec(codec))
	if codec.closes != 1 {
		t.Fatalf("unmatched codec closed %d times, want exactly 1: NewReader resolved the "+
			"built-in null codec and nothing else will ever close this one", codec.closes)
	}
	// The reader must still work, and must not close the supplied codec a
	// second time on the way out.
	var got int64
	if err := rd.Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != 7 {
		t.Fatalf("decoded %d, want 7", got)
	}
	if err := rd.Close(); err != nil {
		t.Fatalf("Reader.Close: %v", err)
	}
	if codec.closes != 1 {
		t.Fatalf("unmatched codec closed %d times after Reader.Close, want exactly 1", codec.closes)
	}
}

// TestRegression_OCFAppendWriterReleasesUnmatchedCodec: same shape on the append
// path, where the codec name likewise comes from the header already on disk.
func TestRegression_OCFAppendWriterReleasesUnmatchedCodec(t *testing.T) {
	schema := avro.MustParse(`"long"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, schema)
	if err := w.Encode(int64(1)); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	mustClose(t, w)

	codec := &leakDetectCodec{name: "zippy"}
	aw := mustNewAppendWriter(t, &seekBuf{data: buf.Bytes()}, WithCodec(codec))
	if codec.closes != 1 {
		t.Fatalf("unmatched codec closed %d times, want exactly 1", codec.closes)
	}
	if err := aw.Encode(int64(2)); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := aw.Close(); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}
	if codec.closes != 1 {
		t.Fatalf("unmatched codec closed %d times after Writer.Close, want exactly 1", codec.closes)
	}
}

// TestRegression_OCFNewWriterReleasesSupersededCodec: WithCodec written twice.
// The last one wins, which leaves the first adopted by nothing — and unlike the
// reader cases there is no name involved, so the only thing that distinguishes
// the two is their position in the option list.
func TestRegression_OCFNewWriterReleasesSupersededCodec(t *testing.T) {
	first := &leakDetectCodec{name: "null"}
	last := &leakDetectCodec{name: "null"}

	w := mustNewWriter(t, &bytes.Buffer{}, avro.MustParse(`"long"`), WithCodec(first), WithCodec(last))
	if first.closes != 1 {
		t.Fatalf("superseded codec closed %d times, want exactly 1", first.closes)
	}
	if last.closes != 0 {
		t.Fatalf("adopted codec closed %d times before the Writer was used, want 0", last.closes)
	}
	if err := w.Encode(int64(1)); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	mustClose(t, w)
	if first.closes != 1 || last.closes != 1 {
		t.Fatalf("after Close: superseded closed %d times, adopted closed %d times, want 1 and 1",
			first.closes, last.closes)
	}
}

// TestRegression_OCFNilCodecOfferIsNeverClosed pins the one shape where a release
// would be a method call on a nil interface. WithCodec(nil) compiles, and when a
// later WithCodec supersedes it the constructor succeeds — the nil never reaches a
// call site — so this call works and must keep working; closing the superseded
// offer without checking would turn a working call into a panic.
//
// Only the superseded position is asserted. A nil codec that is ADOPTED is a
// nil-method call in writeHeader (writer side) and in the name scan of
// resolveCodec (reader side), both reached before any release.
func TestRegression_OCFNilCodecOfferIsNeverClosed(t *testing.T) {
	real := &leakDetectCodec{name: "null"}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, avro.MustParse(`"long"`), WithCodec(nil), WithCodec(real))
	if err != nil {
		t.Fatalf("NewWriter with a superseded nil codec: %v", err)
	}
	if real.closes != 0 {
		t.Fatalf("adopted codec closed %d times by a successful constructor, want 0", real.closes)
	}
	if err := w.Encode(int64(1)); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	mustClose(t, w)
	if real.closes != 1 {
		t.Fatalf("adopted codec closed %d times after Close, want exactly 1", real.closes)
	}
}

// mapCodec is deliberately UNCOMPARABLE: a struct holding a map cannot be
// compared with ==, so a Codec interface value carrying one panics any direct
// equality test and cannot be a map key. Nothing forbids such a codec — the
// interface only asks for four methods — so the release path has to recognize
// repeats of it without ever comparing it the easy way.
type mapCodec struct {
	name   string
	notes  map[string]string
	closes *int
}

func (c mapCodec) Name() string                          { return c.name }
func (c mapCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (c mapCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (c mapCodec) Close() error                          { *c.closes++; return nil }

// TestRegression_OCFUncomparableCodecOfferReleasedOnce drives the arm the
// comparable fast path cannot reach. The counts are the same rule as everywhere
// else — each distinct supplied codec closed exactly once unless it was adopted
// — but reaching it here requires recognizing a repeat WITHOUT the equality
// operator, so a release path that reached for == would panic instead of
// answering.
func TestRegression_OCFUncomparableCodecOfferReleasedOnce(t *testing.T) {
	schema := avro.MustParse(`"long"`)
	mk := func(name string, n *int) mapCodec {
		return mapCodec{name: name, notes: map[string]string{"k": "v"}, closes: n}
	}

	t.Run("declined, offered twice", func(t *testing.T) {
		var declined int
		c := mk("zippy", &declined)
		w := mustNewWriter(t, &bytes.Buffer{}, schema, WithCodec(c), WithCodec(c), WithCodec(&leakDetectCodec{name: "null"}))
		if declined != 1 {
			t.Fatalf("uncomparable codec offered twice closed %d times, want exactly 1", declined)
		}
		mustClose(t, w)
		if declined != 1 {
			t.Fatalf("uncomparable codec closed %d times after Close, want exactly 1", declined)
		}
	})

	t.Run("adopted, offered twice", func(t *testing.T) {
		var n int
		c := mk("null", &n)
		w := mustNewWriter(t, &bytes.Buffer{}, schema, WithCodec(c), WithCodec(c))
		if n != 0 {
			t.Fatalf("adopted uncomparable codec closed %d times by a successful constructor, want 0: "+
				"the Writer is about to compress with it", n)
		}
		if err := w.Encode(int64(1)); err != nil {
			t.Fatalf("Encode: %v", err)
		}
		mustClose(t, w)
		if n != 1 {
			t.Fatalf("adopted uncomparable codec closed %d times after Close, want exactly 1", n)
		}
	})

	t.Run("declined next to a comparable codec", func(t *testing.T) {
		var un int
		comparable := &leakDetectCodec{name: "deflate"}
		w := mustNewWriter(t, &bytes.Buffer{}, schema, WithCodec(mk("zippy", &un)), WithCodec(comparable), WithCodec(&leakDetectCodec{name: "null"}))
		if un != 1 || comparable.closes != 1 {
			t.Fatalf("mixed offers: uncomparable closed %d times, comparable closed %d times, want 1 and 1",
				un, comparable.closes)
		}
		mustClose(t, w)
	})
}

// ---------- decompress_limit_test.go ----------

// ocfWith assembles an OCF: header (schema + codec) + one block (count, size,
// compressed payload, sync).
func ocfWith(schemaJSON, codec string, count int64, compressed []byte) []byte {
	var sync [16]byte
	var b []byte
	b = append(b, 'O', 'b', 'j', 1)
	b = binary.AppendVarint(b, 2) // 2 metadata entries
	put := func(s string) { b = binary.AppendVarint(b, int64(len(s))); b = append(b, s...) }
	put("avro.schema")
	put(schemaJSON)
	put("avro.codec")
	put(codec)
	b = binary.AppendVarint(b, 0) // metadata terminator
	b = append(b, sync[:]...)
	b = binary.AppendVarint(b, count)
	b = binary.AppendVarint(b, int64(len(compressed)))
	b = append(b, compressed...)
	b = append(b, sync[:]...)
	return b
}

// A block whose DECOMPRESSED size exceeds the per-block decompression limit is
// rejected — across deflate (unbounded io.ReadAll), snappy (pre-allocates from
// a declared length) and zstd (library default permits multi-GiB) — and the
// null-codec count loop is bounded by the same limit. Without this, a tiny
// compressed block inflates to a huge allocation / decode loop: an OCF
// decompression-amplification DoS. The compressed-side cap (WithMaxBlockBytes)
// does not bound the decompressed size; WithMaxDecompressedBlockBytes does.
func TestRegression_OCFDecompressionAmplificationBounded(t *testing.T) {
	const limit = 1 << 20   // 1 MiB configured limit (small => tiny test allocations)
	const bombLen = 4 << 20 // 4 MiB decompressed: over the limit
	zeros := make([]byte, bombLen)

	// snappy frame declaring 4 MiB (CRC trailer appended, per the codec).
	snap := snappy.Encode(nil, zeros)
	snap = binary.BigEndian.AppendUint32(snap, 0) // CRC slot (rejected before CRC check)

	// deflate stream that inflates to 4 MiB.
	var defBuf bytes.Buffer
	dw, _ := flate.NewWriter(&defBuf, flate.DefaultCompression)
	dw.Write(zeros)
	dw.Close()

	// zstd stream that inflates to 4 MiB.
	zenc, _ := zstd.NewWriter(nil)
	zst := zenc.EncodeAll(zeros, nil)
	zenc.Close()

	cases := []struct {
		name, codec string
		payload     []byte
	}{
		{"deflate", "deflate", defBuf.Bytes()},
		{"snappy", "snappy", snap},
		{"zstd", "zstandard", zst},
	}
	for _, c := range cases {
		t.Run(c.name+"/rejected-at-limit", func(t *testing.T) {
			data := ocfWith(`"null"`, c.codec, 1, c.payload)
			r, err := NewReader(bytes.NewReader(data), WithMaxDecompressedBlockBytes(limit))
			if err != nil {
				return // header/codec rejection is also a safe outcome
			}
			// Require the limit-specific rejection ("exceeds"): all three
			// codecs report it (snappy/deflate via the in-codec cap, zstd via
			// WithDecoderMaxMemory's "decompressed size exceeds configured
			// limit"). Demanding this token — not merely "some error" —
			// matters because the null schema would ALSO error on trailing
			// bytes if the bomb were allowed to inflate, which would mask a
			// missing cap. The error must come from the limit, not the decode.
			var v any
			err = r.Decode(&v)
			if err == nil || !strings.Contains(err.Error(), "exceeds") {
				t.Errorf("%s block inflating to %d bytes under a %d-byte limit: want an over-limit rejection, got %v", c.codec, bombLen, limit, err)
			}
			r.Close()
		})
		t.Run(c.name+"/accepted-when-raised", func(t *testing.T) {
			// With the limit raised above the decompressed size, the block
			// decompresses fine (decode then fails on the null-schema trailing
			// bytes, which is a normal decode error, not a limit rejection).
			data := ocfWith(`"null"`, c.codec, 1, c.payload)
			r, err := NewReader(bytes.NewReader(data), WithMaxDecompressedBlockBytes(8<<20))
			if err != nil {
				t.Fatalf("raised-limit NewReader: %v", err)
			}
			var v any
			err = r.Decode(&v)
			if err != nil && strings.Contains(err.Error(), "exceeds") {
				t.Errorf("%s block within the raised limit was still rejected as over-limit: %v", c.codec, err)
			}
			r.Close()
		})
	}

	// null codec: DecompressBounded rejects an over-cap raw block (the
	// "decompressed" size IS the input size), which also bounds the count loop.
	// A 2 MiB raw block (no compression) over a 1 MiB limit is rejected before
	// the decode loop runs.
	t.Run("null-count-loop-bounded", func(t *testing.T) {
		raw := make([]byte, 2<<20)
		data := ocfWith(`"null"`, "null", 1, raw)
		r := mustNewReader(t, bytes.NewReader(data), WithMaxDecompressedBlockBytes(limit))
		var v any
		if err := r.Decode(&v); err == nil || !strings.Contains(err.Error(), "exceeds") {
			t.Errorf("2 MiB null block over a 1 MiB limit: want over-limit rejection, got %v", err)
		}
		r.Close()
	})

	// A legitimate small file round-trips under the (large) default limit.
	t.Run("legit-roundtrip-default-limit", func(t *testing.T) {
		s := avro.MustParse(`"string"`)
		var buf bytes.Buffer
		w := mustNewWriter(t, &buf, s, WithCodec(DeflateCodec(1)))
		w.Encode("hello")
		w.Close()
		r := mustNewReader(t, bytes.NewReader(buf.Bytes())) // default 64 MiB limit
		var got string
		if err := r.Decode(&got); err != nil || got != "hello" {
			t.Errorf("legit round-trip under default limit failed: got %q err %v", got, err)
		}
		r.Close()
	})
}

// A user expressing "no practical decompressed-size limit" as math.MaxInt64
// (rather than the documented 0) must still read a valid deflate-compressed OCF.
// deflateCodec.DecompressBounded reads io.LimitReader(r, max+1) to detect
// over-limit without materializing the bomb; at max==MaxInt64 the +1 overflows to
// MinInt64, LimitReader returns 0 bytes, the block decodes as empty, and a valid
// file fails to read. The bound must not invert at its own extreme value; the
// default-limit and limit==0 paths are the boundary-1 controls.
func TestRegression_OCFDeflateDecompressLimitMaxInt(t *testing.T) {
	s := avro.MustParse(`"string"`)
	payload := strings.Repeat("hello world ", 2000) // ~24 KiB, compresses well
	mk := func() []byte {
		var buf bytes.Buffer
		w := mustNewWriter(t, &buf, s, WithCodec(DeflateCodec(1)))
		if err := w.Encode(payload); err != nil {
			t.Fatal(err)
		}
		mustClose(t, w)
		return buf.Bytes()
	}
	// Reader auto-selects the built-in deflate codec from the header; the cap is
	// passed to its DecompressBounded from WithMaxDecompressedBlockBytes.
	for _, tc := range []struct {
		name  string
		limit int64
	}{
		{"max-int64", math.MaxInt64}, // the overflow boundary
		{"unlimited-zero", 0},        // documented "unlimited" control
		{"generous", 64 << 20},       // ordinary large control
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := mustNewReader(t, bytes.NewReader(mk()), WithMaxDecompressedBlockBytes(tc.limit))
			defer r.Close()
			var got string
			if err := r.Decode(&got); err != nil {
				t.Fatalf("limit=%d: Decode of a valid deflate file failed: %v", tc.limit, err)
			}
			if got != payload {
				t.Fatalf("limit=%d: round-trip mismatch: got %d bytes, want %d", tc.limit, len(got), len(payload))
			}
		})
	}
}

// A zstd codec supplied as an INSTANCE via WithCodec (and one wrapped in
// NopCloser, the realistic shared-codec form) is bounded by the reader's
// WithMaxDecompressedBlockBytes, the same as a name-resolved zstd codec: the
// decoder is built lazily with zstd.WithDecoderMaxMemory from the cap. A frame
// inflating past the cap is rejected; the same frame under a raised cap decodes.
func TestRegression_OCFSuppliedZstdInstanceBounded(t *testing.T) {
	const limit = 1 << 20
	const bombLen = 4 << 20
	zeros := make([]byte, bombLen)
	zenc, _ := zstd.NewWriter(nil)
	zst := zenc.EncodeAll(zeros, nil)
	zenc.Close()
	data := ocfWith(`"null"`, "zstandard", 1, zst)

	for _, sc := range []struct {
		name  string
		codec func() Codec
	}{
		{"instance", func() Codec { c, _ := ZstdCodec(nil, nil); return c }},
		{"nopcloser", func() Codec { c, _ := ZstdCodec(nil, nil); return NopCloser(c) }},
	} {
		t.Run(sc.name+"/rejected-at-limit", func(t *testing.T) {
			r := mustNewReader(t, bytes.NewReader(data), WithCodec(sc.codec()), WithMaxDecompressedBlockBytes(limit))
			defer r.Close()
			var v any
			if err := r.Decode(&v); err == nil || !strings.Contains(err.Error(), "exceeds") {
				t.Errorf("supplied zstd %s: 4 MiB frame under a 1 MiB cap: want over-limit rejection, got %v", sc.name, err)
			}
		})
		t.Run(sc.name+"/accepted-when-raised", func(t *testing.T) {
			r := mustNewReader(t, bytes.NewReader(data), WithCodec(sc.codec()), WithMaxDecompressedBlockBytes(8<<20))
			defer r.Close()
			var v any
			if err := r.Decode(&v); err != nil && strings.Contains(err.Error(), "exceeds") {
				t.Errorf("supplied zstd %s under a raised cap was still rejected as over-limit: %v", sc.name, err)
			}
		})
	}
}

// rawCodec is a no-op "compression" codec: the stored block IS the raw bytes.
type rawCodec struct{ name string }

func (c rawCodec) Name() string                        { return c.name }
func (rawCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (rawCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (rawCodec) Close() error                          { return nil }

// boundedRawCodec adds the BoundedDecompressor capability to rawCodec.
type boundedRawCodec struct{ rawCodec }

func (boundedRawCodec) DecompressBounded(src []byte, max int64) ([]byte, error) {
	if max > 0 && int64(len(src)) > max {
		return nil, fmt.Errorf("rawcodec: %d bytes exceeds limit of %d", len(src), max)
	}
	return src, nil
}

// A custom codec implementing BoundedDecompressor is bounded by the reader's
// WithMaxDecompressedBlockBytes; a custom codec that does NOT implement it is
// honestly unbounded — the reader adds no post-decompression backstop (false
// comfort once the block is allocated). This pins the capability contract that
// replaced the type-asserted "is this a built-in instance" recognition.
func TestRegression_OCFCustomCodecBoundedDecompressorContract(t *testing.T) {
	const limit = 1 << 20
	raw := make([]byte, 4<<20) // 4 MiB "compressed" block == 4 MiB decompressed

	t.Run("implements-bounded/rejected", func(t *testing.T) {
		data := ocfWith(`"null"`, "bnd", 1, raw)
		r := mustNewReader(t, bytes.NewReader(data), WithCodec(boundedRawCodec{rawCodec{"bnd"}}), WithMaxDecompressedBlockBytes(limit))
		defer r.Close()
		var v any
		if err := r.Decode(&v); err == nil || !strings.Contains(err.Error(), "exceeds") {
			t.Errorf("bounded custom codec: 4 MiB block over a 1 MiB cap: want rejection, got %v", err)
		}
	})
	t.Run("plain/unbounded", func(t *testing.T) {
		data := ocfWith(`"null"`, "unb", 1, raw)
		r := mustNewReader(t, bytes.NewReader(data), WithCodec(rawCodec{"unb"}), WithMaxDecompressedBlockBytes(limit))
		defer r.Close()
		// No BoundedDecompressor => the cap does not apply. The decode fails on
		// the null schema's trailing bytes, NOT with an over-limit rejection.
		var v any
		if err := r.Decode(&v); err != nil && strings.Contains(err.Error(), "exceeds") {
			t.Errorf("plain custom codec must be unbounded (no over-limit reject), got %v", err)
		}
	})
}

// A sub-1-MiB WithMaxDecompressedBlockBytes must bound a zstd block exactly, not
// silently round up to 1 MiB. The reader builds the zstd decoder with
// zstd.WithDecoderMaxMemory set from the cap, clamped up only to
// zstd.MinWindowSize (1 KiB), so a 512 KiB block is rejected under a 256 KiB cap
// yet accepted under a 1 MiB cap. Were the minimum mistakenly 1 MiB, the 256 KiB
// cap would be raised to 1 MiB and the 512 KiB block would slip through — the
// regression this pins.
func TestRegression_OCFZstdSubMiBCapHonored(t *testing.T) {
	s := avro.MustParse(`"bytes"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithCodec(MustZstdCodec(nil, nil)))
	if err := w.Encode(make([]byte, 512<<10)); err != nil { // one ~512 KiB zstd block
		t.Fatal(err)
	}
	mustClose(t, w)
	file := buf.Bytes()

	read := func(capBytes int64) error {
		r, err := NewReader(bytes.NewReader(file),
			WithCodec(MustZstdCodec(nil, nil)),
			WithMaxDecompressedBlockBytes(capBytes))
		if err != nil {
			return err
		}
		defer r.Close()
		var v []byte
		return r.Decode(&v)
	}

	// 512 KiB block under a 256 KiB cap: the cap is below the block, so it must
	// be rejected as over-limit, not accepted by a silent floor-up to 1 MiB.
	if err := read(256 << 10); err == nil {
		t.Fatalf("512 KiB zstd block accepted under a 256 KiB cap: the cap was floored up instead of honored")
	} else if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("256 KiB cap: want an over-limit rejection, got %v", err)
	}
	// Same block under a 1 MiB cap: above the block, so it decodes.
	if err := read(1 << 20); err != nil && strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("512 KiB block rejected under a 1 MiB cap: %v", err)
	}
}

// A cap below zstd.MinWindowSize (1 KiB) must be raised UP to MinWindowSize, not
// passed through: the decoder rejects any WithDecoderMaxMemory below a frame's
// window, and every frame's window is at least MinWindowSize, so a sub-1-KiB cap
// left as-is would spuriously reject even a tiny valid block. The MinWindowSize
// minimum keeps a small datum decodable; removing it (or lowering it below
// MinWindowSize) makes this block reject — the property this pins.
func TestRegression_OCFZstdTinyCapFloorsAtMinWindow(t *testing.T) {
	s := avro.MustParse(`"bytes"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s, WithCodec(MustZstdCodec(nil, nil)))
	if err := w.Encode(make([]byte, 100)); err != nil { // tiny block, well under MinWindowSize
		t.Fatal(err)
	}
	mustClose(t, w)

	// 512-byte cap is below MinWindowSize; it is raised up to 1 KiB so the tiny
	// frame still decodes rather than tripping the decoder's window minimum.
	r := mustNewReader(t, bytes.NewReader(buf.Bytes()), WithCodec(MustZstdCodec(nil, nil)), WithMaxDecompressedBlockBytes(512))
	defer r.Close()
	var v []byte
	if err := r.Decode(&v); err != nil {
		t.Fatalf("tiny zstd block spuriously rejected under a sub-MinWindowSize cap (floor missing or below MinWindowSize): %v", err)
	}
	if len(v) != 100 {
		t.Fatalf("decoded %d bytes, want 100", len(v))
	}
}

// ---------- dos_battery_test.go ----------

// DoS entry-point battery — OCF package.
//
// Companion to ../dos_battery_test.go. The OCF reader/writer add hostile-input
// classes the core codec does not have: a block is "read a compressed size, then
// inflate to a length declared INSIDE the payload", and a header is "read a
// count, then loop". Each such boundary has TWO limits — the wire-side and the
// materialized side — and a cap on one is not a cap on the other.
//
// Rows: ocf.NewReader (+ Reader.Decode) / ocf.NewWriter (+ WithMetadata).
// Columns:
//   C1 header nesting/size   — deeply-nested avro.schema; metadata entry count.
//   C2 block count / size    — declared block size vs maxBlockBytes; block count
//                              vs len(block)+maxOCFZeroByteSlack; zero-byte run.
//   C4 decompression amplif. — a small compressed block inflating past
//                              WithMaxDecompressedBlockBytes.
//   C5 error-message echo    — unknown codec name; over-cap metadata key.
//
// Same rule as the core battery: cells are never "closed". A later OCF DoS find
// extends this matrix; it does not retire it.

// ocfDosBudget stays ABSOLUTE where the cost cells took growth ratios (see
// costScale in the core package's dos_battery section): what a watchdog asks is
// whether fn RETURNED, and a hang leaves no second measurement to divide by.
const ocfDosBudget = 4 * time.Second

// costScale is the growth claim a cost cell makes: cost measured at two problem
// sizes, and the largest ratio between them that leaves the claim standing. Why
// a complexity claim is a RATIO and not an absolute wall-clock ceiling is
// written out once, beside the core package's copy of this harness in its
// dos_battery section; the short form is that a ceiling measures the machine as
// much as the code, and `go test ./...` runs this package concurrently with
// that one.
//
// It is a copy and not a bridge for the same reason dosRun above is: the core
// package's version lives in its own _test.go files, so it is compiled into the
// avro test binary and not into this one. There is no import that reaches it.
type costScale struct {
	lo, hi int
	tol    float64
	floor  time.Duration
}

const (
	ocfCostMinSamples   = 3
	ocfCostMaxSamples   = 25
	ocfCostSampleBudget = 30 * time.Millisecond
)

// measureOCFCostPair samples the two sides of a ratio in alternating ROUNDS and
// returns the pair from the round whose ratio was smallest. The reasoning is
// written out once beside the core package's copy: a quiet window on a loaded
// host is a stretch of TIME, so phase-separated sampling lets it lower one
// side alone, and two independently-taken minima pair a lucky low with an
// unlucky high that never coexisted. One round is two measurements made
// microseconds apart under one machine state.
func measureOCFCostPair(loFn, hiFn func() error) (tLo, tHi time.Duration, errLo, errHi error) {
	var total time.Duration
	bestRatio := math.Inf(1)
	for n := 0; n < ocfCostMaxSamples; n++ {
		if n >= ocfCostMinSamples && total >= ocfCostSampleBudget {
			break
		}
		hiStart := time.Now()
		if e := hiFn(); e != nil {
			errHi = e
		}
		dHi := time.Since(hiStart)
		loStart := time.Now()
		if e := loFn(); e != nil {
			errLo = e
		}
		dLo := time.Since(loStart)
		total += dHi + dLo
		r := math.Inf(1)
		if dLo > 0 {
			r = float64(dHi) / float64(dLo)
		}
		if n == 0 || r < bestRatio {
			bestRatio, tLo, tHi = r, dLo, dHi
		}
	}
	return tLo, tHi, errLo, errHi
}

// wantAcceptScales asserts fn accepts at both of sc's sizes and that its cost
// grows no faster than sc permits. build takes the magnitude and returns the
// thunk to TIME, so everything the magnitude needs but the bound does not own
// stays outside the measurement.
func wantAcceptScales(t *testing.T, name string, sc costScale, build func(n int) func() error) {
	t.Helper()
	tLo, tHi, errLo, errHi := measureOCFCostPair(build(sc.lo), build(sc.hi))
	if errLo != nil {
		t.Errorf("%s (n=%d): %v", name, sc.lo, errLo)
		return
	}
	if errHi != nil {
		t.Errorf("%s (n=%d): %v", name, sc.hi, errHi)
		return
	}
	ratio := float64(tHi) / float64(tLo)
	lim := max(time.Duration(sc.tol*float64(tLo)), sc.floor)
	t.Logf("%s: %v at %d, %v at %d — ratio %.3gx for a %.3gx size increase (limit %v)",
		name, tLo, sc.lo, tHi, sc.hi, ratio, float64(sc.hi)/float64(sc.lo), lim)
	if tHi > lim {
		t.Errorf("%s: cost grew %.3gx for a %.3gx size increase — %v at %d vs %v at %d (limit %v).\n"+
			"The bound claims to cap this magnitude, so growth past the magnitude's own is the bound missing.\n"+
			"This is a RATIO between two sizes measured moments apart: host load inflates both and divides out.",
			name, ratio, float64(sc.hi)/float64(sc.lo), tLo, sc.lo, tHi, sc.hi, lim)
	}
}

// dosRun runs fn under a watchdog: a hang past the budget (missing bound on a
// non-allocating loop) or a panic (hostile input must error, not panic) fails
// the cell. Package-scoped to ocf; does not collide with the avro twin.
func dosRun(t *testing.T, name string, fn func() error) (error, bool) {
	t.Helper()
	type result struct {
		err error
		pan any
	}
	ch := make(chan result, 1)
	start := time.Now()
	go func() {
		var r result
		defer func() {
			if p := recover(); p != nil {
				r.pan = p
			}
			ch <- r
		}()
		r.err = fn()
	}()
	select {
	case r := <-ch:
		if r.pan != nil {
			t.Errorf("%s: panicked on hostile input (must return an error, not panic): %v", name, r.pan)
			return nil, false
		}
		if d := time.Since(start); d > ocfDosBudget {
			t.Errorf("%s: completed but took %v (> %v) — cost not bounded", name, d, ocfDosBudget)
		}
		return r.err, true
	case <-time.After(ocfDosBudget):
		t.Errorf("%s: did not return within %v — bound missing (hang/unbounded loop)", name, ocfDosBudget)
		return nil, false
	}
}

func wantReject(t *testing.T, name string, fn func() error) {
	t.Helper()
	if err, ok := dosRun(t, name, fn); ok && err == nil {
		t.Errorf("%s: hostile input was accepted (want a fast rejection)", name)
	}
}

// wantTerminate asserts fn returns within the budget, whatever its verdict.
// A legal schema must be ACCEPTED, so the property is termination rather than
// rejection.
func wantTerminate(t *testing.T, name string, fn func() error) {
	t.Helper()
	if err, ok := dosRun(t, name, fn); ok && err != nil {
		t.Errorf("%s: legal input was rejected: %v", name, err)
	}
}

const ocfDosMaxErrLen = 4096

func wantBoundedErr(t *testing.T, name string, fn func() error) {
	t.Helper()
	if err, ok := dosRun(t, name, fn); ok {
		if err == nil {
			t.Errorf("%s: want a (bounded) error, got nil", name)
		} else if n := len(err.Error()); n > ocfDosMaxErrLen {
			t.Errorf("%s: error message is %d bytes (> %d) — hostile input echoed unbounded", name, n, ocfDosMaxErrLen)
		}
	}
}

// ocfHeaderSync writes a real OCF header for schemaJSON (so readHeader accepts
// it) and returns the header bytes plus its 16-byte sync marker, for cells that
// append a hostile block by hand.
func ocfHeaderSync(t *testing.T, schemaJSON string) (hdr, sync []byte) {
	t.Helper()
	s := mustParse(t, schemaJSON)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	mustClose(t, w)
	hdr = buf.Bytes()
	return hdr, hdr[len(hdr)-16:]
}

// deflateBomb returns a deflate stream that inflates to n bytes.
func deflateBomb(n int) []byte {
	var buf bytes.Buffer
	w, _ := flate.NewWriter(&buf, flate.DefaultCompression)
	w.Write(make([]byte, n))
	w.Close()
	return buf.Bytes()
}

//////////////////////////////////////////////////////////////////////////////
// C4 — DECOMPRESSION AMPLIFICATION (a few bytes -> hundreds of MiB)
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_OCF_C4_Decompression(t *testing.T) {
	// A deflate block inflating past WithMaxDecompressedBlockBytes must reject
	// at the cap, not after materializing the bomb. Bound: maxDecompressed
	// enforced INSIDE the codec. Extreme (snappy/deflate/zstd + null backstop):
	// TestRegression_OCFDecompressionAmplificationBounded,
	// TestRegression_OCFDeflateDecompressLimitMaxInt, TestRegression_OCFLargeDatumReaderCap.
	data := ocfWith(`"null"`, "deflate", 1, deflateBomb(8<<20)) // declares 8 MiB decompressed
	wantReject(t, "NewReader+Decode/deflate-bomb", func() error {
		r, err := NewReader(bytes.NewReader(data), WithMaxDecompressedBlockBytes(1<<20))
		if err != nil {
			return err // header/codec rejection is also a safe outcome
		}
		defer r.Close()
		var v any
		return r.Decode(&v)
	})
}

//////////////////////////////////////////////////////////////////////////////
// C2 — BLOCK COUNT / SIZE / ZERO-RUN
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_OCF_C2_BlockCountSize(t *testing.T) {
	// A zero-field record makes every datum consume zero bytes; a hostile block
	// count then drives a near-infinite decode loop over an empty payload.
	// Bound: count > len(block)+maxOCFZeroByteSlack (readBlock) + the
	// consecutive zero-run cap in Decode. Extreme: TestRegression_OCFBlockCountCap,
	// TestReaderZeroRunCapIndependentOfBlockLength.
	zeroByteCount := ocfWith(`{"type":"record","name":"E","fields":[]}`, "null", 1_000_000_000, nil)
	wantReject(t, "NewReader+Decode/zero-byte-record-huge-count", func() error {
		r, err := NewReader(bytes.NewReader(zeroByteCount))
		if err != nil {
			return err
		}
		defer r.Close()
		var v map[string]any
		return r.Decode(&v)
	})

	// A block declaring a huge COMPRESSED size must reject before the read
	// allocates it. Bound: size > maxBlockBytes (default 64 MiB / WithMaxBlockBytes),
	// checked before reading the payload. Extreme: TestWithMaxBlockBytes.
	hdr, sync := ocfHeaderSync(t, `"long"`)
	var hugeSize []byte
	hugeSize = append(hugeSize, hdr...)
	hugeSize = append(hugeSize, binary.AppendVarint(nil, 1)...)     // count = 1
	hugeSize = append(hugeSize, binary.AppendVarint(nil, 1<<40)...) // declared block size = 1 TiB
	hugeSize = append(hugeSize, sync...)
	wantReject(t, "NewReader+Decode/huge-declared-block-size", func() error {
		r, err := NewReader(bytes.NewReader(hugeSize))
		if err != nil {
			return err
		}
		defer r.Close()
		var v int64
		return r.Decode(&v)
	})

	// Same hostile block, but with the reader's cap RAISED above the declared size
	// — as a caller setting a very large / "unlimited" WithMaxBlockBytes would. The
	// size > maxBlockBytes guard no longer fires, so readBlock must still reject
	// gracefully instead of eagerly make([]byte, declaredSize): near the MaxInt64
	// ceiling that allocation is an unrecoverable fatal OOM, and even at realistic
	// raised caps a tiny file forces a multi-GiB spike. readBlock reads
	// incrementally beyond ocfEagerBlockAllocLimit, so a declared-but-absent size
	// fails after consuming the bytes actually present.
	hdrRaised, syncRaised := ocfHeaderSync(t, `"long"`)
	var raisedHuge []byte
	raisedHuge = append(raisedHuge, hdrRaised...)
	raisedHuge = append(raisedHuge, binary.AppendVarint(nil, 1)...)     // count = 1
	raisedHuge = append(raisedHuge, binary.AppendVarint(nil, 1<<48)...) // declared 256 TiB, no payload
	raisedHuge = append(raisedHuge, syncRaised...)
	wantReject(t, "NewReader+Decode/huge-declared-size-raised-cap", func() error {
		r, err := NewReader(bytes.NewReader(raisedHuge), WithMaxBlockBytes(1<<50))
		if err != nil {
			return err
		}
		defer r.Close()
		var v int64
		return r.Decode(&v)
	})
}

//////////////////////////////////////////////////////////////////////////////
// C1 — HEADER (nested schema, metadata entry count)
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_OCF_C1_Header(t *testing.T) {
	// A deeply-nested avro.schema in the header is parsed by avro.Parse, whose
	// checkSchemaNestingDepth pre-scan rejects it (the OCF header inherits the
	// core schema-parse bounds). Bound: avro.checkSchemaNestingDepth.
	deepSchema := strings.Repeat(`{"type":"array","items":`, 6000) + `"int"` + strings.Repeat("}", 6000)
	wantReject(t, "NewReader/deeply-nested-header-schema", func() error {
		r, err := NewReader(bytes.NewReader(ocfWith(deepSchema, "null", 1, nil)))
		if r != nil {
			r.Close()
		}
		return err
	})

	// A header whose metadata map declares a huge entry count must reject when
	// the map is decoded (the OCF metadata map shares the core map-block bound),
	// not loop/allocate per the claimed count. Bound: the map decode's block
	// bound + ocfMetadataSafetyLimit.
	var hugeMetaCount []byte
	hugeMetaCount = append(hugeMetaCount, 'O', 'b', 'j', 1)
	hugeMetaCount = append(hugeMetaCount, binary.AppendVarint(nil, 1<<40)...) // metadata entry count = 2^40
	hugeMetaCount = append(hugeMetaCount, 0x02, 0x00)                         // a couple trailing bytes (short buffer)
	wantReject(t, "NewReader/huge-metadata-entry-count", func() error {
		r, err := NewReader(bytes.NewReader(hugeMetaCount))
		if r != nil {
			r.Close()
		}
		return err
	})

	// A header schema whose named types are REFERENCED more than once is a DAG,
	// not a tree: both spellings bind to one node, so any walk that re-descends
	// per reference does 2^depth work on a header of a couple of kilobytes. This
	// is the entry point where the schema comes from the INPUT rather than from
	// the caller, which sets the class's severity.
	//
	// It needs no nesting at all — the second form declares every level as a
	// sibling field wired by forward reference — so the nesting pre-scan is not
	// the bound for this shape; the bound is that each node is walked once. Both
	// forms must be ACCEPTED, promptly. Driven at TWO depths, since without the
	// memo this is 2^depth and the pair is a 16x separation, where one depth asks
	// only whether that depth finishes. Measured flat: 456us at 26, 317us at 30.
	for _, levels := range headerDepths {
		for _, form := range []struct{ name, schema string }{
			{"nested", dagRefHeaderNested(levels)},
			{"flat-forward-ref", dagRefHeaderFlat(levels)},
		} {
			wantTerminate(t, fmt.Sprintf("NewReader/shared-node-header-schema/%s/levels=%d", form.name, levels), func() error {
				r, err := NewReader(bytes.NewReader(ocfWith(form.schema, "null", 0, nil)))
				if r != nil {
					r.Close()
				}
				return err
			})
		}
	}
	// The same DAG with the WIDTH axis turned up. Depth alone is half of what a
	// walk over this graph costs: a node is recomputed once per path that reaches
	// it and each recomputation iterates that node's OWN field list, so the cost is
	// a product of two magnitudes the header's author chooses independently, and
	// holding the second at two leaves the product untested. Here the chain is
	// cyclic, so nothing memoizes, and the record every path ends at is wide, making
	// the most-revisited node also the most expensive. WIDTH is driven at two values
	// because a per-node charge makes the cost allowance x width while a per-child
	// charge makes it flat. Measured 210ms at 8000 and 295ms at 16000.
	for _, width := range headerWidths {
		wantTerminate(t, fmt.Sprintf("NewReader/wide-cyclic-header-schema/width=%d", width), func() error {
			r, err := NewReader(bytes.NewReader(ocfWith(dagWideCyclicHeader(16, width), "null", 0, nil)))
			if r != nil {
				r.Close()
			}
			return err
		})
	}

	// The third factor: how many CONTAINERS the header points at one subtree. The
	// reader derives a per-element minimum for every array/map in the header, and
	// a header can carry any number of them over one shared cyclic SCC. Depth and
	// width are held where the per-walk bounds engage; the count is turned up. The
	// reader shares one walk across the header's containers, so a fresh walk per
	// container — the product this battery guards — is what this rejects, and this
	// is the file-supplied form of the class, so it fixes the severity. Measured
	// 142ms at 220 and 135ms at 440, flat across a doubling.
	for _, narrays := range headerContainerCounts {
		wantTerminate(t, fmt.Sprintf("NewReader/many-container-header-schema/n=%d", narrays), func() error {
			r, err := NewReader(bytes.NewReader(ocfWith(dagManyContainerHeader(narrays, 26), "null", 0, nil)))
			if r != nil {
				r.Close()
			}
			return err
		})
	}

	// The SAME third factor reached by the other construction path: when the
	// cyclic type is DEFINED FIRST and fully wired at parse-build (inline plus a
	// backward name reference), each container's items resolves to the built node
	// and the per-element minimum is computed on the reader's BUILD path, not
	// finalize. A per-container fresh walk there costs the container count times a
	// full walk — 8.3 s at 32 containers off a 64 KB header before the build path
	// shared the walk. The header is file-supplied, so this is the severity cell
	// for the backward reaching-path.
	for _, narrays := range headerContainerCounts {
		wantTerminate(t, fmt.Sprintf("NewReader/many-container-header-schema-backward/n=%d", narrays), func() error {
			r, err := NewReader(bytes.NewReader(ocfWith(dagManyContainerWiredHeader(narrays, 26), "null", 0, nil)))
			if r != nil {
				r.Close()
			}
			return err
		})
	}
}

// The magnitudes the header cost cells drive. They live here as named
// vocabularies because this package cannot reach the avro package's costCells
// registry — a test package boundary, not a choice — so the cross-package half
// of that registry's guard checks these VALUES appear in this file instead.
// Each is a pair for the same reason every cost cell now drives a pair: one
// magnitude cannot tell a bound from a cost that is merely linear in it.
var (
	headerDepths          = []int{26, 30}
	headerWidths          = []int{8000, 16000}
	headerContainerCounts = []int{220, 440}
)

// dagManyContainerWiredHeader defines the cyclic type FIRST and fully wired at
// build (each level nests the next inline and references it a second time by
// name, deepest closes to the enclosing L0), then N arrays reference "L0" by
// name. A backward reference resolves to the fully built node, so the reader
// computes the per-element minimum at build. Mirrors nContainersOverWiredSCC in
// the avro package.
func dagManyContainerWiredHeader(narrays, levels int) string {
	inner := `["null","L0"]`
	for i := levels - 1; i >= 0; i-- {
		if i == levels-1 {
			inner = fmt.Sprintf(`{"type":"record","name":"L%d","fields":[{"name":"f0","type":["null","L0"]},{"name":"f1","type":["null","L0"]}]}`, i)
			continue
		}
		inner = fmt.Sprintf(`{"type":"record","name":"L%d","fields":[{"name":"f0","type":["null",%s]},{"name":"f1","type":["null","L%d"]}]}`, i, inner, i+1)
	}
	var b strings.Builder
	b.WriteString(`{"type":"record","name":"Root","fields":[{"name":"def","type":` + inner + `}`)
	for j := 0; j < narrays; j++ {
		fmt.Fprintf(&b, `,{"name":"z%d","type":{"type":"array","items":"L0"}}`, j)
	}
	b.WriteString(`]}`)
	return b.String()
}

// dagManyContainerHeader builds a header record with narrays array fields, each
// of items "L0", above a cyclic SCC L0..L{levels-1} -> L0. The count of arrays
// is a magnitude the header author picks independently of the subtree, so the
// walk cost is a product the reader must bound by sharing one walk across the
// containers. Mirrors nContainersOverSCC in the avro package, which this package
// cannot import.
func dagManyContainerHeader(narrays, levels int) string {
	var b strings.Builder
	b.WriteString(`{"type":"record","name":"Root","fields":[`)
	for j := 0; j < narrays; j++ {
		if j > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"name":"z%d","type":{"type":"array","items":"L0"}}`, j)
	}
	for i := 0; i < levels; i++ {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "L0"
		}
		fmt.Fprintf(&b, `,{"name":"d%d","type":{"type":"record","name":"L%d","fields":[{"name":"f0","type":["null","%s"]},{"name":"f1","type":["null","%s"]}]}}`, i, i, next, next)
	}
	b.WriteString(`]}`)
	return b.String()
}

// dagWideCyclicHeader builds the header form of the width shape: a fan-2 chain
// of `levels` records whose deepest member references the shallowest (one
// strongly-connected component, so no result is memoizable) and carries `width`
// zero-minimum filler fields. Mirrors dagWideSCC in the avro package, which
// this package cannot import.
func dagWideCyclicHeader(levels, width int) string {
	var wide strings.Builder
	wide.WriteString(`{"type":"record","name":"W","fields":[{"name":"back","type":"L0"}`)
	for k := range width {
		fmt.Fprintf(&wide, `,{"name":"p%d","type":"null"}`, k)
	}
	wide.WriteString(`]}`)
	inner := wide.String()
	for i := levels - 1; i >= 0; i-- {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "W"
		}
		inner = fmt.Sprintf(`{"type":"record","name":"L%d","fields":[{"name":"f0","type":%s},{"name":"f1","type":"%s"}]}`,
			i, inner, next)
	}
	return `{"type":"array","items":` + inner + `}`
}

// dagRefHeaderNested builds an array-of-record header schema where every level
// declares the next inline and then references it a second time by name, so the
// two bind to one node.
func dagRefHeaderNested(levels int) string {
	inner := `"int"`
	for i := levels - 1; i >= 0; i-- {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "int"
		}
		inner = fmt.Sprintf(`{"type":"record","name":"L%d","fields":[{"name":"f0","type":%s},{"name":"f1","type":"%s"}]}`,
			i, inner, next)
	}
	return `{"type":"array","items":` + inner + `}`
}

// dagRefHeaderFlat expresses the same type graph with a JSON nesting depth of
// four regardless of levels, wiring the levels by forward reference.
func dagRefHeaderFlat(levels int) string {
	var b strings.Builder
	b.WriteString(`{"type":"record","name":"Root","fields":[{"name":"z","type":{"type":"array","items":"L0"}}`)
	for i := range levels {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "int"
		}
		fmt.Fprintf(&b, `,{"name":"d%d","type":{"type":"record","name":"L%d","fields":[{"name":"f0","type":"%s"},{"name":"f1","type":"%s"}]}}`,
			i, i, next, next)
	}
	b.WriteString(`]}`)
	return b.String()
}

//////////////////////////////////////////////////////////////////////////////
// C5 — ERROR-MESSAGE ECHO (read + write directions)
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_OCF_C5_ErrorEcho(t *testing.T) {
	// Read side: an unknown codec name from a hostile header is echoed into the
	// resolveCodec error; truncForError (the ocf-package copy) bounds it.
	// Extreme: TestRegression_OCFUnknownCodecErrorBounded.
	hugeCodec := strings.Repeat("z", 1<<20)
	wantBoundedErr(t, "NewReader/unknown-megabyte-codec-name", func() error {
		r, err := NewReader(bytes.NewReader(ocfWith(`"null"`, hugeCodec, 1, nil)))
		if r != nil {
			r.Close()
		}
		return err
	})

	// Write side: a caller-supplied WithMetadata key over the cap is echoed into
	// the NewWriter error; the same truncForError bounds it. A WithMetadata key
	// is wire-equivalent user input, so the write direction needs the bound too.
	// Extreme: TestRegression_OCFMetadataKeyErrorBounded.
	s := avro.MustParse(`"long"`)
	hugeKey := strings.Repeat("k", 2<<20) // > ocfMetadataSafetyLimit (1 MiB)
	wantBoundedErr(t, "NewWriter/over-cap-metadata-key", func() error {
		var buf bytes.Buffer
		w, err := NewWriter(&buf, s, WithMetadata(map[string][]byte{hugeKey: {1}}))
		if w != nil {
			w.Close()
		}
		return err
	})
}

// ---------- foreign_framing_test.go ----------

// ---------------------------------------------------------------------------
// FOREIGN block framing: container shapes no twmb writer produces (the writer
// never emits a count-0 block — TestWriterBlockFramingContract), which the
// reader must nevertheless handle because they are spec-valid: the spec leaves a
// block's object count unconstrained, and unlike Avro arrays and maps, file data
// blocks have no terminator — end of file is end of stream.
//
// Crosses empty-block POSITION {first, mid, tail, consecutive x3} x CODEC {null,
// deflate, snappy, zstandard} x empty-block PAYLOAD {size 0, >0 decompressing to
// zero bytes, >0 garbage}. Every accept cell asserts the full file content is
// read, and — when a fastavro interpreter is available — that fastavro's
// iterator reads the identical bytes to the identical records. Cells where
// fastavro itself errors are cross-checked as twmb-only with its observed
// verdict recorded: fastavro (like Java) decompresses a count-0 block's payload
// eagerly and rejects an undecompressable one, while this reader skips the block
// without consulting the codec — a deliberate leniency; no records are lost
// either way.
//
// Every empty-block cell reaches the skip arm (readBlock's count==0 continue),
// which sits after payload + sync validation and before decompression. The
// corrupt-sync guard cell errors BEFORE the skip arm, and the writer-side
// framing tests never produce a count-0 block at all.
// ---------------------------------------------------------------------------

var foreignSync = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

// appendRawBlock appends one hand-framed block: count, size, payload, sync.
func appendRawBlock(buf *bytes.Buffer, count int64, payload []byte, sync [16]byte) {
	buf.Write(binary.AppendVarint(nil, count))
	buf.Write(binary.AppendVarint(nil, int64(len(payload))))
	buf.Write(payload)
	buf.Write(sync[:])
}

// foreignFile assembles an OCF whose data blocks are produced by the real
// Writer (header, codec framing, compression) and whose empty blocks are
// hand-framed between flushes. layout is a sequence of 'D' (one-datum data
// block, datums "d0", "d1", ... in order) and 'E' (count-0 block carrying
// emptyPayload). The writer never emits empty blocks itself, so 'E' bytes
// are spliced directly into the output between sealed blocks.
func foreignFile(t *testing.T, codec Codec, layout string, emptyPayload []byte) ([]byte, []string) {
	t.Helper()
	s := avro.MustParse(`"string"`)
	var buf bytes.Buffer
	opts := []WriterOpt{WithSyncMarker(foreignSync)}
	if codec != nil {
		// NopCloser: codec instances are shared across cells; Writer.Close
		// must not release them.
		opts = append(opts, WithCodec(NopCloser(codec)))
	}
	w := mustNewWriter(t, &buf, s, opts...)
	var want []string
	for _, ch := range layout {
		switch ch {
		case 'D':
			d := fmt.Sprintf("d%d", len(want))
			if err := w.Encode(d); err != nil {
				t.Fatalf("Encode: %v", err)
			}
			mustFlush(t, w)
			want = append(want, d)
		case 'E':
			appendRawBlock(&buf, 0, emptyPayload, foreignSync)
		default:
			t.Fatalf("bad layout char %q", ch)
		}
	}
	mustClose(t, w)
	return buf.Bytes(), want
}

// readAllStrings drives a Reader over file to io.EOF, returning every datum.
func readAllStrings(t *testing.T, file []byte) []string {
	t.Helper()
	r := mustNewReader(t, bytes.NewReader(file))
	defer r.Close()
	var got []string
	for {
		var v string
		err := r.Decode(&v)
		if err == io.EOF {
			return got
		}
		if err != nil {
			t.Fatalf("Decode after %d records: %v", len(got), err)
		}
		got = append(got, v)
	}
}

// fastavroOCFReader starts the repo's fastavro oracle and returns a function
// that reads a whole OCF's bytes through fastavro's record iterator,
// returning (values, "") on success or (nil, error message) when fastavro
// rejects the file. Returns nil (and the caller skips fastavro
// cross-checks) when no python/fastavro is available; set
// AVRO_FASTAVRO_PYTHON to point at an interpreter with fastavro installed.
func fastavroOCFReader(t *testing.T) func(file []byte) ([]string, string) {
	t.Helper()
	py := os.Getenv("AVRO_FASTAVRO_PYTHON")
	if py == "" {
		py = "python3"
	}
	if _, err := exec.LookPath(py); err != nil {
		t.Logf("python %q not found; fastavro cross-checks skipped (set AVRO_FASTAVRO_PYTHON)", py)
		return nil
	}
	if err := exec.Command(py, "-c", "import fastavro").Run(); err != nil {
		t.Logf("%q has no fastavro; cross-checks skipped (set AVRO_FASTAVRO_PYTHON)", py)
		return nil
	}
	cmd := exec.Command(py, filepath.Join("..", "testdata", "oracle", "fastavro_oracle.py"))
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting fastavro oracle: %v", err)
	}
	t.Cleanup(func() {
		in.Close()
		cmd.Wait()
	})
	sc := bufio.NewScanner(out)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	return func(file []byte) ([]string, string) {
		req, err := json.Marshal(map[string]any{"op": "ocf", "hex": hex.EncodeToString(file)})
		if err != nil {
			t.Fatalf("marshal oracle job: %v", err)
		}
		if _, err := fmt.Fprintf(in, "%s\n", req); err != nil {
			t.Fatalf("write to fastavro oracle: %v", err)
		}
		if !sc.Scan() {
			t.Fatalf("fastavro oracle exited early: %v", sc.Err())
		}
		var resp struct {
			OK     bool   `json:"ok"`
			Err    string `json:"err"`
			Fatal  string `json:"fatal"`
			Values []any  `json:"values"`
		}
		if err := json.Unmarshal(sc.Bytes(), &resp); err != nil {
			t.Fatalf("bad oracle response %q: %v", sc.Bytes(), err)
		}
		if resp.Fatal != "" {
			t.Fatalf("fastavro oracle fatal: %s", resp.Fatal)
		}
		if !resp.OK {
			return nil, resp.Err
		}
		vals := make([]string, 0, len(resp.Values))
		for _, v := range resp.Values {
			s, ok := v.(string)
			if !ok {
				t.Fatalf("fastavro returned non-string record %T (%v)", v, v)
			}
			vals = append(vals, s)
		}
		return vals, ""
	}
}

// codecUnsupportedByFastavro reports whether errMsg is fastavro's
// missing-optional-dependency error (e.g. snappy without cramjam) — an
// environment limitation, not a divergence. When AVRO_FASTAVRO_PYTHON is
// explicitly set, that limitation FAILS the test instead of skipping:
// these cells log-and-continue rather than t.Skip, so a venv that loses
// cramjam/zstandard silently thins the differential while every skip
// count still reads zero. Only the opportunistic python3 fallback (env
// unset) may skip quietly.
func codecUnsupportedByFastavro(errMsg string) bool {
	return strings.Contains(errMsg, "need to install")
}

func TestReaderForeignEmptyBlockFraming(t *testing.T) {
	fa := fastavroOCFReader(t)

	type payloadCase struct {
		name    string
		payload func(c Codec) []byte
		// fastavroReads: whether fastavro's iterator reads files carrying
		// this empty-block payload — CALIBRATED against fastavro 1.12.2
		// (cramjam snappy, python-zstandard), with each cell's observed
		// verdict recorded on its table row. fastavro decompresses a
		// count-0 block's payload eagerly (Java's DataFileStream does the
		// same via decompressUsing in hasNext); this reader skips the block
		// without consulting the codec, so it reads every reject-verdict
		// cell fully — deliberate leniency, no records lost either way.
		fastavroReads bool
	}
	nilPayload := func(Codec) []byte { return nil }
	// The codec's own compression of zero bytes — a payload the matching
	// decompressor accepts and inflates to nothing.
	compressedEmpty := func(c Codec) []byte {
		p, err := c.Compress(nil)
		if err != nil {
			t.Fatalf("Compress(nil): %v", err)
		}
		return p
	}
	// Bytes no codec produced.
	garbagePayload := func(Codec) []byte { return []byte{0xDE, 0xAD, 0xBE, 0xEF} }

	codecs := []struct {
		name     string
		codec    Codec // nil = null (the default; header omits avro.codec)
		payloads []payloadCase
	}{
		{"null", nil, []payloadCase{
			{"size0", nilPayload, true},
			// The identity codec has no framing to violate: any payload is
			// "valid", fastavro wraps the raw bytes without inspecting them.
			{"arbitrary", garbagePayload, true},
		}},
		{"deflate", DeflateCodec(flate.DefaultCompression), []payloadCase{
			// Raw-inflate of zero bytes yields zero bytes, so fastavro reads.
			{"size0", nilPayload, true},
			{"compressed-empty", compressedEmpty, true},
			// Observed: "Error -3 while decompressing data: invalid block type".
			{"garbage", garbagePayload, false},
		}},
		{"snappy", SnappyCodec(), []payloadCase{
			// Observed: "snappy: corrupt input (expected valid offset but got
			// offset 1027; dst position: 0)" — the empty payload has no CRC
			// tail to slice off, and cramjam rejects the remainder.
			{"size0", nilPayload, false},
			{"compressed-empty", compressedEmpty, true},
			// Observed: "snappy: corrupt input (empty)".
			{"garbage", garbagePayload, false},
		}},
		{"zstandard", MustZstdCodec(nil, nil), []payloadCase{
			// Observed: "Compressed data ended before the end-of-stream
			// marker was reached" — python-zstandard's stream reader wants a
			// complete frame even when there are no records to decode.
			{"size0", nilPayload, false},
			// Observed: the same end-of-stream-marker rejection even for
			// klauspost's well-formed empty frame.
			{"compressed-empty", compressedEmpty, false},
			// Observed: "Unable to decompress Zstandard data: Unknown frame
			// descriptor".
			{"garbage", garbagePayload, false},
		}},
	}
	defer func() {
		for _, c := range codecs {
			if c.codec != nil {
				c.codec.Close()
			}
		}
	}()

	positions := []struct{ name, layout string }{
		{"first", "EDD"},
		{"mid", "DED"},
		{"tail", "DDE"},
		{"consecutive", "DEEED"},
	}

	// Per-codec fastavro support probe: a plain twmb-written file (no
	// foreign framing). An "install" error means the interpreter lacks the
	// codec's optional dependency — skip fastavro for those cells. Any
	// other error is a real differential failure.
	faSupported := map[string]bool{}
	if fa != nil {
		for _, c := range codecs {
			plain, want := foreignFile(t, c.codec, "DD", nil)
			got, errMsg := fa(plain)
			switch {
			case errMsg == "":
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Errorf("codec %s: fastavro read plain twmb file as %v, want %v", c.name, got, want)
				}
				faSupported[c.name] = true
			case codecUnsupportedByFastavro(errMsg):
				if os.Getenv("AVRO_FASTAVRO_PYTHON") != "" {
					t.Errorf("codec %s: fastavro is missing an optional dependency with AVRO_FASTAVRO_PYTHON set (%s) — `pip install cramjam zstandard` into that interpreter so the cross-checks execute", c.name, errMsg)
				} else {
					t.Logf("codec %s: fastavro missing optional dependency (%s); cross-checks skipped", c.name, errMsg)
				}
			default:
				t.Errorf("codec %s: fastavro failed to read a plain twmb-written file: %s", c.name, errMsg)
			}
		}
	}

	for _, c := range codecs {
		for _, pc := range c.payloads {
			for _, pos := range positions {
				t.Run(fmt.Sprintf("%s/%s/%s", c.name, pc.name, pos.name), func(t *testing.T) {
					file, want := foreignFile(t, c.codec, pos.layout, pc.payload(c.codec))
					got := readAllStrings(t, file)
					if fmt.Sprint(got) != fmt.Sprint(want) {
						t.Fatalf("twmb read %v, want %v", got, want)
					}
					if fa == nil || !faSupported[c.name] {
						return
					}
					faGot, faErr := fa(file)
					if pc.fastavroReads {
						if faErr != "" {
							t.Errorf("fastavro rejected a cell it is expected to read: %s", faErr)
						} else if fmt.Sprint(faGot) != fmt.Sprint(want) {
							t.Errorf("fastavro read %v, want %v", faGot, want)
						}
					} else {
						// Documented divergence, recorded not asserted: log
						// drift if a fastavro upgrade starts accepting.
						if faErr == "" {
							t.Logf("fastavro now READS this cell (historically rejected); values=%v", faGot)
						} else {
							t.Logf("fastavro verdict (expected reject): %s", faErr)
						}
					}
				})
			}
		}
	}

	// Hostile and non-canonical block-COUNT / block-SIZE values, spliced at
	// the mid position (D <cell> D) — the position where a wrong verdict
	// silently truncates the data behind the cell. The invariant every cell
	// asserts: a loud error or a consistent skip, never a silent io.EOF
	// before the tail datum.
	t.Run("count-values", func(t *testing.T) {
		vi := func(v int64) []byte { return binary.AppendVarint(nil, v) }
		s := avro.MustParse(`"string"`)
		datum := mustAppendEncode(t, s, nil, "dX")
		cells := []struct {
			name string
			raw  []byte // hand-framed cell bytes spliced between the two data blocks
			// wantErr: substring of the error Decode must return at the cell;
			// "" = consistent skip (both data blocks read, then clean io.EOF).
			wantErr string
		}{
			{
				// A negative count is corruption no writer produces (the
				// spec's count is the number of objects in the block): loud
				// error. Guard: readBlock's `count < 0` reject, reached after
				// both header varints are read and before any payload byte is
				// consumed. fastavro 1.12.2 instead reads the file fully: its
				// record loop `for i in range(block_count)` over a negative
				// count is an empty loop (_read_py.py _iter_avro_records), so
				// it skips the block like a count-0 one — nothing is
				// truncated on either side.
				name:    "negative-count",
				raw:     bytes.Join([][]byte{vi(-1), vi(0), foreignSync[:]}, nil),
				wantErr: "invalid negative block count",
			},
			{
				// A negative size errors even when the count is 0: the
				// count/size guards precede the skip arm, so an "empty" block
				// cannot smuggle a hostile size. Guard: readBlock's
				// `size < 0` reject, before any payload read — the cell
				// deliberately carries no payload or sync; the reader must
				// never get that far. fastavro 1.12.2 also errors (EOFError
				// "Expected -1 bytes").
				name:    "negative-size-on-count0",
				raw:     bytes.Join([][]byte{vi(0), vi(-1)}, nil),
				wantErr: "invalid negative block size",
			},
			{
				// An absurd count over a tiny real payload reaches the
				// deepest guard: the envelope validates (count/size guards
				// pass, payload and sync consumed), the skip arm doesn't fire
				// (count != 0), the codec decompresses, and the
				// count-vs-decompressed-length cap rejects — the
				// CPU-amplification guard (a 7-byte varint must not buy a
				// 2^40-iteration decode loop). Guard: readBlock's
				// `count > int64(len(block))+maxOCFZeroByteSlack`. fastavro
				// 1.12.2 also errors (EOFError once the 3-byte block runs
				// dry).
				name:    "huge-count-tiny-block",
				raw:     bytes.Join([][]byte{vi(1 << 40), vi(int64(len(datum))), datum, foreignSync[:]}, nil),
				wantErr: "records but decompressed block is",
			},
			{
				// An overlong (non-minimal two-byte) varint encoding of count
				// 0: binary.ReadVarint accepts non-canonical varints, so the
				// cell decodes to 0 and takes the same validated-skip arm as
				// a canonical count-0 block — consistent skip, both datums
				// read. fastavro 1.12.2 reads it identically. Pinned so a
				// future varint tightening that rejects overlong counts flips
				// this cell red and forces a deliberate re-pin: a loud error
				// would also satisfy the invariant; silent truncation never
				// does.
				name:    "overlong-varint-count0",
				raw:     bytes.Join([][]byte{{0x80, 0x00}, vi(0), foreignSync[:]}, nil),
				wantErr: "",
			},
		}
		for _, c := range cells {
			t.Run(c.name, func(t *testing.T) {
				var buf bytes.Buffer
				w := mustNewWriter(t, &buf, s, WithSyncMarker(foreignSync))
				if err := w.Encode("d0"); err != nil {
					t.Fatal(err)
				}
				mustFlush(t, w)
				buf.Write(c.raw)
				if err := w.Encode("d1"); err != nil {
					t.Fatal(err)
				}
				mustClose(t, w)

				r := mustNewReader(t, bytes.NewReader(buf.Bytes()))
				defer r.Close()
				var v string
				if err := r.Decode(&v); err != nil || v != "d0" {
					t.Fatalf("first datum: %v %q", err, v)
				}
				err := r.Decode(&v)
				if c.wantErr == "" {
					if err != nil || v != "d1" {
						t.Fatalf("skip cell: second datum %v %q", err, v)
					}
					if err := r.Decode(&v); err != io.EOF {
						t.Fatalf("skip cell: want io.EOF after both datums, got %v", err)
					}
					return
				}
				if err == nil {
					t.Fatalf("want error containing %q at the cell, decoded %q", c.wantErr, v)
				}
				if errors.Is(err, io.EOF) {
					t.Fatalf("cell verdict is io.EOF — silent truncation of the tail datum: %v", err)
				}
				if !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("want error containing %q, got: %v", c.wantErr, err)
				}
			})
		}
	})

	// Corrupt sync on an empty block errors at sync validation, BEFORE the
	// skip arm — skipping must not weaken corruption detection.
	t.Run("corrupt-sync-on-empty", func(t *testing.T) {
		s := avro.MustParse(`"string"`)
		var buf bytes.Buffer
		w := mustNewWriter(t, &buf, s, WithSyncMarker(foreignSync))
		if err := w.Encode("d0"); err != nil {
			t.Fatal(err)
		}
		mustClose(t, w)
		bad := foreignSync
		bad[0] ^= 0xFF
		appendRawBlock(&buf, 0, nil, bad)
		datum := mustAppendEncode(t, s, nil, "d1")
		var rest bytes.Buffer
		appendRawBlock(&rest, 1, datum, foreignSync)
		buf.Write(rest.Bytes())

		r := mustNewReader(t, bytes.NewReader(buf.Bytes()))
		defer r.Close()
		var v string
		if err := r.Decode(&v); err != nil || v != "d0" {
			t.Fatalf("first datum: %v %q", err, v)
		}
		err := r.Decode(&v)
		if err == nil || !strings.Contains(err.Error(), "sync marker mismatch") {
			t.Fatalf("want sync marker mismatch on corrupt-sync empty block, got %v", err)
		}
	})

	// An all-empty-blocks file terminates in bounded time: one Decode call
	// walks every block (18 bytes each) and returns io.EOF — cost linear in
	// the input, no records, no hang.
	//
	// "Linear in the input" is a claim about the BLOCK COUNT, so the cell drives
	// two counts and asserts the ratio. It pinned 10,000 blocks under a 10s
	// ceiling, which is a hang detector wearing a cost assertion's clothes: a
	// reader that failed to advance past a count-0 block never returns at all,
	// and that is caught by the test binary's own timeout, while a reader that
	// merely rescanned from the file's start per block — the quadratic near
	// miss — walks 10,000 blocks in well under 10s and passed. Eight times the
	// blocks is eight times a linear walk and sixty-four times a rescanning one.
	t.Run("ten-thousand-empty-blocks", func(t *testing.T) {
		s := avro.MustParse(`"string"`)
		build := func(blocks int) []byte {
			var buf bytes.Buffer
			w := mustNewWriter(t, &buf, s, WithSyncMarker(foreignSync))
			mustClose(t, w)
			for range blocks {
				appendRawBlock(&buf, 0, nil, foreignSync)
			}
			return buf.Bytes()
		}
		wantAcceptScales(t, "Reader/all-empty-blocks", costScale{lo: 1_250, hi: 10_000, tol: 25, floor: 500 * time.Microsecond},
			func(blocks int) func() error {
				file := build(blocks)
				return func() error {
					if got := readAllStrings(t, file); len(got) != 0 {
						return fmt.Errorf("read %v from an all-empty file", got)
					}
					return nil
				}
			})
		if fa != nil && faSupported["null"] {
			file := build(10_000)
			faGot, faErr := fa(file)
			if faErr != "" || len(faGot) != 0 {
				t.Errorf("fastavro on all-empty file: values=%v err=%s", faGot, faErr)
			}
		}
	})
}

// TestReaderMetaMapFraming hand-frames the OCF HEADER's metadata map — the one
// wire map every reader must parse before it knows anything about the file —
// across the container framings and hostile values the spec's map grammar
// admits. The writer always emits a single-block canonical meta map, so foreign
// framings reach this parser only from other writers.
//
//   - Duplicate keys: the spec is silent; Java's DataFileStream reads meta into
//     a HashMap and fastavro's header lands in a dict, both last-wins, as is
//     decodeMap's m[key]=val. Pinned with the codec key, where a first-wins
//     regression would resolve the WRONG codec, and the schema key.
//   - Multi-block and size-prefixed framings: legal per the map grammar; must
//     parse identically to the canonical single block.
//   - MinInt64 block count: the negation-overflow guard must reject loudly.
//
// Every accept cell asserts the records read fully, and — when a fastavro
// interpreter is available — that fastavro reads the identical records.
func TestReaderMetaMapFraming(t *testing.T) {
	fa := fastavroOCFReader(t)

	schemaJSON := []byte(`"string"`)
	s := avro.MustParse(string(schemaJSON))
	datum := mustAppendEncode(t, s, nil, "d0")
	deflated := func() []byte {
		var b bytes.Buffer
		zw, _ := flate.NewWriter(&b, flate.DefaultCompression)
		zw.Write(datum)
		zw.Close()
		return b.Bytes()
	}()

	entry := func(k string, v []byte) []byte {
		e := binary.AppendVarint(nil, int64(len(k)))
		e = append(e, k...)
		e = binary.AppendVarint(e, int64(len(v)))
		e = append(e, v...)
		return e
	}
	// file assembles magic + the given meta-map bytes + sync + one data
	// block carrying payload.
	file := func(metaMap []byte, payload []byte) []byte {
		var buf bytes.Buffer
		buf.Write(magic[:])
		buf.Write(metaMap)
		buf.Write(foreignSync[:])
		appendRawBlock(&buf, 1, payload, foreignSync)
		return buf.Bytes()
	}
	block := func(entries ...[]byte) []byte {
		b := binary.AppendVarint(nil, int64(len(entries)))
		for _, e := range entries {
			b = append(b, e...)
		}
		return b
	}
	terminator := []byte{0x00}

	schemaEntry := entry("avro.schema", schemaJSON)

	accepts := []struct {
		name    string
		metaMap []byte
		payload []byte
	}{
		{
			// Two avro.codec entries: an unresolvable name, then null. Only
			// last-wins reads this file; first-wins fails codec resolution.
			name: "dup-codec-bogus-then-null",
			metaMap: append(block(
				schemaEntry,
				entry("avro.codec", []byte("bogus")),
				entry("avro.codec", []byte("null")),
			), terminator...),
			payload: datum,
		},
		{
			// Two avro.codec entries naming two REAL codecs, with the data
			// block compressed by the second: only the last-wins winner
			// decompresses it.
			name: "dup-codec-null-then-deflate",
			metaMap: append(block(
				schemaEntry,
				entry("avro.codec", []byte("null")),
				entry("avro.codec", []byte("deflate")),
			), terminator...),
			payload: deflated,
		},
		{
			// Two avro.schema entries: garbage JSON, then the real schema.
			name: "dup-schema-bogus-then-valid",
			metaMap: append(block(
				entry("avro.schema", []byte("{not json")),
				schemaEntry,
			), terminator...),
			payload: datum,
		},
		{
			// The meta map split across two blocks.
			name: "meta-two-blocks",
			metaMap: append(append(
				block(schemaEntry),
				block(entry("avro.codec", []byte("null")))...,
			), terminator...),
			payload: datum,
		},
		{
			// Negative-count size-prefixed meta block (count -2 + byte size).
			name: "meta-size-prefixed-block",
			metaMap: func() []byte {
				entries := append(append([]byte{}, schemaEntry...), entry("avro.codec", []byte("null"))...)
				m := binary.AppendVarint(nil, -2)
				m = binary.AppendVarint(m, int64(len(entries)))
				m = append(m, entries...)
				return append(m, terminator...)
			}(),
			payload: datum,
		},
	}
	for _, c := range accepts {
		t.Run(c.name, func(t *testing.T) {
			f := file(c.metaMap, c.payload)
			got := readAllStrings(t, f)
			if len(got) != 1 || got[0] != "d0" {
				t.Fatalf("read %v, want [d0]", got)
			}
			if fa != nil {
				faGot, faErr := fa(f)
				if faErr != "" || len(faGot) != 1 || faGot[0] != "d0" {
					t.Errorf("fastavro: values=%v err=%s", faGot, faErr)
				}
			}
		})
	}

	t.Run("meta-minint64-count", func(t *testing.T) {
		// MinInt64's negation is itself: the guard must reject before the
		// count drives anything. The negative-count grammar puts a byte size
		// next (present so foreign readers see well-formed framing); the
		// reject fires before it is read. fastavro 1.12.2 also errors (its
		// count-driven header read walks entry reads into EOF).
		m := binary.AppendVarint(nil, int64(-1)<<63)
		m = binary.AppendVarint(m, 0)
		m = append(m, terminator...)
		f := file(m, datum)
		_, err := NewReader(bytes.NewReader(f))
		if err == nil {
			t.Fatal("reader accepted a MinInt64 meta-map block count")
		}
		if !strings.Contains(err.Error(), "invalid metadata map block count") {
			t.Fatalf("error %q, want the metadata block-count reject", err)
		}
		if fa != nil {
			if faGot, faErr := fa(f); faErr == "" {
				t.Errorf("fastavro read the MinInt64-meta-count file: %v (recalibrate this cell)", faGot)
			}
		}
	})
}

// ---------- foreign_writer_differential_test.go ----------

// ---------------------------------------------------------------------------
// FOREIGN writer: whole container files produced by fastavro's WRITER, read back
// by this package. Everything upstream of the record bytes is foreign — the
// header carries fastavro's own rendering of the schema, block sizing follows
// its sync_interval accounting, and each codec's framing is the real library
// implementation (cramjam snappy with its 4-byte big-endian CRC suffix,
// python-zstandard frames, zlib raw-deflate, stdlib bzip2/xz).
//
// The per-file contract: every record read back exactly (byte parity through a
// re-encode), the header schema canonically equal to the schema it was written
// with, user metadata surfaced, and clean io.EOF at the end. Append mode must
// extend a foreign file so BOTH implementations read the combined records, and
// WithReaderSchema must resolve over a foreign header.
// ---------------------------------------------------------------------------

// foreignOracle is a long-lived fastavro subprocess speaking the repo
// oracle's line protocol; this file uses its container ops (ocfwrite,
// ocfread), which transport records as schemaless Avro bytes so no
// cross-language value coercion is involved. Calls take the CALLING
// (sub)test's t so a cell failure fails that cell, not the parent.
type foreignOracle struct {
	in io.WriteCloser
	sc *bufio.Scanner
}

type foreignOracleResp struct {
	OK      bool     `json:"ok"`
	Err     string   `json:"err"`
	Fatal   string   `json:"fatal"`
	Hex     string   `json:"hex"`
	Records []string `json:"records"`
}

// startForeignOracle skips the calling test when no fastavro interpreter is
// available; set AVRO_FASTAVRO_PYTHON to run the differential.
func startForeignOracle(t *testing.T) *foreignOracle {
	t.Helper()
	py := os.Getenv("AVRO_FASTAVRO_PYTHON")
	if py == "" {
		py = "python3"
	}
	if _, err := exec.LookPath(py); err != nil {
		t.Skipf("python %q not found; set AVRO_FASTAVRO_PYTHON to run the foreign-writer differential", py)
	}
	if err := exec.Command(py, "-c", "import fastavro").Run(); err != nil {
		t.Skipf("%q has no fastavro; set AVRO_FASTAVRO_PYTHON to run the foreign-writer differential", py)
	}
	cmd := exec.Command(py, filepath.Join("..", "testdata", "oracle", "fastavro_oracle.py"))
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting fastavro oracle: %v", err)
	}
	t.Cleanup(func() {
		in.Close()
		cmd.Wait()
	})
	sc := bufio.NewScanner(out)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	return &foreignOracle{in: in, sc: sc}
}

func (o *foreignOracle) call(t *testing.T, job map[string]any) foreignOracleResp {
	t.Helper()
	req, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal oracle job: %v", err)
	}
	if _, err := fmt.Fprintf(o.in, "%s\n", req); err != nil {
		t.Fatalf("write to fastavro oracle: %v", err)
	}
	if !o.sc.Scan() {
		t.Fatalf("fastavro oracle exited early: %v", o.sc.Err())
	}
	var resp foreignOracleResp
	if err := json.Unmarshal(o.sc.Bytes(), &resp); err != nil {
		t.Fatalf("bad oracle response %q: %v", o.sc.Bytes(), err)
	}
	if resp.Fatal != "" {
		t.Fatalf("fastavro oracle fatal: %s", resp.Fatal)
	}
	return resp
}

// requireOK fails the (sub)test on an oracle error. A missing optional
// codec dependency (cramjam, zstandard) is an environment limitation, not a
// divergence — but with AVRO_FASTAVRO_PYTHON explicitly set the environment
// is a deliberate oracle choice and the missing dep fails loudly rather
// than thinning the differential; only the opportunistic python3 fallback
// may skip.
func (o *foreignOracle) requireOK(t *testing.T, resp foreignOracleResp, what string) {
	t.Helper()
	if resp.OK {
		return
	}
	if codecUnsupportedByFastavro(resp.Err) {
		if os.Getenv("AVRO_FASTAVRO_PYTHON") != "" {
			t.Fatalf("%s: fastavro is missing an optional dependency with AVRO_FASTAVRO_PYTHON set (%s) - `pip install cramjam zstandard` into that interpreter so the cells execute", what, resp.Err)
		}
		t.Skipf("%s: fastavro missing optional dependency (%s)", what, resp.Err)
	}
	t.Fatalf("%s: %s", what, resp.Err)
}

// encodeSchemaless encodes every value with twmb's schemaless encoder.
// These bytes are both the transport into the oracle's writer and the
// independent per-record byte oracle the read legs compare against.
func encodeSchemaless(t *testing.T, s *avro.Schema, values []any) (raw [][]byte, hexes []string) {
	t.Helper()
	for i, v := range values {
		b, err := s.AppendEncode(nil, v)
		if err != nil {
			t.Fatalf("twmb encode record %d (%T): %v", i, v, err)
		}
		raw = append(raw, b)
		hexes = append(hexes, hex.EncodeToString(b))
	}
	return raw, hexes
}

// fastavroWriteFile has the oracle's writer produce a whole OCF from
// twmb-encoded records.
func fastavroWriteFile(t *testing.T, o *foreignOracle, schema string, recordHexes []string, codec string, syncInterval int, meta map[string]string) []byte {
	t.Helper()
	job := map[string]any{
		"op":      "ocfwrite",
		"schema":  json.RawMessage(schema),
		"records": recordHexes,
		"codec":   codec,
	}
	if syncInterval > 0 {
		job["syncInterval"] = syncInterval
	}
	if len(meta) > 0 {
		job["meta"] = meta
	}
	resp := o.call(t, job)
	o.requireOK(t, resp, "fastavro ocfwrite "+codec)
	file, err := hex.DecodeString(resp.Hex)
	if err != nil {
		t.Fatalf("bad file hex from oracle: %v", err)
	}
	return file
}

// verifyForeignFile drives this package's reader over a foreign file and
// asserts the full read-side contract: canonical schema parity, metadata
// surfacing, per-record byte parity via re-encode, terminal io.EOF, and —
// when minBlocks > 0 — that the file really is framed into at least that
// many data blocks (the sync marker trails the header and every block, so
// a multiblock cell that silently degenerated to one block is caught
// rather than passing vacuously).
func verifyForeignFile(t *testing.T, file []byte, s *avro.Schema, wantRecs [][]byte, wantMeta map[string]string, minBlocks int) {
	t.Helper()
	rd, err := NewReader(bytes.NewReader(file))
	if err != nil {
		t.Fatalf("NewReader on fastavro-written file: %v", err)
	}
	defer rd.Close()
	if got, want := rd.Schema().Canonical(), s.Canonical(); !bytes.Equal(got, want) {
		t.Fatalf("header schema canonical mismatch:\n got  %s\n want %s", got, want)
	}
	for k, v := range wantMeta {
		if got, ok := rd.Metadata()[k]; !ok || !bytes.Equal(got, []byte(v)) {
			t.Fatalf("metadata %q: got %q (present=%v), want %q", k, got, ok, v)
		}
	}
	for i, want := range wantRecs {
		var got any
		if err := rd.Decode(&got); err != nil {
			t.Fatalf("Decode record %d: %v", i, err)
		}
		re, err := s.AppendEncode(nil, got)
		if err != nil {
			t.Fatalf("re-encode record %d (%T): %v", i, got, err)
		}
		if !bytes.Equal(re, want) {
			t.Fatalf("record %d byte mismatch:\n got  %x\n want %x", i, re, want)
		}
	}
	var sink any
	if err := rd.Decode(&sink); err != io.EOF {
		t.Fatalf("after %d records want io.EOF, got %v", len(wantRecs), err)
	}
	if minBlocks > 0 {
		sync := file[len(file)-16:]
		if n := bytes.Count(file, sync); n < minBlocks+1 {
			t.Fatalf("sync marker appears %d times, want >= %d (header + >=%d blocks)", n, minBlocks+1, minBlocks)
		}
	}
}

const foreignRichSchema = `{"type":"record","name":"Rich","fields":[
	{"name":"s","type":"string"},
	{"name":"i","type":"int"},
	{"name":"l","type":"long"},
	{"name":"f","type":"float"},
	{"name":"d","type":"double"},
	{"name":"b","type":"boolean"},
	{"name":"u","type":["null","long"]},
	{"name":"by","type":"bytes"},
	{"name":"fx","type":{"type":"fixed","name":"Fx","size":4}},
	{"name":"e","type":{"type":"enum","name":"En","symbols":["A","B","C"]}},
	{"name":"arr","type":{"type":"array","items":"int"}},
	{"name":"m","type":{"type":"map","values":"string"}}]}`

const foreignDoubleSchema = `{"type":"record","name":"Dbl","fields":[
	{"name":"d1","type":"double"},
	{"name":"d2","type":"double"},
	{"name":"d3","type":"double"},
	{"name":"d4","type":"double"}]}`

const foreignLogicalSchema = `{"type":"record","name":"Lg","fields":[
	{"name":"dec","type":{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}},
	{"name":"dt","type":{"type":"int","logicalType":"date"}},
	{"name":"tsm","type":{"type":"long","logicalType":"timestamp-millis"}},
	{"name":"tsu","type":{"type":"long","logicalType":"timestamp-micros"}}]}`

func foreignStringValues(n int) []any {
	vs := make([]any, n)
	for i := range vs {
		vs[i] = fmt.Sprintf("rec-%03d", i)
	}
	return vs
}

func TestDifferentialFastavroOCFForeignWriter(t *testing.T) {
	o := startForeignOracle(t)

	richValues := []any{
		map[string]any{
			"s": "one", "i": int32(7), "l": int64(1 << 40), "f": float32(1.5),
			"d": 2.25, "b": true, "u": int64(9),
			"by": []byte{0x00, 0x01, 0xfe}, "fx": []byte{0xde, 0xad, 0xbe, 0xef},
			"e": "B", "arr": []int32{1, -2, 3}, "m": map[string]string{"k": "v"},
		},
		map[string]any{
			"s": "two", "i": int32(-1), "l": int64(-5), "f": float32(-0.25),
			"d": -3.5, "b": false, "u": nil,
			"by": []byte{}, "fx": []byte{0, 0, 0, 0},
			"e": "C", "arr": []int32{}, "m": map[string]string{},
		},
	}
	doubleValues := []any{
		map[string]any{"d1": math.NaN(), "d2": math.Inf(1), "d3": math.Inf(-1), "d4": math.Copysign(0, -1)},
	}
	logicalValues := []any{
		map[string]any{
			"dec": big.NewRat(12345, 100),
			"dt":  time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
			"tsm": time.Date(2024, 3, 1, 12, 30, 15, 250_000_000, time.UTC),
			"tsu": time.Date(2024, 3, 1, 12, 30, 15, 250_125_000, time.UTC),
		},
	}

	cells := []struct {
		name         string
		schema       string
		values       []any
		codec        string
		syncInterval int
		minBlocks    int
		meta         map[string]string
	}{
		{name: "null-plain", schema: `"string"`, values: foreignStringValues(5), codec: "null", minBlocks: 1},
		{name: "deflate-plain", schema: `"string"`, values: foreignStringValues(5), codec: "deflate", minBlocks: 1},
		{name: "snappy-plain", schema: `"string"`, values: foreignStringValues(5), codec: "snappy", minBlocks: 1},
		{name: "zstandard-plain", schema: `"string"`, values: foreignStringValues(5), codec: "zstandard", minBlocks: 1},

		// A tiny sync_interval forces fastavro to seal many small blocks:
		// foreign block sizing, verified non-degenerate via minBlocks.
		{name: "null-multiblock", schema: `"string"`, values: foreignStringValues(60), codec: "null", syncInterval: 64, minBlocks: 3},
		{name: "deflate-multiblock", schema: `"string"`, values: foreignStringValues(60), codec: "deflate", syncInterval: 64, minBlocks: 3},
		{name: "snappy-multiblock", schema: `"string"`, values: foreignStringValues(60), codec: "snappy", syncInterval: 64, minBlocks: 3},
		{name: "zstandard-multiblock", schema: `"string"`, values: foreignStringValues(60), codec: "zstandard", syncInterval: 64, minBlocks: 3},

		// Zero records: header-only (or empty-block) file, immediate EOF.
		{name: "null-empty", schema: `"string"`, values: nil, codec: "null"},
		{name: "deflate-empty", schema: `"string"`, values: nil, codec: "deflate"},
		{name: "snappy-empty", schema: `"string"`, values: nil, codec: "snappy"},
		{name: "zstandard-empty", schema: `"string"`, values: nil, codec: "zstandard"},

		// Foreign schema SPELLING: fastavro re-renders this nested record
		// (fully-qualified names, object-wrapped types) in the header; the
		// reader must parse that rendering and agree canonically.
		{name: "rich-schema-deflate", schema: foreignRichSchema, values: richValues, codec: "deflate", minBlocks: 1},

		// Non-finite and signed-zero doubles must survive both writers'
		// framing bit-for-bit (compression crosses the raw IEEE payloads).
		{name: "special-doubles-null", schema: foreignDoubleSchema, values: doubleValues, codec: "null", minBlocks: 1},
		{name: "special-doubles-zstandard", schema: foreignDoubleSchema, values: doubleValues, codec: "zstandard", minBlocks: 1},

		// Logical types: fastavro materializes Decimal/date/datetime and its
		// writer re-encodes them; byte parity proves neither side drifts on
		// the logical representations.
		{name: "logicals-snappy", schema: foreignLogicalSchema, values: logicalValues, codec: "snappy", minBlocks: 1},

		// User metadata written by fastavro must surface from Metadata().
		{name: "custom-meta-null", schema: `"string"`, values: foreignStringValues(2), codec: "null",
			meta: map[string]string{"user.key": "user-value", "app": "corpus-v1"}},
	}

	for _, c := range cells {
		t.Run("corpus/"+c.name, func(t *testing.T) {
			s := avro.MustParse(c.schema)
			raw, hexes := encodeSchemaless(t, s, c.values)
			file := fastavroWriteFile(t, o, c.schema, hexes, c.codec, c.syncInterval, c.meta)
			verifyForeignFile(t, file, s, raw, c.meta, c.minBlocks)
		})
	}

	// Append-onto-foreign: NewAppendWriter adopts the foreign header's
	// schema, codec, and sync marker; after appending, BOTH implementations
	// must read the combined file exactly.
	for _, codec := range []string{"null", "deflate", "snappy", "zstandard"} {
		t.Run("append-onto-foreign/"+codec, func(t *testing.T) {
			s := avro.MustParse(`"string"`)
			baseVals := foreignStringValues(5)
			appendVals := []any{"appended-0", "appended-1", "appended-2"}
			allVals := append(append([]any{}, baseVals...), appendVals...)
			allRaw, allHexes := encodeSchemaless(t, s, allVals)
			file := fastavroWriteFile(t, o, `"string"`, allHexes[:5], codec, 0, nil)

			sb := &seekBuf{data: append([]byte(nil), file...)}
			w, err := NewAppendWriter(sb)
			if err != nil {
				t.Fatalf("NewAppendWriter on fastavro-written file: %v", err)
			}
			for i, v := range appendVals {
				if err := w.Encode(v); err != nil {
					t.Fatalf("append Encode %d: %v", i, err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatalf("append Close: %v", err)
			}

			verifyForeignFile(t, sb.data, s, allRaw, nil, 2)

			resp := o.call(t, map[string]any{"op": "ocfread", "hex": hex.EncodeToString(sb.data)})
			o.requireOK(t, resp, "fastavro ocfread of appended "+codec+" file")
			if len(resp.Records) != len(allRaw) {
				t.Fatalf("fastavro read %d records from the appended file, want %d", len(resp.Records), len(allRaw))
			}
			for i, h := range resp.Records {
				if h != allHexes[i] {
					t.Fatalf("fastavro record %d byte mismatch:\n got  %s\n want %s", i, h, allHexes[i])
				}
			}
		})
	}

	// WithReaderSchema over a foreign header: promotion (int->long) and a
	// reader-added defaulted field resolve against fastavro's spelling of
	// the writer schema.
	t.Run("reader-schema-over-foreign", func(t *testing.T) {
		const writer = `{"type":"record","name":"R","fields":[
			{"name":"a","type":"int"},
			{"name":"b","type":"string"}]}`
		const reader = `{"type":"record","name":"R","fields":[
			{"name":"a","type":"long"},
			{"name":"b","type":"string"},
			{"name":"c","type":"string","default":"dflt"}]}`
		ws := avro.MustParse(writer)
		rs := avro.MustParse(reader)
		vals := []any{
			map[string]any{"a": int32(0), "b": "b0"},
			map[string]any{"a": int32(1), "b": "b1"},
			map[string]any{"a": int32(2), "b": "b2"},
		}
		_, hexes := encodeSchemaless(t, ws, vals)
		file := fastavroWriteFile(t, o, writer, hexes, "deflate", 0, nil)

		rd, err := NewReader(bytes.NewReader(file), WithReaderSchema(rs))
		if err != nil {
			t.Fatalf("NewReader(WithReaderSchema) on fastavro-written file: %v", err)
		}
		defer rd.Close()
		for i := range vals {
			var got map[string]any
			if err := rd.Decode(&got); err != nil {
				t.Fatalf("resolved Decode %d: %v", i, err)
			}
			if a, ok := got["a"].(int64); !ok || a != int64(i) {
				t.Fatalf("record %d field a: got %T %v, want int64 %d (int->long promotion)", i, got["a"], got["a"], i)
			}
			if b := got["b"]; b != fmt.Sprintf("b%d", i) {
				t.Fatalf("record %d field b: got %v", i, b)
			}
			if cv := got["c"]; cv != "dflt" {
				t.Fatalf("record %d field c: got %T %v, want reader default %q", i, cv, cv, "dflt")
			}
		}
		var sink map[string]any
		if err := rd.Decode(&sink); err != io.EOF {
			t.Fatalf("want io.EOF after %d records, got %v", len(vals), err)
		}
	})

	// Spec codecs this package does not implement must reject loudly at
	// NewReader — naming the codec — never misparse the compressed blocks.
	for _, codec := range []string{"bzip2", "xz"} {
		t.Run("unsupported-codec-reject/"+codec, func(t *testing.T) {
			_, hexes := encodeSchemaless(t, avro.MustParse(`"string"`), foreignStringValues(3))
			file := fastavroWriteFile(t, o, `"string"`, hexes, codec, 0, nil)
			_, err := NewReader(bytes.NewReader(file))
			if err == nil {
				t.Fatalf("NewReader accepted a %s-coded file; unsupported codecs must reject loudly", codec)
			}
			if !strings.Contains(err.Error(), "unknown codec") || !strings.Contains(err.Error(), codec) {
				t.Fatalf("reject error %q does not name the unsupported codec %q", err, codec)
			}
		})
	}
}

// ---------- fuzz_test.go ----------

// mustOCF builds a valid OCF with the given schema, values, and writer options.
func mustOCF(f *testing.F, schema *avro.Schema, values []any, opts ...WriterOpt) []byte {
	var buf bytes.Buffer
	w := mustNewWriter(f, &buf, schema, opts...)
	for _, v := range values {
		if err := w.Encode(v); err != nil {
			f.Fatal(err)
		}
	}
	mustClose(f, w)
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
		// Keep each execution fast and bounded so the reader LOGIC is what gets
		// explored, not throughput. Two bounds, both about fuzzer hygiene: cap the
		// input size, because the coordinator's minimization of an interesting
		// multi-MB OCF re-runs it dozens of times and trips the -fuzztime shutdown
		// deadline; and a tight WithMaxDecompressedBlockBytes, which bounds
		// per-exec decode work AND exercises the decompression-amplification
		// rejection.
		if len(data) > 256<<10 {
			return
		}
		r, err := NewReader(bytes.NewReader(data), WithMaxDecompressedBlockBytes(1<<20))
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

// FuzzOCFWriterHostile exercises the OCF writer against malformed and
// adversarial Go values: nil, wrong-type for the schema, NaN floats,
// non-string-keyed maps as records, and cyclic structures. The writer
// should return an error, never panic. Encoder cycle protection on
// the avro side is exercised here transitively via the writer.
func FuzzOCFWriterHostile(f *testing.F) {
	f.Add(uint8(0))
	f.Add(uint8(5))
	f.Add(uint8(11))

	f.Fuzz(func(t *testing.T, mode uint8) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic: %v", r)
			}
		}()
		schemas := []*avro.Schema{
			avro.MustParse(`"int"`),
			avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`),
			avro.MustParse(`{"type":"record","name":"Node","fields":[
				{"name":"v","type":"int"},
				{"name":"next","type":["null","Node"]}
			]}`),
		}
		// Build a value based on mode.
		var (
			s *avro.Schema
			v any
		)
		switch mode % 12 {
		case 0:
			s, v = schemas[0], nil
		case 1:
			s, v = schemas[0], "string mismatched against int"
		case 2:
			s, v = schemas[0], int32(42)
		case 3:
			s, v = schemas[1], map[string]any{"a": "wrong type"}
		case 4:
			s, v = schemas[1], map[int]int{1: 2} // non-string-keyed
		case 5:
			s, v = schemas[1], map[string]any{"a": int32(1)}
		case 6:
			// Cyclic against recursive schema.
			node := map[string]any{"v": int32(1)}
			node["next"] = node
			s, v = schemas[2], node
		case 7:
			s, v = schemas[1], map[string]any{} // missing required field
		case 8:
			s, v = schemas[0], any(nil)
		case 9:
			s, v = schemas[2], map[string]any{"v": int32(1), "next": nil}
		case 10:
			s, v = schemas[1], int32(1) // wrong shape
		case 11:
			s, v = schemas[2], map[string]any{} // missing required
		}
		var buf bytes.Buffer
		w, err := NewWriter(&buf, s)
		if err != nil {
			return
		}
		w.Encode(v)
		w.Close()
	})
}

// validOCFHeader returns a canonical OCF header for stringSchema with
// the null codec and the given sync marker. Used by the block-envelope
// fuzz so the reader gets past header parsing and the fuzz iterations
// can focus on the block-level state machine (count + size + data +
// sync) that the recent readBlock count=0 sync-validation fix lives
// inside. Without a fixed header up front, every iteration would burn
// time exploring header-parse rejections — the existing FuzzOCFReader
// already does that.
func validOCFHeader(sync [16]byte) []byte {
	// "Obj\x01" magic + metadata map + sync.
	// metadata map: { avro.codec: null, avro.schema: "\"string\"" }
	// Header is: magic, metadata block (count varint, items, 0
	// terminator), sync.
	codecKey := []byte("avro.codec")
	codecVal := []byte("null")
	schemaKey := []byte("avro.schema")
	schemaVal := []byte(`"string"`)
	out := []byte{'O', 'b', 'j', 0x01}
	// Block with 2 entries: count varint + entries + 0 terminator.
	out = binary.AppendVarint(out, 2)
	// codecKey + codecVal.
	out = binary.AppendVarint(out, int64(len(codecKey)))
	out = append(out, codecKey...)
	out = binary.AppendVarint(out, int64(len(codecVal)))
	out = append(out, codecVal...)
	// schemaKey + schemaVal.
	out = binary.AppendVarint(out, int64(len(schemaKey)))
	out = append(out, schemaKey...)
	out = binary.AppendVarint(out, int64(len(schemaVal)))
	out = append(out, schemaVal...)
	// Terminator.
	out = binary.AppendVarint(out, 0)
	// Sync marker.
	out = append(out, sync[:]...)
	return out
}

// FuzzOCFBlockEnvelope fuzzes the block-level state machine in readBlock: count
// varint, size varint, data, sync marker. The fuzz builds a valid header and then
// appends a fuzz-driven block payload, so every iteration explores the block
// envelope rather than the header parser. Targets the count=0 sync-validation path
// (TestRegression_BlockCountZeroValidatesSync) — pre-fix readBlock bailed at
// count==0 without reading size + sync, so a tail-truncated file whose count byte
// happened to read as 0 was silently accepted — and exercises the negative-count /
// negative-size / size>max guards (TestRegression_OCFBlockEnvelopeInvariant).
func FuzzOCFBlockEnvelope(f *testing.F) {
	// Seeds: each chosen to hit a specific control-flow arm.
	// (count, size, data, hasGoodSync) → fuzz format:
	//   [16]byte sync + varint count + varint size + size bytes data
	//   + 16-byte sync trailer.
	// We feed the post-header bytes to the fuzzer; the header (and
	// expected sync) is fixed at fuzz init.
	addCase := func(count, size int64, data []byte, syncMode uint8) {
		var sync [16]byte
		for i := range sync {
			sync[i] = byte(i) + 1
		}
		var trailer [16]byte
		switch syncMode {
		case 0:
			trailer = sync
		case 1:
			// Corrupt sync — should error with "sync marker mismatch".
		case 2:
			// Partial corrupt sync (last byte off).
			trailer = sync
			trailer[15] ^= 0xFF
		}
		blk := []byte{}
		blk = binary.AppendVarint(blk, count)
		blk = binary.AppendVarint(blk, size)
		blk = append(blk, data...)
		blk = append(blk, trailer[:]...)
		f.Add(blk, syncMode)
	}
	// count=0 + good sync: a validated empty block is skipped; at the
	// tail (as here) the next count read hits real EOF — clean end.
	addCase(0, 0, nil, 0)
	// count=0 + corrupt sync must error, not read as a clean end.
	addCase(0, 0, nil, 1)
	// count=0 with non-zero size and good sync — valid empty block; the
	// payload is consumed but never decompressed.
	addCase(0, 5, []byte("hello"), 0)
	// Negative count.
	addCase(-1, 0, nil, 0)
	// Negative size.
	addCase(1, -10, nil, 0)
	// Size > safety limit (64 MiB default — we encode a value past it).
	addCase(1, int64(1)<<27, nil, 0)
	// Valid one-item block holding a string("hi").
	addCase(1, 3, []byte{0x04, 'h', 'i'}, 0)
	// Empty.
	f.Add([]byte{}, uint8(0))

	// Fixed sync used by validOCFHeader.
	var fixedSync [16]byte
	for i := range fixedSync {
		fixedSync[i] = byte(i) + 1
	}
	header := validOCFHeader(fixedSync)

	f.Fuzz(func(t *testing.T, blockBytes []byte, _ uint8) {
		// Build: header + blockBytes.
		full := append(append([]byte{}, header...), blockBytes...)
		r, err := NewReader(bytes.NewReader(full))
		if err != nil {
			return
		}
		// Drive the reader to EOF or error; both are fine. The fuzz
		// only asserts no panic / no hang. A bounded loop guards
		// against any reader bug that could yield infinite zero-
		// length blocks.
		for range 10000 {
			var v any
			if err := r.Decode(&v); err != nil {
				break
			}
		}
		r.Close()
	})
}

// FuzzOCFWriterReaderCodecCycle drives the Writer.Close →
// codec.Close → Reader.NewReader → Reader.Close cycle through arbitrary
// codec selections and write counts. The pre-fix bug Writer.Close had
// (the codec was not closed when w.err was set) was caught by a
// regression test; this fuzz keeps the same surface under arbitrary
// codec × payload combinations so any future drift in either Close
// path produces a panic the fuzz will surface. Bonus: exercises the
// NewReader codec-close-on-error path (read-only header, mutated
// metadata) via a corruption oracle.
func FuzzOCFWriterReaderCodecCycle(f *testing.F) {
	schemas := []*avro.Schema{
		avro.MustParse(`"int"`),
		avro.MustParse(`"string"`),
		avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`),
	}
	// nil entry → default codec (null). Public codec constructors don't include a
	// NullCodec wrapper; the default already exercises it.
	//
	// The zstd codec uses minimum-footprint options because the fuzz constructs and
	// closes a codec PER EXECUTION — that lifecycle being the fuzzed surface — and
	// default-option zstd costs ~573µs + 1.64MB of garbage per cycle against ~126µs
	// + 0.30MB with these. At fuzz rates that churn keeps the GC saturated on small
	// CI runners, and a starved worker can miss the coordinator's shutdown deadline.
	// The options only shrink buffers and effort; the surface is unchanged.
	codecs := []func() WriterOpt{
		nil,
		func() WriterOpt { return WithCodec(DeflateCodec(1)) },
		func() WriterOpt { return WithCodec(SnappyCodec()) },
		func() WriterOpt {
			return WithCodec(MustZstdCodec(
				[]zstd.EOption{zstd.WithWindowSize(zstd.MinWindowSize), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithLowerEncoderMem(true)},
				[]zstd.DOption{zstd.WithDecoderLowmem(true)},
			))
		},
	}

	f.Add(uint8(0), uint8(0), uint16(0))
	f.Add(uint8(0), uint8(1), uint16(1))
	f.Add(uint8(1), uint8(2), uint16(5))
	f.Add(uint8(2), uint8(3), uint16(3))
	f.Add(uint8(0), uint8(3), uint16(10))
	f.Add(uint8(2), uint8(0), uint16(100))

	f.Fuzz(func(t *testing.T, schemaIdx, codecIdx uint8, n uint16) {
		s := schemas[int(schemaIdx)%len(schemas)]
		copt := codecs[int(codecIdx)%len(codecs)]
		// Cap n so the fuzz iteration cost is bounded.
		if n > 200 {
			n = 200
		}
		var buf bytes.Buffer
		var w *Writer
		var err error
		if copt == nil {
			w, err = NewWriter(&buf, s)
		} else {
			w, err = NewWriter(&buf, s, copt())
		}
		if err != nil {
			return
		}
		for i := uint16(0); i < n; i++ {
			var val any
			switch s {
			case schemas[0]:
				val = int32(i)
			case schemas[1]:
				val = "v"
			case schemas[2]:
				val = map[string]any{"a": int32(i), "b": "v"}
			}
			if err := w.Encode(val); err != nil {
				break
			}
		}
		// Close is the new path: codec.Close must run even when the
		// writer is in a poisoned w.err state (I/O or compression
		// errors; value errors recover). The fuzz cannot directly
		// inject a poison, but it can drive enough variation that
		// codec resource leaks would surface in -race + leak
		// detector setups.
		w.Close()
		// Now read it back. Every codec must round-trip; if the
		// reader fails on what the writer produced, that's a bug.
		r, err := NewReader(bytes.NewReader(buf.Bytes()))
		if err != nil {
			return
		}
		for i := 0; i < int(n)+1; i++ {
			var v any
			if err := r.Decode(&v); err != nil {
				break
			}
		}
		r.Close()
	})
}

// ---------- matrix_ocf_test.go ----------

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
				w := mustNewWriter(t, &buf, schema, WithCodec(cm.mk(t)), WithBlockCount(2))
				for r := 0; r < rounds; r++ {
					for _, v := range fr.values {
						if err := w.Encode(v); err != nil {
							t.Fatalf("Encode: %v", err)
						}
					}
				}
				mustClose(t, w)

				r := mustNewReader(t, bytes.NewReader(buf.Bytes()), WithCodec(cm.mk(t)))
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
				mustClose(t, w)
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
	mustClose(t, w)

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

// ---------- matrix_stateful_test.go ----------

// ---------------------------------------------------------------------------
// OCF stateful model: random programs of Writer operations — good encodes,
// VALUE-ERROR encodes (documented to discard only the failed datum and
// leave the Writer usable), raw Write of pre-encoded datums, explicit
// Flushes, Close, and append-mode reopen — with the model tracking exactly
// which datums were accepted. The reader must observe precisely the
// accepted sequence. Seeds are fixed, so every program is reproducible.
// ---------------------------------------------------------------------------

func TestMatrixOCF_StatefulPrograms(t *testing.T) {
	schemaJSON := `{"type":"record","name":"SP","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":["null","string"],"default":null}]}`
	schema := avro.MustParse(schemaJSON)

	mkGood := func(i int) any {
		var b any
		if i%3 == 0 {
			b = fmt.Sprintf("s%d", i)
		}
		return map[string]any{"a": int32(i), "b": b}
	}
	bad := map[string]any{"a": "not-an-int", "b": nil}

	for seed := int64(1); seed <= 12; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			var buf bytes.Buffer
			w := mustNewWriter(t, &buf, schema, WithBlockCount(1+rng.Intn(4)))
			var expected []any
			n := 30 + rng.Intn(40)
			for i := 0; i < n; i++ {
				switch rng.Intn(10) {
				case 0, 1, 2, 3, 4, 5: // good Encode
					v := mkGood(i)
					if err := w.Encode(v); err != nil {
						t.Fatalf("op %d: good Encode failed: %v", i, err)
					}
					expected = append(expected, v)
				case 6, 7: // VALUE-error Encode: rejected, Writer stays usable
					if err := w.Encode(bad); err == nil {
						t.Fatalf("op %d: bad Encode unexpectedly accepted", i)
					}
				case 8: // raw Write of a pre-encoded datum
					v := mkGood(1000 + i)
					enc, err := schema.AppendEncode(nil, v)
					if err != nil {
						t.Fatalf("op %d: pre-encode: %v", i, err)
					}
					if _, err := w.Write(enc); err != nil {
						t.Fatalf("op %d: Write: %v", i, err)
					}
					expected = append(expected, v)
				case 9: // explicit Flush (empty flush is a no-op)
					if err := w.Flush(); err != nil {
						t.Fatalf("op %d: Flush: %v", i, err)
					}
				}
			}
			mustClose(t, w)

			// The reader must see exactly the accepted datums, in order.
			r := mustNewReader(t, bytes.NewReader(buf.Bytes()))
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
			if len(got) != len(expected) {
				t.Fatalf("read %d datums, model accepted %d", len(got), len(expected))
			}
			for i := range expected {
				want, err := schema.AppendEncode(nil, expected[i])
				if err != nil {
					t.Fatal(err)
				}
				gotW, err := schema.AppendEncode(nil, got[i])
				if err != nil || !bytes.Equal(gotW, want) {
					t.Fatalf("datum %d differs: got %#v want %#v", i, got[i], expected[i])
				}
			}
		})
	}
}

// The same model across an append boundary: a random program writes and
// closes a file, NewAppendWriter continues it with another random program,
// and the reader sees both programs' accepted datums in order.
func TestMatrixOCF_StatefulAppendPrograms(t *testing.T) {
	schemaJSON := `{"type":"record","name":"SA","fields":[{"name":"a","type":"int"}]}`
	schema := avro.MustParse(schemaJSON)
	bad := map[string]any{"a": "nope"}

	for seed := int64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			path := filepath.Join(t.TempDir(), "f.avro")
			var expected []any

			runProgram := func(w *Writer, base, ops int) {
				t.Helper()
				for i := 0; i < ops; i++ {
					switch rng.Intn(8) {
					case 0, 1, 2, 3, 4:
						v := map[string]any{"a": int32(base + i)}
						if err := w.Encode(v); err != nil {
							t.Fatalf("Encode: %v", err)
						}
						expected = append(expected, v)
					case 5, 6:
						if err := w.Encode(bad); err == nil {
							t.Fatal("bad Encode accepted")
						}
					case 7:
						mustFlush(t, w)
					}
				}
				mustClose(t, w)
			}

			f, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			w1, err := NewWriter(f, schema, WithBlockCount(2))
			if err != nil {
				t.Fatalf("NewWriter: %v", err)
			}
			runProgram(w1, 0, 15+rng.Intn(20))
			f.Close()

			f2, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			w2, err := NewAppendWriter(f2, WithBlockCount(3))
			if err != nil {
				t.Fatalf("NewAppendWriter: %v", err)
			}
			runProgram(w2, 1000, 15+rng.Intn(20))
			f2.Close()

			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			r, err := NewReader(bytes.NewReader(data))
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			defer r.Close()
			var i int
			for {
				var v map[string]any
				err := r.Decode(&v)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("Decode #%d: %v", i, err)
				}
				want := expected[i].(map[string]any)["a"]
				if v["a"] != want {
					t.Fatalf("datum %d: got %v want %v", i, v["a"], want)
				}
				i++
			}
			if i != len(expected) {
				t.Fatalf("read %d, model accepted %d", i, len(expected))
			}
		})
	}
}

// I/O-error poisoning: once the sink fails, every subsequent operation
// returns the sticky error, and Close still releases the codec.
func TestMatrixOCF_StatefulPoison(t *testing.T) {
	schema := avro.MustParse(`"int"`)
	fw := &failAfterWriter{n: 200}
	w := mustNewWriter(t, fw, schema, WithBlockCount(1))
	var poisoned bool
	for i := 0; i < 1000; i++ {
		if err := w.Encode(int32(i)); err != nil {
			poisoned = true
			// Sticky: the next ops fail with an error too.
			if err2 := w.Encode(int32(i)); err2 == nil {
				t.Fatal("Encode succeeded after I/O poison")
			}
			if err2 := w.Flush(); err2 == nil {
				t.Fatal("Flush succeeded after I/O poison")
			}
			break
		}
	}
	if !poisoned {
		t.Fatal("failing writer never tripped")
	}
	_ = w.Close() // must not panic; codec released regardless
}

type failAfterWriter struct{ n int }

func (f *failAfterWriter) Write(p []byte) (int, error) {
	if f.n <= 0 {
		return 0, errors.New("sink failed")
	}
	f.n -= len(p)
	return len(p), nil
}

// toggleSink writes into buf until fail is flipped, then fails every Write
// with err. Letting the test flip fail at a precise moment makes a chosen I/O
// step (a specific block write, the Reset old-block flush) the one that fails
// while every earlier write — including the header NewWriter emits — succeeds.
type toggleSink struct {
	buf  bytes.Buffer
	fail bool
	err  error
}

func (s *toggleSink) Write(p []byte) (int, error) {
	if s.fail {
		return 0, s.err
	}
	return s.buf.Write(p)
}

// Class invariant (NOT_BUGS #28): EVERY fallible I/O step in EVERY Writer
// method must poison the Writer — once a sink write or the sync-marker source
// fails, no later Encode/Flush silently succeeds and no further bytes land that
// a reader would accept. TestMatrixOCF_StatefulPoison covers only Encode/Flush
// after a block-write failure; this crosses the remaining (method × I/O step)
// cells. Reset has THREE fallible steps — the old-block flush (before the
// repoint), sync-marker generation, and the header write (both after the
// repoint) — and the sync/header cells are the ones a prior gap missed
// (Reset cleared w.err then failed without re-setting it, so the writer kept
// emitting a headerless stream onto the new sink).
func TestMatrixOCF_WriterIOFailurePoisonsEveryStep(t *testing.T) {
	schema := avro.MustParse(`"int"`)
	v := int32(7)

	// Not-yet-closed Writer: poisoned with the exact sentinel on every call.
	assertSticky := func(t *testing.T, w *Writer, sentinel error) {
		t.Helper()
		if err := w.Encode(&v); !errors.Is(err, sentinel) {
			t.Fatalf("Encode after failure: want sticky %v, got %v", sentinel, err)
		}
		if err := w.Flush(); !errors.Is(err, sentinel) {
			t.Fatalf("Flush after failure: want sticky %v, got %v", sentinel, err)
		}
		if err := w.Close(); !errors.Is(err, sentinel) {
			t.Fatalf("Close after failure: want sticky %v, got %v", sentinel, err)
		}
	}
	// Possibly-closed Writer (the Close cell): must never silently accept more.
	assertNoSilentSuccess := func(t *testing.T, w *Writer) {
		t.Helper()
		if err := w.Encode(&v); err == nil {
			t.Fatal("Encode after failure silently succeeded")
		}
		if err := w.Flush(); err == nil {
			t.Fatal("Flush after failure silently succeeded")
		}
	}

	t.Run("encode-block-write", func(t *testing.T) {
		s := &toggleSink{err: errors.New("encblk")}
		w := mustNewWriter(t, s, schema, WithBlockCount(1)) // header written
		s.fail = true
		if err := w.Encode(&v); !errors.Is(err, s.err) { // 1-count → block write
			t.Fatalf("Encode block write: want %v, got %v", s.err, err)
		}
		assertSticky(t, w, s.err)
	})

	t.Run("flush-block-write", func(t *testing.T) {
		s := &toggleSink{err: errors.New("flushblk")}
		w := mustNewWriter(t, s, schema, WithBlockCount(1000))
		if err := w.Encode(&v); err != nil { // buffered, no flush yet
			t.Fatal(err)
		}
		s.fail = true
		if err := w.Flush(); !errors.Is(err, s.err) {
			t.Fatalf("Flush block write: want %v, got %v", s.err, err)
		}
		assertSticky(t, w, s.err)
	})

	t.Run("close-final-flush", func(t *testing.T) {
		s := &toggleSink{err: errors.New("closeblk")}
		w := mustNewWriter(t, s, schema, WithBlockCount(1000))
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
		s.fail = true
		if err := w.Close(); !errors.Is(err, s.err) {
			t.Fatalf("Close final flush: want %v, got %v", s.err, err)
		}
		// Close legitimately closes; subsequent ops error (errClosed), not silent.
		assertNoSilentSuccess(t, w)
	})

	t.Run("reset-old-block-flush", func(t *testing.T) {
		a := &toggleSink{err: errors.New("resetoldblk")}
		w := mustNewWriter(t, a, schema, WithBlockCount(1000))
		if err := w.Encode(&v); err != nil { // buffered in the OLD sink
			t.Fatal(err)
		}
		a.fail = true
		var b bytes.Buffer
		if err := w.Reset(&b); !errors.Is(err, a.err) { // old-block flush fails
			t.Fatalf("Reset old-block flush: want %v, got %v", a.err, err)
		}
		assertSticky(t, w, a.err)
	})

	t.Run("reset-sync-generation", func(t *testing.T) {
		var a bytes.Buffer
		w := mustNewWriter(t, &a, schema) // initial sync generated here
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
		orig := randRead
		boom := errors.New("resetsync")
		randRead = func(b []byte) (int, error) { return 0, boom }
		defer func() { randRead = orig }()
		var b bytes.Buffer
		if err := w.Reset(&b); !errors.Is(err, boom) {
			t.Fatalf("Reset sync gen: want %v, got %v", boom, err)
		}
		if b.Len() != 0 { // sync fails before any header write
			t.Fatalf("new sink touched after sync-gen failure: %d bytes", b.Len())
		}
		assertSticky(t, w, boom)
	})

	t.Run("reset-header-write", func(t *testing.T) {
		var a bytes.Buffer
		w, err := NewWriter(&a, schema)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
		boom := errors.New("resethdr")
		b := &failFirstWriteSink{err: boom} // header write to new sink fails
		if err := w.Reset(b); !errors.Is(err, boom) {
			t.Fatalf("Reset header write: want %v, got %v", boom, err)
		}
		// Un-poisoned, the post-Reset Encode/Flush would emit a headerless block
		// onto b; poisoned, nothing lands and b holds no readable OCF.
		if _, err := NewReader(bytes.NewReader(b.buf.Bytes())); err == nil {
			t.Fatalf("new sink holds a readable OCF (%d bytes)", b.buf.Len())
		}
		assertSticky(t, w, boom)
	})
}

var _ = reflect.DeepEqual // keep reflect imported for future model asserts

// ---------- metadata_compat_test.go ----------

// wideSchema builds a record whose JSON text exceeds n bytes (a wide, shallow
// record — many simple fields — which parses fine but produces large
// avro.schema header metadata).
func wideSchema(t *testing.T, minBytes int) (s *avro.Schema, js string, nFields int) {
	t.Helper()
	var b strings.Builder
	b.WriteString(`{"type":"record","name":"Wide","fields":[`)
	for b.Len() < minBytes {
		if nFields > 0 {
			b.WriteByte(',')
		}
		// A default lets Encode accept a sparse record without building all
		// nFields entries (and without the expensive s.Root() walk).
		fmt.Fprintf(&b, `{"name":"f%d","type":"long","default":0}`, nFields)
		nFields++
	}
	b.WriteString(`]}`)
	js = b.String()
	return avro.MustParse(js), js, nFields
}

// TestRegression_OCFLargeSchemaSelfReadable pins that an OCF file whose
// avro.schema header metadata exceeds the generic 1 MiB metadata cap is still
// readable. A wide record's JSON legitimately exceeds 1 MiB (and Java /
// fastavro read such files), but the reader's decodeMap capped every metadata
// value at ocfMetadataSafetyLimit (1 MiB) — so the writer produced a file
// NewReader (and NewAppendWriter, which re-reads the header) then rejected:
// self-incompatible, and unable to read Java's large-schema files. The
// self-describing avro.schema value now has a dedicated larger bound (its
// parse cost is independently bounded by the schema parser's own guards).
func TestRegression_OCFLargeSchemaSelfReadable(t *testing.T) {
	s, js, _ := wideSchema(t, ocfMetadataSafetyLimit+(64<<10)) // > 1 MiB
	if len(js) <= ocfMetadataSafetyLimit {
		t.Fatalf("test setup: schema JSON %d not over the cap %d", len(js), ocfMetadataSafetyLimit)
	}

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	// Every field has a default, so a sparse record encodes fine — this
	// exercises the HEADER (large avro.schema metadata), which is the point.
	if err := w.Encode(map[string]any{"f0": int64(1)}); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	mustClose(t, w)

	// The writer's own output must be readable by the reader.
	if _, err := NewReader(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("SELF-INCOMPATIBILITY: wrote a %d-byte file with a %d-byte schema it cannot read: %v",
			buf.Len(), len(js), err)
	}
}

// TestRegression_OCFLargeUserMetadataProducerCompliance pins producer-side
// compliance for user metadata against the reader's per-entry cap. Arbitrary
// user metadata (WithMetadata) is opaque, unbounded-by-anything-else data, so
// the 1 MiB reader cap is a reasonable DoS limit — but the writer enforced no
// matching bound, so a >1 MiB WithMetadata value produced a file the reader
// rejected. The writer now refuses to write metadata the reader cannot read,
// with a clear error, rather than emitting a self-incompatible file.
func TestRegression_OCFLargeUserMetadataProducerCompliance(t *testing.T) {
	s := avro.MustParse(`"long"`)

	// At the cap: writes and reads back.
	atCap := bytes.Repeat([]byte{'x'}, ocfMetadataSafetyLimit)
	var ok bytes.Buffer
	w, err := NewWriter(&ok, s, WithMetadata(map[string][]byte{"m": atCap}))
	if err != nil {
		t.Fatalf("NewWriter at cap: %v", err)
	}
	if err := w.Encode(int64(1)); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close at cap: %v", err)
	}
	if _, err := NewReader(bytes.NewReader(ok.Bytes())); err != nil {
		t.Fatalf("SELF-INCOMPATIBILITY: %d-byte user metadata at the cap unreadable: %v", len(atCap), err)
	}

	// Over the cap: the writer must refuse (producer compliance), surfacing a
	// clear error rather than a file the reader rejects.
	over := bytes.Repeat([]byte{'x'}, ocfMetadataSafetyLimit+100)
	var bad bytes.Buffer
	w2, err := NewWriter(&bad, s, WithMetadata(map[string][]byte{"m": over}))
	werr := err
	if werr == nil {
		werr = w2.Encode(int64(1))
		if werr == nil {
			werr = w2.Close()
		}
	}
	if werr == nil {
		t.Fatal("writer produced a file with >1 MiB user metadata the reader rejects; want a write-time error")
	}
	if !strings.Contains(werr.Error(), "metadata") {
		t.Fatalf("over-cap write rejected, but not with a metadata reason: %v", werr)
	}
}

// TestRegression_OCFMetadataKeyErrorBounded pins that the writer's metadata-key
// rejection errors stay bounded in length: the caller-supplied key is wrapped
// with truncForError, the same helper the read-side codec-name error uses. The
// key comes from WithMetadata (commonly populated from untrusted upstream data
// like tenant ids or labels), so echoing it verbatim is a 1:1 log/RPC/metric
// amplification — an ~8 MiB key produced an ~8 MiB error string. The rejection
// itself is correct and fast; only the message size was unbounded.
func TestRegression_OCFMetadataKeyErrorBounded(t *testing.T) {
	s := avro.MustParse(`"long"`)
	const cap = 4096 // generous ceiling; truncForError caps at 80

	t.Run("reserved-prefix-key", func(t *testing.T) {
		huge := "avro." + strings.Repeat("k", 8<<20)
		_, err := NewWriter(&bytes.Buffer{}, s, WithMetadata(map[string][]byte{huge: []byte("v")}))
		if err == nil {
			t.Fatal("expected reserved-key rejection")
		}
		if len(err.Error()) > cap {
			t.Errorf("reserved-key error is %d bytes (unbounded echo); want <= %d", len(err.Error()), cap)
		}
	})
	t.Run("overlong-key", func(t *testing.T) {
		huge := strings.Repeat("k", 2<<20)
		_, err := NewWriter(&bytes.Buffer{}, s, WithMetadata(map[string][]byte{huge: []byte("v")}))
		if err == nil {
			t.Fatal("expected overlong-key rejection")
		}
		if len(err.Error()) > cap {
			t.Errorf("overlong-key error is %d bytes (unbounded echo); want <= %d", len(err.Error()), cap)
		}
	})
}

// ---------- truncation_eof_test.go ----------

// io.EOF is Decode's end-of-stream sentinel: it must be returned only when the
// stream ends cleanly at a block boundary. A block-header truncation is an error,
// and that error must NOT satisfy errors.Is(err, io.EOF) — otherwise the idiomatic
// termination check reads a truncated stream as complete and silently drops the
// promised tail. The hazard is specific to the zero-bytes-available cuts:
// io.ReadFull and binary.ReadVarint return bare io.EOF when no bytes remain, so
// exactly those would leak the sentinel through a %w wrap. fastavro errors at
// every one of these cuts, and the spec makes all four block parts mandatory.
func TestRegression_TruncatedBlockHeaderNotEOF(t *testing.T) {
	s := mustParse(t, `"int"`)
	// One complete block with one record.
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	if err := w.Encode(int32(7)); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)
	full := buf.Bytes()

	cuts := []struct {
		name  string
		bytes func() []byte
	}{
		{"after-count-varint", func() []byte {
			// A complete count varint promising a second block, then EOF
			// before the size varint.
			return binary.AppendVarint(append([]byte{}, full...), 1)
		}},
		{"after-size-varint", func() []byte {
			// Count and size complete, zero data bytes present.
			d := binary.AppendVarint(append([]byte{}, full...), 1)
			return binary.AppendVarint(d, 100)
		}},
		{"at-sync-start", func() []byte {
			// Data complete, zero sync bytes present.
			d := binary.AppendVarint(append([]byte{}, full...), 1)
			d = binary.AppendVarint(d, 1)
			return append(d, 0x02)
		}},
	}
	for _, c := range cuts {
		t.Run(c.name, func(t *testing.T) {
			r := mustNewReader(t, bytes.NewReader(c.bytes()))
			defer r.Close()
			var v int32
			if err := r.Decode(&v); err != nil || v != 7 {
				t.Fatalf("first record: v=%d err=%v", v, err)
			}
			err := r.Decode(&v)
			if err == nil {
				t.Fatal("expected error for truncated second block")
			}
			if errors.Is(err, io.EOF) {
				t.Fatalf("truncation error satisfies errors.Is(err, io.EOF): %v", err)
			}
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("truncation error should match io.ErrUnexpectedEOF, got: %v", err)
			}
		})
	}

	// Control: a true end-of-stream is the bare io.EOF sentinel.
	r := mustNewReader(t, bytes.NewReader(full))
	defer r.Close()
	var v int32
	if err := r.Decode(&v); err != nil {
		t.Fatal(err)
	}
	if err := r.Decode(&v); err != io.EOF {
		t.Fatalf("clean end must be bare io.EOF, got %v", err)
	}
}

// The same sentinel contract through the incremental (io.CopyN) block-data
// arm, reached when the declared size exceeds the eager-allocation window
// and the reader cap is raised to allow it. io.CopyN returns bare io.EOF on
// ANY shortfall — zero bytes available AND a partial copy — so both cells
// are pinned (unlike io.ReadFull, whose partial reads already return
// io.ErrUnexpectedEOF).
func TestRegression_TruncatedLargeBlockDataNotEOF(t *testing.T) {
	s := mustParse(t, `"int"`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	if err := w.Encode(int32(7)); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)
	full := buf.Bytes()

	for _, c := range []struct {
		name    string
		partial []byte // data bytes present before the cut
	}{
		{"zero-data-bytes", nil},
		{"partial-data-bytes", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
	} {
		t.Run(c.name, func(t *testing.T) {
			data := binary.AppendVarint(append([]byte{}, full...), 1) // count
			data = binary.AppendVarint(data, (64<<20)+1)              // size past the eager window
			data = append(data, c.partial...)
			r := mustNewReader(t, bytes.NewReader(data), WithMaxBlockBytes(128<<20))
			defer r.Close()
			var v int32
			if err := r.Decode(&v); err != nil || v != 7 {
				t.Fatalf("first record: v=%d err=%v", v, err)
			}
			err := r.Decode(&v)
			if err == nil {
				t.Fatal("expected error for truncated block data")
			}
			if errors.Is(err, io.EOF) {
				t.Fatalf("truncation error satisfies errors.Is(err, io.EOF): %v", err)
			}
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("truncation error should match io.ErrUnexpectedEOF, got: %v", err)
			}
		})
	}
}

// ---------- truncation_sweep_test.go ----------

// TestMatrix_TruncationTerminalErrorIdentity sweeps a multi-block file cut at
// EVERY byte offset from end-of-header to one byte short of the full file,
// across codecs, pinning the terminal-error identity contract as a class: a cut
// exactly at a block boundary is a clean end of stream, with BARE io.EOF and
// exactly the records of the complete blocks before it; every other cut is
// truncation, whose terminal error does NOT satisfy errors.Is(err, io.EOF); and
// no cut ever yields records beyond the blocks complete before it.
//
// The spliced count-0 block puts the skip arm's reads inside the sweep. Counts
// of 100 and 70 make the block-header count varints multi-byte, so mid-varint
// cuts participate. Both codecs share the invariant and differ in the data-read
// arms traversed.
func TestMatrix_TruncationTerminalErrorIdentity(t *testing.T) {
	s := mustParse(t, `"int"`)
	for _, codec := range []struct {
		name string
		opts []WriterOpt
	}{
		{"null", nil},
		{"deflate", []WriterOpt{WithCodec(DeflateCodec(6))}},
	} {
		t.Run(codec.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := mustNewWriter(t, &buf, s, codec.opts...)
			headerEnd := buf.Len()
			for i := range 100 {
				if err := w.Encode(int32(i)); err != nil {
					t.Fatal(err)
				}
			}
			mustFlush(t, w)
			block1End := buf.Len()
			for i := range 70 {
				if err := w.Encode(int32(1000 + i)); err != nil {
					t.Fatal(err)
				}
			}
			mustClose(t, w)
			raw := buf.Bytes()
			if headerEnd == 0 || block1End <= headerEnd || len(raw) <= block1End {
				t.Fatalf("layout snapshots out of order: header=%d block1=%d len=%d", headerEnd, block1End, len(raw))
			}

			// Splice a count-0 block (count 0, size 0, sync) between the two
			// record blocks, using the file's own sync marker.
			sync := raw[block1End-16 : block1End]
			var file []byte
			file = append(file, raw[:block1End]...)
			file = append(file, 0x00, 0x00)
			file = append(file, sync...)
			emptyEnd := block1End + 2 + 16
			file = append(file, raw[block1End:]...)

			// recordsAt maps each clean boundary to the records readable at it.
			recordsAt := map[int]int{
				headerEnd: 0,
				block1End: 100,
				emptyEnd:  100,
				len(file): 170,
			}

			readAll := func(prefix []byte) (n int, sum int64, term error) {
				r, err := NewReader(bytes.NewReader(prefix))
				if err != nil {
					return 0, 0, err
				}
				defer r.Close()
				for {
					var v int32
					err := r.Decode(&v)
					if err != nil {
						return n, sum, err
					}
					n++
					sum += int64(v)
				}
			}

			// Baseline: the spliced file reads fully with the empty block
			// skipped.
			if n, _, term := readAll(file); n != 170 || term != io.EOF {
				t.Fatalf("spliced baseline: n=%d term=%v", n, term)
			}

			for L := headerEnd; L <= len(file); L++ {
				n, _, term := readAll(file[:L])
				want, boundary := recordsAt[L]
				if !boundary {
					// Records never exceed the complete blocks before the cut.
					switch {
					case L < block1End && n != 0, L >= block1End && n != 100:
						t.Fatalf("L=%d: %d records beyond the blocks complete before the cut", L, n)
					}
					if term == nil {
						t.Fatalf("L=%d: truncated stream read as complete (%d records, no error)", L, n)
					}
					if errors.Is(term, io.EOF) {
						t.Fatalf("L=%d: truncation error satisfies errors.Is(err, io.EOF): %v", L, term)
					}
					continue
				}
				if n != want {
					t.Fatalf("boundary L=%d: read %d records, want %d", L, n, want)
				}
				if term != io.EOF {
					t.Fatalf("boundary L=%d: terminal error must be bare io.EOF, got %v", L, term)
				}
			}
		})
	}
}

// ---------- zero_byte_block_test.go ----------

// Files this package writes must be readable by this package: the Reader
// bounds a block's declared count to len(block)+maxOCFZeroByteSlack (and
// caps consecutive zero-byte records at the same slack), so the Writer
// must start a new block before a run of zero-byte datums (top-level
// "null", a record of only null fields, a size-0 fixed) exceeds that
// bound. The byte-driven flush alone never fires for such datums — they
// contribute no bytes — so without a count-side bound every datum lands
// in one giant block the Reader then rejects.
func TestWriterZeroByteDatumsSelfReadable(t *testing.T) {
	const n = 3*maxOCFZeroByteSlack + 17 // several blocks' worth
	for _, tc := range []struct {
		name   string
		schema string
		value  any
	}{
		{"null", `"null"`, nil},
		{"size-0 fixed", `{"type":"fixed","name":"F","size":0}`, []byte{}},
		{"all-null record", `{"type":"record","name":"R","fields":[
			{"name":"a","type":"null"}]}`, map[string]any{"a": nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := avro.MustParse(tc.schema)
			var buf bytes.Buffer
			w := mustNewWriter(t, &buf, s)
			for i := 0; i < n; i++ {
				if err := w.Encode(tc.value); err != nil {
					t.Fatalf("Encode #%d: %v", i, err)
				}
			}
			mustClose(t, w)

			r := mustNewReader(t, bytes.NewReader(buf.Bytes()))
			defer r.Close()
			var got int
			for {
				var v any
				err := r.Decode(&v)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("Decode #%d: %v", got, err)
				}
				got++
			}
			if got != n {
				t.Fatalf("read %d of %d datums back", got, n)
			}
		})
	}
}

// Datums that consume ≥1 wire byte (here: union values — even the null
// branch writes its index varint) keep count ≤ len(block), so the
// count-side flush bound never fires for them; block sizing stays purely
// byte-driven and the file reads back exactly as before.
func TestWriterOneByteDatumsUnaffectedByCountBound(t *testing.T) {
	s := avro.MustParse(`["null","int"]`)
	const n = 2*maxOCFZeroByteSlack + 11
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	for i := 0; i < n; i++ {
		var v any
		if i%37 == 0 {
			v = int32(i) // occasional real bytes
		}
		if err := w.Encode(v); err != nil {
			t.Fatalf("Encode #%d: %v", i, err)
		}
	}
	mustClose(t, w)
	r := mustNewReader(t, bytes.NewReader(buf.Bytes()))
	defer r.Close()
	var got int
	for {
		var v any
		err := r.Decode(&v)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Decode #%d: %v", got, err)
		}
		got++
	}
	if got != n {
		t.Fatalf("read %d of %d datums back", got, n)
	}
}

// The per-block consecutive-zero-byte-record CAP (Reader.Decode's zeroRun
// counter) is a SECOND, independent DoS guard from readBlock's
// count <= len(block)+slack check. They must be tested independently: a
// block whose decompressed bytes are large (here, padded with ignored
// trailing garbage) passes the count-vs-length check, so only the zeroRun
// counter stops a hostile count from driving the decode loop billions of
// times against a zero-byte schema ("null"). A mutant that inverts the
// counter (zeroRun++ -> --) or shifts its boundary survives every
// round-trip and every count-vs-length test — this drives the counter
// directly by hand-framing such a block.
func TestReaderZeroRunCapIndependentOfBlockLength(t *testing.T) {
	sync := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	build := func(count int64, payloadLen int) []byte {
		var buf bytes.Buffer
		// header: magic + metadata map (schema only) + sync
		buf.WriteString("Obj\x01")
		meta := encodeMap(nil, []kv{{"avro.schema", []byte(`"null"`)}})
		buf.Write(meta)
		buf.Write(sync[:])
		// one data block: count, size, payload (garbage the null decode
		// ignores — it consumes 0 bytes per datum regardless), sync
		block := binary.AppendVarint(nil, count)
		block = binary.AppendVarint(block, int64(payloadLen))
		block = append(block, bytes.Repeat([]byte{0xAB}, payloadLen)...)
		block = append(block, sync[:]...)
		buf.Write(block)
		return buf.Bytes()
	}

	// A hostile count far above the slack, with a payload long enough to
	// clear the count-vs-length check (count <= len(block)+slack), so the
	// ONLY thing that can stop the decode loop is the zeroRun counter.
	hostile := int64(maxOCFZeroByteSlack) + 100000
	data := build(hostile, int(hostile)) // len(block) >= count: length check passes

	r := mustNewReader(t, bytes.NewReader(data))
	defer r.Close()

	var consumed int
	for {
		var v any
		err := r.Decode(&v)
		if err == nil {
			consumed++
			if consumed > maxOCFZeroByteSlack+10 {
				t.Fatalf("zeroRun cap did not fire: decoded %d zero-byte records (cap %d)", consumed, maxOCFZeroByteSlack)
			}
			continue
		}
		// Expected: the zeroRun cap rejects past maxOCFZeroByteSlack
		// consecutive zero-byte datums.
		if !strings.Contains(err.Error(), "zero-byte records") {
			t.Fatalf("expected zero-byte-record cap error, got: %v", err)
		}
		break
	}
	// The cap must fire at the slack boundary, not before and not (per the
	// guard above) unboundedly after.
	if consumed != maxOCFZeroByteSlack {
		t.Fatalf("zeroRun cap fired after %d records, want exactly %d", consumed, maxOCFZeroByteSlack)
	}
}

// writeInts encodes int32(0) through int32(n-1) into w.
func writeInts(t testing.TB, w *Writer, n int) {
	t.Helper()
	for i := range n {
		v := int32(i)
		if err := w.Encode(&v); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	}
}
