// Package ocf implements Avro [Object Container Files] (OCF).
//
// An OCF is a self-describing binary file format: it embeds the Avro schema
// in the file header so readers do not need out-of-band schema information.
// Data is stored in compressed blocks separated by sync markers, making files
// splittable for parallel processing. OCF is the standard format for storing
// Avro data on disk; for sending individual values over the wire, see
// [avro.AppendSingleObject] instead.
//
// See the [Avro specification] for the full format definition.
//
// # Writing
//
//	schema := avro.MustParse(`{
//	    "type": "record",
//	    "name": "User",
//	    "fields": [
//	        {"name": "name", "type": "string"},
//	        {"name": "age", "type": "int"}
//	    ]
//	}`)
//
//	f, err := os.Create("users.avro")
//	if err != nil { ... }
//	w, err := ocf.NewWriter(f, schema, ocf.WithCodec(ocf.SnappyCodec()))
//	if err != nil { ... }
//	for _, u := range users {
//	    if err := w.Encode(&u); err != nil { ... }
//	}
//	if err := w.Close(); err != nil { ... }
//
// # Reading
//
//	f, err := os.Open("users.avro")
//	if err != nil { ... }
//	r, err := ocf.NewReader(f)
//	if err != nil { ... }
//	for {
//	    var u User
//	    if err := r.Decode(&u); err != nil {
//	        if err == io.EOF { break }
//	        ...
//	    }
//	    fmt.Println(u)
//	}
//
// # Appending
//
// Use [NewAppendWriter] to add records to an existing file without
// rewriting it.
//
// # Codecs
//
// Null, deflate, snappy, and zstandard are built in. Custom codecs can be
// provided via [WithCodec].
//
// [Object Container Files]: https://avro.apache.org/docs/current/specification/#object-container-files
// [Avro specification]: https://avro.apache.org/docs/current/specification/#object-container-files
package ocf

import (
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strings"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/twmb/avro"
)

// Codec compresses and decompresses OCF data blocks.
type Codec interface {
	// Name returns the codec identifier for the "avro.codec" metadata key
	// (e.g. "null", "deflate", "snappy", "zstandard").
	Name() string

	// Compress encodes a raw data block for storage.
	Compress(src []byte) ([]byte, error)

	// Decompress decodes a stored data block back to raw bytes.
	Decompress(src []byte) ([]byte, error)

	// Close releases any resources held by the codec. Codecs that hold no
	// resources may return nil.
	Close() error
}

// NopCloser returns a Codec that wraps c but has a no-op Close method. This
// is useful when sharing a single codec across multiple writers or readers
// so that individual [Writer.Close] or [Reader.Close] calls do not release
// shared resources. The caller is responsible for closing the underlying
// codec when it is no longer needed.
func NopCloser(c Codec) Codec { return nopCloser{c} }

type nopCloser struct{ Codec }

func (nopCloser) Close() error { return nil }

// WriterOpt is an option for [NewWriter].
type WriterOpt interface{ writerOpt() }

// ReaderOpt is an option for [NewReader].
type ReaderOpt interface{ readerOpt() }

// Opt is an option that applies to both [NewWriter] and [NewReader].
type Opt interface {
	WriterOpt
	ReaderOpt
}

type optCodec struct{ c Codec }

func (optCodec) writerOpt() {}
func (optCodec) readerOpt() {}

type (
	optBlockCount    struct{ n int }
	optBlockBytes    struct{ n int }
	optMetadata      struct{ m map[string][]byte }
	optSyncMarker    struct{ sync [16]byte }
	optSchema        struct{ s string }
	optReaderSchema  struct{ s *avro.Schema }
	optMaxBlockBytes struct{ n int64 }
)

func (optBlockCount) writerOpt()    {}
func (optBlockBytes) writerOpt()    {}
func (optMetadata) writerOpt()      {}
func (optSyncMarker) writerOpt()    {}
func (optSchema) writerOpt()        {}
func (optReaderSchema) readerOpt()  {}
func (optMaxBlockBytes) readerOpt() {}

// WithCodec sets the compression codec. The default is null (no compression).
// WithCodec can be used as both a [WriterOpt] and a [ReaderOpt]. The four
// built-in codecs (null, deflate, snappy, zstandard) do not need to be
// registered for reading. A custom codec whose name matches a built-in
// overrides it.
//
// The codec's Close method is called by [Writer.Close] and [Reader.Close].
// Codecs that should not be closed (e.g. shared across multiple writers)
// should return nil from Close.
func WithCodec(c Codec) Opt { return optCodec{c} }

// WithBlockCount sets the maximum number of items per block. The default is
// 0 (unlimited). If both WithBlockCount and [WithBlockBytes] are set,
// whichever limit is hit first triggers a flush.
func WithBlockCount(n int) WriterOpt { return optBlockCount{n} }

// WithBlockBytes sets the maximum uncompressed size of a block in bytes
// before it is flushed. The default is 64 KiB. If both [WithBlockCount] and
// WithBlockBytes are set, whichever limit is hit first triggers a flush.
func WithBlockBytes(n int) WriterOpt { return optBlockBytes{n} }

// WithMetadata adds custom metadata to the file header. Keys starting with
// "avro." are reserved by the spec. Multiple calls are cumulative.
func WithMetadata(m map[string][]byte) WriterOpt { return optMetadata{m} }

// WithSyncMarker sets the 16-byte sync marker written between blocks. By
// default a random marker is generated. This is primarily useful for
// deterministic test output.
func WithSyncMarker(sync [16]byte) WriterOpt { return optSyncMarker{sync} }

// WithSchema overrides the schema JSON written to the file header. By default
// [avro.Schema.Canonical] is used, which strips non-essential properties like
// doc strings and aliases. Use this to preserve those properties or to write
// a custom schema string.
func WithSchema(schema string) WriterOpt { return optSchema{schema} }

// WithReaderSchema provides a reader schema for schema evolution. If set, the
// file's writer schema is resolved against the reader schema using
// [avro.Resolve], and all decoded values use the reader schema's format.
// Fields added in the reader schema must have defaults; fields removed from
// the writer schema are skipped.
func WithReaderSchema(s *avro.Schema) ReaderOpt { return optReaderSchema{s} }

// WithMaxBlockBytes sets the maximum compressed block size in bytes that the
// reader will accept. The default is 64 MiB. This guards against malicious
// or corrupt files that declare very large blocks.
func WithMaxBlockBytes(n int64) ReaderOpt { return optMaxBlockBytes{n} }

// DeflateCodec returns a [Codec] using raw DEFLATE compression at the given
// level (e.g. [flate.DefaultCompression]).
func DeflateCodec(level int) Codec { return deflateCodec{level} }

// SnappyCodec returns a [Codec] using Snappy compression with a trailing
// CRC-32 checksum per block, as required by the Avro spec.
func SnappyCodec() Codec { return snappyCodec{} }

// ZstdCodec returns a [Codec] using Zstandard compression. Encoder options
// (eopts) and decoder options (dopts) are passed to [zstd.NewWriter] and
// [zstd.NewReader] respectively. Both may be nil for defaults.
//
// [zstd.WithEncoderConcurrency](1) and [zstd.WithDecoderConcurrency](1) are
// prepended to the options; pass a different concurrency to override.
//
// A single ZstdCodec is safe to share across multiple readers and writers
// via [NopCloser].
func ZstdCodec(eopts []zstd.EOption, dopts []zstd.DOption) (Codec, error) {
	eopts = append([]zstd.EOption{zstd.WithEncoderConcurrency(1)}, eopts...)
	dopts = append([]zstd.DOption{zstd.WithDecoderConcurrency(1)}, dopts...)
	enc, err := zstd.NewWriter(nil, eopts...)
	if err != nil {
		return nil, fmt.Errorf("ocf: creating zstd encoder: %w", err)
	}
	dec, err := zstd.NewReader(nil, dopts...)
	if err != nil {
		enc.Close()
		return nil, fmt.Errorf("ocf: creating zstd decoder: %w", err)
	}
	return &zstdCodec{enc: enc, dec: dec}, nil
}

// MustZstdCodec is like [ZstdCodec] but panics on error. This is useful for
// inline codec creation with static options:
//
//	w, err := ocf.NewWriter(f, schema, ocf.WithCodec(ocf.MustZstdCodec(nil, nil)))
func MustZstdCodec(eopts []zstd.EOption, dopts []zstd.DOption) Codec {
	c, err := ZstdCodec(eopts, dopts)
	if err != nil {
		panic(err)
	}
	return c
}

var magic = [4]byte{'O', 'b', 'j', 0x01}

// randRead is used to generate sync markers. It is a variable so tests can
// override it to simulate errors.
var randRead = rand.Read

// Writer encodes Avro values into an OCF. Values are buffered into blocks
// that are compressed and flushed automatically. Close must be called to
// flush remaining items.
type Writer struct {
	w          io.Writer
	schema     *avro.Schema
	schemaJSON string
	codec      Codec
	sync       [16]byte
	buf        []byte
	count      int
	maxCount   int
	maxBytes   int
	err        error
	userMeta   []kv
	hasSync    bool
}

const defaultBlockBytes = 64 << 10 // 64 KiB

func (w *Writer) shouldFlush() bool {
	return (w.maxCount > 0 && w.count >= w.maxCount) || len(w.buf) >= w.maxBytes
}

// Schema returns the schema used by this Writer.
func (wr *Writer) Schema() *avro.Schema { return wr.schema }

// NewWriter creates a Writer that writes an OCF to w. The file header is
// written immediately.
func NewWriter(w io.Writer, s *avro.Schema, opts ...WriterOpt) (*Writer, error) {
	wr := &Writer{
		w:      w,
		schema: s,
		codec:  nullCodec{},
	}

	for _, o := range opts {
		switch o := o.(type) {
		case optCodec:
			wr.codec = o.c
		case optBlockCount:
			wr.maxCount = o.n
		case optBlockBytes:
			wr.maxBytes = o.n
		case optMetadata:
			for k, v := range o.m {
				if strings.HasPrefix(k, "avro.") {
					return nil, fmt.Errorf("ocf: metadata key %q is reserved (avro.* namespace)", k)
				}
				wr.userMeta = append(wr.userMeta, kv{k, v})
			}
		case optSyncMarker:
			wr.sync = o.sync
			wr.hasSync = true
		case optSchema:
			wr.schemaJSON = o.s
		}
	}
	if wr.maxCount < 0 {
		wr.maxCount = 0
	}
	if wr.maxBytes <= 0 {
		wr.maxBytes = defaultBlockBytes
	}

	if !wr.hasSync {
		if _, err := randRead(wr.sync[:]); err != nil {
			return nil, fmt.Errorf("ocf: generating sync marker: %w", err)
		}
	}

	if err := wr.writeHeader(); err != nil {
		return nil, err
	}
	return wr, nil
}

func (w *Writer) writeHeader() error {
	schemaBytes := w.schema.Canonical()
	if w.schemaJSON != "" {
		schemaBytes = []byte(w.schemaJSON)
	}
	meta := []kv{{"avro.schema", schemaBytes}}
	if name := w.codec.Name(); name != "null" {
		meta = append(meta, kv{"avro.codec", []byte(name)})
	}
	meta = append(meta, w.userMeta...)

	var hdr []byte
	hdr = append(hdr, magic[:]...)
	hdr = encodeMap(hdr, meta)
	hdr = append(hdr, w.sync[:]...)
	if _, err := w.w.Write(hdr); err != nil {
		return fmt.Errorf("ocf: writing header: %w", err)
	}
	return nil
}

// Encode serializes v and appends it to the current block. The block is
// flushed automatically when it hits the count or byte limit.
//
// After any error the Writer is poisoned: all subsequent calls return the
// same error.
func (w *Writer) Encode(v any) error {
	if w.err != nil {
		return w.err
	}
	var err error
	w.buf, err = w.schema.AppendEncode(w.buf, v)
	if err != nil {
		w.err = err
		return err
	}
	w.count++
	if w.shouldFlush() {
		return w.flush()
	}
	return nil
}

// Write appends pre-encoded Avro bytes as a single datum to the current
// block. The caller must ensure p is exactly one datum encoded with the
// writer's schema. Auto-flushing rules are the same as [Encode].
func (w *Writer) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	w.buf = append(w.buf, p...)
	w.count++
	if w.shouldFlush() {
		if err := w.flush(); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// Flush writes any buffered items as a block. The Writer remains usable.
func (w *Writer) Flush() error {
	if w.err != nil {
		return w.err
	}
	if w.count > 0 {
		return w.flush()
	}
	return nil
}

// Close flushes any remaining items and closes the codec.
func (w *Writer) Close() error {
	if w.err != nil {
		return w.err
	}
	if w.count > 0 {
		if err := w.flush(); err != nil {
			return err
		}
	}
	return w.codec.Close()
}

func (w *Writer) flush() error {
	compressed, err := w.codec.Compress(w.buf)
	if err != nil {
		w.err = err
		return fmt.Errorf("ocf: compressing block: %w", err)
	}
	var block []byte
	block = binary.AppendVarint(block, int64(w.count))
	block = binary.AppendVarint(block, int64(len(compressed)))
	block = append(block, compressed...)
	block = append(block, w.sync[:]...)
	if _, err := w.w.Write(block); err != nil {
		w.err = err
		return fmt.Errorf("ocf: writing block: %w", err)
	}
	w.buf = w.buf[:0]
	w.count = 0
	return nil
}

// Reset flushes buffered items to the current destination, then starts a
// new OCF on dst reusing the original schema, codec, and options. If the
// Writer is in an error state the flush is skipped and the error is cleared.
func (w *Writer) Reset(dst io.Writer) error {
	if w.err == nil && w.count > 0 {
		if err := w.flush(); err != nil {
			return err
		}
	}
	w.w = dst
	w.buf = w.buf[:0]
	w.count = 0
	w.err = nil
	if !w.hasSync {
		if _, err := randRead(w.sync[:]); err != nil {
			return fmt.Errorf("ocf: generating sync marker: %w", err)
		}
	}
	return w.writeHeader()
}

// NewAppendWriter opens an existing OCF for appending. It reads the header
// to recover the schema, codec, and sync marker, then seeks to the end.
//
// [WithBlockCount] and [WithBlockBytes] are honored. [WithCodec] can
// provide a codec implementation for non-built-in codecs (matched by name
// against the header). Other options are ignored.
func NewAppendWriter(rws io.ReadWriteSeeker, opts ...WriterOpt) (*Writer, error) {
	br := bufio.NewReader(rws)
	schema, meta, sync, err := readHeader(br)
	if err != nil {
		return nil, err
	}

	codecName := "null"
	if c, ok := meta["avro.codec"]; ok {
		codecName = string(c)
	}
	var customCodecs []Codec
	for _, o := range opts {
		if o, ok := o.(optCodec); ok {
			customCodecs = append(customCodecs, o.c)
		}
	}
	codec, err := resolveCodec(codecName, customCodecs)
	if err != nil {
		return nil, err
	}

	if _, err := rws.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("ocf: seeking to end of file: %w", err)
	}

	wr := &Writer{
		w:       rws,
		schema:  schema,
		codec:   codec,
		sync:    sync,
		hasSync: true,
	}

	for _, o := range opts {
		switch o := o.(type) {
		case optBlockCount:
			wr.maxCount = o.n
		case optBlockBytes:
			wr.maxBytes = o.n
		}
	}
	if wr.maxCount < 0 {
		wr.maxCount = 0
	}
	if wr.maxBytes <= 0 {
		wr.maxBytes = defaultBlockBytes
	}
	return wr, nil
}

// Reader decodes Avro values from an OCF.
type Reader struct {
	r             *bufio.Reader
	schema        *avro.Schema
	codec         Codec
	sync          [16]byte
	meta          map[string][]byte
	block         []byte
	remain        int64
	maxBlockBytes int64
}

// readHeader reads and validates the OCF header, returning the parsed
// schema, raw metadata, and sync marker.
func readHeader(br *bufio.Reader) (schema *avro.Schema, meta map[string][]byte, sync [16]byte, err error) {
	var m [4]byte
	if _, err = io.ReadFull(br, m[:]); err != nil {
		return nil, nil, sync, fmt.Errorf("ocf: reading magic: %w", err)
	}
	if m != magic {
		return nil, nil, sync, fmt.Errorf("ocf: invalid magic %x", m)
	}

	meta, err = decodeMap(br)
	if err != nil {
		return nil, nil, sync, fmt.Errorf("ocf: reading metadata: %w", err)
	}

	schemaBytes, ok := meta["avro.schema"]
	if !ok {
		return nil, nil, sync, errors.New("ocf: missing avro.schema in metadata")
	}
	schema, err = avro.Parse(string(schemaBytes))
	if err != nil {
		return nil, nil, sync, fmt.Errorf("ocf: parsing schema: %w", err)
	}

	if _, err = io.ReadFull(br, sync[:]); err != nil {
		return nil, nil, sync, fmt.Errorf("ocf: reading sync marker: %w", err)
	}

	return schema, meta, sync, nil
}

// NewReader creates a Reader that decodes an OCF from r. The header is read
// immediately. Use [WithCodec] if the file uses a non-built-in codec.
func NewReader(r io.Reader, opts ...ReaderOpt) (*Reader, error) {
	var customCodecs []Codec
	var readerSchema *avro.Schema
	var maxBlockBytes int64
	for _, o := range opts {
		switch o := o.(type) {
		case optCodec:
			customCodecs = append(customCodecs, o.c)
		case optReaderSchema:
			readerSchema = o.s
		case optMaxBlockBytes:
			maxBlockBytes = o.n
		}
	}
	if maxBlockBytes <= 0 {
		maxBlockBytes = 1 << 26 // 64 MiB default
	}

	br := bufio.NewReader(r)
	schema, meta, sync, err := readHeader(br)
	if err != nil {
		return nil, err
	}

	// Resolve codec.
	codecName := "null"
	if c, ok := meta["avro.codec"]; ok {
		codecName = string(c)
	}
	codec, err := resolveCodec(codecName, customCodecs)
	if err != nil {
		return nil, err
	}

	// Apply schema evolution if a reader schema was provided.
	if readerSchema != nil {
		resolved, err := avro.Resolve(schema, readerSchema)
		if err != nil {
			return nil, fmt.Errorf("ocf: resolving reader schema: %w", err)
		}
		schema = resolved
	}

	return &Reader{
		r:             br,
		schema:        schema,
		codec:         codec,
		sync:          sync,
		meta:          meta,
		maxBlockBytes: maxBlockBytes,
	}, nil
}

// Decode reads the next datum into v, returning [io.EOF] at end of file.
func (rd *Reader) Decode(v any) error {
	if rd.remain == 0 {
		if err := rd.readBlock(); err != nil {
			return err
		}
	}
	rest, err := rd.schema.Decode(rd.block, v)
	if err != nil {
		return fmt.Errorf("ocf: decoding datum: %w", err)
	}
	rd.block = rest
	rd.remain--
	if rd.remain == 0 && len(rd.block) != 0 {
		return fmt.Errorf("ocf: %d trailing bytes in block", len(rd.block))
	}
	return nil
}

// Schema returns the schema parsed from the file header.
func (rd *Reader) Schema() *avro.Schema { return rd.schema }

// Metadata returns the raw metadata from the file header, including both
// "avro.*" and user-defined keys. The returned map must not be modified.
func (rd *Reader) Metadata() map[string][]byte { return rd.meta }

// Close closes the codec, releasing any resources it holds.
func (rd *Reader) Close() error {
	return rd.codec.Close()
}

func (rd *Reader) readBlock() error {
	count, err := binary.ReadVarint(rd.r)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return io.EOF
		}
		return fmt.Errorf("ocf: reading block count: %w", err)
	}
	size, err := binary.ReadVarint(rd.r)
	if err != nil {
		return fmt.Errorf("ocf: reading block size: %w", err)
	}
	if count < 0 {
		return fmt.Errorf("ocf: invalid negative block count %d", count)
	}
	if size < 0 {
		return fmt.Errorf("ocf: invalid negative block size %d", size)
	}
	if size > rd.maxBlockBytes {
		return fmt.Errorf("ocf: block size %d exceeds safety limit of %d", size, rd.maxBlockBytes)
	}
	compressed := make([]byte, int(size))
	if _, err := io.ReadFull(rd.r, compressed); err != nil {
		return fmt.Errorf("ocf: reading block data: %w", err)
	}
	var sync [16]byte
	if _, err := io.ReadFull(rd.r, sync[:]); err != nil {
		return fmt.Errorf("ocf: reading block sync marker: %w", err)
	}
	if sync != rd.sync {
		return errors.New("ocf: sync marker mismatch")
	}
	block, err := rd.codec.Decompress(compressed)
	if err != nil {
		return fmt.Errorf("ocf: decompressing block: %w", err)
	}
	rd.block = block
	rd.remain = count
	return nil
}

// ---------- codecs ----------

type nullCodec struct{}

func (nullCodec) Name() string                          { return "null" }
func (nullCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (nullCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (nullCodec) Close() error                          { return nil }

type deflateCodec struct{ level int }

func (deflateCodec) Name() string { return "deflate" }
func (deflateCodec) Close() error { return nil }

func (c deflateCodec) Compress(src []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, c.level)
	if err != nil {
		return nil, err
	}
	// bytes.Buffer.Write never errors, so neither will flate's Write/Close.
	w.Write(src)
	w.Close()
	return buf.Bytes(), nil
}

func (deflateCodec) Decompress(src []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(src))
	defer r.Close()
	return io.ReadAll(r)
}

type snappyCodec struct{}

func (snappyCodec) Name() string { return "snappy" }
func (snappyCodec) Close() error { return nil }

func (snappyCodec) Compress(src []byte) ([]byte, error) {
	dst := snappy.Encode(nil, src)
	dst = binary.BigEndian.AppendUint32(dst, crc32.ChecksumIEEE(src))
	return dst, nil
}

func (snappyCodec) Decompress(src []byte) ([]byte, error) {
	if len(src) < 4 {
		return nil, errors.New("ocf: snappy data too short for CRC checksum")
	}
	decoded, err := snappy.Decode(nil, src[:len(src)-4])
	if err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(decoded) != binary.BigEndian.Uint32(src[len(src)-4:]) {
		return nil, errors.New("ocf: snappy CRC mismatch")
	}
	return decoded, nil
}

type zstdCodec struct {
	enc *zstd.Encoder
	dec *zstd.Decoder
}

func (*zstdCodec) Name() string { return "zstandard" }

func (c *zstdCodec) Compress(src []byte) ([]byte, error) {
	return c.enc.EncodeAll(src, nil), nil
}

func (c *zstdCodec) Decompress(src []byte) ([]byte, error) {
	return c.dec.DecodeAll(src, nil)
}

func (c *zstdCodec) Close() error {
	c.enc.Close()
	c.dec.Close()
	return nil
}

func resolveCodec(name string, custom []Codec) (Codec, error) {
	for _, c := range custom {
		if c.Name() == name {
			return c, nil
		}
	}
	switch name {
	case "null":
		return nullCodec{}, nil
	case "deflate":
		return deflateCodec{flate.DefaultCompression}, nil
	case "snappy":
		return snappyCodec{}, nil
	case "zstandard":
		return ZstdCodec(nil, nil)
	}
	return nil, fmt.Errorf("ocf: unknown codec %q", name)
}

// ---------- Avro map encoding helpers ----------

type kv struct {
	key string
	val []byte
}

func encodeMap(dst []byte, entries []kv) []byte {
	if len(entries) == 0 {
		return append(dst, 0) // zero-count block terminates empty map
	}
	dst = binary.AppendVarint(dst, int64(len(entries)))
	for _, e := range entries {
		dst = binary.AppendVarint(dst, int64(len(e.key)))
		dst = append(dst, e.key...)
		dst = binary.AppendVarint(dst, int64(len(e.val)))
		dst = append(dst, e.val...)
	}
	return append(dst, 0) // terminating zero-count block
}

func decodeMap(r *bufio.Reader) (map[string][]byte, error) {
	m := make(map[string][]byte)
	for {
		count, err := binary.ReadVarint(r)
		if err != nil {
			return nil, err
		}
		if count == 0 {
			return m, nil
		}
		if count < 0 {
			count = -count
			// Skip block byte-size.
			if _, err := binary.ReadVarint(r); err != nil {
				return nil, err
			}
		}
		if count > 1<<20 {
			return nil, fmt.Errorf("map block count %d exceeds safety limit", count)
		}
		for range int(count) {
			keyLen, err := binary.ReadVarint(r)
			if err != nil {
				return nil, err
			}
			if keyLen < 0 || keyLen > 1<<20 {
				return nil, fmt.Errorf("map key length %d out of range", keyLen)
			}
			key := make([]byte, int(keyLen))
			if _, err := io.ReadFull(r, key); err != nil {
				return nil, err
			}
			valLen, err := binary.ReadVarint(r)
			if err != nil {
				return nil, err
			}
			if valLen < 0 || valLen > 1<<20 {
				return nil, fmt.Errorf("map value length %d out of range", valLen)
			}
			val := make([]byte, int(valLen))
			if _, err := io.ReadFull(r, val); err != nil {
				return nil, err
			}
			m[string(key)] = val
		}
	}
}
