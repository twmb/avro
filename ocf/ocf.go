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
// # Block size limits
//
// The reader caps both the compressed block it reads off the wire
// ([WithMaxBlockBytes]) and the size that block decompresses to
// ([WithMaxDecompressedBlockBytes]), each defaulting to 64 MiB, to bound
// memory and decode time on untrusted input. The writer has no such cap
// (matching Java's DataFileWriter and fastavro): it writes whatever blocks it
// is given.
//
// A single Avro datum cannot be split across blocks, so a value larger than
// the reader default — e.g. an 80 MiB blob — is written as one block that a
// default reader then refuses, with an error naming the option to raise. The
// caps are a reader-side defense, so they live on the reader; to read a file
// whose blocks exceed the default, raise the matching cap there:
//
//	r, err := ocf.NewReader(f, ocf.WithMaxDecompressedBlockBytes(128<<20))
//
// Configure the cap to match the largest block your writer produces (which,
// for single large values, is governed by the datum size, not [WithBlockBytes]).
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
	"math"
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
	optBlockCount       struct{ n int }
	optBlockBytes       struct{ n int }
	optMetadata         struct{ m map[string][]byte }
	optSyncMarker       struct{ sync [16]byte }
	optSchema           struct{ s string }
	optReaderSchema     struct{ s *avro.Schema }
	optReaderSchemaFunc struct {
		fn func(*Reader) (*avro.Schema, error)
	}
	optMaxBlockBytes        struct{ n int64 }
	optMaxDecompressedBytes struct{ n int64 }
)

func (optBlockCount) writerOpt()           {}
func (optBlockBytes) writerOpt()           {}
func (optMetadata) writerOpt()             {}
func (optSyncMarker) writerOpt()           {}
func (optSchema) writerOpt()               {}
func (optReaderSchema) readerOpt()         {}
func (optReaderSchemaFunc) readerOpt()     {}
func (optMaxBlockBytes) readerOpt()        {}
func (optMaxDecompressedBytes) readerOpt() {}

type optSchemaOpts []avro.SchemaOpt

func (optSchemaOpts) readerOpt() {}

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
// [avro.Schema.String] is used (the original JSON passed to [avro.Parse]
// with all properties preserved — logicalType, precision, scale, doc,
// aliases, default — matching Java's DataFileWriter and fastavro). Use
// this only to write a deliberately-different schema text (e.g. the
// Parsing Canonical Form via [avro.Schema.Canonical] for strict-PCF
// downstream consumers).
func WithSchema(schema string) WriterOpt { return optSchema{schema} }

// WithReaderSchema provides the reader schema to resolve the file's writer
// schema against via [avro.Resolve]. Subsequent [Reader.Decode] calls use
// the resolved schema. Fields added in the reader schema must have defaults;
// writer fields absent from the reader schema are skipped.
//
// Use [WithReaderSchemaFunc] when the reader schema must be chosen based on
// the file's header (metadata or writer-schema shape).
//
// At most one of [WithReaderSchema] and [WithReaderSchemaFunc] may be used.
func WithReaderSchema(s *avro.Schema) ReaderOpt { return optReaderSchema{s} }

// WithReaderSchemaFunc is the dynamic counterpart to [WithReaderSchema]. The
// callback is invoked by [NewReader] after the OCF header has been parsed, so
// it can inspect the file's writer schema and metadata via rd.Schema() and
// rd.Metadata() before deciding which reader schema to resolve against.
//
// If the callback returns a non-nil schema, the writer schema is resolved
// against it via [avro.Resolve] and subsequent [Reader.Decode] calls use the
// resolved schema.
//
// If the callback returns (nil, nil), no resolution is performed and records
// decode against the writer schema directly — equivalent to not passing any
// reader-schema option at all.
//
// If the callback returns a non-nil error, [NewReader] returns that error.
//
// The callback must not call rd.Decode or rd.Close; rd is only valid for
// read-only header inspection during the callback.
//
// At most one of [WithReaderSchema] and [WithReaderSchemaFunc] may be used.
func WithReaderSchemaFunc(fn func(rd *Reader) (*avro.Schema, error)) ReaderOpt {
	return optReaderSchemaFunc{fn}
}

// WithMaxBlockBytes sets the maximum compressed block size in bytes that the
// reader will accept. The default is 64 MiB. This guards against malicious
// or corrupt files that declare very large blocks.
func WithMaxBlockBytes(n int64) ReaderOpt { return optMaxBlockBytes{n} }

// defaultMaxDecompressedBytes bounds the DECOMPRESSED size of a single block.
// It is the twin of the 64 MiB compressed limit (WithMaxBlockBytes): a block
// is read compressed off the wire, then inflated, and a built-in codec
// allocates the inflated size from a length declared inside the compressed
// payload — so without this an ~89-byte snappy frame can demand ~200 MiB (up
// to ~4 GiB at the format ceiling), and deflate's streaming reader is
// unbounded. 64 MiB is 1024× any default-writer block (64 KiB) while keeping a
// hostile block's footprint and decode time small (a crafted block of 1-byte
// records decodes in ~3 s rather than the tens of seconds a larger cap allows);
// producers writing larger blocks raise it with WithMaxDecompressedBlockBytes.
const defaultMaxDecompressedBytes = 64 << 20

// WithMaxDecompressedBlockBytes sets the maximum DECOMPRESSED size in bytes of
// a single block that the reader will accept. The default is 64 MiB.
// [WithMaxBlockBytes] bounds the compressed size read off the wire; this
// bounds what that compressed block inflates to, guarding against
// decompression-amplification ("zip bomb") inputs where a tiny compressed
// block declares or expands to a huge output. Because a block's record count
// is bounded by its decompressed length, this also bounds the per-block
// decode loop. Pass a larger value if you legitimately write blocks (via
// [WithBlockBytes]) that decompress beyond the default.
func WithMaxDecompressedBlockBytes(n int64) ReaderOpt { return optMaxDecompressedBytes{n} }

// WithSchemaOpts passes [avro.SchemaOpt] values (such as [avro.CustomType])
// to the [avro.Parse] call that parses the file header's embedded schema.
// This allows registering custom type conversions for the reader's schema.
func WithSchemaOpts(opts ...avro.SchemaOpt) ReaderOpt { return optSchemaOpts(opts) }

// DeflateCodec returns a [Codec] using raw DEFLATE compression at the given
// level (e.g. [flate.DefaultCompression]).
func DeflateCodec(level int) Codec { return deflateCodec{level: level} }

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
	closed     bool
}

// errClosed is returned by Writer/Reader methods invoked after Close. A closed
// OCF object is permanently unusable; mutating it would corrupt the file or
// reuse a released codec.
var errClosed = errors.New("ocf: operation on a closed OCF")

const defaultBlockBytes = 64 << 10 // 64 KiB

func (w *Writer) shouldFlush() bool {
	// The third clause keeps every written block within the Reader's
	// zero-byte bounds: readBlock rejects count > len(block)+
	// maxOCFZeroByteSlack, and Decode caps a CONSECUTIVE zero-byte run at
	// the same slack (both strict >). Datums that encode to ≥1 byte keep
	// count ≤ len(buf), so this clause fires only when zero-byte datums
	// (top-level "null", all-null records, size-0 fixed) accumulate —
	// without it they never grow the buffer, the byte-driven flush never
	// triggers, and the whole file lands in one block the Reader rejects.
	// Flushing at equality is exact: each datum raises count by 1 and the
	// buffer by ≥0, so the boundary cannot be jumped, and a block sealed
	// at count == len+slack passes both reader checks.
	return (w.maxCount > 0 && w.count >= w.maxCount) ||
		len(w.buf) >= w.maxBytes ||
		w.count >= len(w.buf)+maxOCFZeroByteSlack
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
	// Per Avro 1.11.3 spec ("Object Container Files → Header"): the
	// `avro.schema` metadata entry stores the schema of objects in the
	// file as JSON data. The spec is unqualified — Java writes
	// Schema.toString() (full JSON via writeProps, preserving
	// logicalType/precision/scale/doc/aliases/default; DataFileWriter.java
	// setMetaInternal) and fastavro writes json.dumps(schema) (full
	// schema dict; _write_py.py metadata["avro.schema"]).
	//
	// Pre-fix this used Schema.Canonical() — the Parsing Canonical Form
	// — which the spec defines for FINGERPRINTING (SchemaNormalization
	// section). PCF [STRIP] strips logicalType, precision, scale, doc,
	// aliases, default, etc. Three observable consequences:
	//   1. Downstream consumers relying on the self-describing OCF
	//      header to convey logical-type info got the raw underlying
	//      type (e.g. "long" instead of "long+timestamp-millis").
	//   2. ocf.NewReader(..., WithSchemaOpts(CustomType{LogicalType:X}))
	//      silently never matched, because the parsed header schema
	//      had no logicalType to dispatch on.
	//   3. Schema.Root().Fields[i].Type.Precision on a decoded OCF
	//      returned 0 even when the writer specified precision=10.
	//
	// Schema.String() returns Schema.full = the original JSON passed
	// to Parse, preserving every attribute that Java/fastavro also
	// preserve. WithSchema override (w.schemaJSON) is honored for
	// callers who deliberately want a different header schema text.
	schemaBytes := []byte(w.schema.String())
	if w.schemaJSON != "" {
		schemaBytes = []byte(w.schemaJSON)
	}
	meta := []kv{{"avro.schema", schemaBytes}}
	if name := w.codec.Name(); name != "null" {
		meta = append(meta, kv{"avro.codec", []byte(name)})
	}
	meta = append(meta, w.userMeta...)

	// Producer-side compliance with decodeMap's caps: refuse to write metadata
	// the reader would reject (and which NewAppendWriter, which re-reads the
	// header, would also reject) — emitting it would be a self-incompatible
	// file. decodeMap caps three things, so the writer mirrors all three: the
	// per-block ENTRY COUNT, each KEY length, and each VALUE length
	// (avro.schema gets the larger schema ceiling; every other key + all
	// values the generic 1 MiB cap).
	if int64(len(meta)) > ocfMetadataSafetyLimit {
		return fmt.Errorf("ocf: %d metadata entries exceed the %d-entry limit", len(meta), ocfMetadataSafetyLimit)
	}
	for _, e := range meta {
		if int64(len(e.key)) > ocfMetadataSafetyLimit {
			return fmt.Errorf("ocf: metadata key %q length %d exceeds the %d-byte limit", e.key, len(e.key), ocfMetadataSafetyLimit)
		}
		if lim := metadataValueLimit(e.key); int64(len(e.val)) > lim {
			return fmt.Errorf("ocf: metadata %q value length %d exceeds the %d-byte limit", e.key, len(e.val), lim)
		}
	}

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
// flushed automatically when it hits the count or byte limit, or when a
// run of zero-byte datums reaches the Reader's per-block bound (so files
// of zero-byte datums — "null", all-null records, size-0 fixed — are
// always readable back).
//
// A value error (v does not fit the schema) discards the failed datum
// and leaves the Writer usable: the datum was only ever appended to the
// in-memory block buffer, never the file, so previously accepted datums
// are intact and continue to flush.
//
// After an I/O or compression error — where the sink's state is not
// knowable — the Writer is poisoned: all subsequent calls return the
// same error.
func (w *Writer) Encode(v any) error {
	if w.closed {
		return errClosed
	}
	if w.err != nil {
		return w.err
	}
	// AppendEncode is append-only into w.buf: on error the first
	// len(w.buf) bytes are still exactly the previously accepted datums,
	// and the failed datum's partial bytes sit past them in the backing
	// array (hidden by the unchanged length, overwritten by the next
	// append). Not assigning the returned slice IS the recovery.
	buf, err := w.schema.AppendEncode(w.buf, v)
	if err != nil {
		return err
	}
	w.buf = buf
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
	if w.closed {
		return 0, errClosed
	}
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
	if w.closed {
		return errClosed
	}
	if w.err != nil {
		return w.err
	}
	if w.count > 0 {
		return w.flush()
	}
	return nil
}

// Close flushes any remaining items and closes the codec. The codec
// is closed even if the writer is in a poisoned state — zstd and
// similar codecs hold goroutines and buffers whose lifetime must be
// bounded; mirrors Java DataFileWriter.close's try { flush } finally
// { codec.close }.
//
// Close is idempotent: subsequent calls return nil without re-closing the
// codec. After Close, Encode, Write, Flush, and Reset all return an error
// rather than silently extending the file.
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	var flushErr error
	if w.err == nil && w.count > 0 {
		flushErr = w.flush()
	}
	closeErr := w.codec.Close()
	switch {
	case w.err != nil:
		return w.err
	case flushErr != nil:
		return flushErr
	default:
		return closeErr
	}
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
// Reset returns an error if the Writer has been closed — its codec is no
// longer usable.
func (w *Writer) Reset(dst io.Writer) error {
	if w.closed {
		return errClosed
	}
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
	schema, meta, sync, err := readHeader(br, nil)
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
	// 0 = unlimited: the append-writer compresses new data and never
	// decompresses untrusted blocks, so the read-side decompression cap
	// does not apply here.
	codec, err := resolveCodec(codecName, customCodecs, 0)
	if err != nil {
		return nil, err
	}

	if _, err := rws.Seek(0, io.SeekEnd); err != nil {
		codec.Close()
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
	r               *bufio.Reader
	schema          *avro.Schema
	codec           Codec
	sync            [16]byte
	meta            map[string][]byte
	block           []byte
	remain          int64
	zeroRun         int64 // consecutive zero-byte-consuming datums in the current block
	maxBlockBytes   int64
	maxDecompressed int64
	closed          bool
}

// readHeader reads and validates the OCF header, returning the parsed
// schema, raw metadata, and sync marker.
func readHeader(br *bufio.Reader, schemaOpts []avro.SchemaOpt) (schema *avro.Schema, meta map[string][]byte, sync [16]byte, err error) {
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
	schema, err = avro.Parse(string(schemaBytes), schemaOpts...)
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
func NewReader(r io.Reader, opts ...ReaderOpt) (_ *Reader, err error) {
	var customCodecs []Codec
	var readerSchema *avro.Schema
	var readerSchemaFn func(*Reader) (*avro.Schema, error)
	var maxBlockBytes int64
	var maxDecompressed int64
	var schemaOpts []avro.SchemaOpt
	for _, o := range opts {
		switch o := o.(type) {
		case optCodec:
			customCodecs = append(customCodecs, o.c)
		case optReaderSchema:
			readerSchema = o.s
		case optReaderSchemaFunc:
			readerSchemaFn = o.fn
		case optMaxBlockBytes:
			maxBlockBytes = o.n
		case optMaxDecompressedBytes:
			maxDecompressed = o.n
		case optSchemaOpts:
			schemaOpts = append(schemaOpts, o...)
		}
	}
	if readerSchema != nil && readerSchemaFn != nil {
		return nil, errors.New("ocf: WithReaderSchema and WithReaderSchemaFunc are mutually exclusive")
	}
	if maxBlockBytes <= 0 {
		maxBlockBytes = 1 << 26 // 64 MiB default
	}
	if maxDecompressed <= 0 {
		maxDecompressed = defaultMaxDecompressedBytes
	}

	br := bufio.NewReader(r)
	schema, meta, sync, err := readHeader(br, schemaOpts)
	if err != nil {
		return nil, err
	}

	// Resolve codec.
	codecName := "null"
	if c, ok := meta["avro.codec"]; ok {
		codecName = string(c)
	}
	codec, err := resolveCodec(codecName, customCodecs, maxDecompressed)
	if err != nil {
		return nil, err
	}
	// resolveCodec succeeded; from here, any error path must release
	// the codec's resources (zstd holds goroutines + buffers). Named
	// return so deferred error handling sees the final err value.
	defer func() {
		if err != nil {
			codec.Close()
		}
	}()

	rd := &Reader{
		r:               br,
		schema:          schema,
		codec:           codec,
		sync:            sync,
		meta:            meta,
		maxBlockBytes:   maxBlockBytes,
		maxDecompressed: maxDecompressed,
	}

	// If a reader-schema callback was provided, invoke it now that the
	// header has been parsed so it can inspect writer schema and metadata.
	if readerSchemaFn != nil {
		var chosen *avro.Schema
		chosen, err = readerSchemaFn(rd)
		if err != nil {
			return nil, fmt.Errorf("ocf: reader schema func: %w", err)
		}
		readerSchema = chosen
	}

	// Apply schema evolution if a reader schema was provided.
	if readerSchema != nil {
		var resolved *avro.Schema
		resolved, err = avro.Resolve(schema, readerSchema)
		if err != nil {
			return nil, fmt.Errorf("ocf: resolving reader schema: %w", err)
		}
		rd.schema = resolved
	}

	return rd, nil
}

// Decode reads the next datum into v, returning [io.EOF] at end of file.
func (rd *Reader) Decode(v any) error {
	if rd.closed {
		return errClosed
	}
	if rd.remain == 0 {
		if err := rd.readBlock(); err != nil {
			return err
		}
	}
	rest, err := rd.schema.Decode(rd.block, v)
	if err != nil {
		return fmt.Errorf("ocf: decoding datum: %w", err)
	}
	// Bound zero-byte records. A datum that consumes 0 wire bytes (the "null"
	// schema, or a record whose every field is null-typed) lets a block's
	// declared count drive the decode loop without ever exhausting the block,
	// so a hostile count amplifies a tiny input into a multi-second loop.
	// readBlock caps count against len(block)+slack, but a block padded with
	// ignored garbage (or one that decompressed large) inflates that bound;
	// this caps consecutive zero-consumption datums per block absolutely,
	// matching the maxZeroByteItems philosophy applied per-block. Non-zero-byte
	// records consume bytes and are bounded by len(block) directly.
	if len(rest) == len(rd.block) {
		rd.zeroRun++
		if rd.zeroRun > maxOCFZeroByteSlack {
			return fmt.Errorf("ocf: block yields more than %d zero-byte records (corrupt or hostile count)", maxOCFZeroByteSlack)
		}
	} else {
		rd.zeroRun = 0
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

// Close closes the codec, releasing any resources it holds. Close is
// idempotent: subsequent calls return nil without re-closing the codec. After
// Close, Decode returns an error.
func (rd *Reader) Close() error {
	if rd.closed {
		return nil
	}
	rd.closed = true
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
		return fmt.Errorf("ocf: block size %d exceeds safety limit of %d (raise WithMaxBlockBytes)", size, rd.maxBlockBytes)
	}
	// Guard against int truncation on 32-bit even when the user-configured
	// limit allows large values (e.g. larger than MaxInt32).
	if size > math.MaxInt {
		return fmt.Errorf("ocf: block size %d exceeds platform max int", size)
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
	// count == 0 still requires reading size + (zero-or-otherwise) data
	// + 16-byte sync, per spec ("Each block consists of: count, size,
	// objects, sync marker"). Java's DataFileStream.nextRawBlock and
	// fastavro's _iter_avro_records both validate the sync on count=0
	// blocks; bailing early on count alone meant a tail-truncated file
	// with a corrupt sync (where count happens to read as 0) was
	// silently accepted as clean EOF. After the sync is validated as a
	// real block boundary, an empty block is end-of-stream.
	if count == 0 {
		return io.EOF
	}
	block, err := rd.codec.Decompress(compressed)
	if err != nil {
		return fmt.Errorf("ocf: decompressing block: %w", err)
	}
	// Backstop the decompressed size after the fact. The built-in codecs cap
	// their own allocation; this also covers custom codecs (whose Decompress
	// the reader can't bound internally) and, because the count check below
	// is relative to len(block), bounds the per-block decode loop.
	if rd.maxDecompressed > 0 && int64(len(block)) > rd.maxDecompressed {
		return fmt.Errorf("ocf: decompressed block %d bytes exceeds limit of %d (raise WithMaxDecompressedBlockBytes)", len(block), rd.maxDecompressed)
	}
	// Bound count against the decompressed block length plus a small
	// slack for zero-byte-record schemas (EmptyRecord, records of all
	// null-typed fields). Each Avro record encodes to at least 0 bytes;
	// for non-zero-byte schemas count > len(block) is corruption, and
	// for zero-byte schemas count can grow unboundedly relative to len
	// (block) unless capped — without this check a 5-byte zigzag varint
	// claiming count=10^9 against a zero-byte schema would force the
	// user's `for rd.Decode(&v) == nil` loop to iterate that many times
	// (each call advancing rd.block by 0 bytes), producing a ~10^9 CPU
	// amplification on a tiny attacker input.
	//
	// Mirrors the maxZeroByteItems philosophy in deser.go:558 (Avro
	// array<null> / array<EmptyRecord> block-count cap): legitimate use
	// of zero-byte records with more than a few thousand per block is
	// essentially always a schema-design problem; tighter producers can
	// split into multiple blocks.
	//
	// Java's DataFileStream (DataFileStream.java:303) and fastavro's
	// _iter_avro_records (_read_py.py:807) leave this uncapped. twmb's
	// defense-in-depth strategy already applies the same shape to Avro
	// arrays and maps; OCF blocks are the structural twin.
	if count > int64(len(block))+maxOCFZeroByteSlack {
		return fmt.Errorf("ocf: block claims %d records but decompressed block is %d bytes (zero-byte slack: %d)",
			count, len(block), maxOCFZeroByteSlack)
	}
	rd.block = block
	rd.remain = count
	rd.zeroRun = 0
	return nil
}

// maxOCFZeroByteSlack is the cap on count - len(block) for an OCF block.
// Records that encode to >= 1 wire byte are bounded by len(block) directly;
// records that encode to 0 bytes (EmptyRecord and records whose every field
// is "null"-typed) consume this slack instead. Matches maxZeroByteItems in
// deser.go for Avro array<null>/array<EmptyRecord> block-counts.
const maxOCFZeroByteSlack = 4 << 10

// ---------- codecs ----------

// Memory bounds (security note).
//
// Two independent limits guard a block: WithMaxBlockBytes bounds the
// *compressed* size read off the wire (default 64 MiB), and
// WithMaxDecompressedBlockBytes bounds the *decompressed* size (default
// 64 MiB). The decompressed limit is the one that stops amplification:
// each built-in codec's Decompress would otherwise
// allocate a size declared inside the compressed payload, before the
// payload is validated —
//
//   - snappy: snappy.Decode pre-allocates from a varint header that
//     can declare up to ~4 GiB inside a 5-byte frame; bounded here by a
//     snappy.DecodedLen pre-check before Decode allocates.
//   - deflate: flate.NewReader streams without a header bound (io.ReadAll
//     would grow until the stream ends); bounded here by an
//     io.LimitReader(maxOut+1).
//   - zstd: the library default permits multi-GiB output; bounded here by
//     zstd.WithDecoderMaxMemory on the reader's decoder.
//
// readBlock also re-checks len(decompressed) against the limit as a
// backstop for custom codecs (whose Decompress the reader can't bound
// internally), and that bound transitively caps the per-block decode loop
// since a block's record count cannot exceed its decompressed length plus
// the zero-byte slack. Java's SnappyCodec (ByteBuffer.allocate(Snappy.
// uncompressedLength(...))) and fastavro's python-snappy decompress leave
// the decompressed side unbounded — the cap is twmb defense-in-depth, in
// the same family as WithMaxBlockBytes and maxZeroByteItems.

type nullCodec struct{}

func (nullCodec) Name() string                          { return "null" }
func (nullCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (nullCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (nullCodec) Close() error                          { return nil }

type deflateCodec struct {
	level int
	// maxOut bounds the decompressed output (0 = unlimited). The reader
	// sets it from WithMaxDecompressedBlockBytes; direct DeflateCodec
	// callers leave it unlimited. flate.NewReader streams without a header
	// length, so io.ReadAll grows unbounded without this — a deflate
	// "zip bomb" decompresses to arbitrary size.
	maxOut int64
}

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

func (c deflateCodec) Decompress(src []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(src))
	defer r.Close()
	if c.maxOut <= 0 {
		return io.ReadAll(r)
	}
	// Read at most maxOut+1 bytes: if the stream yields more, it exceeds the
	// limit and we reject without materializing the whole bomb.
	out, err := io.ReadAll(io.LimitReader(r, c.maxOut+1))
	if err != nil {
		return nil, err
	}
	if int64(len(out)) > c.maxOut {
		return nil, fmt.Errorf("ocf: decompressed block exceeds limit of %d bytes (raise WithMaxDecompressedBlockBytes)", c.maxOut)
	}
	return out, nil
}

type snappyCodec struct {
	// maxOut bounds the decompressed output (0 = unlimited). snappy.Decode
	// pre-allocates from a length declared in the frame header (up to ~4 GiB
	// in a few bytes), so the cap is checked via DecodedLen BEFORE Decode
	// allocates.
	maxOut int64
}

func (snappyCodec) Name() string { return "snappy" }
func (snappyCodec) Close() error { return nil }

func (snappyCodec) Compress(src []byte) ([]byte, error) {
	dst := snappy.Encode(nil, src)
	dst = binary.BigEndian.AppendUint32(dst, crc32.ChecksumIEEE(src))
	return dst, nil
}

func (c snappyCodec) Decompress(src []byte) ([]byte, error) {
	if len(src) < 4 {
		return nil, errors.New("ocf: snappy data too short for CRC checksum")
	}
	body := src[:len(src)-4]
	if c.maxOut > 0 {
		// snappy.Decode allocates the declared length up front; reject an
		// over-limit declaration before that allocation happens.
		n, err := snappy.DecodedLen(body)
		if err != nil {
			return nil, err
		}
		if int64(n) > c.maxOut {
			return nil, fmt.Errorf("ocf: decompressed block (%d bytes) exceeds limit of %d bytes (raise WithMaxDecompressedBlockBytes)", n, c.maxOut)
		}
	}
	decoded, err := snappy.Decode(nil, body)
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

// resolveCodec returns the codec named in the file header. maxDecompressed
// (0 = unlimited) bounds the decompressed size of a block for the built-in
// codecs that inflate untrusted input; a custom codec matched by name is
// returned as-is (its allocation is the implementor's responsibility — the
// reader still applies the post-decompress length backstop).
func resolveCodec(name string, custom []Codec, maxDecompressed int64) (Codec, error) {
	for _, c := range custom {
		if c.Name() == name {
			return c, nil
		}
	}
	switch name {
	case "null":
		return nullCodec{}, nil
	case "deflate":
		return deflateCodec{level: flate.DefaultCompression, maxOut: maxDecompressed}, nil
	case "snappy":
		return snappyCodec{maxOut: maxDecompressed}, nil
	case "zstandard":
		return ZstdCodec(nil, zstdReaderDopts(maxDecompressed))
	}
	return nil, fmt.Errorf("ocf: unknown codec %q", truncForError(name))
}

// zstdReaderDopts caps the zstd decoder's output memory so DecodeAll rejects a
// block that would inflate beyond maxDecompressed, instead of allocating up to
// the library default (64 GiB). A floor keeps the value within the decoder's
// accepted range for very small caps.
func zstdReaderDopts(maxDecompressed int64) []zstd.DOption {
	if maxDecompressed <= 0 {
		return nil
	}
	m := uint64(maxDecompressed)
	if m < 1<<20 {
		m = 1 << 20
	}
	return []zstd.DOption{zstd.WithDecoderMaxMemory(m)}
}

// truncForError caps a wire-derived string at 80 chars for inclusion in
// error messages. The OCF metadata-map value cap is 1 MiB per entry
// (ocfMetadataSafetyLimit), so without this an unknown-codec name from
// a hostile producer would echo up to 1 MiB into the parse error and
// 1:1-amplify through logging / RPC error trailers. Mirrors the
// truncForError helper in the root avro package (which is unexported
// and not reachable from this subpackage).
func truncForError(s string) string {
	const max = 80
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
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

// ocfMetadataSafetyLimit caps the per-entry length and per-block count
// for the OCF metadata map. 1 MiB is generous (real metadata keys/
// values are tens of bytes); the cap bounds hostile-input memory
// amplification (a malicious header claiming 2^62 entries would
// otherwise drive an unbounded make).
const ocfMetadataSafetyLimit = 1 << 20

// ocfSchemaSafetyLimit is the larger dedicated bound for the self-describing
// avro.schema metadata VALUE. A wide record's JSON legitimately exceeds 1 MiB
// (Java/fastavro read such files), so capping it at ocfMetadataSafetyLimit
// makes the writer's own large-schema files unreadable. The schema's parse
// cost is independently bounded by the schema parser's own guards, so a
// generous-but-finite ceiling (still bounding the make([]byte, valLen) alloc)
// is the right shape. The OCF writer enforces the same bound (producer-side
// compliance), so a twmb-written file is always twmb-readable.
const ocfSchemaSafetyLimit = 1 << 26 // 64 MiB

// metadataValueLimit returns the per-key value-length bound: the larger
// schema ceiling for the self-describing avro.schema key, the generic cap
// otherwise.
func metadataValueLimit(key string) int64 {
	if key == "avro.schema" {
		return ocfSchemaSafetyLimit
	}
	return ocfMetadataSafetyLimit
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
			if count < 0 {
				return nil, errors.New("ocf: invalid metadata map block count")
			}
			// Skip block byte-size.
			if _, err := binary.ReadVarint(r); err != nil {
				return nil, err
			}
		}
		if count > ocfMetadataSafetyLimit {
			return nil, fmt.Errorf("map block count %d exceeds safety limit", count)
		}
		for range int(count) {
			keyLen, err := binary.ReadVarint(r)
			if err != nil {
				return nil, err
			}
			if keyLen < 0 || keyLen > ocfMetadataSafetyLimit {
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
			if valLen < 0 || valLen > metadataValueLimit(string(key)) {
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
