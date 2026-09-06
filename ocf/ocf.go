// Package ocf implements Avro [Object Container Files] (OCF).
//
// An OCF is self-describing: the schema lives in the file header, so you do
// not need it out of band. Data sits in compressed blocks separated by sync
// markers, which makes files splittable for parallel processing. OCF is the
// standard way to store Avro on disk. To send individual values over the
// wire, see [avro.AppendSingleObject] instead.
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
// Null, deflate, snappy, and zstandard are built in. You can supply your own
// via [WithCodec].
//
// # Block size limits
//
// We cap both the compressed block we read off the wire
// ([WithMaxBlockBytes]) and what that block decompresses to
// ([WithMaxDecompressedBlockBytes]), each 64 MiB by default, to bound memory
// and decode time on untrusted input. We do not cap the writer, matching
// Java's DataFileWriter and fastavro: we write whatever blocks you give us.
//
// A single Avro datum cannot be split across blocks, so a value larger than
// the reader's default cap (say an 80 MiB blob) is written as one block that
// a default reader refuses. The error names the option to raise:
//
//	r, err := ocf.NewReader(f, ocf.WithMaxDecompressedBlockBytes(128<<20))
//
// Match the cap to the largest block your writer produces. For single large
// values that is the datum size, not [WithBlockBytes].
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
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/twmb/avro"
	"github.com/twmb/avro/internal/optmark"
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

	// Close releases whatever the codec holds. A codec that holds nothing
	// can return nil.
	Close() error
}

// BoundedDecompressor is an optional interface a [Codec] can implement. If
// yours does, we call DecompressBounded with our per-block cap (from
// [WithMaxDecompressedBlockBytes]) rather than [Codec.Decompress], so you can
// refuse an oversized block *before* allocating it. That is the only real
// defense against a decompression bomb, since checking the size afterward
// means the allocation already happened. A Codec without this method
// decompresses unbounded, so for untrusted data supply one that bounds
// itself. All built-in codecs do.
//
// Note that a wrapper embedding [Codec] does not inherit this method, because
// embedding an interface promotes only that interface's methods. Your wrapper
// must forward DecompressBounded itself, or it silently disables bounding for
// the codec it wraps. [NopCloser] forwards it.
//
// max <= 0 means no limit. max is constant across all calls for a given
// [Reader], so a codec that caches a configured decoder (say a zstd decoder
// built with a memory limit) may honor only the first call's max.
type BoundedDecompressor interface {
	DecompressBounded(src []byte, max int64) ([]byte, error)
}

// NopCloser returns a Codec that wraps c with a no-op Close, so that closing
// a [Writer] or [Reader] (or a constructor failing and releasing the codec it
// was handed) does not close a codec you share with another writer or reader.
// You close the underlying codec yourself once you are done with it.
//
// If c implements [BoundedDecompressor], so does the result: we forward the
// reader's decompression bound to c.
func NopCloser(c Codec) Codec { return nopCloser{c} }

type nopCloser struct{ Codec }

func (nopCloser) Close() error { return nil }

// DecompressBounded forwards the reader's per-block cap to the wrapped codec
// when it implements [BoundedDecompressor], and otherwise falls back to the
// unbounded [Codec.Decompress]: an unbounded codec honestly stays unbounded.
// We always define this method so that wrapping a bounding codec (every
// built-in) preserves the bound. Embedding the [Codec] interface alone would
// not, since it promotes only the interface's own methods.
func (n nopCloser) DecompressBounded(src []byte, max int64) ([]byte, error) {
	if b, ok := n.Codec.(BoundedDecompressor); ok {
		return b.DecompressBounded(src, max)
	}
	return n.Codec.Decompress(src)
}

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
func (optSchemaOpts) writerOpt() {}

type optDecodeOpts []avro.Opt

func (optDecodeOpts) readerOpt() {}

// WithCodec sets the compression codec, overriding the default of null (no
// compression). It is both a [WriterOpt] and a [ReaderOpt]. You do not need
// to register the four built-in codecs (null, deflate, snappy, zstandard) to
// read them. A custom codec whose name matches a built-in overrides it.
//
// We take ownership of the codec you pass and close it exactly once: in
// [Writer.Close] or [Reader.Close] when the constructor succeeds, or in the
// constructor itself when it fails or does not use the codec. Note that the
// last case is easy to hit: [NewReader] and [NewAppendWriter] only use a
// codec whose Name matches the header's avro.codec, NewWriter only uses the
// last WithCodec you pass, and we close every codec we do not use.
//
// If you share one codec across several writers, readers, or files, give it
// a Close that returns nil, or wrap it in [NopCloser].
//
// We ignore a nil codec, whether a nil Codec or a non-nil Codec holding a nil
// pointer, on every constructor: we never name, adopt, or close it, and the
// constructor behaves as though you had not passed it. If it is the only
// codec supplied for a name the file uses, the constructor reports an
// unknown codec.
//
// For reader-side decompression bounding, see [WithMaxDecompressedBlockBytes].
// It applies to any codec you supply that implements [BoundedDecompressor],
// which every built-in does, including one wrapped by [NopCloser].
func WithCodec(c Codec) Opt { return optCodec{c} }

// WithBlockCount caps the number of items per block, overriding the default
// of 0 (unlimited). We flush a block when either this or [WithBlockBytes] is
// reached.
func WithBlockCount(n int) WriterOpt { return optBlockCount{n} }

// WithBlockBytes caps a block's uncompressed bytes, overriding the default of
// 64 KiB. We flush a block when either this or [WithBlockCount] is reached.
func WithBlockBytes(n int) WriterOpt { return optBlockBytes{n} }

// WithMetadata adds custom metadata to the file header. The spec reserves
// keys starting with "avro.". Repeated calls are cumulative.
func WithMetadata(m map[string][]byte) WriterOpt { return optMetadata{m} }

// WithSyncMarker sets the 16-byte sync marker between blocks, overriding the
// random one we generate. Mostly useful for deterministic test output.
func WithSyncMarker(sync [16]byte) WriterOpt { return optSyncMarker{sync} }

// WithSchema sets the schema JSON we write to the file header, overriding the
// default of [avro.Schema.String]: the original JSON you passed to
// [avro.Parse], with logicalType, precision, scale, doc, aliases, and default
// all preserved, matching Java's DataFileWriter and fastavro. Use this if you
// want different header text, say the Parsing Canonical Form from
// [avro.Schema.Canonical].
func WithSchema(schema string) WriterOpt { return optSchema{schema} }

// WithReaderSchema gives us a reader schema to resolve the file's writer
// schema against via [avro.Resolve]. [Reader.Decode] then uses the resolved
// schema. Fields you add in the reader schema must have defaults, and we skip
// writer fields your reader schema omits.
//
// Use [WithReaderSchemaFunc] if you must pick the reader schema from the
// file's header. You can use at most one of the two.
func WithReaderSchema(s *avro.Schema) ReaderOpt { return optReaderSchema{s} }

// WithReaderSchemaFunc is the dynamic counterpart to [WithReaderSchema]. We
// call fn from [NewReader] once the OCF header is parsed, so it can inspect
// the file's writer schema and metadata through rd.Schema() and rd.Metadata()
// before choosing what to resolve against.
//
// A non-nil schema is resolved against the writer schema via [avro.Resolve],
// and [Reader.Decode] then uses the result. (nil, nil) means no resolution:
// records decode against the writer schema directly, as if you passed
// no reader-schema option at all. A non-nil error is returned from
// [NewReader].
//
// Your fn must not call rd.Decode or rd.Close; rd is valid only for
// read-only header inspection during the call. You can use at most one of
// [WithReaderSchema] and WithReaderSchemaFunc.
func WithReaderSchemaFunc(fn func(rd *Reader) (*avro.Schema, error)) ReaderOpt {
	return optReaderSchemaFunc{fn}
}

// WithMaxBlockBytes sets the maximum compressed block size in bytes we accept
// when reading, overriding the default of 64 MiB. It guards against malicious
// or corrupt files that declare very large blocks.
func WithMaxBlockBytes(n int64) ReaderOpt { return optMaxBlockBytes{n} }

// defaultMaxDecompressedBytes bounds the decompressed size of a single block,
// the twin of the compressed limit. A built-in codec allocates the inflated
// size from a length declared inside the compressed payload, so without this
// an ~89-byte snappy frame can demand ~200 MiB. 64 MiB is 1024x any
// default-writer block and keeps a hostile block's decode time to seconds.
const defaultMaxDecompressedBytes = 64 << 20

// defaultMaxBlockBytes is the reader's default ceiling on a block's *declared
// compressed* size, i.e. the [WithMaxBlockBytes] default. We name it rather
// than write it inline because a bound with no name is invisible to any guard
// keyed on names. Such a guard classifies each cap by its constant, and this
// half of the reader-only block pair could not be classified while it was a
// literal.
const defaultMaxBlockBytes = 1 << 26

// ocfEagerBlockAllocLimit is the largest declared compressed block size that
// readBlock allocates in one shot. It is the default WithMaxBlockBytes,
// derived rather than restated, so a reader at the default cap never leaves
// the eager path; a larger block, reachable only once you raise the cap, is
// read incrementally so a declared-but-absent size cannot force an
// allocation up to the raised cap.
const ocfEagerBlockAllocLimit = defaultMaxBlockBytes

// WithMaxDecompressedBlockBytes sets the maximum decompressed size in bytes
// of a single block we accept when reading, overriding the default of 64 MiB.
// [WithMaxBlockBytes] bounds the compressed size we read off the wire; this
// bounds what that block inflates to, guarding against "zip bomb" inputs
// where a tiny compressed block expands to a huge output. A block's record
// count is bounded by its decompressed length, so this also bounds the
// per-block decode loop. Raise it if you write blocks (via [WithBlockBytes])
// that decompress beyond the default.
//
// We pass the limit to the codec's [BoundedDecompressor.DecompressBounded],
// which refuses an over-cap block before allocating it. This applies to every
// codec implementing that interface: one we resolve by name from the file
// header, and one you supply via [WithCodec], including one wrapped by
// [NopCloser]. All four built-in codecs implement it. A codec that does not
// decompresses unbounded, and there is no post-decompression check, since the
// allocation would already have happened. Supply a self-bounding codec for
// untrusted data.
func WithMaxDecompressedBlockBytes(n int64) ReaderOpt { return optMaxDecompressedBytes{n} }

// WithSchemaOpts passes [avro.SchemaOpt] values, such as [avro.CustomType] or
// [avro.WithLaxNames], to the [avro.Parse] of the header's embedded schema.
// [NewReader] uses it to register custom type conversions and to accept
// lax-named header schemas. [NewAppendWriter] needs it whenever the header
// schema requires an option to parse at all. [NewWriter] ignores it: you
// already parsed its schema.
func WithSchemaOpts(opts ...avro.SchemaOpt) Opt { return optSchemaOpts(opts) }

// WithDecodeOpts passes [avro.Opt] values to the [avro.Schema.Decode] behind
// every [Reader.Decode], overriding the default of no options. Without it you
// cannot use [avro.TaggedUnions] or [avro.TagLogicalTypes], which change what
// a union decodes to in an *any target. Repeated calls are cumulative.
// [NewWriter] and [NewAppendWriter] ignore it.
//
// We drop any option that would make decoded values point into the decode
// input, such as [avro.AliasInput]. A Reader decodes out of a block buffer it
// owns, and we do not promise that buffer outlives the next read.
func WithDecodeOpts(opts ...avro.Opt) ReaderOpt {
	kept := make(optDecodeOpts, 0, len(opts))
	for _, o := range opts {
		if _, aliases := o.(optmark.AliasesInput); !aliases {
			kept = append(kept, o)
		}
	}
	return kept
}

// DeflateCodec returns a [Codec] using raw DEFLATE compression at the given
// level (e.g. [flate.DefaultCompression]).
func DeflateCodec(level int) Codec { return deflateCodec{level: level} }

// SnappyCodec returns a [Codec] using Snappy compression with a trailing
// CRC-32 checksum per block, as required by the Avro spec.
func SnappyCodec() Codec { return snappyCodec{} }

// ZstdCodec returns a [Codec] using Zstandard compression. We pass eopts to
// [zstd.NewWriter] and dopts to [zstd.NewReader]; both may be nil for
// defaults. We prepend [zstd.WithEncoderConcurrency](1) and
// [zstd.WithDecoderConcurrency](1), so pass a different concurrency to
// override. You can share one ZstdCodec across readers and writers via
// [NopCloser].
//
// ZstdCodec implements [BoundedDecompressor]: we build the decoder lazily on
// first read with [zstd.WithDecoderMaxMemory] set from the reader's
// [WithMaxDecompressedBlockBytes] cap, and then cache that decoder. Note that
// a ZstdCodec shared across readers with different caps therefore honors the
// first reader's cap; set [zstd.WithDecoderMaxMemory] in dopts to pin a bound
// regardless of reader. We raise a cap below [zstd.MinWindowSize] (1 KiB) up
// to it, since a smaller limit would reject every frame.
func ZstdCodec(eopts []zstd.EOption, dopts []zstd.DOption) (Codec, error) {
	eopts = append([]zstd.EOption{zstd.WithEncoderConcurrency(1)}, eopts...)
	enc, err := zstd.NewWriter(nil, eopts...)
	if err != nil {
		return nil, fmt.Errorf("ocf: creating zstd encoder: %w", err)
	}
	// The decoder is built lazily by DecompressBounded so the reader's
	// per-block cap can be folded in as WithDecoderMaxMemory; store the
	// caller's decoder options until then.
	return &zstdCodec{enc: enc, dopts: dopts}, nil
}

// MustZstdCodec is like [ZstdCodec] but panics on error.
func MustZstdCodec(eopts []zstd.EOption, dopts []zstd.DOption) Codec {
	c, err := ZstdCodec(eopts, dopts)
	if err != nil {
		panic(err)
	}
	return c
}

var magic = [4]byte{'O', 'b', 'j', 0x01}

// randRead generates sync markers. It is a variable so tests can override it
// to simulate errors.
var randRead = rand.Read

// Writer encodes Avro values into an OCF. We buffer values into blocks and
// compress and flush them for you. You must Close to flush what remains.
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
	// zero-byte bounds: zero-byte datums never grow the buffer, so without
	// it the byte-driven flush never triggers and the whole file ends up in
	// one block the Reader rejects. Flushing at equality is exact, since
	// each datum raises count by 1.
	return (w.maxCount > 0 && w.count >= w.maxCount) ||
		len(w.buf) >= w.maxBytes ||
		w.count >= len(w.buf)+maxOCFZeroByteSlack
}

// Schema returns the schema used by this Writer.
func (wr *Writer) Schema() *avro.Schema { return wr.schema }

// NewWriter creates a Writer that writes an OCF to w. We write the file
// header immediately.
func NewWriter(w io.Writer, s *avro.Schema, opts ...WriterOpt) (_ *Writer, err error) {
	wr := &Writer{
		w:      w,
		schema: s,
		codec:  nullCodec{},
	}
	// Any error return from here on must release whatever codec wr holds: a
	// failing constructor returns no Writer, and the codec is commonly built
	// inline in the call, leaving no handle to close. We register the defer
	// before the option loop so it covers the codec whenever the loop adopts
	// it. NewReader carries the same defer.
	defer func() {
		if err != nil {
			wr.codec.Close()
		}
	}()

	// The other half of the same rule: WithCodec written more than once
	// adopts the last and drops the rest, and a dropped codec has no owner;
	// see releaseUnadopted. An error return inside the loop leaves adopted
	// at -1, which releases every codec collected so far.
	var supplied []Codec
	adopted := -1
	defer func() { releaseUnadopted(supplied, adopted) }()

	for _, o := range opts {
		switch o := o.(type) {
		case optCodec:
			supplied = append(supplied, o.c)
		case optBlockCount:
			wr.maxCount = o.n
		case optBlockBytes:
			wr.maxBytes = o.n
		case optMetadata:
			for k, v := range o.m {
				wr.userMeta = append(wr.userMeta, kv{k, v})
			}
		case optSyncMarker:
			wr.sync = o.sync
			wr.hasSync = true
		case optSchema:
			wr.schemaJSON = o.s
		}
	}
	// Last non-nil WithCodec wins. Adopting after the loop keeps the
	// superseded codecs in supplied with adopted pointing past them, so the
	// deferred sweep can tell dropped from taken. A nil offer is not a
	// codec and cannot win, as resolveCodec skips nils on the reader side;
	// adopting it would defer the crash to writeHeader.
	for i := len(supplied) - 1; i >= 0; i-- {
		if !isNilCodec(supplied[i]) {
			adopted = i
			wr.codec = supplied[i]
			break
		}
	}
	// Validating reserved metadata keys after the option loop rather than
	// inside it keeps the rejection independent of where WithMetadata sits
	// among the options: rejecting mid-loop returned before a later WithCodec
	// had been adopted and after an earlier one had, so one spelling released
	// the codec and the other never took it. Collect first, then check.
	for _, e := range wr.userMeta {
		if strings.HasPrefix(e.key, "avro.") {
			return nil, fmt.Errorf("ocf: metadata key %q is reserved (avro.* namespace)", truncForError(e.key))
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
	// The spec's avro.schema entry is the schema as JSON, and Java and
	// fastavro both write the full schema, logical types and docs included.
	// Not Canonical(), whose STRIP rule would cost the header its logical
	// types. WithSchema overrides the text.
	schemaBytes := []byte(w.schema.String())
	if w.schemaJSON != "" {
		schemaBytes = []byte(w.schemaJSON)
	}
	meta := []kv{{"avro.schema", schemaBytes}}
	if name := w.codec.Name(); name != "null" {
		meta = append(meta, kv{"avro.codec", []byte(name)})
	}
	meta = append(meta, w.userMeta...)

	// We refuse to write metadata the reader would reject, since emitting it
	// would be a self-incompatible file: decodeMap caps the entry count, each
	// key length, and each value length, so we mirror all three.
	if int64(len(meta)) > ocfMetadataSafetyLimit {
		return fmt.Errorf("ocf: %d metadata entries exceed the %d-entry limit", len(meta), ocfMetadataSafetyLimit)
	}
	for _, e := range meta {
		if int64(len(e.key)) > ocfMetadataSafetyLimit {
			return fmt.Errorf("ocf: metadata key %q length %d exceeds the %d-byte limit", truncForError(e.key), len(e.key), ocfMetadataSafetyLimit)
		}
		if lim := metadataValueLimit(e.key); int64(len(e.val)) > lim {
			return fmt.Errorf("ocf: metadata %q value length %d exceeds the %d-byte limit", truncForError(e.key), len(e.val), lim)
		}
	}

	var hdr []byte
	hdr = append(hdr, magic[:]...)
	hdr = encodeMap(hdr, meta)
	hdr = append(hdr, w.sync[:]...)
	if err := writeFull(w.w, hdr); err != nil {
		return fmt.Errorf("ocf: writing header: %w", err)
	}
	return nil
}

// Encode serializes v and appends it to the current block. We flush the block
// when it hits the count or byte limit, or when a run of zero-byte datums
// reaches the Reader's per-block bound, so a file of zero-byte datums
// ("null", all-null records, size-0 fixed) always reads back.
//
// If v does not fit the schema, we discard that datum and the Writer remains
// usable: we had only appended it to the in-memory block, never the file, so
// datums we already accepted are intact and still flush.
//
// After an I/O or compression error, where we cannot know the sink's state,
// we poison the Writer: every subsequent call returns the same error.
func (w *Writer) Encode(v any) error {
	if w.closed {
		return errClosed
	}
	if w.err != nil {
		return w.err
	}
	// AppendEncode is append-only into w.buf: on error the first len(w.buf)
	// bytes are still exactly the previously accepted datums, and the failed
	// datum's partial bytes sit past them in the backing array (hidden by the
	// unchanged length, overwritten by the next append). Not assigning the
	// returned slice *is* the recovery.
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
// block. You must ensure p is exactly one datum encoded with the writer's
// schema. We flush by the same rules as [Encode].
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

// Close flushes any remaining items and closes the codec. We close the codec
// even when the writer is poisoned, because zstd and similar codecs hold
// goroutines and buffers that must be released.
//
// Close is idempotent: later calls return nil without re-closing the codec.
// After Close, Encode, Write, Flush, and Reset all return an error rather
// than silently extending the file.
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
	if err := writeFull(w.w, block); err != nil {
		w.err = err
		return fmt.Errorf("ocf: writing block: %w", err)
	}
	w.buf = w.buf[:0]
	w.count = 0
	return nil
}

// Reset flushes buffered items to the current destination, then starts a new
// OCF on dst with the original schema, codec, and options. If the Writer is in
// an error state we skip the flush and clear the error. Reset errors if you
// already closed the Writer, since its codec is no longer usable.
//
// If Reset fails after switching to dst, on either a sync-marker generation
// error or a header write error, we poison the Writer as a failed
// [Writer.Encode] or [Writer.Flush] does: every subsequent Encode, Flush, and
// Close returns the error until a later successful Reset clears it. (A failed
// flush of the old destination also poisons.) Otherwise, ignoring Reset's
// error and writing on would emit a headerless byte stream onto dst.
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
	// The two steps below run *after* the sink has been repointed to dst. A
	// failure here leaves the Writer half-reset (new sink, no header written),
	// so we poison it: otherwise a caller that ignores the returned error and
	// keeps writing emits a silent headerless stream onto dst. A later Reset
	// clears w.err and recovers, matching the flush arm above.
	if !w.hasSync {
		if _, err := randRead(w.sync[:]); err != nil {
			w.err = fmt.Errorf("ocf: generating sync marker: %w", err)
			return w.err
		}
	}
	if err := w.writeHeader(); err != nil {
		w.err = err
		return w.err
	}
	return nil
}

// NewAppendWriter opens an existing OCF for appending. We read the header to
// recover the schema, codec, and sync marker, then seek to the end.
//
// We honor [WithBlockCount] and [WithBlockBytes]. [WithCodec] supplies an
// implementation for a non-built-in codec, matched by name against the
// header. [WithSchemaOpts] applies to the header-schema parse, which you need
// whenever that schema requires an option to parse at all (say
// [avro.WithLaxNames] for a file written with a lax-named schema). We ignore
// [WithSchema], [WithSyncMarker], and [WithMetadata]: the header is already
// on disk and we never rewrite it, so the schema, sync marker, and metadata
// always come from the existing file. Java's DataFileWriter.appendTo and
// fastavro's append mode behave the same. We likewise ignore any other
// option.
func NewAppendWriter(rws io.ReadWriteSeeker, opts ...WriterOpt) (*Writer, error) {
	var schemaOpts []avro.SchemaOpt
	var customCodecs []Codec
	adopted := -1
	// The header names one codec, so every other supplied codec is an offer
	// this constructor declines and nothing else will ever close; see
	// releaseUnadopted. Collected and deferred before the header read so a
	// failure there, which adopts nothing, still releases all of them.
	defer func() { releaseUnadopted(customCodecs, adopted) }()
	for _, o := range opts {
		switch o := o.(type) {
		case optSchemaOpts:
			schemaOpts = append(schemaOpts, o...)
		case optCodec:
			customCodecs = append(customCodecs, o.c)
		}
	}
	br := bufio.NewReader(&checkedReader{r: rws})
	schema, meta, sync, err := readHeader(br, schemaOpts)
	if err != nil {
		return nil, err
	}

	codecName := "null"
	if c, ok := meta["avro.codec"]; ok {
		codecName = string(c)
	}
	// The append-writer compresses new data and never decompresses untrusted
	// blocks, so the read-side decompression cap does not apply here.
	var codec Codec
	codec, adopted, err = resolveCodec(codecName, customCodecs)
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

// checkedReader converts an io.Reader contract violation, a count outside
// [0, len(p)] with a nil error, into a named error before bufio's buffer
// arithmetic panics on it. It also bounds a reader stuck returning (0, nil),
// which bufio hands through verbatim on large direct reads.
type checkedReader struct {
	r          io.Reader
	emptyReads int
}

// maxConsecutiveEmptyReads mirrors bufio's bound before it reports
// io.ErrNoProgress, applied here uniformly so every read path shares it.
const maxConsecutiveEmptyReads = 100

func (c *checkedReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n < 0 || n > len(p) {
		return 0, fmt.Errorf("ocf: reader returned invalid count %d for a %d-byte read", n, len(p))
	}
	if n == 0 && err == nil && len(p) > 0 {
		c.emptyReads++
		if c.emptyReads >= maxConsecutiveEmptyReads {
			return 0, io.ErrNoProgress
		}
	} else {
		c.emptyReads = 0
	}
	return n, err
}

// writeFull converts an io.Writer contract violation (a nil error with
// n != len(p)) into an error. Trusting the lying count would silently
// truncate the file, detectable only when a reader later hits the corruption.
// A short count maps to io.ErrShortWrite, io.Copy's and bufio.Writer's
// discipline; a count outside [0, len(p)] we name.
func writeFull(w io.Writer, p []byte) error {
	n, err := w.Write(p)
	if err != nil {
		return err
	}
	if n != len(p) {
		if n < 0 || n > len(p) {
			return fmt.Errorf("ocf: writer returned invalid count %d for a %d-byte write", n, len(p))
		}
		return io.ErrShortWrite
	}
	return nil
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
	// decodeOpts are the caller's [WithDecodeOpts], already stripped of
	// anything that would alias block.
	decodeOpts []avro.Opt
}

// noEOF converts a bare io.EOF from a mid-structure read into
// io.ErrUnexpectedEOF. io.EOF is Decode's end-of-stream sentinel, so it must
// be reachable only from the block-count read at the top of readBlock; the
// stdlib readers return bare io.EOF for any other cut, and a %w wrap would
// make the idiomatic termination check read a truncated file as complete.
func noEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

// readHeader never returns a bare io.EOF: NewReader has no end-of-stream
// sentinel, since a stream that ends anywhere inside the header is truncated.
// We normalize every read error through noEOF, for uniformity with readBlock.
func readHeader(br *bufio.Reader, schemaOpts []avro.SchemaOpt) (schema *avro.Schema, meta map[string][]byte, sync [16]byte, err error) {
	var m [4]byte
	if _, err = io.ReadFull(br, m[:]); err != nil {
		return nil, nil, sync, fmt.Errorf("ocf: reading magic: %w", noEOF(err))
	}
	if m != magic {
		return nil, nil, sync, fmt.Errorf("ocf: invalid magic %x", m)
	}

	meta, err = decodeMap(br)
	if err != nil {
		// Single normalization chokepoint for every stream read inside
		// decodeMap (map counts, key/value lengths, key/value bytes).
		return nil, nil, sync, fmt.Errorf("ocf: reading metadata: %w", noEOF(err))
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
		return nil, nil, sync, fmt.Errorf("ocf: reading sync marker: %w", noEOF(err))
	}

	return schema, meta, sync, nil
}

// NewReader creates a Reader that decodes an OCF from r. We read the header
// immediately. Use [WithCodec] if the file uses a non-built-in codec.
func NewReader(r io.Reader, opts ...ReaderOpt) (_ *Reader, err error) {
	var customCodecs []Codec
	var readerSchema *avro.Schema
	var readerSchemaFn func(*Reader) (*avro.Schema, error)
	var maxBlockBytes int64
	var maxDecompressed int64
	var schemaOpts []avro.SchemaOpt
	var decodeOpts []avro.Opt
	adopted := -1
	// The header names one codec; every other supplied codec is an offer this
	// constructor declines and nothing else will ever close; see
	// releaseUnadopted. Registered before the option loop so it covers whatever
	// the loop collected however the constructor exits, including the arms that
	// return before a codec has been chosen at all (adopted is still -1 there, so
	// all of them are released).
	defer func() { releaseUnadopted(customCodecs, adopted) }()
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
		case optDecodeOpts:
			decodeOpts = append(decodeOpts, o...)
		}
	}
	if readerSchema != nil && readerSchemaFn != nil {
		return nil, errors.New("ocf: WithReaderSchema and WithReaderSchemaFunc are mutually exclusive")
	}
	if maxBlockBytes <= 0 {
		maxBlockBytes = defaultMaxBlockBytes
	}
	if maxDecompressed <= 0 {
		maxDecompressed = defaultMaxDecompressedBytes
	}

	br := bufio.NewReader(&checkedReader{r: r})
	schema, meta, sync, err := readHeader(br, schemaOpts)
	if err != nil {
		return nil, err
	}

	codecName := "null"
	if c, ok := meta["avro.codec"]; ok {
		codecName = string(c)
	}
	var codec Codec
	codec, adopted, err = resolveCodec(codecName, customCodecs)
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
		decodeOpts:      decodeOpts,
	}

	// After the header parse, so the callback can inspect the writer schema
	// and metadata through rd.
	if readerSchemaFn != nil {
		var chosen *avro.Schema
		chosen, err = readerSchemaFn(rd)
		if err != nil {
			return nil, fmt.Errorf("ocf: reader schema func: %w", err)
		}
		readerSchema = chosen
	}

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

// Decode reads the next datum into v, returning [io.EOF] at end of file. We
// return [io.EOF] only at a clean end of stream, where the file ends exactly
// at a block boundary. A stream truncated mid-block, with a promised block
// header, data, or sync marker cut short, returns an error matching
// [io.ErrUnexpectedEOF] instead, never one matching [io.EOF].
func (rd *Reader) Decode(v any) error {
	if rd.closed {
		return errClosed
	}
	if rd.remain == 0 {
		if err := rd.readBlock(); err != nil {
			return err
		}
	}
	rest, err := rd.schema.Decode(rd.block, v, rd.decodeOpts...)
	if err != nil {
		// noEOF: a datum error matching io.EOF (e.g. a CustomType decode
		// callback returning it) must not come back as the clean-end sentinel.
		return fmt.Errorf("ocf: decoding datum: %w", noEOF(err))
	}
	// Bound zero-byte records: a datum that consumes 0 wire bytes lets a
	// block's declared count drive the decode loop without exhausting the
	// block. readBlock caps count against len(block) plus slack, but padding
	// inflates that bound, so this caps consecutive zero-consumption datums
	// per block absolutely.
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

// Metadata returns the raw metadata from the file header, both "avro.*" and
// your own keys. Do not modify the returned map.
func (rd *Reader) Metadata() map[string][]byte { return rd.meta }

// Close closes the codec, releasing what it holds. Close is idempotent: later
// calls return nil without re-closing. After Close, Decode returns an error.
func (rd *Reader) Close() error {
	if rd.closed {
		return nil
	}
	rd.closed = true
	return rd.codec.Close()
}

// readBlock advances to the next block containing at least one datum. We skip
// validated count-0 blocks. io.EOF means the block-count read hit the true
// end of the stream.
func (rd *Reader) readBlock() error {
	for {
		count, err := binary.ReadVarint(rd.r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return io.EOF
			}
			return fmt.Errorf("ocf: reading block count: %w", err)
		}
		size, err := binary.ReadVarint(rd.r)
		if err != nil {
			// A complete count varint promised a block; a stream ending
			// here is truncated, not ended, and noEOF keeps the io.EOF
			// sentinel exclusive to the count read above.
			return fmt.Errorf("ocf: reading block size: %w", noEOF(err))
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
		// size is the attacker-declared block length, bounded only by
		// WithMaxBlockBytes. We allocate it up front only within the default
		// cap, and read incrementally beyond, so a declared-but-absent size
		// fails after consuming what is there rather than forcing an
		// allocation up to a raised cap.
		var compressed []byte
		if size <= ocfEagerBlockAllocLimit {
			compressed = make([]byte, int(size))
			if _, err := io.ReadFull(rd.r, compressed); err != nil {
				return fmt.Errorf("ocf: reading block data: %w", noEOF(err))
			}
		} else {
			var buf bytes.Buffer
			if n, err := io.CopyN(&buf, rd.r, size); err != nil {
				// io.CopyN reports *any* shortfall, zero bytes or a partial
				// copy, as bare io.EOF, unlike io.ReadFull's
				// ErrUnexpectedEOF on partial reads; both shapes normalize.
				return fmt.Errorf("ocf: reading block data (%d of %d bytes): %w", n, size, noEOF(err))
			}
			compressed = buf.Bytes()
		}
		var sync [16]byte
		if _, err := io.ReadFull(rd.r, sync[:]); err != nil {
			return fmt.Errorf("ocf: reading block sync marker: %w", noEOF(err))
		}
		if sync != rd.sync {
			return errors.New("ocf: sync marker mismatch")
		}
		// A count=0 block still requires reading size, data and sync, per
		// spec; bailing on count alone would accept a tail-truncated file as
		// a clean end. We then skip the block, as fastavro does; Java never
		// writes one. The skipped payload is consumed off the wire, bounded
		// like any block, but never handed to the codec.
		if count == 0 {
			continue
		}
		// The codec's bounded path refuses an over-cap block before
		// allocating it; every built-in implements BoundedDecompressor, and a
		// custom codec that does not is unbounded.
		var block []byte
		if b, ok := rd.codec.(BoundedDecompressor); ok {
			block, err = b.DecompressBounded(compressed, rd.maxDecompressed)
		} else {
			block, err = rd.codec.Decompress(compressed)
		}
		if err != nil {
			// noEOF: a codec error matching io.EOF must not come back as Decode's
			// clean-end sentinel (the user's failing Decompress would read as a
			// silently shorter file).
			return fmt.Errorf("ocf: decompressing block: %w", noEOF(err))
		}
		// Bound count against the decompressed length plus slack for
		// zero-byte schemas, where count is otherwise unbounded relative to
		// len(block) and a 5-byte varint claiming 10^9 would iterate your
		// Decode loop that many times. Java and fastavro leave this uncapped.
		if count > int64(len(block))+maxOCFZeroByteSlack {
			return fmt.Errorf("ocf: block claims %d records but decompressed block is %d bytes (zero-byte slack: %d)",
				count, len(block), maxOCFZeroByteSlack)
		}
		rd.block = block
		rd.remain = count
		rd.zeroRun = 0
		return nil
	}
}

// maxOCFZeroByteSlack is the cap on count - len(block) for an OCF block.
// Records that encode to >= 1 wire byte are bounded by len(block) directly;
// records that encode to 0 bytes (EmptyRecord and records whose every field
// is "null"-typed) consume this slack instead. Matches maxZeroByteItems in
// deser.go for Avro array<null>/array<EmptyRecord> block-counts.
const maxOCFZeroByteSlack = 4 << 10

// ---------- codecs ----------

// Memory bounds. WithMaxBlockBytes bounds the compressed size read off the
// wire and WithMaxDecompressedBlockBytes bounds what that block inflates to.
// Each built-in codec implements BoundedDecompressor and refuses an over-cap
// block before allocating it: snappy through a DecodedLen pre-check, deflate
// through an io.LimitReader, zstd through WithDecoderMaxMemory, and null by
// rejecting an over-cap raw block. Java and fastavro leave the decompressed
// side unbounded.

type nullCodec struct{}

func (nullCodec) Name() string                        { return "null" }
func (nullCodec) Compress(src []byte) ([]byte, error) { return src, nil }

func (c nullCodec) Decompress(src []byte) ([]byte, error) { return c.DecompressBounded(src, 0) }

func (nullCodec) DecompressBounded(src []byte, max int64) ([]byte, error) {
	if max > 0 && int64(len(src)) > max {
		return nil, fmt.Errorf("ocf: decompressed block %d bytes exceeds limit of %d bytes (raise WithMaxDecompressedBlockBytes)", len(src), max)
	}
	return src, nil
}

func (nullCodec) Close() error { return nil }

type deflateCodec struct {
	level int
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
	return c.DecompressBounded(src, 0)
}

func (deflateCodec) DecompressBounded(src []byte, max int64) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(src))
	defer r.Close()
	if max <= 0 {
		// flate.NewReader streams without a header length, so io.ReadAll grows
		// unbounded: a deflate "zip bomb" decompresses to arbitrary size. Only
		// the trusted writer path (and an explicit "no limit") reaches here.
		return io.ReadAll(r)
	}
	// Read at most max+1 bytes: if the stream yields more, it exceeds the limit
	// and we reject without materializing the whole bomb. We guard the +1
	// against int64 overflow, since max == MaxInt64 ("effectively unlimited")
	// would wrap to MinInt64, making io.LimitReader read zero bytes and
	// silently truncate a valid block to empty.
	limit := max
	if limit < math.MaxInt64 {
		limit++
	}
	out, err := io.ReadAll(io.LimitReader(r, limit))
	if err != nil {
		return nil, err
	}
	if int64(len(out)) > max {
		return nil, fmt.Errorf("ocf: decompressed block exceeds limit of %d bytes (raise WithMaxDecompressedBlockBytes)", max)
	}
	return out, nil
}

type snappyCodec struct{}

func (snappyCodec) Name() string { return "snappy" }
func (snappyCodec) Close() error { return nil }

func (snappyCodec) Compress(src []byte) ([]byte, error) {
	dst := snappy.Encode(nil, src)
	dst = binary.BigEndian.AppendUint32(dst, crc32.ChecksumIEEE(src))
	return dst, nil
}

func (c snappyCodec) Decompress(src []byte) ([]byte, error) {
	return c.DecompressBounded(src, 0)
}

func (snappyCodec) DecompressBounded(src []byte, max int64) ([]byte, error) {
	if len(src) < 4 {
		return nil, errors.New("ocf: snappy data too short for CRC checksum")
	}
	body := src[:len(src)-4]
	if max > 0 {
		// snappy.Decode allocates the declared length up front; reject an
		// over-limit declaration before that allocation happens.
		n, err := snappy.DecodedLen(body)
		if err != nil {
			return nil, err
		}
		if int64(n) > max {
			return nil, fmt.Errorf("ocf: decompressed block (%d bytes) exceeds limit of %d bytes (raise WithMaxDecompressedBlockBytes)", n, max)
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
	enc   *zstd.Encoder
	dopts []zstd.DOption

	decOnce sync.Once
	dec     *zstd.Decoder
	decErr  error
}

func (*zstdCodec) Name() string { return "zstandard" }

func (c *zstdCodec) Compress(src []byte) ([]byte, error) {
	return c.enc.EncodeAll(src, nil), nil
}

func (c *zstdCodec) Decompress(src []byte) ([]byte, error) {
	return c.DecompressBounded(src, 0)
}

// DecompressBounded lazily builds the decoder on first use, applying max as
// zstd.WithDecoderMaxMemory so DecodeAll refuses a frame whose declared window
// would inflate past the cap: the window bound an output limiter cannot
// provide. We cache and reuse the decoder (preserving zstd's decoder reuse),
// so per the [BoundedDecompressor] contract only the first call's max is
// honored (max is constant across a Reader's blocks).
func (c *zstdCodec) DecompressBounded(src []byte, max int64) ([]byte, error) {
	c.decOnce.Do(func() {
		dopts := append([]zstd.DOption{zstd.WithDecoderConcurrency(1)}, c.dopts...)
		if max > 0 {
			m := uint64(max)
			// Raise a sub-1-KiB cap up to zstd.MinWindowSize: the decoder gives
			// every frame a window of at least MinWindowSize and clamps its
			// max-window down to WithDecoderMaxMemory, so a cap below
			// MinWindowSize would reject even a tiny valid frame. At or above
			// that bound the cap is exact: a sub-MiB
			// WithMaxDecompressedBlockBytes bounds zstd to the byte.
			if m < zstd.MinWindowSize {
				m = zstd.MinWindowSize
			}
			dopts = append(dopts, zstd.WithDecoderMaxMemory(m))
		}
		c.dec, c.decErr = zstd.NewReader(nil, dopts...)
	})
	if c.decErr != nil {
		return nil, fmt.Errorf("ocf: creating zstd decoder: %w", c.decErr)
	}
	return c.dec.DecodeAll(src, nil)
}

func (c *zstdCodec) Close() error {
	c.enc.Close()
	if c.dec != nil {
		c.dec.Close()
	}
	return nil
}

// resolveCodec returns the codec named in the file header, along with the
// index into custom that supplied it, or -1 when the name resolved to a
// built-in. The decompression bound is not injected here; the reader passes
// it at decode time through BoundedDecompressor, so it governs name-resolved
// and WithCodec-supplied codecs alike. The index lets releaseUnadopted tell
// which supplied codecs went unused.
func resolveCodec(name string, custom []Codec) (Codec, int, error) {
	for i, c := range custom {
		// We skip a nil offer rather than ask its Name, since this scan runs
		// over offers about to be declined. Both choosers ask isNilCodec, so
		// a nil offer has one answer.
		if isNilCodec(c) {
			continue
		}
		if c.Name() == name {
			return c, i, nil
		}
	}
	switch name {
	case "null":
		return nullCodec{}, -1, nil
	case "deflate":
		return deflateCodec{level: flate.DefaultCompression}, -1, nil
	case "snappy":
		return snappyCodec{}, -1, nil
	case "zstandard":
		c, err := ZstdCodec(nil, nil)
		return c, -1, err
	}
	return nil, -1, fmt.Errorf("ocf: unknown codec %q", truncForError(name))
}

// releaseUnadopted closes every codec you supplied that the constructor did
// not adopt, since a dropped codec has no owner and the inline
// WithCodec(MustZstdCodec(nil, nil)) form leaves no handle. adopted is the
// index of the taken offer, or -1. We skip the adopted codec by identity
// rather than index, or WithCodec(c), WithCodec(c) would close c and return
// a Writer using it, and we close each distinct codec once, since
// Codec.Close is not documented to be idempotent. Nils are never closed and
// stay out of the repeat bookkeeping. A comparable codec goes in a map; an
// uncomparable one is matched by type, which is conservative in the safe
// direction, since a skipped Close leaks where a wrong Close hands back a
// Writer built on a released codec.
func releaseUnadopted(supplied []Codec, adopted int) {
	var decided map[Codec]bool      // comparable codecs already accounted for
	var uncomparable []reflect.Type // the rest, tracked by type

	// seen records c if it is new, so each distinct codec is decided once.
	seen := func(c Codec) bool {
		t := reflect.TypeOf(c)
		if t.Comparable() {
			if decided[c] {
				return true
			}
			if decided == nil {
				decided = make(map[Codec]bool, len(supplied))
			}
			decided[c] = true
			return false
		}
		if slices.Contains(uncomparable, t) {
			return true
		}
		uncomparable = append(uncomparable, t)
		return false
	}

	// The adopted codec is accounted for first and never closed, so a later
	// offer of it reads as a repeat. The nil test is unreachable today, since
	// both choosers refuse to adopt a nil, but recording a nil would put its
	// type in the seen list and leak a later real codec of that type.
	if adopted >= 0 && !isNilCodec(supplied[adopted]) {
		seen(supplied[adopted])
	}
	for i, c := range supplied {
		if isNilCodec(c) || i == adopted {
			continue
		}
		if !seen(c) {
			c.Close()
		}
	}
}

// isNilCodec reports whether c holds nothing to call a method on: a non-nil
// interface wrapping a nil pointer passes c != nil and panics at the first
// method call. The kinds listed are those reflect.Value.IsNil accepts. Every
// site that reaches into your offers asks this one function.
func isNilCodec(c Codec) bool {
	if c == nil {
		return true
	}
	switch v := reflect.ValueOf(c); v.Kind() {
	case reflect.Pointer, reflect.UnsafePointer, reflect.Map,
		reflect.Slice, reflect.Func, reflect.Chan:
		return v.IsNil()
	}
	return false
}

// truncForError caps a wire-derived string at 80 chars for error messages,
// since a metadata value can be 1 MiB. It mirrors the root package's
// unexported helper.
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

// ocfSchemaSafetyLimit is the larger bound for the avro.schema metadata
// value, since a wide record's JSON legitimately exceeds 1 MiB. The schema
// parser's own guards bound the parse cost. The writer enforces the same
// bound, so a file we write is a file we can read.
const ocfSchemaSafetyLimit = 1 << 26 // 64 MiB

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
