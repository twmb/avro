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
	"reflect"
	"slices"
	"strings"
	"sync"

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

// BoundedDecompressor is an optional capability a [Codec] may implement. If a
// Codec implements it, the [Reader] calls DecompressBounded with its per-block
// decompression cap (from [WithMaxDecompressedBlockBytes]) instead of
// [Codec.Decompress], so the codec can refuse-early or stream-limit BEFORE
// allocating the whole block. That is the only effective defense against a
// decompression bomb — a post-decompression size check is false comfort, since
// the over-cap allocation has already happened. A Codec that does NOT implement
// BoundedDecompressor decompresses unbounded; for untrusted data, supply a
// codec that bounds itself (all built-in codecs do).
//
// A wrapper type that embeds [Codec] (e.g. a [NopCloser] result) does NOT
// inherit this capability — embedding an interface promotes only that
// interface's methods — so such a wrapper must forward DecompressBounded
// explicitly or it silently disables bounding for the codec it wraps.
//
// max <= 0 means no limit. max is constant across all calls for a given
// [Reader], so a codec that caches a configured decoder (e.g. a zstd decoder
// built with a memory limit) may honor only the first call's max.
type BoundedDecompressor interface {
	DecompressBounded(src []byte, max int64) ([]byte, error)
}

// NopCloser returns a Codec that wraps c but has a no-op Close method. This
// is useful when sharing a single codec across multiple writers or readers
// so that an individual [Writer.Close] or [Reader.Close] — or a constructor
// that fails and releases the codec it was handed — does not release shared
// resources. The caller is responsible for closing the underlying codec when
// it is no longer needed.
//
// If c implements [BoundedDecompressor], so does the returned Codec (the
// reader's decompression bound is forwarded to c), so wrapping a built-in codec
// for sharing does not silently drop its bounding.
func NopCloser(c Codec) Codec { return nopCloser{c} }

type nopCloser struct{ Codec }

func (nopCloser) Close() error { return nil }

// DecompressBounded forwards the reader's per-block cap to the wrapped codec
// when it implements [BoundedDecompressor]; otherwise it falls back to the
// unbounded [Codec.Decompress] (honest: an unbounded wrapped codec stays
// unbounded). nopCloser always exposes this method so that wrapping a bounding
// codec (every built-in) preserves the bound — embedding the [Codec] interface
// alone would not, since it promotes only the interface's own methods.
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

// WithCodec sets the compression codec. The default is null (no compression).
// WithCodec can be used as both a [WriterOpt] and a [ReaderOpt]. The four
// built-in codecs (null, deflate, snappy, zstandard) do not need to be
// registered for reading. A custom codec whose name matches a built-in
// overrides it.
//
// Passing a codec hands it over. Whatever happens next, it is closed exactly
// once: by [Writer.Close] or [Reader.Close] when the constructor returns a
// usable one; by the constructor itself when it fails, since it returns no
// Writer or Reader for the caller to close and the codec is often built inline
// in the call; and by the constructor when it succeeds without using the codec
// at all. That last case is easy to reach and gives no sign it happened —
// [NewReader] and [NewAppendWriter] take a supplied codec only when its Name
// matches the header's avro.codec, and NewWriter takes only the last WithCodec
// written — so an offer that is declined is released rather than dropped.
//
// The consequence for a codec used more than once: a caller that shares one
// codec across several writers, readers, or files must give it a Close that
// returns nil, or wrap it in [NopCloser]. That was already true — an adopted
// codec is closed by [Writer.Close] and [Reader.Close], so a shared codec passed
// bare was already closed out from under the next user — and it now holds for a
// codec that is offered and declined as well, which makes the rule the same one
// everywhere instead of one that depends on whether the offer was taken.
//
// For reader-side decompression bounding of a supplied codec, see
// [WithMaxDecompressedBlockBytes]: it reaches any supplied codec implementing
// [BoundedDecompressor] (every built-in does, including one wrapped by
// [NopCloser]). A custom codec that does not implement BoundedDecompressor
// decompresses unbounded — supply one that bounds itself for untrusted data.
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

// defaultMaxBlockBytes is the reader's default ceiling on a block's DECLARED
// COMPRESSED size — the [WithMaxBlockBytes] default. It is named rather than
// written inline at its use because a bound with no name is invisible to any
// guard keyed on names: the producer-compliance table classifies each cap by
// its constant, and this half of the reader-only block pair could not be
// classified while it was a literal.
const defaultMaxBlockBytes = 1 << 26

// ocfEagerBlockAllocLimit is the largest declared compressed block size that
// readBlock allocates in one shot (make + ReadFull, the fast common path).
// It IS the default WithMaxBlockBytes — derived from it rather than restated,
// so the property it depends on ("a reader at the default cap never leaves the
// eager path") cannot be broken by changing one of two spellings of the same
// number. A block larger than this is only reachable when the caller raises
// WithMaxBlockBytes; those are read incrementally so an attacker-declared-but-
// absent size cannot force an allocation up to the raised cap. See readBlock.
const ocfEagerBlockAllocLimit = defaultMaxBlockBytes

// WithMaxDecompressedBlockBytes sets the maximum DECOMPRESSED size in bytes of
// a single block that the reader will accept. The default is 64 MiB.
// [WithMaxBlockBytes] bounds the compressed size read off the wire; this
// bounds what that compressed block inflates to, guarding against
// decompression-amplification ("zip bomb") inputs where a tiny compressed
// block declares or expands to a huge output. Because a block's record count
// is bounded by its decompressed length, this also bounds the per-block
// decode loop. Pass a larger value if you legitimately write blocks (via
// [WithBlockBytes]) that decompress beyond the default.
//
// Enforcement scope: this limit is applied UP FRONT (the over-cap allocation is
// prevented, not merely caught afterward) by passing it to the codec's
// [BoundedDecompressor.DecompressBounded] at decode time. It therefore reaches
// every codec implementing that capability uniformly — a codec resolved by name
// from the file header (the common case), AND a codec supplied as an instance
// via [WithCodec], including one wrapped by [NopCloser]. All four built-in
// codecs (null, deflate, snappy, zstandard) implement it. A custom codec that
// does NOT implement [BoundedDecompressor] decompresses unbounded — this limit
// does not apply to it (no post-decompression backstop; that is false comfort
// once the allocation has happened), so supply a self-bounding codec for
// untrusted data.
func WithMaxDecompressedBlockBytes(n int64) ReaderOpt { return optMaxDecompressedBytes{n} }

// WithSchemaOpts passes [avro.SchemaOpt] values (such as [avro.CustomType]
// or [avro.WithLaxNames]) to the [avro.Parse] call that parses the file
// header's embedded schema. [NewReader] uses it to register custom type
// conversions (and to accept lax-named header schemas); [NewAppendWriter]
// needs it whenever the header schema requires an option to parse at all.
// [NewWriter] ignores it: its schema is already parsed by the caller.
func WithSchemaOpts(opts ...avro.SchemaOpt) Opt { return optSchemaOpts(opts) }

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
//
// ZstdCodec implements [BoundedDecompressor], so a reader applies its
// [WithMaxDecompressedBlockBytes] cap to a supplied ZstdCodec the same as a
// name-resolved one: the decoder is built lazily on first read with
// [zstd.WithDecoderMaxMemory] set from the cap. The decoder is then cached, so
// a ZstdCodec shared across readers with different caps honors the first
// reader's cap (set [zstd.WithDecoderMaxMemory] in dopts to pin a specific
// bound regardless of reader). A cap below [zstd.MinWindowSize] (1 KiB) is
// raised up to it — the smallest window the decoder accepts; a smaller limit
// would reject every frame, since each frame's window is at least
// MinWindowSize. At or above that bound a sub-MiB cap is honored exactly.
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
func NewWriter(w io.Writer, s *avro.Schema, opts ...WriterOpt) (_ *Writer, err error) {
	wr := &Writer{
		w:      w,
		schema: s,
		codec:  nullCodec{},
	}
	// Any error return from here on must release whatever codec wr holds. A
	// failing constructor returns no Writer, so there is nothing left for the
	// caller to Close, and the codec is commonly built inline in the call —
	// WithCodec(MustZstdCodec(nil, nil)) — leaving no handle either. Named
	// return so the deferred check sees the final err value; NewReader carries
	// the same defer for the same reason. Registered BEFORE the option loop
	// rather than after it: the closure reads wr.codec when it runs, so this
	// covers the codec whenever the loop adopts it, and a future error return
	// added inside the loop is guarded without anyone having to notice. Until
	// WithCodec is seen wr.codec is nullCodec, whose Close is a no-op. Callers
	// sharing one codec across writers wrap it in NopCloser, likewise a no-op.
	defer func() {
		if err != nil {
			wr.codec.Close()
		}
	}()

	// The other half of the same rule: WithCodec written more than once adopts
	// the last and drops the rest, and a dropped codec has no owner at all — see
	// releaseUnadopted. Registered before the loop for the same reason the defer
	// above is: the closure reads both variables when it runs, so it covers
	// whatever the loop collected however the loop exits. An error return added
	// inside the loop leaves adopted at -1, which releases every codec collected
	// so far rather than none of them.
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
	// Last WithCodec wins, as it always has. Adopting after the loop rather than
	// inside it is what makes the supersede case observable: the superseded
	// codecs stay in supplied with adopted pointing past them, so the deferred
	// sweep can tell "dropped" from "taken" instead of watching one field be
	// overwritten.
	if len(supplied) > 0 {
		adopted = len(supplied) - 1
		wr.codec = supplied[adopted]
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
// bounded. (Deliberately more careful than Java: DataFileWriter.close
// is a plain flush-then-close sequence with no finally, so a failing
// flush skips its close — DataFileWriter.java:483-489.)
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
	if err := writeFull(w.w, block); err != nil {
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
//
// If Reset fails after it has repointed to dst — a sync-marker generation
// error or a header write error — the Writer is poisoned exactly as a failed
// [Writer.Encode] or [Writer.Flush] is: every subsequent Encode/Flush/Close
// returns the sticky error until a later successful Reset clears it. (The
// flush of the OLD destination, which runs before the repoint, also poisons
// on failure.) Without this a caller that ignores Reset's returned error and
// keeps writing would emit a silent headerless byte stream onto dst.
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
	// The two steps below run AFTER the sink has been repointed to dst. A
	// failure here leaves the Writer half-reset (new sink, no header written),
	// so poison it — otherwise a caller that ignores the returned error and
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

// NewAppendWriter opens an existing OCF for appending. It reads the header
// to recover the schema, codec, and sync marker, then seeks to the end.
//
// [WithBlockCount] and [WithBlockBytes] are honored. [WithCodec] can
// provide a codec implementation for non-built-in codecs (matched by name
// against the header). [WithSchemaOpts] passes schema options to the
// header-schema parse — required when the header schema needs an option
// to parse at all (e.g. [avro.WithLaxNames] for a file written with a
// lax-named schema). [WithSchema], [WithSyncMarker], and [WithMetadata]
// are ignored: the header is already on disk and is never rewritten, so
// the schema, sync marker, and metadata always come from the existing
// file. (Reference implementations behave the same on append — neither
// Java's DataFileWriter.appendTo nor fastavro's append mode lands new
// metadata in the file.) Any remaining options are likewise ignored.
func NewAppendWriter(rws io.ReadWriteSeeker, opts ...WriterOpt) (*Writer, error) {
	var schemaOpts []avro.SchemaOpt
	var customCodecs []Codec
	adopted := -1
	// The header names ONE codec, so every other supplied codec is an offer this
	// constructor declines and nothing else will ever close — see
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

// checkedReader converts an io.Reader contract violation — a returned
// count outside [0, len(p)] with a nil error — into a named error before
// bufio's buffer arithmetic sees it. Unguarded, a negative count trips
// bufio's own panic and an over-length count drives the buffer slice out
// of range: a panic through NewReader / NewAppendWriter that the caller
// cannot recover from. It also bounds a reader stuck returning (0, nil):
// bufio applies its io.ErrNoProgress bound only on its buffered path and
// hands large direct reads through verbatim, where the block-data
// io.ReadFull would spin forever. A contract-abiding Read passes through
// untouched.
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

// writeFull writes p and converts an io.Writer contract violation (a nil
// error with n != len(p)) into an error: trusting the lying count would
// silently truncate the file, detectable only when a reader later hits
// the corruption. A short count maps to io.ErrShortWrite (io.Copy's and
// bufio.Writer's discipline); a count outside [0, len(p)] is named.
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
}

// noEOF converts a bare io.EOF from a mid-structure read into
// io.ErrUnexpectedEOF before the caller wraps it with %w. io.EOF is Decode's
// end-of-stream sentinel, so it must be reachable only from the clean-end
// path (the block-count read at the top of readBlock, with zero bytes
// consumed); a stream that ends after a complete count varint, after the
// size varint, mid block data, or at the sync boundary promised more bytes
// and is truncated, not ended. The stdlib readers return bare io.EOF for
// exactly those cuts — io.ReadFull and binary.ReadVarint when zero bytes
// remain, and io.CopyN on ANY shortfall (partial copies included) — and a
// %w wrap keeps errors.Is(err, io.EOF) true, which would make the idiomatic
// termination check read a truncated file as a clean, complete one.
func noEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

// readHeader reads and validates the OCF header, returning the parsed
// schema, raw metadata, and sync marker. Header reads never return a bare
// io.EOF: NewReader has no end-of-stream sentinel (a stream that ends
// anywhere inside the header is truncated), so every read error is
// normalized via noEOF for uniformity with readBlock.
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

// NewReader creates a Reader that decodes an OCF from r. The header is read
// immediately. Use [WithCodec] if the file uses a non-built-in codec.
func NewReader(r io.Reader, opts ...ReaderOpt) (_ *Reader, err error) {
	var customCodecs []Codec
	var readerSchema *avro.Schema
	var readerSchemaFn func(*Reader) (*avro.Schema, error)
	var maxBlockBytes int64
	var maxDecompressed int64
	var schemaOpts []avro.SchemaOpt
	adopted := -1
	// The header names ONE codec; every other supplied codec is an offer this
	// constructor declines and nothing else will ever close — see
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

	// Resolve codec.
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
// [io.EOF] is returned only at a clean end of stream — the file ends exactly
// at a block boundary; a stream truncated mid-block (a promised block header,
// data, or sync marker cut short) returns an error matching
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
	rest, err := rd.schema.Decode(rd.block, v)
	if err != nil {
		// noEOF: a datum error matching io.EOF (e.g. a CustomType decode
		// callback returning it) must not surface as the clean-end sentinel.
		return fmt.Errorf("ocf: decoding datum: %w", noEOF(err))
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

// readBlock advances the reader to the next block containing at least one
// datum. Validated count-0 blocks are skipped; io.EOF means the block-count
// read hit the true end of the stream.
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
			// here is truncated, not ended — noEOF keeps the io.EOF
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
		// size is the attacker-declared block length, bounded only by the
		// user-configurable WithMaxBlockBytes. Eagerly allocating it would let a
		// tiny hostile file (a huge declared size with few bytes behind it) force
		// an allocation up to that cap: at a raised cap a multi-GiB transient
		// spike, and near the cap's MaxInt64 ceiling an unrecoverable
		// out-of-memory the caller cannot recover() from. Only allocate the full
		// size up front when it is within a bounded window (the common path, where
		// it never exceeds the default cap); beyond that, read incrementally so
		// the buffer grows only to the bytes actually present and a declared-but-
		// absent size fails after consuming what is there — the same bounded-
		// allocation discipline the decompressed side already applies.
		var compressed []byte
		if size <= ocfEagerBlockAllocLimit {
			compressed = make([]byte, int(size))
			if _, err := io.ReadFull(rd.r, compressed); err != nil {
				return fmt.Errorf("ocf: reading block data: %w", noEOF(err))
			}
		} else {
			var buf bytes.Buffer
			if n, err := io.CopyN(&buf, rd.r, size); err != nil {
				// io.CopyN reports ANY shortfall — zero bytes or a partial
				// copy — as bare io.EOF, unlike io.ReadFull's
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
		// A count=0 block still requires reading size + data + 16-byte sync
		// first, per spec ("Each block consists of: count, size, objects,
		// sync marker") — bailing on count alone would accept a
		// tail-truncated file whose count byte reads as 0 as a clean end.
		// Once the sync validates, the empty block is SKIPPED and reading
		// continues: the spec leaves a block's object count unconstrained
		// (unlike Avro arrays and maps, whose zero count is an explicit
		// terminator, file data blocks have none — end of file is simply
		// end of stream), so io.EOF comes only from the count read at the
		// top of this loop. fastavro reads past empty blocks the same way
		// (_read_py.py _iter_avro_records: a count-0 block yields no
		// records, skip_sync validates the marker, the while loop
		// continues). Java never writes one (DataFileWriter.writeBlock is
		// guarded by blockCount > 0) and its for-each reader stops at one —
		// though a re-called hasNext() advances past it — so treating the
		// shape as end-of-stream silently truncated files only foreign
		// writers produce; goavro errors on it, avro-rs stops. The skipped
		// payload was consumed off the wire (bounded by WithMaxBlockBytes
		// like any block) but is NOT handed to the codec: there are no
		// records to decode, so nothing is decompressed. fastavro and Java
		// both decompress count-0 payloads eagerly and so error on an
		// undecompressable one that this reader skips — deliberate
		// leniency, no records are lost either way.
		if count == 0 {
			continue
		}
		// Prefer the codec's bounded path: it refuses an over-cap block BEFORE
		// allocating it (the only effective defense — a post-decompression size
		// check is false comfort once the allocation has happened). Every built-in
		// implements BoundedDecompressor. A custom codec that does not is honestly
		// unbounded; for untrusted data supply a codec that bounds itself. For a
		// BoundedDecompressor, len(block) <= maxDecompressed, which also caps the
		// per-block decode loop below (count is bounded relative to len(block)).
		var block []byte
		if b, ok := rd.codec.(BoundedDecompressor); ok {
			block, err = b.DecompressBounded(compressed, rd.maxDecompressed)
		} else {
			block, err = rd.codec.Decompress(compressed)
		}
		if err != nil {
			// noEOF: a codec error matching io.EOF must not surface as Decode's
			// clean-end sentinel (the user's failing Decompress would read as a
			// silently shorter file).
			return fmt.Errorf("ocf: decompressing block: %w", noEOF(err))
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
// Each built-in codec implements [BoundedDecompressor]: the reader passes its
// WithMaxDecompressedBlockBytes cap to DecompressBounded, which refuses an
// over-cap block BEFORE allocating it (a decompression bomb otherwise inflates
// a tiny compressed block to a huge allocation). Decompress is the unbounded
// (max == 0) form, kept for the Codec interface and the trusted writer path:
//
//   - snappy: snappy.Decode pre-allocates from a varint header that can declare
//     up to ~4 GiB inside a 5-byte frame; DecompressBounded rejects via a
//     snappy.DecodedLen pre-check before Decode allocates.
//   - deflate: flate.NewReader streams without a header bound (io.ReadAll would
//     grow until the stream ends); DecompressBounded reads via an
//     io.LimitReader(max+1) and rejects past the cap.
//   - zstd: the library default permits multi-GiB output; DecompressBounded
//     builds the decoder with zstd.WithDecoderMaxMemory(max), which bounds the
//     decode window an output limiter cannot.
//   - null: the "decompressed" size IS the input size; DecompressBounded
//     rejects an over-cap raw block, which also caps the per-block decode loop
//     (a block's record count cannot exceed its decompressed length plus the
//     zero-byte slack).
//
// Java's SnappyCodec (ByteBuffer.allocate(Snappy.uncompressedLength(...))) and
// fastavro's snappy decompress (cramjam decompress_raw; python-snappy is its
// deprecated fallback) leave the decompressed side unbounded — the cap is twmb
// defense-in-depth, in the same family as WithMaxBlockBytes and
// maxZeroByteItems.

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
		// unbounded — a deflate "zip bomb" decompresses to arbitrary size. Only
		// the trusted writer path (and an explicit "no limit") reaches here.
		return io.ReadAll(r)
	}
	// Read at most max+1 bytes: if the stream yields more, it exceeds the limit
	// and we reject without materializing the whole bomb. Guard the +1 against
	// int64 overflow — max == MaxInt64 ("effectively unlimited") would wrap to
	// MinInt64, making io.LimitReader read zero bytes and silently truncate a
	// valid block to empty.
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
// would inflate past the cap — the window bound an output limiter cannot
// provide. The decoder is cached and reused (preserving zstd's decoder reuse),
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
			// that bound the cap is exact — a sub-MiB
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

// resolveCodec returns the codec named in the file header, along with the index
// into custom that supplied it, or -1 when the name resolved to a built-in. A
// custom codec matched by name is returned as-is. The per-block decompression
// bound (WithMaxDecompressedBlockBytes) is NOT injected here: the reader passes
// it to the codec at decode time via [BoundedDecompressor.DecompressBounded],
// which every built-in implements — so the bound governs name-resolved and
// WithCodec-supplied codecs uniformly, including a codec instance constructed
// before NewReader. A custom codec that does not implement BoundedDecompressor
// decompresses unbounded.
//
// The index is what lets one caller answer "which supplied codecs went unused"
// (releaseUnadopted); every constructor that offers codecs here gets that answer
// from this one return rather than working it out again.
func resolveCodec(name string, custom []Codec) (Codec, int, error) {
	for i, c := range custom {
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

// releaseUnadopted closes every codec the caller supplied that the constructor
// did not adopt. [WithCodec] OFFERS a codec; at most one offer is taken — the
// first whose Name matches the header's avro.codec for the two reader-side
// constructors, the last one written for [NewWriter] — so any other supplied
// codec is dropped.
//
// A dropped codec has no owner. The constructor SUCCEEDS, so nothing signals the
// caller that their codec went unused, and the documented inline form —
// WithCodec(MustZstdCodec(nil, nil)) — leaves no handle to close it with. That is
// the same argument that makes a failing constructor release the codec it DID
// adopt; this is the other half of it, and stating it in one function is what
// keeps a constructor added later from re-deriving it.
//
// adopted is the INDEX of the offer that was taken, or -1 when none was — a
// built-in resolved by name, or a constructor that failed before it chose, in
// which case every supplied codec is released. An index rather than a codec
// value because the position is what the choosers actually decide; but position
// alone is not enough to decide whether to CLOSE, because the same codec can
// occupy more than one position. What gets released is therefore each DISTINCT
// supplied codec other than the adopted one, exactly once:
//
//   - Skipping the adopted codec by identity, not just by index, is what keeps
//     WithCodec(c), WithCodec(c) from closing c and then handing back a Writer
//     that compresses with it. Same on the reader side, where two offers of one
//     codec can straddle the name match.
//   - Skipping an earlier identical offer keeps a codec from being closed twice.
//     [Codec.Close] is documented to release the codec's resources; it is not
//     documented to be idempotent, and a caller's codec need not be.
//
// A nil codec — WithCodec(nil) — is never closed: calling a method on it would
// panic, and it holds nothing to release.
//
// Close errors are dropped: this runs on paths that already have an outcome to
// report (a constructor error, or a Writer/Reader the caller is about to use),
// and a codec whose Close fails is no more usable than one whose Close was never
// called.
//
// Recognizing repeats is a set-membership question, so a comparable codec goes
// in a map and costs one lookup — the map's key equality IS the == this would
// otherwise write by hand. A codec whose dynamic type is UNCOMPARABLE (a struct
// with a map, slice, or func field) cannot be a key at all, and comparing two of
// them with == panics rather than answering, so those are tracked separately and
// matched by TYPE: two values of one uncomparable type cannot be told apart by
// anything, so identical types answer "same codec". That answer is conservative
// in the safe direction — "same" means the caller SKIPS a Close, and a skipped
// Close leaks where a wrong Close hands back a Writer or Reader built on a
// released codec.
//
// The split is on reflect.Type.Comparable, a property of the type, not a guess
// about how many options a caller writes; the ordinary case (every built-in, and
// any codec held by pointer) stays linear.
func releaseUnadopted(supplied []Codec, adopted int) {
	var decided map[Codec]bool      // comparable codecs already accounted for
	var uncomparable []reflect.Type // the rest, tracked by type

	// seen reports whether c has already been accounted for, recording it if
	// not, so each distinct codec is decided exactly once.
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

	// The adopted codec is accounted for FIRST and never closed, so any later
	// offer of that same codec is recognized as a repeat and left open too.
	if adopted >= 0 && supplied[adopted] != nil {
		seen(supplied[adopted])
	}
	for i, c := range supplied {
		if c == nil || i == adopted {
			continue
		}
		if !seen(c) {
			c.Close()
		}
	}
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
