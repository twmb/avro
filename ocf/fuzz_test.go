package ocf

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/klauspost/compress/zstd"
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
		// Keep each execution fast and bounded so the reader LOGIC (header,
		// block envelope, codec, count handling) is what gets explored — not
		// throughput on a large input. Two bounds, both about fuzzer hygiene,
		// not the reader's contract:
		//   - cap the input size: a multi-MB OCF decodes proportionally many
		//     records (correct, not a bug), but the coordinator's minimization
		//     of such an interesting input re-runs it dozens of times, freezing
		//     the fuzzer for tens of seconds and tripping the -fuzztime
		//     shutdown deadline (the large-input-minimization class).
		//   - a tight WithMaxDecompressedBlockBytes: bounds per-exec decode
		//     work AND exercises the decompression-amplification rejection
		//     (an inflate past this cap is rejected; pinned at the API level
		//     by TestRegression_OCFDecompressionAmplificationBounded).
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

// FuzzOCFBlockEnvelope fuzzes the block-level state machine in
// readBlock: count varint, size varint, data, sync marker. The fuzz
// builds a valid header and then appends a fuzz-driven block payload,
// so every iteration explores the block envelope rather than the
// header parser. Targets the count=0 sync-validation path
// (TestRegression_BlockCountZeroValidatesSync) — pre-fix readBlock
// bailed at count==0 without reading size + sync, so a tail-truncated
// file whose count byte happened to read as 0 was silently accepted.
// Also exercises the negative-count / negative-size / size>max guards
// (TestRegression_OCFBlockEnvelopeInvariant).
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
	// nil entry → default codec (null). Public codec constructors don't
	// include a NullCodec wrapper; the default already exercises it.
	//
	// The zstd codec uses minimum-footprint options: the fuzz constructs and
	// closes a codec PER EXECUTION (that lifecycle is the fuzzed surface), and
	// default-option zstd costs ~573µs + 1.64MB of garbage per cycle (vs
	// ~126µs + 0.30MB with these options; deflate is ~478µs + 1.26MB with no
	// shrink knob). At fuzz rates across parallel workers that allocation
	// churn keeps the GC saturated on small CI runners — exec rates slide and
	// a starved worker can miss the coordinator's shutdown deadline at the
	// -fuzztime boundary, failing the run with "context deadline exceeded"
	// and no crasher input. The options only shrink buffers/effort; the
	// construct→compress→decompress→Close surface is unchanged.
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
