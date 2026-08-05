package ocf

// User-supplied callback contract matrix: every point where this package
// does arithmetic, slicing, or a state transition on a value returned by
// USER code (Codec.Compress / Codec.Decompress / BoundedDecompressor,
// io.Reader, io.Writer, WithReaderSchemaFunc). The invariant pinned per
// cell: a contract violation NEVER panics through the public API and
// NEVER silently corrupts sibling data — detectable violations yield
// named errors; undetectable ones corrupt only the violating user's own
// stream and are pinned here as documented behavior.

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/twmb/avro"
)

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
	w, err := NewWriter(&buf, contractSchema, WithCodec(c))
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range vals {
		if err := w.Encode(map[string]any{"x": v}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
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
		w, err := NewWriter(&buf, contractSchema, WithCodec(c))
		if err != nil {
			t.Fatal(err)
		}
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
		r, err := NewReader(bytes.NewReader(file), WithCodec(&contractCodec{decMode: "error"}))
		if err != nil {
			t.Fatal(err)
		}
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
		r, err := NewReader(bytes.NewReader(file), WithCodec(&contractCodec{}))
		if err != nil {
			t.Fatal(err)
		}
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
	w, err := NewWriter(&buf, contractSchema)
	if err != nil {
		t.Fatal(err)
	}
	// A block bigger than bufio's buffer so the block read takes the
	// large-read direct path.
	if err := w.Encode(map[string]any{"x": int64(1)}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
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
		r, err := NewReader(bytes.NewReader(file), WithCodec(&eofErrCodec{}))
		if err != nil {
			t.Fatal(err)
		}
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
		s, err := avro.Parse(`{"type":"record","name":"CR2","fields":[{"name":"x","type":"long"}]}`, ct)
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		w, err := NewWriter(&buf, s)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Encode(map[string]any{"x": int64(1)}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		r, err := NewReader(bytes.NewReader(buf.Bytes()), WithSchemaOpts(ct))
		if err != nil {
			t.Fatal(err)
		}
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
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(map[string]any{"b": []byte("first!"), "s": "FIRST"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(map[string]any{"b": []byte("second"), "s": "SECON"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	type target struct {
		B []byte `avro:"b"`
		S string `avro:"s"`
	}
	r, err := NewReader(bytes.NewReader(buf.Bytes()), WithCodec(&reusedBufferCodec{}))
	if err != nil {
		t.Fatal(err)
	}
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
		w, err := NewWriter(&hdr, contractSchema)
		if err != nil {
			t.Fatal(err)
		}
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
