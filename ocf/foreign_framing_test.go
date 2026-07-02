package ocf

import (
	"bufio"
	"bytes"
	"compress/flate"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// FOREIGN block framing: container shapes no twmb writer produces (the writer
// never emits a count-0 block — TestWriterBlockFramingContract), which the
// reader must nevertheless handle because they are spec-valid: the spec
// leaves a block's object count unconstrained (unlike Avro arrays and maps,
// whose zero count is an explicit terminator, file data blocks have no
// terminator — end of file is simply end of stream).
//
// The matrix crosses empty-block POSITION {first, mid, tail, consecutive×3}
// × CODEC {null, deflate, snappy, zstandard} × empty-block PAYLOAD {size 0,
// >0 decompressing to zero bytes, >0 garbage}. Every accept cell asserts the
// full file content is read (both records, in order, then io.EOF), and —
// when a fastavro interpreter is available — that fastavro's record iterator
// reads the identical bytes to the identical records. Cells where fastavro
// itself errors are cross-checked as twmb-only, with fastavro's observed
// verdict recorded on the cell: fastavro (like Java) decompresses a count-0
// block's payload eagerly and so rejects an undecompressable one, while this
// package's reader skips the block without consulting the codec — a
// deliberate leniency; no records are lost either way.
//
// Which cells REACH the skip arm (readBlock's count==0 continue): every
// empty-block cell in the matrix — the skip sits after payload + sync
// validation and before decompression, so position, codec, and payload shape
// all funnel through it. The corrupt-sync guard cell errors at sync
// validation, BEFORE the skip arm, and the writer-side framing tests never
// produce a count-0 block at all.
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
	w, err := NewWriter(&buf, s, opts...)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	var want []string
	for _, ch := range layout {
		switch ch {
		case 'D':
			d := fmt.Sprintf("d%d", len(want))
			if err := w.Encode(d); err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if err := w.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			want = append(want, d)
		case 'E':
			appendRawBlock(&buf, 0, emptyPayload, foreignSync)
		default:
			t.Fatalf("bad layout char %q", ch)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes(), want
}

// readAllStrings drives a Reader over file to io.EOF, returning every datum.
func readAllStrings(t *testing.T, file []byte) []string {
	t.Helper()
	r, err := NewReader(bytes.NewReader(file))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
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
// environment limitation, not a divergence.
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
				t.Logf("codec %s: fastavro missing optional dependency (%s); cross-checks skipped", c.name, errMsg)
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

	// Corrupt sync on an empty block errors at sync validation, BEFORE the
	// skip arm — skipping must not weaken corruption detection.
	t.Run("corrupt-sync-on-empty", func(t *testing.T) {
		s := avro.MustParse(`"string"`)
		var buf bytes.Buffer
		w, err := NewWriter(&buf, s, WithSyncMarker(foreignSync))
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Encode("d0"); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		bad := foreignSync
		bad[0] ^= 0xFF
		appendRawBlock(&buf, 0, nil, bad)
		datum, err := s.AppendEncode(nil, "d1")
		if err != nil {
			t.Fatal(err)
		}
		var rest bytes.Buffer
		appendRawBlock(&rest, 1, datum, foreignSync)
		buf.Write(rest.Bytes())

		r, err := NewReader(bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		var v string
		if err := r.Decode(&v); err != nil || v != "d0" {
			t.Fatalf("first datum: %v %q", err, v)
		}
		err = r.Decode(&v)
		if err == nil || !strings.Contains(err.Error(), "sync marker mismatch") {
			t.Fatalf("want sync marker mismatch on corrupt-sync empty block, got %v", err)
		}
	})

	// An all-empty-blocks file terminates in bounded time: one Decode call
	// walks every block (18 bytes each) and returns io.EOF — cost linear in
	// the input, no records, no hang.
	t.Run("ten-thousand-empty-blocks", func(t *testing.T) {
		s := avro.MustParse(`"string"`)
		var buf bytes.Buffer
		w, err := NewWriter(&buf, s, WithSyncMarker(foreignSync))
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		for range 10_000 {
			appendRawBlock(&buf, 0, nil, foreignSync)
		}
		start := time.Now()
		got := readAllStrings(t, buf.Bytes())
		if len(got) != 0 {
			t.Fatalf("read %v from an all-empty file", got)
		}
		if d := time.Since(start); d > 10*time.Second {
			t.Fatalf("all-empty file took %v to reach io.EOF", d)
		}
		if fa != nil && faSupported["null"] {
			faGot, faErr := fa(buf.Bytes())
			if faErr != "" || len(faGot) != 0 {
				t.Errorf("fastavro on all-empty file: values=%v err=%s", faGot, faErr)
			}
		}
	})
}
