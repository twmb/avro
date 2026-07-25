package ocf

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// FOREIGN writer: whole container files produced by fastavro's WRITER, read
// back by this package. Everything upstream of the record bytes is foreign —
// the header carries fastavro's own rendering of the schema (fully-qualified
// names, object-wrapped primitives), block sizing follows its sync_interval
// accounting, and each codec's framing is the real library implementation
// (cramjam snappy with its 4-byte big-endian CRC suffix, python-zstandard
// frames, zlib raw-deflate, stdlib bzip2/xz). The reader-side contract per
// file: every record read back exactly (byte parity through a re-encode
// against the original schemaless encoding), the header schema canonically
// equal to the schema the file was written with, user metadata surfaced,
// and clean io.EOF at the end. Append mode must extend a foreign file such
// that BOTH implementations read the combined records exactly, and
// WithReaderSchema must resolve over a foreign header. A fastavro or
// codec-library upgrade that changes any of this surface fails here.
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
