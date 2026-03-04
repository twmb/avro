package conformance

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"io"
	"reflect"
	"testing"

	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

// -----------------------------------------------------------------------
// OCF Compliance
// Spec: "Object Container Files" — file header, metadata, block layout,
// sync markers, and codec support.
//   - Magic: 4-byte header "Obj\x01"
//   - Metadata: avro.schema required, avro.codec optional (null default)
//   - Reserved keys: user keys must not start with "avro."
//   - Block layout: long count + long size + data + 16-byte sync marker
//   - Codecs: null (required), deflate, snappy, zstandard (optional)
//   - Schema evolution: reader schema applied via resolution
// https://avro.apache.org/docs/1.12.0/specification/#object-container-files
// -----------------------------------------------------------------------

func TestSpecOCFMagicValidation(t *testing.T) {
	badMagics := []struct {
		name  string
		magic [4]byte
	}{
		{"all zeros", [4]byte{0, 0, 0, 0}},
		{"wrong version", [4]byte{'O', 'b', 'j', 0x02}},
		{"random", [4]byte{0xDE, 0xAD, 0xBE, 0xEF}},
		{"reversed", [4]byte{0x01, 'j', 'b', 'O'}},
	}

	for _, tc := range badMagics {
		t.Run(tc.name, func(t *testing.T) {
			buf := bytes.NewReader(tc.magic[:])
			_, err := ocf.NewReader(buf)
			if err == nil {
				t.Fatal("expected error for bad magic, got nil")
			}
		})
	}
}

func TestSpecOCFMissingCodecDefaultsNull(t *testing.T) {
	schema := avro.MustParse(`"string"`)

	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, schema)
	if err != nil {
		t.Fatal(err)
	}
	s := "hello"
	if err := w.Encode(&s); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := ocf.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	var got string
	if err := r.Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Fatalf("got %q, want hello", got)
	}

	meta := r.Metadata()
	if _, exists := meta["avro.codec"]; exists {
		t.Fatal("null codec should not write avro.codec to metadata")
	}
}

func TestSpecOCFBlockLayout(t *testing.T) {
	schema := avro.MustParse(`"int"`)
	syncMarker := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, schema, ocf.WithSyncMarker(syncMarker), ocf.WithBlockCount(2))
	if err != nil {
		t.Fatal(err)
	}
	v1 := int32(1)
	v2 := int32(2)
	if err := w.Encode(&v1); err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(&v2); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()

	if !bytes.Equal(data[:4], []byte{'O', 'b', 'j', 0x01}) {
		t.Fatalf("magic: got %x, want 4f626a01", data[:4])
	}

	headerSyncIdx := bytes.Index(data[4:], syncMarker[:])
	if headerSyncIdx < 0 {
		t.Fatal("sync marker not found in header")
	}
	headerSyncIdx += 4

	blockStart := headerSyncIdx + 16

	blockData := data[blockStart:]
	count, n := binary.Varint(blockData)
	if count != 2 {
		t.Fatalf("block count: got %d, want 2", count)
	}
	blockData = blockData[n:]

	size, n := binary.Varint(blockData)
	if size <= 0 {
		t.Fatalf("block size: got %d, want positive", size)
	}
	blockData = blockData[n:]

	if int(size) > len(blockData) {
		t.Fatalf("block data truncated: size=%d, have=%d", size, len(blockData))
	}
	blockData = blockData[size:]

	if len(blockData) < 16 {
		t.Fatal("missing sync marker after block data")
	}
	if !bytes.Equal(blockData[:16], syncMarker[:]) {
		t.Fatalf("block sync: got %x, want %x", blockData[:16], syncMarker)
	}
}

func TestSpecOCFSchemaEvolution(t *testing.T) {
	type WriterRecord struct {
		Name string `avro:"name"`
		Age  int32  `avro:"age"`
		Old  string `avro:"old_field"`
	}
	type ReaderRecord struct {
		Name  string `avro:"name"`
		Age   int64  `avro:"age"`
		Email string `avro:"email"`
	}

	writerSchemaStr := `{
		"type":"record","name":"Person",
		"fields":[
			{"name":"name","type":"string"},
			{"name":"age","type":"int"},
			{"name":"old_field","type":"string"}
		]
	}`
	readerSchemaStr := `{
		"type":"record","name":"Person",
		"fields":[
			{"name":"name","type":"string"},
			{"name":"age","type":"long"},
			{"name":"email","type":"string","default":"unknown"}
		]
	}`

	writerSchema := avro.MustParse(writerSchemaStr)
	readerSchema := avro.MustParse(readerSchemaStr)

	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, writerSchema)
	if err != nil {
		t.Fatal(err)
	}
	records := []WriterRecord{
		{"Alice", 30, "x"},
		{"Bob", 25, "y"},
	}
	for _, r := range records {
		if err := w.Encode(&r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := ocf.NewReader(bytes.NewReader(buf.Bytes()), ocf.WithReaderSchema(readerSchema))
	if err != nil {
		t.Fatal(err)
	}
	var results []ReaderRecord
	for {
		var rec ReaderRecord
		if err := r.Decode(&rec); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		results = append(results, rec)
	}

	expected := []ReaderRecord{
		{"Alice", 30, "unknown"},
		{"Bob", 25, "unknown"},
	}
	if !reflect.DeepEqual(results, expected) {
		t.Fatalf("got %+v, want %+v", results, expected)
	}
}

func TestSpecOCFReservedMetadataKeys(t *testing.T) {
	schema := avro.MustParse(`"string"`)
	var buf bytes.Buffer
	_, err := ocf.NewWriter(&buf, schema, ocf.WithMetadata(map[string][]byte{
		"avro.custom": []byte("x"),
	}))
	if err == nil {
		t.Fatal("expected error for reserved avro.* metadata key")
	}
}

func TestSpecOCFRequiredMetadataPresent(t *testing.T) {
	schema := avro.MustParse(`"string"`)
	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, schema)
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

	r, err := ocf.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	meta := r.Metadata()
	if _, ok := meta["avro.schema"]; !ok {
		t.Fatal("expected avro.schema metadata")
	}
	if _, ok := meta["avro.codec"]; ok {
		t.Fatal("null codec should omit avro.codec")
	}
}

func TestSpecOCFSupportedCodecsRoundTrip(t *testing.T) {
	cases := []struct {
		name      string
		codecName string
		opts      func(t *testing.T) []ocf.WriterOpt
	}{
		{
			name:      "deflate",
			codecName: "deflate",
			opts: func(t *testing.T) []ocf.WriterOpt {
				return []ocf.WriterOpt{ocf.WithCodec(ocf.DeflateCodec(flate.DefaultCompression))}
			},
		},
		{
			name:      "snappy",
			codecName: "snappy",
			opts: func(t *testing.T) []ocf.WriterOpt {
				return []ocf.WriterOpt{ocf.WithCodec(ocf.SnappyCodec())}
			},
		},
		{
			name:      "zstd",
			codecName: "zstandard",
			opts: func(t *testing.T) []ocf.WriterOpt {
				c, err := ocf.ZstdCodec()
				if err != nil {
					t.Fatal(err)
				}
				return []ocf.WriterOpt{ocf.WithCodec(c)}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := avro.MustParse(`"string"`)
			var buf bytes.Buffer
			w, err := ocf.NewWriter(&buf, schema, tc.opts(t)...)
			if err != nil {
				t.Fatal(err)
			}
			in := "hello"
			if err := w.Encode(&in); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			r, err := ocf.NewReader(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			var out string
			if err := r.Decode(&out); err != nil {
				t.Fatal(err)
			}
			if out != in {
				t.Fatalf("got %q, want %q", out, in)
			}
			meta := r.Metadata()
			if got := string(meta["avro.codec"]); got != tc.codecName {
				t.Fatalf("codec metadata: got %q, want %q", got, tc.codecName)
			}
		})
	}
}

func TestSpecOCFRejectsNegativeBlockCount(t *testing.T) {
	schema := avro.MustParse(`"int"`)
	syncMarker := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, schema, ocf.WithSyncMarker(syncMarker), ocf.WithBlockCount(1))
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

	data := append([]byte(nil), buf.Bytes()...)
	headerSyncIdx := bytes.Index(data[4:], syncMarker[:])
	if headerSyncIdx < 0 {
		t.Fatal("sync marker not found in header")
	}
	headerSyncIdx += 4
	blockStart := headerSyncIdx + 16
	if blockStart >= len(data) {
		t.Fatal("block start out of range")
	}

	// Corrupt count from +1 (0x02) to -1 (0x01).
	data[blockStart] = 0x01

	r, err := ocf.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var out int32
	if err := r.Decode(&out); err == nil {
		t.Fatal("expected error for negative OCF block count")
	}
}
