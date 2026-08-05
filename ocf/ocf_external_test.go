package ocf_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

// ---------- example_test.go ----------

func ExampleNewWriter() {
	schema := avro.MustParse(`{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age",  "type": "int"}
		]
	}`)

	type User struct {
		Name string `avro:"name"`
		Age  int32  `avro:"age"`
	}

	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, schema)
	if err != nil {
		log.Fatal(err)
	}
	for _, u := range []User{
		{Name: "Alice", Age: 30},
		{Name: "Bob", Age: 25},
	} {
		if err := w.Encode(&u); err != nil {
			log.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}

	// Read back.
	r, err := ocf.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()
	for {
		var u User
		if err := r.Decode(&u); err != nil {
			break
		}
		fmt.Printf("%s is %d\n", u.Name, u.Age)
	}
	// Output:
	// Alice is 30
	// Bob is 25
}

// ExampleWithReaderSchemaFunc demonstrates choosing the reader schema based
// on state that's only available after the OCF header is parsed — for
// example, a metadata key that distinguishes between old and new file
// variants, or a writer-schema shape that changed between versions of the
// producer. The callback runs after NewReader has read the header, so
// rd.Schema() and rd.Metadata() are populated; whatever schema it returns
// becomes the reader schema for resolution against the writer schema.
func ExampleWithReaderSchemaFunc() {
	// Producer v1 wrote records with a legacy field name:
	v1Schema := avro.MustParse(`{
		"type": "record", "name": "Event",
		"fields": [{"name": "legacy_ts", "type": "long"}]
	}`)

	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, v1Schema,
		ocf.WithMetadata(map[string][]byte{"producer-version": []byte("1")}))
	if err != nil {
		log.Fatal(err)
	}
	if err := w.Encode(map[string]any{"legacy_ts": int64(1700000000)}); err != nil {
		log.Fatal(err)
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}

	// Our application reads with two reader schemas — one per producer
	// version — each using the spec-correct field name "ts" but declaring
	// the old name as an alias so records from either version decode into
	// the same struct without coalescing.
	v1Reader := avro.MustParse(`{
		"type": "record", "name": "Event",
		"fields": [{"name": "ts", "type": "long", "aliases": ["legacy_ts"]}]
	}`)
	v2Reader := avro.MustParse(`{
		"type": "record", "name": "Event",
		"fields": [{"name": "ts", "type": "long"}]
	}`)

	type Event struct {
		TS int64 `avro:"ts"`
	}

	r, err := ocf.NewReader(bytes.NewReader(buf.Bytes()),
		ocf.WithReaderSchemaFunc(func(rd *ocf.Reader) (*avro.Schema, error) {
			// Header has been parsed. Pick the reader schema based on
			// whichever producer wrote the file.
			if string(rd.Metadata()["producer-version"]) == "1" {
				return v1Reader, nil
			}
			return v2Reader, nil
		}))
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	var e Event
	if err := r.Decode(&e); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("ts=%d\n", e.TS)
	// Output:
	// ts=1700000000
}

func ExampleNewReader_evolution() {
	// Write v1 data (name only).
	v1Schema := avro.MustParse(`{
		"type": "record", "name": "User",
		"fields": [{"name": "name", "type": "string"}]
	}`)

	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, v1Schema)
	if err != nil {
		log.Fatal(err)
	}
	for _, name := range []string{"Alice", "Bob"} {
		if err := w.Encode(map[string]any{"name": name}); err != nil {
			log.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}

	// Read with a v2 schema that added an age field with a default.
	v2Schema := avro.MustParse(`{
		"type": "record", "name": "User",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age",  "type": "int", "default": 0}
		]
	}`)

	type User struct {
		Name string `avro:"name"`
		Age  int32  `avro:"age"`
	}

	r, err := ocf.NewReader(bytes.NewReader(buf.Bytes()), ocf.WithReaderSchema(v2Schema))
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()
	for {
		var u User
		if err := r.Decode(&u); err != nil {
			break
		}
		fmt.Printf("%s age=%d\n", u.Name, u.Age)
	}
	// Output:
	// Alice age=0
	// Bob age=0
}

// ---------- block_alloc_regression_test.go ----------

// TestRegression_OCFRaisedBlockCapDoesNotEagerAllocate pins that a reader with a
// raised WithMaxBlockBytes does not eagerly allocate an attacker-declared block
// size before reading the payload.
//
// A block frame declares its compressed size, which the reader bounds by
// WithMaxBlockBytes. A caller who raises that cap to a very large value (the
// natural way to express "accept big blocks", mirroring the decompressed side's
// MaxInt64 "effectively unlimited" sentinel) used to expose readBlock's
// make([]byte, declaredSize): a tiny hostile file declaring a 256 TiB block
// with no payload behind it drove that allocation to an unrecoverable
// "fatal error: out of memory" — a runtime.throw a caller cannot recover() from.
//
// The reader now reads the block incrementally once the declared size exceeds
// the eager-allocation window, so the buffer grows only to the bytes actually
// present and a declared-but-absent size fails with an ordinary error instead.
// Reaching the assertion without the process dying IS the pin; the boundary-1
// case (a legitimately large block reading back under a raised cap) is held by
// TestRegression_OCFLargeDatumReaderCap, which exercises the same incremental
// path with real payload bytes.
func TestRegression_OCFRaisedBlockCapDoesNotEagerAllocate(t *testing.T) {
	// A valid header for "long", reused for its embedded 16-byte sync marker.
	var hb bytes.Buffer
	w, err := ocf.NewWriter(&hb, avro.MustParse(`"long"`))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	hdr := hb.Bytes()
	sync := hdr[len(hdr)-16:]

	// One hostile block: count=1, a 256 TiB declared compressed size, and NO
	// payload bytes (the file ends right after the size + sync framing).
	var file bytes.Buffer
	file.Write(hdr)
	file.Write(binary.AppendVarint(nil, 1))     // count
	file.Write(binary.AppendVarint(nil, 1<<48)) // declared size = 256 TiB
	file.Write(sync)

	// Cap raised ABOVE the declared size, so the size>maxBlockBytes guard does
	// not fire and the read path itself must stay bounded.
	r, err := ocf.NewReader(bytes.NewReader(file.Bytes()), ocf.WithMaxBlockBytes(1<<50))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	var v int64
	if err := r.Decode(&v); err == nil {
		t.Fatal("expected an error for a 256 TiB declared-size block with no payload, got nil")
	}
}

// ---------- codec_cap_regression_test.go ----------

// A reader configured with a codec instance via WithCodec must enforce
// WithMaxDecompressedBlockBytes the same way a name-resolved codec does: by
// PREVENTING the over-cap allocation, not by decompressing the whole block and
// rejecting after. The reader passes its cap to the codec's DecompressBounded
// (the BoundedDecompressor capability) at decode time, so the bound reaches a
// supplied instance — AND a NopCloser-wrapped instance, which forwards the
// capability — exactly like the name-resolved built-in. Without this, deflate
// decompresses via an unbounded io.ReadAll: a tiny deflate bomb materializes in
// full (OOM on a real bomb) before any rejection.
//
// The pin is the ALLOCATION: a block declaring far more decompressed bytes than
// the cap must be rejected having allocated only on the order of the cap, not
// the full decompressed size. Reaching the assertion without materializing the
// whole datum is the property; an over-cap allocation would show as a TotalAlloc
// delta near the decompressed size. The NopCloser rows pin that wrapping a
// built-in for sharing does not silently drop its bounding.
func TestRegression_OCFUserBuiltinCodecBoundsDecompression(t *testing.T) {
	const datumSize = 8 << 20 // 8 MiB decompressed (highly compressible -> tiny compressed)
	const cap = 256 << 10     // 256 KiB decompressed cap
	s := avro.MustParse(`"bytes"`)

	mkFile := func(codec ocf.Codec) []byte {
		var buf bytes.Buffer
		w, err := ocf.NewWriter(&buf, s, ocf.WithCodec(codec))
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		if err := w.Encode(make([]byte, datumSize)); err != nil {
			t.Fatalf("encode: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		return buf.Bytes()
	}

	decodeAlloc := func(file []byte, codec ocf.Codec) (uint64, error) {
		r, err := ocf.NewReader(bytes.NewReader(file),
			ocf.WithCodec(codec),
			ocf.WithMaxDecompressedBlockBytes(cap))
		if err != nil {
			return 0, err
		}
		var m0, m1 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m0)
		var v []byte
		derr := r.Decode(&v)
		runtime.ReadMemStats(&m1)
		return m1.TotalAlloc - m0.TotalAlloc, derr
	}

	for _, tc := range []struct {
		name  string
		write ocf.Codec
		read  ocf.Codec
	}{
		{"deflate", ocf.DeflateCodec(9), ocf.DeflateCodec(9)},
		{"snappy", ocf.SnappyCodec(), ocf.SnappyCodec()},
		// NopCloser-wrapped built-ins (the realistic shared-codec form): the
		// wrapper forwards BoundedDecompressor, so the bound still applies.
		{"deflate_nopcloser", ocf.DeflateCodec(9), ocf.NopCloser(ocf.DeflateCodec(9))},
		{"snappy_nopcloser", ocf.SnappyCodec(), ocf.NopCloser(ocf.SnappyCodec())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := mkFile(tc.write)
			alloc, err := decodeAlloc(file, tc.read)
			if err == nil {
				t.Fatalf("expected the over-cap block to be rejected, got nil")
			}
			if !strings.Contains(err.Error(), "exceeds limit") {
				t.Fatalf("unexpected error (want a decompression-limit reject): %v", err)
			}
			// The bounded codec stops near the cap; the unbounded codec would
			// materialize all datumSize bytes (8 MiB). A threshold of half the
			// decompressed size cleanly separates "prevented" from "materialized
			// then rejected".
			if alloc >= datumSize/2 {
				t.Fatalf("codec materialized the over-cap block before rejecting: allocated %d bytes for a %d-byte cap (decompressed size %d)", alloc, cap, datumSize)
			}
		})
	}
}

// ---------- differential_ocf_test.go ----------

// The .avro files in testdata/avro-share are vendored from Apache Avro
// (apache/avro), Apache License 2.0: https://www.apache.org/licenses/LICENSE-2.0

// TestDifferentialOCFCorpus decodes the real, Java-produced OCF files shipped
// in Apache Avro's share/test/data and checks the decoded records against the
// known contents of weather.json. This proves twmb reads actual reference
// output across every codec it supports (null/deflate/snappy/zstd) and that
// the decoded VALUES are correct — an external oracle, not the author's
// belief. See CORRECTNESS_PLAN.md §T1a'.
//
// The corpus is vendored at ocf/testdata/avro-share (see its PROVENANCE.md),
// so this runs by default with no external dependency. Point
// AVRO_SHARE_DATA at a live <apache-avro>/share/test/data clone to run
// against upstream instead.
type weatherRec struct {
	Station string `avro:"station"`
	Time    int64  `avro:"time"`
	Temp    int32  `avro:"temp"`
}

func sortWeather(r []weatherRec) {
	sort.Slice(r, func(i, j int) bool {
		if r[i].Station != r[j].Station {
			return r[i].Station < r[j].Station
		}
		return r[i].Time < r[j].Time
	})
}

func TestDifferentialOCFCorpus(t *testing.T) {
	dir := os.Getenv("AVRO_SHARE_DATA")
	if dir == "" {
		dir = filepath.Join("testdata", "avro-share") // vendored corpus
	}
	if _, err := os.Stat(dir); err != nil {
		t.Skipf("OCF corpus dir %q not present: %v", dir, err)
	}

	// Ground truth from weather.json (the records the OCF files encode).
	want := []weatherRec{
		{"011990-99999", -619524000000, 0},
		{"011990-99999", -619506000000, 22},
		{"011990-99999", -619484400000, -11},
		{"012650-99999", -655531200000, 111},
		{"012650-99999", -655509600000, 78},
	}

	readWeather := func(t *testing.T, name string) []weatherRec {
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			t.Skipf("corpus file %s not present: %v", name, err)
		}
		defer f.Close()
		r, err := ocf.NewReader(f)
		if err != nil {
			t.Fatalf("%s: NewReader: %v", name, err)
		}
		var got []weatherRec
		for {
			var rec weatherRec
			if err := r.Decode(&rec); err != nil {
				if err == io.EOF {
					break
				}
				t.Fatalf("%s: Decode: %v", name, err)
			}
			got = append(got, rec)
		}
		return got
	}

	// Codec variants: records decode in writer order, exactly matching the
	// Java-produced ground truth.
	for _, name := range []string{"weather.avro", "weather-deflate.avro", "weather-snappy.avro", "weather-zstd.avro"} {
		t.Run(name, func(t *testing.T) {
			got := readWeather(t, name)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s decoded\n got  %+v\n want %+v", name, got, want)
			}
		})
	}

	// Sorted variant: same records, writer reordered them — compare as a set.
	t.Run("weather-sorted.avro", func(t *testing.T) {
		got := readWeather(t, "weather-sorted.avro")
		sortWeather(got)
		w := append([]weatherRec(nil), want...)
		sortWeather(w)
		if !reflect.DeepEqual(got, w) {
			t.Errorf("weather-sorted.avro decoded (sorted)\n got  %+v\n want %+v", got, w)
		}
	})

	// syncInMeta.avro carries the sync marker in the file metadata (a
	// different schema); just confirm twmb reads every record without error.
	t.Run("syncInMeta.avro", func(t *testing.T) {
		f, err := os.Open(filepath.Join(dir, "syncInMeta.avro"))
		if err != nil {
			t.Skipf("syncInMeta.avro not present: %v", err)
		}
		defer f.Close()
		r, err := ocf.NewReader(f)
		if err != nil {
			t.Fatalf("syncInMeta: NewReader: %v", err)
		}
		n := 0
		for {
			var v any
			if err := r.Decode(&v); err != nil {
				if err == io.EOF {
					break
				}
				t.Fatalf("syncInMeta: Decode: %v", err)
			}
			n++
		}
		t.Logf("syncInMeta.avro: decoded %d records", n)
	})
}

// ---------- large_datum_test.go ----------

// TestRegression_OCFLargeDatumReaderCap documents, via test, the OCF block-size
// contract: the writer writes freely (no producer-side cap, matching Java's
// DataFileWriter and fastavro), while the reader caps block size for DoS safety
// (defaults 64 MiB). A single Avro datum cannot be split across blocks, so a
// value larger than the reader default forms one block a DEFAULT reader refuses
// — but with an ACTIONABLE error naming the option to raise — and it reads back
// once the reader's caps are raised to match. (We deliberately do NOT enforce a
// producer-side cap; the reader is where the DoS knob lives.)
func TestRegression_OCFLargeDatumReaderCap(t *testing.T) {
	s := avro.MustParse(`"bytes"`)
	const n = 80 << 20 // 80 MiB > the 64 MiB reader default
	blob := make([]byte, n)
	blob[0], blob[n-1] = 0xAB, 0xCD // sentinels for an integrity spot-check

	// The writer accepts a large datum freely — no producer-side cap.
	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(blob); err != nil {
		t.Fatalf("writer must accept a large datum freely: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A DEFAULT reader refuses the oversized block — with an error that names
	// the option to raise (not a silent failure, and not a cryptic one).
	rDefault, err := ocf.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader (default): %v", err)
	}
	var got []byte
	derr := rDefault.Decode(&got)
	if derr == nil {
		t.Fatal("default reader should refuse a block over its cap")
	}
	if !strings.Contains(derr.Error(), "WithMaxBlockBytes") &&
		!strings.Contains(derr.Error(), "WithMaxDecompressedBlockBytes") {
		t.Fatalf("reader error must name the option to raise, got: %v", derr)
	}

	// Raising the reader's caps to match reads the same file back.
	rRaised, err := ocf.NewReader(bytes.NewReader(buf.Bytes()),
		ocf.WithMaxBlockBytes(128<<20), ocf.WithMaxDecompressedBlockBytes(128<<20))
	if err != nil {
		t.Fatalf("NewReader (raised): %v", err)
	}
	var got2 []byte
	if err := rRaised.Decode(&got2); err != nil {
		t.Fatalf("raised reader must read the file back: %v", err)
	}
	if len(got2) != n || got2[0] != 0xAB || got2[n-1] != 0xCD {
		t.Fatalf("round-trip mismatch: len=%d sentinels=%x,%x", len(got2), got2[0], got2[n-1])
	}
}
