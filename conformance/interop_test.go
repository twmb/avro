package conformance

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/twmb/avro"
)

// -----------------------------------------------------------------------
// Interop — Reference Bytes and Fingerprints
// -----------------------------------------------------------------------

func TestInteropReferenceBytes(t *testing.T) {
	t.Run("record with all primitive fields", func(t *testing.T) {
		type AllPrim struct {
			B   bool    `avro:"b"`
			I   int32   `avro:"i"`
			L   int64   `avro:"l"`
			F   float32 `avro:"f"`
			D   float64 `avro:"d"`
			S   string  `avro:"s"`
			Byt []byte  `avro:"byt"`
		}
		schema := `{
			"type":"record","name":"AllPrim",
			"fields":[
				{"name":"b","type":"boolean"},
				{"name":"i","type":"int"},
				{"name":"l","type":"long"},
				{"name":"f","type":"float"},
				{"name":"d","type":"double"},
				{"name":"s","type":"string"},
				{"name":"byt","type":"bytes"}
			]
		}`
		input := AllPrim{
			B:   true,
			I:   42,
			L:   2147483648,
			F:   3.14,
			D:   2.718281828,
			S:   "hello",
			Byt: []byte{0xCA, 0xFE},
		}

		dst := encode(t, schema, &input)
		want := buildReferenceBytes(true, 42, 2147483648, 3.14, 2.718281828, "hello", []byte{0xCA, 0xFE})

		if !bytes.Equal(dst, want) {
			t.Fatalf("encoding mismatch:\ngot  %x\nwant %x", dst, want)
		}

		var output AllPrim
		decode(t, schema, dst, &output)
		if output.B != input.B || output.I != input.I || output.L != input.L ||
			output.F != input.F || output.D != input.D || output.S != input.S ||
			!bytes.Equal(output.Byt, input.Byt) {
			t.Fatalf("round-trip mismatch: got %+v, want %+v", output, input)
		}
	})

	t.Run("nested array and map", func(t *testing.T) {
		schema := `{
			"type":"record","name":"Container",
			"fields":[
				{"name":"arr","type":{"type":"array","items":"int"}},
				{"name":"m","type":{"type":"map","values":"string"}}
			]
		}`
		type Container struct {
			Arr []int32           `avro:"arr"`
			M   map[string]string `avro:"m"`
		}
		input := Container{
			Arr: []int32{1, 2},
			M:   map[string]string{"k": "v"},
		}
		dst := encode(t, schema, &input)

		if dst[0] != 0x04 {
			t.Fatalf("array count: got %x, want 04", dst[0])
		}
	})

	t.Run("boolean false", func(t *testing.T) {
		schema := `"boolean"`
		v := false
		dst := encode(t, schema, &v)
		if !bytes.Equal(dst, []byte{0x00}) {
			t.Fatalf("false: got %x, want 00", dst)
		}
	})

	t.Run("fixed encoding", func(t *testing.T) {
		schema := `{"type":"fixed","name":"F4","size":4}`
		v := [4]byte{0xDE, 0xAD, 0xBE, 0xEF}
		dst := encode(t, schema, &v)
		if !bytes.Equal(dst, []byte{0xDE, 0xAD, 0xBE, 0xEF}) {
			t.Fatalf("fixed: got %x, want deadbeef", dst)
		}
	})

	t.Run("float specific bits", func(t *testing.T) {
		schema := `"float"`
		v := float32(1.0)
		dst := encode(t, schema, &v)
		want := encodeUint32LE(math.Float32bits(1.0))
		if !bytes.Equal(dst, want) {
			t.Fatalf("float 1.0: got %x, want %x", dst, want)
		}
	})
}

func TestSpecCRC64AVROFingerprint(t *testing.T) {
	t.Run("empty fingerprint", func(t *testing.T) {
		h := avro.NewRabin()
		fp := h.Sum64()
		if fp != 0xc15d213aa4d7a795 {
			t.Fatalf("empty: got %016x, want c15d213aa4d7a795", fp)
		}
	})

	t.Run("null schema", func(t *testing.T) {
		s := mustParse(t, `"null"`)
		h := avro.NewRabin()
		fp := s.Fingerprint(h)
		if len(fp) != 8 {
			t.Fatalf("fingerprint length: got %d, want 8", len(fp))
		}
		h2 := avro.NewRabin()
		fp2 := s.Fingerprint(h2)
		if !bytes.Equal(fp, fp2) {
			t.Fatal("fingerprint not deterministic")
		}
	})

	t.Run("known vectors", func(t *testing.T) {
		vectors := []struct {
			canonical string
			fpBE      uint64
		}{
			{`"null"`, 0x63dd24e7cc258f8a},
			{`"boolean"`, 0x9f42fc78a4d4f764},
			{`"int"`, 0x7275d51a3f395c8f},
			{`"long"`, 0xd054e14493f41db7},
			{`"float"`, 0x4d7c02cb3ea8d790},
			{`"double"`, 0x8e7535c032ab957e},
			{`"string"`, 0x8f014872634503c7},
			{`"bytes"`, 0x4fc016dac3201965},
		}

		for _, v := range vectors {
			t.Run(v.canonical, func(t *testing.T) {
				h := avro.NewRabin()
				h.Write([]byte(v.canonical))
				got := h.Sum64()
				var gotBE [8]byte
				binary.BigEndian.PutUint64(gotBE[:], got)
				var wantBE [8]byte
				binary.BigEndian.PutUint64(wantBE[:], v.fpBE)
				if gotBE != wantBE {
					t.Fatalf("fingerprint for %s: got %016x, want %016x", v.canonical, got, v.fpBE)
				}
			})
		}
	})

	t.Run("schema fingerprint via Schema", func(t *testing.T) {
		s := mustParse(t, `{"type":"int","doc":"ignored in canonical"}`)
		h1 := avro.NewRabin()
		fp1 := s.Fingerprint(h1)

		h2 := avro.NewRabin()
		h2.Write(s.Canonical())
		fp2 := h2.Sum(nil)

		if !bytes.Equal(fp1, fp2) {
			t.Fatalf("Schema.Fingerprint doesn't match manual: %x vs %x", fp1, fp2)
		}
	})
}
