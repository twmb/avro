package ocf

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// Files this package writes must be readable by this package: the Reader
// bounds a block's declared count to len(block)+maxOCFZeroByteSlack (and
// caps consecutive zero-byte records at the same slack), so the Writer
// must start a new block before a run of zero-byte datums (top-level
// "null", a record of only null fields, a size-0 fixed) exceeds that
// bound. The byte-driven flush alone never fires for such datums — they
// contribute no bytes — so without a count-side bound every datum lands
// in one giant block the Reader then rejects.
func TestWriterZeroByteDatumsSelfReadable(t *testing.T) {
	const n = 3*maxOCFZeroByteSlack + 17 // several blocks' worth
	for _, tc := range []struct {
		name   string
		schema string
		value  any
	}{
		{"null", `"null"`, nil},
		{"size-0 fixed", `{"type":"fixed","name":"F","size":0}`, []byte{}},
		{"all-null record", `{"type":"record","name":"R","fields":[
			{"name":"a","type":"null"}]}`, map[string]any{"a": nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := avro.MustParse(tc.schema)
			var buf bytes.Buffer
			w, err := NewWriter(&buf, s)
			if err != nil {
				t.Fatalf("NewWriter: %v", err)
			}
			for i := 0; i < n; i++ {
				if err := w.Encode(tc.value); err != nil {
					t.Fatalf("Encode #%d: %v", i, err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			r, err := NewReader(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			defer r.Close()
			var got int
			for {
				var v any
				err := r.Decode(&v)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("Decode #%d: %v", got, err)
				}
				got++
			}
			if got != n {
				t.Fatalf("read %d of %d datums back", got, n)
			}
		})
	}
}

// Datums that consume ≥1 wire byte (here: union values — even the null
// branch writes its index varint) keep count ≤ len(block), so the
// count-side flush bound never fires for them; block sizing stays purely
// byte-driven and the file reads back exactly as before.
func TestWriterOneByteDatumsUnaffectedByCountBound(t *testing.T) {
	s := avro.MustParse(`["null","int"]`)
	const n = 2*maxOCFZeroByteSlack + 11
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for i := 0; i < n; i++ {
		var v any
		if i%37 == 0 {
			v = int32(i) // occasional real bytes
		}
		if err := w.Encode(v); err != nil {
			t.Fatalf("Encode #%d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	var got int
	for {
		var v any
		err := r.Decode(&v)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Decode #%d: %v", got, err)
		}
		got++
	}
	if got != n {
		t.Fatalf("read %d of %d datums back", got, n)
	}
}

// The per-block consecutive-zero-byte-record CAP (Reader.Decode's zeroRun
// counter) is a SECOND, independent DoS guard from readBlock's
// count <= len(block)+slack check. They must be tested independently: a
// block whose decompressed bytes are large (here, padded with ignored
// trailing garbage) passes the count-vs-length check, so only the zeroRun
// counter stops a hostile count from driving the decode loop billions of
// times against a zero-byte schema ("null"). A mutant that inverts the
// counter (zeroRun++ -> --) or shifts its boundary survives every
// round-trip and every count-vs-length test — this drives the counter
// directly by hand-framing such a block.
func TestReaderZeroRunCapIndependentOfBlockLength(t *testing.T) {
	sync := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	build := func(count int64, payloadLen int) []byte {
		var buf bytes.Buffer
		// header: magic + metadata map (schema only) + sync
		buf.WriteString("Obj\x01")
		meta := encodeMap(nil, []kv{{"avro.schema", []byte(`"null"`)}})
		buf.Write(meta)
		buf.Write(sync[:])
		// one data block: count, size, payload (garbage the null decode
		// ignores — it consumes 0 bytes per datum regardless), sync
		block := binary.AppendVarint(nil, count)
		block = binary.AppendVarint(block, int64(payloadLen))
		block = append(block, bytes.Repeat([]byte{0xAB}, payloadLen)...)
		block = append(block, sync[:]...)
		buf.Write(block)
		return buf.Bytes()
	}

	// A hostile count far above the slack, with a payload long enough to
	// clear the count-vs-length check (count <= len(block)+slack), so the
	// ONLY thing that can stop the decode loop is the zeroRun counter.
	hostile := int64(maxOCFZeroByteSlack) + 100000
	data := build(hostile, int(hostile)) // len(block) >= count: length check passes

	r, err := NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	var consumed int
	for {
		var v any
		err := r.Decode(&v)
		if err == nil {
			consumed++
			if consumed > maxOCFZeroByteSlack+10 {
				t.Fatalf("zeroRun cap did not fire: decoded %d zero-byte records (cap %d)", consumed, maxOCFZeroByteSlack)
			}
			continue
		}
		// Expected: the zeroRun cap rejects past maxOCFZeroByteSlack
		// consecutive zero-byte datums.
		if !strings.Contains(err.Error(), "zero-byte records") {
			t.Fatalf("expected zero-byte-record cap error, got: %v", err)
		}
		break
	}
	// The cap must fire at the slack boundary, not before and not (per the
	// guard above) unboundedly after.
	if consumed != maxOCFZeroByteSlack {
		t.Fatalf("zeroRun cap fired after %d records, want exactly %d", consumed, maxOCFZeroByteSlack)
	}
}
