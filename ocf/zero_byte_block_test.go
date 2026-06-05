package ocf

import (
	"bytes"
	"errors"
	"io"
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
