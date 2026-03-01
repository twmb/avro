package ocf

import (
	"bytes"
	"testing"

	"github.com/twmb/avro"
)

func FuzzOCFReader(f *testing.F) {
	// Build a valid null-codec OCF programmatically.
	nullSchema := avro.MustParse(`"string"`)
	var nullBuf bytes.Buffer
	w, err := NewWriter(&nullBuf, nullSchema)
	if err != nil {
		f.Fatal(err)
	}
	if err := w.Encode("hello"); err != nil {
		f.Fatal(err)
	}
	if err := w.Encode("world"); err != nil {
		f.Fatal(err)
	}
	if err := w.Close(); err != nil {
		f.Fatal(err)
	}
	f.Add(nullBuf.Bytes())

	// Build a valid deflate-codec OCF.
	var deflateBuf bytes.Buffer
	w, err = NewWriter(&deflateBuf, nullSchema, WithCodec(DeflateCodec(1)))
	if err != nil {
		f.Fatal(err)
	}
	if err := w.Encode("compressed"); err != nil {
		f.Fatal(err)
	}
	if err := w.Close(); err != nil {
		f.Fatal(err)
	}
	f.Add(deflateBuf.Bytes())

	// Empty input.
	f.Add([]byte{})

	// Just the magic bytes.
	f.Add([]byte{'O', 'b', 'j', 1})

	f.Fuzz(func(t *testing.T, data []byte) {
		r, err := NewReader(bytes.NewReader(data))
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
