package ocf

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// OCF stateful model: random programs of Writer operations — good encodes,
// VALUE-ERROR encodes (documented to discard only the failed datum and
// leave the Writer usable), raw Write of pre-encoded datums, explicit
// Flushes, Close, and append-mode reopen — with the model tracking exactly
// which datums were accepted. The reader must observe precisely the
// accepted sequence. Seeds are fixed, so every program is reproducible.
// ---------------------------------------------------------------------------

func TestMatrixOCF_StatefulPrograms(t *testing.T) {
	schemaJSON := `{"type":"record","name":"SP","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":["null","string"],"default":null}]}`
	schema := avro.MustParse(schemaJSON)

	mkGood := func(i int) any {
		var b any
		if i%3 == 0 {
			b = fmt.Sprintf("s%d", i)
		}
		return map[string]any{"a": int32(i), "b": b}
	}
	bad := map[string]any{"a": "not-an-int", "b": nil}

	for seed := int64(1); seed <= 12; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			var buf bytes.Buffer
			w, err := NewWriter(&buf, schema, WithBlockCount(1+rng.Intn(4)))
			if err != nil {
				t.Fatalf("NewWriter: %v", err)
			}
			var expected []any
			n := 30 + rng.Intn(40)
			for i := 0; i < n; i++ {
				switch rng.Intn(10) {
				case 0, 1, 2, 3, 4, 5: // good Encode
					v := mkGood(i)
					if err := w.Encode(v); err != nil {
						t.Fatalf("op %d: good Encode failed: %v", i, err)
					}
					expected = append(expected, v)
				case 6, 7: // VALUE-error Encode: rejected, Writer stays usable
					if err := w.Encode(bad); err == nil {
						t.Fatalf("op %d: bad Encode unexpectedly accepted", i)
					}
				case 8: // raw Write of a pre-encoded datum
					v := mkGood(1000 + i)
					enc, err := schema.AppendEncode(nil, v)
					if err != nil {
						t.Fatalf("op %d: pre-encode: %v", i, err)
					}
					if _, err := w.Write(enc); err != nil {
						t.Fatalf("op %d: Write: %v", i, err)
					}
					expected = append(expected, v)
				case 9: // explicit Flush (empty flush is a no-op)
					if err := w.Flush(); err != nil {
						t.Fatalf("op %d: Flush: %v", i, err)
					}
				}
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			// The reader must see exactly the accepted datums, in order.
			r, err := NewReader(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			defer r.Close()
			var got []any
			for {
				var v any
				err := r.Decode(&v)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("Decode #%d: %v", len(got), err)
				}
				got = append(got, v)
			}
			if len(got) != len(expected) {
				t.Fatalf("read %d datums, model accepted %d", len(got), len(expected))
			}
			for i := range expected {
				want, err := schema.AppendEncode(nil, expected[i])
				if err != nil {
					t.Fatal(err)
				}
				gotW, err := schema.AppendEncode(nil, got[i])
				if err != nil || !bytes.Equal(gotW, want) {
					t.Fatalf("datum %d differs: got %#v want %#v", i, got[i], expected[i])
				}
			}
		})
	}
}

// The same model across an append boundary: a random program writes and
// closes a file, NewAppendWriter continues it with another random program,
// and the reader sees both programs' accepted datums in order.
func TestMatrixOCF_StatefulAppendPrograms(t *testing.T) {
	schemaJSON := `{"type":"record","name":"SA","fields":[{"name":"a","type":"int"}]}`
	schema := avro.MustParse(schemaJSON)
	bad := map[string]any{"a": "nope"}

	for seed := int64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			path := filepath.Join(t.TempDir(), "f.avro")
			var expected []any

			runProgram := func(w *Writer, base, ops int) {
				t.Helper()
				for i := 0; i < ops; i++ {
					switch rng.Intn(8) {
					case 0, 1, 2, 3, 4:
						v := map[string]any{"a": int32(base + i)}
						if err := w.Encode(v); err != nil {
							t.Fatalf("Encode: %v", err)
						}
						expected = append(expected, v)
					case 5, 6:
						if err := w.Encode(bad); err == nil {
							t.Fatal("bad Encode accepted")
						}
					case 7:
						if err := w.Flush(); err != nil {
							t.Fatalf("Flush: %v", err)
						}
					}
				}
				if err := w.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
			}

			f, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			w1, err := NewWriter(f, schema, WithBlockCount(2))
			if err != nil {
				t.Fatalf("NewWriter: %v", err)
			}
			runProgram(w1, 0, 15+rng.Intn(20))
			f.Close()

			f2, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			w2, err := NewAppendWriter(f2, WithBlockCount(3))
			if err != nil {
				t.Fatalf("NewAppendWriter: %v", err)
			}
			runProgram(w2, 1000, 15+rng.Intn(20))
			f2.Close()

			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			r, err := NewReader(bytes.NewReader(data))
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			defer r.Close()
			var i int
			for {
				var v map[string]any
				err := r.Decode(&v)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("Decode #%d: %v", i, err)
				}
				want := expected[i].(map[string]any)["a"]
				if v["a"] != want {
					t.Fatalf("datum %d: got %v want %v", i, v["a"], want)
				}
				i++
			}
			if i != len(expected) {
				t.Fatalf("read %d, model accepted %d", i, len(expected))
			}
		})
	}
}

// I/O-error poisoning: once the sink fails, every subsequent operation
// returns the sticky error, and Close still releases the codec.
func TestMatrixOCF_StatefulPoison(t *testing.T) {
	schema := avro.MustParse(`"int"`)
	fw := &failAfterWriter{n: 200}
	w, err := NewWriter(fw, schema, WithBlockCount(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	var poisoned bool
	for i := 0; i < 1000; i++ {
		if err := w.Encode(int32(i)); err != nil {
			poisoned = true
			// Sticky: the next ops fail with an error too.
			if err2 := w.Encode(int32(i)); err2 == nil {
				t.Fatal("Encode succeeded after I/O poison")
			}
			if err2 := w.Flush(); err2 == nil {
				t.Fatal("Flush succeeded after I/O poison")
			}
			break
		}
	}
	if !poisoned {
		t.Fatal("failing writer never tripped")
	}
	_ = w.Close() // must not panic; codec released regardless
}

type failAfterWriter struct{ n int }

func (f *failAfterWriter) Write(p []byte) (int, error) {
	if f.n <= 0 {
		return 0, errors.New("sink failed")
	}
	f.n -= len(p)
	return len(p), nil
}

var _ = reflect.DeepEqual // keep reflect imported for future model asserts
