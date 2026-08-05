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

// toggleSink writes into buf until fail is flipped, then fails every Write
// with err. Letting the test flip fail at a precise moment makes a chosen I/O
// step (a specific block write, the Reset old-block flush) the one that fails
// while every earlier write — including the header NewWriter emits — succeeds.
type toggleSink struct {
	buf  bytes.Buffer
	fail bool
	err  error
}

func (s *toggleSink) Write(p []byte) (int, error) {
	if s.fail {
		return 0, s.err
	}
	return s.buf.Write(p)
}

// Class invariant (NOT_BUGS #28): EVERY fallible I/O step in EVERY Writer
// method must poison the Writer — once a sink write or the sync-marker source
// fails, no later Encode/Flush silently succeeds and no further bytes land that
// a reader would accept. TestMatrixOCF_StatefulPoison covers only Encode/Flush
// after a block-write failure; this crosses the remaining (method × I/O step)
// cells. Reset has THREE fallible steps — the old-block flush (before the
// repoint), sync-marker generation, and the header write (both after the
// repoint) — and the sync/header cells are the ones a prior gap missed
// (Reset cleared w.err then failed without re-setting it, so the writer kept
// emitting a headerless stream onto the new sink).
func TestMatrixOCF_WriterIOFailurePoisonsEveryStep(t *testing.T) {
	schema := avro.MustParse(`"int"`)
	v := int32(7)

	// Not-yet-closed Writer: poisoned with the exact sentinel on every call.
	assertSticky := func(t *testing.T, w *Writer, sentinel error) {
		t.Helper()
		if err := w.Encode(&v); !errors.Is(err, sentinel) {
			t.Fatalf("Encode after failure: want sticky %v, got %v", sentinel, err)
		}
		if err := w.Flush(); !errors.Is(err, sentinel) {
			t.Fatalf("Flush after failure: want sticky %v, got %v", sentinel, err)
		}
		if err := w.Close(); !errors.Is(err, sentinel) {
			t.Fatalf("Close after failure: want sticky %v, got %v", sentinel, err)
		}
	}
	// Possibly-closed Writer (the Close cell): must never silently accept more.
	assertNoSilentSuccess := func(t *testing.T, w *Writer) {
		t.Helper()
		if err := w.Encode(&v); err == nil {
			t.Fatal("Encode after failure silently succeeded")
		}
		if err := w.Flush(); err == nil {
			t.Fatal("Flush after failure silently succeeded")
		}
	}

	t.Run("encode-block-write", func(t *testing.T) {
		s := &toggleSink{err: errors.New("encblk")}
		w, err := NewWriter(s, schema, WithBlockCount(1)) // header written
		if err != nil {
			t.Fatal(err)
		}
		s.fail = true
		if err := w.Encode(&v); !errors.Is(err, s.err) { // 1-count → block write
			t.Fatalf("Encode block write: want %v, got %v", s.err, err)
		}
		assertSticky(t, w, s.err)
	})

	t.Run("flush-block-write", func(t *testing.T) {
		s := &toggleSink{err: errors.New("flushblk")}
		w, err := NewWriter(s, schema, WithBlockCount(1000))
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Encode(&v); err != nil { // buffered, no flush yet
			t.Fatal(err)
		}
		s.fail = true
		if err := w.Flush(); !errors.Is(err, s.err) {
			t.Fatalf("Flush block write: want %v, got %v", s.err, err)
		}
		assertSticky(t, w, s.err)
	})

	t.Run("close-final-flush", func(t *testing.T) {
		s := &toggleSink{err: errors.New("closeblk")}
		w, err := NewWriter(s, schema, WithBlockCount(1000))
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
		s.fail = true
		if err := w.Close(); !errors.Is(err, s.err) {
			t.Fatalf("Close final flush: want %v, got %v", s.err, err)
		}
		// Close legitimately closes; subsequent ops error (errClosed), not silent.
		assertNoSilentSuccess(t, w)
	})

	t.Run("reset-old-block-flush", func(t *testing.T) {
		a := &toggleSink{err: errors.New("resetoldblk")}
		w, err := NewWriter(a, schema, WithBlockCount(1000))
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Encode(&v); err != nil { // buffered in the OLD sink
			t.Fatal(err)
		}
		a.fail = true
		var b bytes.Buffer
		if err := w.Reset(&b); !errors.Is(err, a.err) { // old-block flush fails
			t.Fatalf("Reset old-block flush: want %v, got %v", a.err, err)
		}
		assertSticky(t, w, a.err)
	})

	t.Run("reset-sync-generation", func(t *testing.T) {
		var a bytes.Buffer
		w, err := NewWriter(&a, schema) // initial sync generated here
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
		orig := randRead
		boom := errors.New("resetsync")
		randRead = func(b []byte) (int, error) { return 0, boom }
		defer func() { randRead = orig }()
		var b bytes.Buffer
		if err := w.Reset(&b); !errors.Is(err, boom) {
			t.Fatalf("Reset sync gen: want %v, got %v", boom, err)
		}
		if b.Len() != 0 { // sync fails before any header write
			t.Fatalf("new sink touched after sync-gen failure: %d bytes", b.Len())
		}
		assertSticky(t, w, boom)
	})

	t.Run("reset-header-write", func(t *testing.T) {
		var a bytes.Buffer
		w, err := NewWriter(&a, schema)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Encode(&v); err != nil {
			t.Fatal(err)
		}
		boom := errors.New("resethdr")
		b := &failFirstWriteSink{err: boom} // header write to new sink fails
		if err := w.Reset(b); !errors.Is(err, boom) {
			t.Fatalf("Reset header write: want %v, got %v", boom, err)
		}
		// Un-poisoned, the post-Reset Encode/Flush would emit a headerless block
		// onto b; poisoned, nothing lands and b holds no readable OCF.
		if _, err := NewReader(bytes.NewReader(b.buf.Bytes())); err == nil {
			t.Fatalf("new sink holds a readable OCF (%d bytes)", b.buf.Len())
		}
		assertSticky(t, w, boom)
	})
}

var _ = reflect.DeepEqual // keep reflect imported for future model asserts
