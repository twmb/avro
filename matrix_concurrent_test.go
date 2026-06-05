package avro_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Concurrency hammer: one *Schema shared by many goroutines doing every
// operation simultaneously. A *Schema is documented goroutine-safe; the
// risks are the lazily-initialized internals (sync.Once field tables, the
// slab pool, cached canonical bytes) and any shared scratch state. Every
// result is compared byte-for-byte against the precomputed single-threaded
// answer, under the race detector in CI's `go test -race ./...`.
// ---------------------------------------------------------------------------

func TestMatrix_ConcurrentSchemaUse(t *testing.T) {
	cases := []struct {
		label  string
		schema string
		value  any
	}{
		{"int", `"int"`, int32(7)},
		{"string", `"string"`, "concurrent"},
		{"record", `{"type":"record","name":"CC","fields":[
			{"name":"a","type":"int"},{"name":"b","type":["null","string"],"default":null}]}`,
			map[string]any{"a": int32(1), "b": "x"}},
		{"decimal", `{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}`,
			[]byte{0x30, 0x39}},
		{"timestamp", `{"type":"long","logicalType":"timestamp-millis"}`, int64(1717243496789)},
		{"recursive", `{"type":"record","name":"CN","fields":[
			{"name":"v","type":"int"},{"name":"next","type":["null","CN"],"default":null}]}`,
			map[string]any{"v": int32(1), "next": map[string]any{"v": int32(2), "next": nil}}},
		{"array-of-union", `{"type":"array","items":["null","long","string"]}`,
			[]any{int64(5), nil, "s"}},
	}
	const goroutines = 8
	const iters = 100

	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			// FRESH schema per case so the lazy once-init paths (field
			// table construction, canonical caching) are themselves raced.
			s := avro.MustParse(c.schema)
			ref := avro.MustParse(c.schema) // single-threaded reference
			wantWire, err := ref.AppendEncode(nil, c.value)
			if err != nil {
				t.Fatalf("reference encode: %v", err)
			}
			wantJSON, err := ref.AppendEncodeJSON(nil, c.value)
			if err != nil {
				t.Fatalf("reference encodeJSON: %v", err)
			}
			var wantDec any
			if _, err := ref.Decode(wantWire, &wantDec); err != nil {
				t.Fatalf("reference decode: %v", err)
			}
			wantCanon := string(ref.Canonical())

			res, err := avro.Resolve(avro.MustParse(c.schema), avro.MustParse(c.schema))
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}

			var wg sync.WaitGroup
			errs := make(chan error, goroutines)
			for g := 0; g < goroutines; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < iters; i++ {
						w, err := s.AppendEncode(nil, c.value)
						if err != nil || !bytes.Equal(w, wantWire) {
							errs <- fmt.Errorf("encode diverged: err=%v w=%x", err, w)
							return
						}
						var a any
						if _, err := s.Decode(w, &a); err != nil || !matEqual(a, wantDec) {
							errs <- fmt.Errorf("decode diverged: err=%v a=%#v", err, a)
							return
						}
						j, err := s.AppendEncodeJSON(nil, a)
						if err != nil || !bytes.Equal(j, wantJSON) {
							errs <- fmt.Errorf("encodeJSON diverged: err=%v j=%s", err, j)
							return
						}
						var aj any
						if err := s.DecodeJSON(j, &aj); err != nil || !matEqual(aj, wantDec) {
							errs <- fmt.Errorf("decodeJSON diverged: err=%v", err)
							return
						}
						var ar any
						if _, err := res.Decode(w, &ar); err != nil || !matEqual(ar, wantDec) {
							errs <- fmt.Errorf("resolved decode diverged: err=%v", err)
							return
						}
						if i%16 == 0 {
							if got := string(s.Canonical()); got != wantCanon {
								errs <- fmt.Errorf("Canonical diverged: %s", got)
								return
							}
							root := s.Root()
							if root.Type == "" {
								errs <- fmt.Errorf("Root returned zero node")
								return
							}
						}
					}
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Error(err)
			}
		})
	}
}

// Concurrent SchemaCache use: many goroutines parsing the same and
// different named schemas through one cache, then cross-referencing.
func TestMatrix_ConcurrentSchemaCache(t *testing.T) {
	var cache avro.SchemaCache
	def := `{"type":"record","name":"CCD","fields":[{"name":"a","type":"int"}]}`
	if _, err := cache.Parse(def); err != nil {
		t.Fatalf("seed parse: %v", err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if _, err := cache.Parse(def); err != nil {
					errs <- fmt.Errorf("re-parse def: %v", err)
					return
				}
				ref, err := cache.Parse(`{"type":"array","items":"CCD"}`)
				if err != nil {
					errs <- fmt.Errorf("ref parse: %v", err)
					return
				}
				w, err := ref.AppendEncode(nil, []any{map[string]any{"a": int32(g)}})
				if err != nil {
					errs <- fmt.Errorf("encode: %v", err)
					return
				}
				var a any
				if _, err := ref.Decode(w, &a); err != nil {
					errs <- fmt.Errorf("decode: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// Concurrent FIRST use: goroutines hit a freshly parsed schema's decode
// path simultaneously, racing the lazily-built per-record skip/field
// tables (sync.Once internals).
func TestMatrix_ConcurrentFirstUse(t *testing.T) {
	for round := 0; round < 10; round++ {
		w := avro.MustParse(`{"type":"record","name":"FU","fields":[
			{"name":"drop","type":{"type":"record","name":"Inner","fields":[
				{"name":"x","type":"string"},{"name":"y","type":{"type":"array","items":"long"}}]}},
			{"name":"keep","type":"int"}]}`)
		r := avro.MustParse(`{"type":"record","name":"FU","fields":[
			{"name":"keep","type":"int"}]}`)
		res, err := avro.Resolve(w, r)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		wire, err := w.AppendEncode(nil, map[string]any{
			"drop": map[string]any{"x": "s", "y": []any{int64(1), int64(2)}},
			"keep": int32(9),
		})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var got map[string]any
				if _, err := res.Decode(wire, &got); err != nil {
					errs <- fmt.Errorf("first-use resolved decode: %v", err)
					return
				}
				if got["keep"] != int32(9) {
					errs <- fmt.Errorf("first-use skip corrupted: %#v", got)
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
	}
}
