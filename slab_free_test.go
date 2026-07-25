package avro_test

// Issue #41: Decode of a slab-free schema (scalar leaf, no custom wiring)
// bypasses the internal slab pool entirely and runs on a nil *slab. These
// tests pin the classifier and prove, two-sidedly against the matrix corpus,
// that the classification exactly matches which compiled desers touch the
// slab. Internal state is reached via the export_test.go bridges.

import (
	"runtime"
	"testing"

	avro "github.com/twmb/avro"
)

// TestScalarDecodeNoAllocAfterGC pins issue #41: Decode of a scalar schema
// must not allocate even when GC has drained the slab pool (primary and
// victim caches), which is the steady state of a low-allocation application.
// Before the fix, Decode unconditionally pulled a slab from the pool, so
// every post-GC decode paid a fresh slab allocation that the decode never
// used. The min-over-iterations guards against unrelated background mallocs:
// a genuinely slab-free Decode hits 0 on every quiet iteration, while the
// pre-fix pool refill allocates on EVERY iteration (two GCs empty both pool
// halves, so Get must call New).
func TestScalarDecodeNoAllocAfterGC(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	s := avro.MustParse(`"long"`)
	wire := []byte{4}
	var v int64
	if _, err := s.Decode(wire, &v); err != nil { // warm one-time state
		t.Fatal(err)
	}
	minMallocs := ^uint64(0)
	var before, after runtime.MemStats
	for i := 0; i < 5; i++ {
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&before)
		if _, err := s.Decode(wire, &v); err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&after)
		minMallocs = min(minMallocs, after.Mallocs-before.Mallocs)
	}
	if v != 2 {
		t.Fatalf("decoded %d, want 2", v)
	}
	if minMallocs != 0 {
		t.Errorf("scalar Decode allocated on every post-GC iteration (min %d mallocs/op); slab pool should be bypassed for slab-free schemas", minMallocs)
	}
}

// TestSlabFreeClassifier pins slab-free membership across the axes that
// decide it: schema kind × logical type × custom wiring × cache-inherited
// custom × resolution × opts presence.
func TestSlabFreeClassifier(t *testing.T) {
	free := []string{
		`"null"`, `"boolean"`, `"int"`, `"long"`, `"float"`, `"double"`, `"bytes"`,
		`{"type":"fixed","name":"F","size":4}`,
		`{"type":"enum","name":"E","symbols":["A"]}`,
		`{"type":"int","logicalType":"date"}`,
		`{"type":"int","logicalType":"time-millis"}`,
		`{"type":"long","logicalType":"time-micros"}`,
		`{"type":"long","logicalType":"timestamp-millis"}`,
		`{"type":"long","logicalType":"timestamp-nanos"}`,
		`{"type":"long","logicalType":"local-timestamp-micros"}`,
		`{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
		`{"type":"fixed","name":"FD","size":4,"logicalType":"decimal","precision":4,"scale":2}`,
		`{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`,
		`{"type":"fixed","name":"D","size":12,"logicalType":"duration"}`,
	}
	needsSlab := []string{
		`"string"`,
		`{"type":"string","logicalType":"uuid"}`,
		`{"type":"record","name":"R","fields":[{"name":"f","type":"long"}]}`,
		`{"type":"array","items":"long"}`,
		`{"type":"map","values":"long"}`,
		`["null","long"]`,
		// Recursive and diamond named-type shapes: the second-occurrence
		// REFERENCE path can only appear under a composite kind, so it can
		// never smuggle a slab-needing node beneath a slab-free top.
		`{"type":"record","name":"L","fields":[{"name":"next","type":["null","L"]}]}`,
		`{"type":"record","name":"Dia","fields":[{"name":"a","type":{"type":"fixed","name":"Sh","size":2}},{"name":"b","type":"Sh"}]}`,
	}
	for _, sch := range free {
		if s := avro.MustParse(sch); !s.SlabFreeForTest() {
			t.Errorf("schema %s: slabFree=false, want true", sch)
		}
	}
	for _, sch := range needsSlab {
		if s := avro.MustParse(sch); s.SlabFreeForTest() {
			t.Errorf("schema %s: slabFree=true, want false", sch)
		}
	}

	// A custom decoder on a scalar forces the pool: the wrapper reads and
	// writes slab state (customMatches / bypassCustom).
	ct := avro.CustomType{
		AvroType: "fixed",
		Decode:   func(v any, _ *avro.SchemaNode) (any, error) { return v, nil },
	}
	sc, err := avro.Parse(`{"type":"fixed","name":"CF","size":2}`, ct)
	if err != nil {
		t.Fatalf("custom parse: %v", err)
	}
	if sc.SlabFreeForTest() {
		t.Error("custom-wired fixed: slabFree=true, want false")
	}
	var fxAny any
	if _, err := sc.Decode([]byte{1, 2}, &fxAny); err != nil {
		t.Errorf("custom-wired fixed decode: %v", err)
	}

	// Cache-inherited custom wraps: the DEFINING parse wires the custom; a
	// later reference parse (re-registering the same custom, as the cache
	// requires) inherits the baked deser while its own overlay may stay
	// empty (applyCustomTypes visits only newly built nodes) — it must
	// classify slab-needing via customBaked. The custom-free twin inherits
	// a plain deser and stays slab-free.
	ctEnum := avro.CustomType{
		AvroType: "enum",
		Decode:   func(v any, _ *avro.SchemaNode) (any, error) { return v, nil },
	}
	cc := &avro.SchemaCache{}
	if _, err := cc.Parse(`{"type":"enum","name":"CE","symbols":["A","B"]}`, ctEnum); err != nil {
		t.Fatalf("cache defining parse: %v", err)
	}
	ref, err := cc.Parse(`"CE"`, ctEnum)
	if err != nil {
		t.Fatalf("cache reference parse: %v", err)
	}
	if ref.SlabFreeForTest() {
		t.Error("cache-inherited custom enum reference: slabFree=true, want false")
	}
	cc2 := &avro.SchemaCache{}
	if _, err := cc2.Parse(`{"type":"enum","name":"CE","symbols":["A","B"]}`); err != nil {
		t.Fatalf("cache defining parse (no custom): %v", err)
	}
	ref2, err := cc2.Parse(`"CE"`)
	if err != nil {
		t.Fatalf("cache reference parse (no custom): %v", err)
	}
	if !ref2.SlabFreeForTest() {
		t.Error("cache-inherited plain enum reference: slabFree=false, want true")
	}
	var sym string
	if _, err := ref2.Decode([]byte{2}, &sym); err != nil || sym != "B" {
		t.Errorf("inherited enum nil-slab decode: %q, %v", sym, err)
	}

	// Non-identity resolution keeps the pool (zero value on the fresh
	// Schema): promote/skip paths use the slab — bytes→string promotion
	// slab-copies, and resolved record skips bump the recursion depth.
	res, err := avro.Resolve(avro.MustParse(`"bytes"`), avro.MustParse(`"string"`))
	if err != nil {
		t.Fatalf("resolve bytes→string: %v", err)
	}
	if res.SlabFreeForTest() {
		t.Error("resolved bytes→string: slabFree=true, want false")
	}
	var str string
	if _, err := res.Decode([]byte{2, 'x'}, &str); err != nil || str != "x" {
		t.Errorf("resolved bytes→string decode: %q, %v", str, err)
	}
	// Identity resolution returns the reader itself; its own
	// classification applies because its own deser runs.
	ident, err := avro.Resolve(avro.MustParse(`"long"`), avro.MustParse(`"long"`))
	if err != nil {
		t.Fatalf("identity resolve: %v", err)
	}
	if !ident.SlabFreeForTest() {
		t.Error("identity-resolved long: slabFree=false, want true")
	}

	// Opts on a slab-free schema take the pooled path (opts only ever
	// alter slab state) and must stay correct.
	var lv int64
	if _, err := avro.MustParse(`"long"`).Decode([]byte{4}, &lv, avro.TaggedUnions()); err != nil || lv != 2 {
		t.Errorf("slab-free decode with opts: %d, %v", lv, err)
	}
}

// TestSlabFreeMatchesNilSlabOracle is the two-sided generative net: for
// every matrix fragment in every matrix context, the slab-free
// classification must EXACTLY equal the independent oracle "decoding every
// encoded value with a nil slab does not panic". A slab-free schema whose
// deser secretly touches the slab panics (classification too eager: a
// user-visible crash in Decode); a pooled schema whose deser never touches
// it survives nil (classification too conservative: issue #41 regresses for
// that shape). Composite kinds panic at entry via the recursion-depth bump
// and string leaves via the slab string copy, so the oracle discriminates
// every cell.
//
// Non-vacuity was verified by neutering: adding "string" to slabFreeKinds
// makes the string cells fail here in the panicked direction, and hardcoding
// slabFree=false makes every scalar cell fail in the other direction.
func TestSlabFreeMatchesNilSlabOracle(t *testing.T) {
	u := &uniq{}
	var freeCells, pooledCells int
	for _, fr := range matFrags() {
		for _, cx := range matCtxs() {
			if cx.skip != nil && cx.skip(fr.kind) {
				continue
			}
			schema := cx.schema(fr.schema(u), fr.kind, u)
			s, err := avro.Parse(schema)
			if err != nil {
				t.Fatalf("parse %s: %v", schema, err)
			}
			for _, val := range fr.values {
				wv := cx.wrap(val)
				wire, err := s.Encode(wv)
				if err != nil {
					t.Fatalf("encode %s %v: %v", schema, wv, err)
				}
				var nilGot any
				panicked := false
				var derr error
				var rest []byte
				func() {
					defer func() {
						if r := recover(); r != nil {
							panicked = true
						}
					}()
					rest, derr = s.DeserNilSlabForTest(wire, &nilGot)
				}()
				if panicked == s.SlabFreeForTest() {
					t.Fatalf("schema %s (frag %s, ctx %s): slabFree=%v but nil-slab decode panicked=%v",
						schema, fr.label, cx.label, s.SlabFreeForTest(), panicked)
				}
				if panicked {
					pooledCells++
					continue
				}
				freeCells++
				if derr != nil {
					t.Fatalf("schema %s: nil-slab decode error: %v", schema, derr)
				}
				if len(rest) != 0 {
					t.Fatalf("schema %s: nil-slab decode left %d bytes", schema, len(rest))
				}
				var poolGot any
				if _, err := s.Decode(wire, &poolGot); err != nil {
					t.Fatalf("schema %s: pooled decode error: %v", schema, err)
				}
				if !matEqual(nilGot, poolGot) {
					t.Fatalf("schema %s value %v: nil-slab decode %v != pooled decode %v", schema, wv, nilGot, poolGot)
				}
			}
		}
	}
	if freeCells == 0 || pooledCells == 0 {
		t.Fatalf("vacuous net: %d slab-free cells, %d pooled cells — both sides must be exercised", freeCells, pooledCells)
	}
	t.Logf("oracle cells: %d slab-free, %d pooled", freeCells, pooledCells)
}
