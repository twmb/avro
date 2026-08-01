package ocf

// DoS entry-point battery — OCF package.
//
// Companion to ../dos_battery_test.go. The OCF reader/writer add hostile-input
// classes the core codec does not have: a block is "read a compressed size,
// then inflate to a length declared INSIDE the payload", and a header is "read
// a count, then loop". Each such boundary has TWO limits — the wire-side and
// the materialized side — and a cap on one is not a cap on the other.
//
// Rows: ocf.NewReader (+ Reader.Decode) / ocf.NewWriter (+ WithMetadata).
// Columns:
//   C1 header nesting/size   — deeply-nested avro.schema; metadata entry count.
//   C2 block count / size    — declared block size vs maxBlockBytes; block
//                              count vs len(block)+maxOCFZeroByteSlack;
//                              zero-byte-record run.
//   C4 decompression amplif. — a small compressed block inflating past
//                              WithMaxDecompressedBlockBytes.
//   C5 error-message echo    — unknown codec name; over-cap metadata key.
//
// Same rule as the core battery: cells are never "closed". A later OCF DoS find
// extends this matrix; it does not retire it.

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/avro"
)

const ocfDosBudget = 4 * time.Second

// dosRun runs fn under a watchdog: a hang past the budget (missing bound on a
// non-allocating loop) or a panic (hostile input must error, not panic) fails
// the cell. Package-scoped to ocf; does not collide with the avro twin.
func dosRun(t *testing.T, name string, fn func() error) (error, bool) {
	t.Helper()
	type result struct {
		err error
		pan any
	}
	ch := make(chan result, 1)
	start := time.Now()
	go func() {
		var r result
		defer func() {
			if p := recover(); p != nil {
				r.pan = p
			}
			ch <- r
		}()
		r.err = fn()
	}()
	select {
	case r := <-ch:
		if r.pan != nil {
			t.Errorf("%s: panicked on hostile input (must return an error, not panic): %v", name, r.pan)
			return nil, false
		}
		if d := time.Since(start); d > ocfDosBudget {
			t.Errorf("%s: completed but took %v (> %v) — cost not bounded", name, d, ocfDosBudget)
		}
		return r.err, true
	case <-time.After(ocfDosBudget):
		t.Errorf("%s: did not return within %v — bound missing (hang/unbounded loop)", name, ocfDosBudget)
		return nil, false
	}
}

func wantReject(t *testing.T, name string, fn func() error) {
	t.Helper()
	if err, ok := dosRun(t, name, fn); ok && err == nil {
		t.Errorf("%s: hostile input was accepted (want a fast rejection)", name)
	}
}

// wantTerminate asserts fn returns within the budget, whatever its verdict.
// A legal schema must be ACCEPTED, so the property is termination rather than
// rejection.
func wantTerminate(t *testing.T, name string, fn func() error) {
	t.Helper()
	if err, ok := dosRun(t, name, fn); ok && err != nil {
		t.Errorf("%s: legal input was rejected: %v", name, err)
	}
}

const ocfDosMaxErrLen = 4096

func wantBoundedErr(t *testing.T, name string, fn func() error) {
	t.Helper()
	if err, ok := dosRun(t, name, fn); ok {
		if err == nil {
			t.Errorf("%s: want a (bounded) error, got nil", name)
		} else if n := len(err.Error()); n > ocfDosMaxErrLen {
			t.Errorf("%s: error message is %d bytes (> %d) — hostile input echoed unbounded", name, n, ocfDosMaxErrLen)
		}
	}
}

// ocfHeaderSync writes a real OCF header for schemaJSON (so readHeader accepts
// it) and returns the header bytes plus its 16-byte sync marker, for cells that
// append a hostile block by hand.
func ocfHeaderSync(t *testing.T, schemaJSON string) (hdr, sync []byte) {
	t.Helper()
	s, err := avro.Parse(schemaJSON)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	hdr = buf.Bytes()
	return hdr, hdr[len(hdr)-16:]
}

// deflateBomb returns a deflate stream that inflates to n bytes.
func deflateBomb(n int) []byte {
	var buf bytes.Buffer
	w, _ := flate.NewWriter(&buf, flate.DefaultCompression)
	w.Write(make([]byte, n))
	w.Close()
	return buf.Bytes()
}

//////////////////////////////////////////////////////////////////////////////
// C4 — DECOMPRESSION AMPLIFICATION (a few bytes -> hundreds of MiB)
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_OCF_C4_Decompression(t *testing.T) {
	// A deflate block inflating past WithMaxDecompressedBlockBytes must reject
	// at the cap, not after materializing the bomb. Bound: maxDecompressed
	// enforced INSIDE the codec. Extreme (snappy/deflate/zstd + null backstop):
	// TestRegression_OCFDecompressionAmplificationBounded,
	// TestRegression_OCFDeflateDecompressLimitMaxInt, TestRegression_OCFLargeDatumReaderCap.
	data := ocfWith(`"null"`, "deflate", 1, deflateBomb(8<<20)) // declares 8 MiB decompressed
	wantReject(t, "NewReader+Decode/deflate-bomb", func() error {
		r, err := NewReader(bytes.NewReader(data), WithMaxDecompressedBlockBytes(1<<20))
		if err != nil {
			return err // header/codec rejection is also a safe outcome
		}
		defer r.Close()
		var v any
		return r.Decode(&v)
	})
}

//////////////////////////////////////////////////////////////////////////////
// C2 — BLOCK COUNT / SIZE / ZERO-RUN
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_OCF_C2_BlockCountSize(t *testing.T) {
	// A zero-field record makes every datum consume zero bytes; a hostile block
	// count then drives a near-infinite decode loop over an empty payload.
	// Bound: count > len(block)+maxOCFZeroByteSlack (readBlock) + the
	// consecutive zero-run cap in Decode. Extreme: TestRegression_OCFBlockCountCap,
	// TestReaderZeroRunCapIndependentOfBlockLength.
	zeroByteCount := ocfWith(`{"type":"record","name":"E","fields":[]}`, "null", 1_000_000_000, nil)
	wantReject(t, "NewReader+Decode/zero-byte-record-huge-count", func() error {
		r, err := NewReader(bytes.NewReader(zeroByteCount))
		if err != nil {
			return err
		}
		defer r.Close()
		var v map[string]any
		return r.Decode(&v)
	})

	// A block declaring a huge COMPRESSED size must reject before the read
	// allocates it. Bound: size > maxBlockBytes (default 64 MiB / WithMaxBlockBytes),
	// checked before reading the payload. Extreme: TestWithMaxBlockBytes.
	hdr, sync := ocfHeaderSync(t, `"long"`)
	var hugeSize []byte
	hugeSize = append(hugeSize, hdr...)
	hugeSize = append(hugeSize, binary.AppendVarint(nil, 1)...)     // count = 1
	hugeSize = append(hugeSize, binary.AppendVarint(nil, 1<<40)...) // declared block size = 1 TiB
	hugeSize = append(hugeSize, sync...)
	wantReject(t, "NewReader+Decode/huge-declared-block-size", func() error {
		r, err := NewReader(bytes.NewReader(hugeSize))
		if err != nil {
			return err
		}
		defer r.Close()
		var v int64
		return r.Decode(&v)
	})

	// Same hostile block, but with the reader's cap RAISED above the declared
	// size — as a caller setting a very large / "unlimited" WithMaxBlockBytes
	// would. The size > maxBlockBytes guard no longer fires, so readBlock must
	// still reject gracefully instead of eagerly make([]byte, declaredSize):
	// near the MaxInt64 ceiling that allocation is an unrecoverable fatal OOM,
	// and even at realistic raised caps a tiny file forces a multi-GiB spike.
	// Bound: readBlock reads incrementally beyond ocfEagerBlockAllocLimit, so a
	// declared-but-absent size fails after consuming the bytes actually present.
	// Extreme: TestRegression_OCFRaisedBlockCapDoesNotEagerAllocate.
	hdrRaised, syncRaised := ocfHeaderSync(t, `"long"`)
	var raisedHuge []byte
	raisedHuge = append(raisedHuge, hdrRaised...)
	raisedHuge = append(raisedHuge, binary.AppendVarint(nil, 1)...)     // count = 1
	raisedHuge = append(raisedHuge, binary.AppendVarint(nil, 1<<48)...) // declared 256 TiB, no payload
	raisedHuge = append(raisedHuge, syncRaised...)
	wantReject(t, "NewReader+Decode/huge-declared-size-raised-cap", func() error {
		r, err := NewReader(bytes.NewReader(raisedHuge), WithMaxBlockBytes(1<<50))
		if err != nil {
			return err
		}
		defer r.Close()
		var v int64
		return r.Decode(&v)
	})
}

//////////////////////////////////////////////////////////////////////////////
// C1 — HEADER (nested schema, metadata entry count)
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_OCF_C1_Header(t *testing.T) {
	// A deeply-nested avro.schema in the header is parsed by avro.Parse, whose
	// checkSchemaNestingDepth pre-scan rejects it (the OCF header inherits the
	// core schema-parse bounds). Bound: avro.checkSchemaNestingDepth.
	deepSchema := strings.Repeat(`{"type":"array","items":`, 6000) + `"int"` + strings.Repeat("}", 6000)
	wantReject(t, "NewReader/deeply-nested-header-schema", func() error {
		r, err := NewReader(bytes.NewReader(ocfWith(deepSchema, "null", 1, nil)))
		if r != nil {
			r.Close()
		}
		return err
	})

	// A header whose metadata map declares a huge entry count must reject when
	// the map is decoded (the OCF metadata map shares the core map-block bound),
	// not loop/allocate per the claimed count. Bound: the map decode's block
	// bound + ocfMetadataSafetyLimit.
	var hugeMetaCount []byte
	hugeMetaCount = append(hugeMetaCount, 'O', 'b', 'j', 1)
	hugeMetaCount = append(hugeMetaCount, binary.AppendVarint(nil, 1<<40)...) // metadata entry count = 2^40
	hugeMetaCount = append(hugeMetaCount, 0x02, 0x00)                         // a couple trailing bytes (short buffer)
	wantReject(t, "NewReader/huge-metadata-entry-count", func() error {
		r, err := NewReader(bytes.NewReader(hugeMetaCount))
		if r != nil {
			r.Close()
		}
		return err
	})

	// A header schema whose named types are REFERENCED more than once is a
	// DAG, not a tree: both spellings bind to one node, so any walk that
	// re-descends per reference does 2^depth work on a header of a couple of
	// kilobytes. This is the entry point where the schema comes from the INPUT
	// rather than from the caller, which is what sets the class's severity.
	//
	// It needs no nesting at all — the second form declares every level as a
	// sibling field wired by forward reference, so the header's JSON is four
	// deep whatever the fan-out — and that is why the nesting pre-scan above
	// is not the bound for this shape. The bound is that each node is walked
	// once. Both forms must be ACCEPTED, promptly: they are legal schemas.
	for _, form := range []struct{ name, schema string }{
		{"nested", dagRefHeaderNested(26)},
		{"flat-forward-ref", dagRefHeaderFlat(26)},
	} {
		wantTerminate(t, "NewReader/shared-node-header-schema/"+form.name, func() error {
			r, err := NewReader(bytes.NewReader(ocfWith(form.schema, "null", 0, nil)))
			if r != nil {
				r.Close()
			}
			return err
		})
	}
	// The same DAG with the WIDTH axis turned up. Depth alone is only half of
	// what a walk over this graph costs: a node is recomputed once per path
	// that reaches it, and each recomputation iterates that node's OWN field
	// list, so the cost is a product of two magnitudes the header's author
	// chooses independently. Holding the second at two — which every depth
	// cell above does, because two is what makes the path count grow — leaves
	// the product untested. Here the chain is cyclic (so nothing memoizes) and
	// the record every path ends at is wide, which is the combination that
	// makes the most-revisited node also the most expensive one.
	wantTerminate(t, "NewReader/wide-cyclic-header-schema", func() error {
		r, err := NewReader(bytes.NewReader(ocfWith(dagWideCyclicHeader(16, 8000), "null", 0, nil)))
		if r != nil {
			r.Close()
		}
		return err
	})
}

// dagWideCyclicHeader builds the header form of the width shape: a fan-2 chain
// of `levels` records whose deepest member references the shallowest (one
// strongly-connected component, so no result is memoizable) and carries `width`
// zero-minimum filler fields. Mirrors dagWideSCC in the avro package, which
// this package cannot import.
func dagWideCyclicHeader(levels, width int) string {
	var wide strings.Builder
	wide.WriteString(`{"type":"record","name":"W","fields":[{"name":"back","type":"L0"}`)
	for k := range width {
		fmt.Fprintf(&wide, `,{"name":"p%d","type":"null"}`, k)
	}
	wide.WriteString(`]}`)
	inner := wide.String()
	for i := levels - 1; i >= 0; i-- {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "W"
		}
		inner = fmt.Sprintf(`{"type":"record","name":"L%d","fields":[{"name":"f0","type":%s},{"name":"f1","type":"%s"}]}`,
			i, inner, next)
	}
	return `{"type":"array","items":` + inner + `}`
}

// dagRefHeaderNested builds an array-of-record header schema where every level
// declares the next inline and then references it a second time by name, so the
// two bind to one node.
func dagRefHeaderNested(levels int) string {
	inner := `"int"`
	for i := levels - 1; i >= 0; i-- {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "int"
		}
		inner = fmt.Sprintf(`{"type":"record","name":"L%d","fields":[{"name":"f0","type":%s},{"name":"f1","type":"%s"}]}`,
			i, inner, next)
	}
	return `{"type":"array","items":` + inner + `}`
}

// dagRefHeaderFlat expresses the same type graph with a JSON nesting depth of
// four regardless of levels, wiring the levels by forward reference.
func dagRefHeaderFlat(levels int) string {
	var b strings.Builder
	b.WriteString(`{"type":"record","name":"Root","fields":[{"name":"z","type":{"type":"array","items":"L0"}}`)
	for i := range levels {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "int"
		}
		fmt.Fprintf(&b, `,{"name":"d%d","type":{"type":"record","name":"L%d","fields":[{"name":"f0","type":"%s"},{"name":"f1","type":"%s"}]}}`,
			i, i, next, next)
	}
	b.WriteString(`]}`)
	return b.String()
}

//////////////////////////////////////////////////////////////////////////////
// C5 — ERROR-MESSAGE ECHO (read + write directions)
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_OCF_C5_ErrorEcho(t *testing.T) {
	// Read side: an unknown codec name from a hostile header is echoed into the
	// resolveCodec error; truncForError (the ocf-package copy) bounds it.
	// Extreme: TestRegression_OCFUnknownCodecErrorBounded.
	hugeCodec := strings.Repeat("z", 1<<20)
	wantBoundedErr(t, "NewReader/unknown-megabyte-codec-name", func() error {
		r, err := NewReader(bytes.NewReader(ocfWith(`"null"`, hugeCodec, 1, nil)))
		if r != nil {
			r.Close()
		}
		return err
	})

	// Write side: a caller-supplied WithMetadata key over the cap is echoed into
	// the NewWriter error; the same truncForError bounds it. A WithMetadata key
	// is wire-equivalent user input, so the write direction needs the bound too.
	// Extreme: TestRegression_OCFMetadataKeyErrorBounded.
	s := avro.MustParse(`"long"`)
	hugeKey := strings.Repeat("k", 2<<20) // > ocfMetadataSafetyLimit (1 MiB)
	wantBoundedErr(t, "NewWriter/over-cap-metadata-key", func() error {
		var buf bytes.Buffer
		w, err := NewWriter(&buf, s, WithMetadata(map[string][]byte{hugeKey: {1}}))
		if w != nil {
			w.Close()
		}
		return err
	})
}
