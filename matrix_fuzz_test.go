package avro_test

import (
	"testing"

	"github.com/twmb/avro"
)

// FuzzMatrixCore bridges the curated matrix and the fuzzer: the fuzz input
// selects a (fragment, context, value) cell and supplies wire mutations, so
// CI fuzz time explores cell combinations and hostile-byte interactions the
// curated sweeps don't enumerate. The relational core invariants must hold
// for every selected cell; mutated wires must never panic the decoder.
func FuzzMatrixCore(f *testing.F) {
	frags := matFrags()
	ctxs := matCtxs()
	f.Add(uint8(0), uint8(0), uint8(0), []byte{})
	f.Add(uint8(3), uint8(8), uint8(1), []byte{0x00, 0xFF})
	f.Add(uint8(10), uint8(4), uint8(0), []byte{0x80, 0x80, 0x80})
	f.Fuzz(func(t *testing.T, fi, ci, vi uint8, mut []byte) {
		fr := frags[int(fi)%len(frags)]
		cx := ctxs[int(ci)%len(ctxs)]
		if cx.skip != nil && cx.skip(fr.kind) {
			return
		}
		u := &uniq{}
		schemaJSON := cx.schema(fr.schema(u), fr.kind, u)
		v := fr.values[int(vi)%len(fr.values)]
		vin := cx.wrap(v)

		s, err := avro.Parse(schemaJSON)
		if err != nil {
			t.Fatalf("matrix schema failed to parse: %v\n%s", err, schemaJSON)
		}
		w1, err := s.AppendEncode(nil, vin)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var a1 any
		if _, err := s.Decode(w1, &a1); err != nil {
			t.Fatalf("decode: %v", err)
		}
		w2, err := s.AppendEncode(nil, a1)
		if err != nil || string(w2) != string(w1) {
			t.Fatalf("re-encode unstable: err=%v\n w1=%x\n w2=%x\nschema: %s", err, w1, w2, schemaJSON)
		}

		// Fuzz-driven hostile mutation: XOR the fuzzer's bytes over the
		// valid wire and decode — errors are fine, panics are findings
		// (the fuzz engine catches them itself).
		if len(mut) > 0 && len(w1) > 0 {
			hostile := append([]byte{}, w1...)
			for i, b := range mut {
				hostile[i%len(hostile)] ^= b
			}
			var sink any
			_, _ = s.Decode(hostile, &sink)
			_ = s.DecodeJSON(hostile, &sink)
		}
	})
}
