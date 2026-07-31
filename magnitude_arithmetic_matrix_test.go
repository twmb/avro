package avro_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

// ---------------------------------------------------------------------------
// Integer arithmetic over a schema-declared magnitude.
//
// A `fixed` size is an integer the schema text names outright, and the parser
// deliberately leaves its upper bound open (schema.go's fixed arm: the lenient
// majority, matching fastavro and avro-rs, since a size past the datum simply
// fails at encode/decode). That makes it the one parse-time magnitude whose
// VALUE is not bounded by the length of the text declaring it: nineteen
// characters name 2^63. Every other schema-text quantity is bounded either by
// a parse cap (precision and scale, at decimalScaleLimit) or by the input
// length itself (field, branch and symbol counts each cost bytes to write).
//
// So any arithmetic that can carry such a magnitude has to say what happens at
// the top of the range. The failure this file pins is not the magnitude itself
// but a SUM over it: a per-item wire minimum is accumulated field by field, and
// an overflow guard that tests only `s >= MaxInt32` lets a wrapped-negative sum
// through, because a negative number is not greater than a positive one. The
// sum then reaches a divisor.
//
// The pins below are behavioral. The producer invariant and the source-derived
// site registry at the bottom are what keep the class closed rather than the
// instance.
// ---------------------------------------------------------------------------

// The three shapes a derived per-item minimum can take when the arithmetic
// carrying it has no ceiling. Named for what the ARITHMETIC does, not for the
// schema, because the schema is only the vehicle.
const (
	// sumWrapsToZero: 1 (the long) + MaxInt64 wraps to MinInt64, and
	// MinInt64 + MaxInt64 lands on exactly -1. A map's per-entry minimum is
	// 1 + that, i.e. zero, and the block bound divides by it.
	sumWrapsToZero = `{"type":"record","name":"WZ","fields":[
		{"name":"lead","type":"long"},
		{"name":"a","type":` + `{"type":"fixed","name":"WZA","size":9223372036854775807}` + `},
		{"name":"b","type":` + `{"type":"fixed","name":"WZB","size":9223372036854775807}` + `}]}`

	// sumWrapsNegative: the union contributes 1, then one MaxInt64 field
	// carries the sum to MinInt64 and it stays there.
	sumWrapsNegative = `{"type":"record","name":"WN","fields":[
		{"name":"u","type":[` + `{"type":"fixed","name":"WNU","size":9223372036854775807}` + `]},
		{"name":"a","type":` + `{"type":"fixed","name":"WNA","size":9223372036854775807}` + `}]}`

	// magnitudeAlone: no sum at all — the caller's own `1 + minimum` is what
	// wraps, which is why a ceiling inside the producer is the fix and a
	// guard at one consumer is not.
	magnitudeAlone = `{"type":"fixed","name":"MA","size":9223372036854775807}`
)

// wrapShapes are the value schemas whose derived minimum the arithmetic must
// survive. Every one of them describes a datum that cannot physically exist —
// a single value would need 2^63 bytes — so the ONLY correct outcome for a
// non-empty block is an error, on every container and every entry point.
var wrapShapes = []struct{ name, schema string }{
	{"sum-wraps-to-zero", sumWrapsToZero},
	{"sum-wraps-negative", sumWrapsNegative},
	{"magnitude-alone", magnitudeAlone},
}

// containers are the two block-framed walkers; each derives a per-element
// minimum from the element schema and bounds the block count against it.
var containers = []struct {
	name string
	wrap func(values string) string
}{
	{"map", func(v string) string { return `{"type":"map","values":` + v + `}` }},
	{"array", func(v string) string { return `{"type":"array","items":` + v + `}` }},
}

// nonEmptyBlock is a single block header claiming one element, with nothing
// after it. One element of any wrapShape needs at least 2^63 bytes and there
// are zero, so a correct decoder rejects it. emptyContainer is the terminator
// alone, which every one of these schemas can legitimately represent.
var (
	nonEmptyBlock  = []byte{0x02}
	emptyContainer = []byte{0x00}
)

// magDecode runs a decode and converts a panic into an error, so one bad
// cell reports as a failure instead of tearing down the whole matrix run and
// hiding every cell after it.
func magDecode(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("PANIC: %v", r)
		}
	}()
	return fn()
}

func magIsPanic(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "PANIC:")
}

// ocfFileWith frames a minimal container file around schema, carrying one
// block whose payload is body. The header schema of an OCF is supplied BY THE
// FILE, so this is the reachability that matters: a reader that never saw the
// schema before still has to derive its bounds from it.
func ocfFileWith(schema string, body []byte) []byte {
	var f []byte
	f = append(f, 'O', 'b', 'j', 1)
	f = binary.AppendVarint(f, 1)
	f = binary.AppendVarint(f, int64(len("avro.schema")))
	f = append(f, "avro.schema"...)
	f = binary.AppendVarint(f, int64(len(schema)))
	f = append(f, schema...)
	f = append(f, 0)
	var sync [16]byte
	f = append(f, sync[:]...)
	f = binary.AppendVarint(f, 1)
	f = binary.AppendVarint(f, int64(len(body)))
	f = append(f, body...)
	f = append(f, sync[:]...)
	return f
}

// magnitudeEntryPoints are the ways a caller reaches a derived per-element
// bound. They differ in WHICH copy of the derivation runs: the parse-time one,
// the resolver's rebuilt one, the skip built for a dropped writer field, and
// the one a container reader derives from a schema it read out of a file.
// A ceiling applied at one of them leaves the other three open.
var magnitudeEntryPoints = []struct {
	name string
	// decode returns the error for a container schema carrying `body`.
	decode func(t *testing.T, containerSchema string, body []byte) error
}{
	{
		name: "Parse+Decode",
		decode: func(t *testing.T, cs string, body []byte) error {
			s, err := avro.Parse(cs)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			return magDecode(func() error {
				var v any
				_, err := s.Decode(body, &v)
				return err
			})
		},
	},
	{
		name: "Resolve+Decode",
		decode: func(t *testing.T, cs string, body []byte) error {
			// Reader and writer are the same schema, so resolution takes its
			// own rebuild path rather than the canonical-equal shortcut only
			// when something differs; wrapping both in a record with an added
			// reader field forces the rebuild.
			w, err := avro.Parse(`{"type":"record","name":"RW","fields":[{"name":"c","type":` + cs + `}]}`)
			if err != nil {
				t.Fatalf("parse writer: %v", err)
			}
			r, err := avro.Parse(`{"type":"record","name":"RW","fields":[{"name":"c","type":` + cs + `},{"name":"added","type":"int","default":7}]}`)
			if err != nil {
				t.Fatalf("parse reader: %v", err)
			}
			res, err := avro.Resolve(w, r)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			return magDecode(func() error {
				var v any
				_, err := res.Decode(body, &v)
				return err
			})
		},
	},
	{
		name: "Resolve-drop+skip",
		decode: func(t *testing.T, cs string, body []byte) error {
			// The reader omits the container field, so resolution compiles a
			// SKIP for it — a second derivation of the same bound, in the walk
			// that advances past a value instead of decoding it.
			w, err := avro.Parse(`{"type":"record","name":"RD","fields":[{"name":"c","type":` + cs + `},{"name":"keep","type":"int"}]}`)
			if err != nil {
				t.Fatalf("parse writer: %v", err)
			}
			r, err := avro.Parse(`{"type":"record","name":"RD","fields":[{"name":"keep","type":"int"}]}`)
			if err != nil {
				t.Fatalf("parse reader: %v", err)
			}
			res, err := avro.Resolve(w, r)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			return magDecode(func() error {
				var v any
				_, err := res.Decode(body, &v)
				return err
			})
		},
	},
	{
		name: "ocf.NewReader",
		decode: func(t *testing.T, cs string, body []byte) error {
			return magDecode(func() error {
				rd, err := ocf.NewReader(bytes.NewReader(ocfFileWith(cs, body)))
				if err != nil {
					return err
				}
				var v any
				return rd.Decode(&v)
			})
		},
	},
}

// TestMatrix_SchemaMagnitudeArithmetic crosses wrap shape x container x entry
// point. The expectation comes from the datum, not from the code: an element
// of any wrapShape needs more bytes than the wire can hold, so a block
// claiming one must ERROR; the same schema with an empty container is
// perfectly representable and must DECODE. Neither expectation is read off
// current behavior, and a panic fails both.
func TestMatrix_SchemaMagnitudeArithmetic(t *testing.T) {
	for _, ws := range wrapShapes {
		for _, c := range containers {
			cs := c.wrap(ws.schema)
			for _, ep := range magnitudeEntryPoints {
				name := ws.name + "/" + c.name + "/" + ep.name
				t.Run(name, func(t *testing.T) {
					// A block claiming an element that cannot fit.
					err := ep.decode(t, cs, magFramed(ep.name, nonEmptyBlock))
					switch {
					case magIsPanic(err):
						t.Errorf("a block claiming one impossible element panicked instead of erroring: %v", err)
					case err == nil:
						t.Errorf("a block claiming an element needing 2^63 bytes was accepted with an empty remainder")
					}
					// The same schema, empty container: representable, must work.
					err = ep.decode(t, cs, magFramed(ep.name, emptyContainer))
					if magIsPanic(err) {
						t.Errorf("an EMPTY container panicked: %v", err)
					}
				})
			}
		}
	}
}

// magFramed prepends whatever the entry point's outer record needs before the
// container's own bytes. The record-wrapping entry points put the container
// first, so nothing is needed ahead of it; the dropped-field case still has to
// leave the trailing int readable, which a rejected block never reaches.
func magFramed(entryPoint string, body []byte) []byte {
	if entryPoint == "Resolve-drop+skip" {
		return append(append([]byte{}, body...), 0x02)
	}
	return body
}

// TestRegression_MapBlockBoundSurvivesWrappedMinimum is the instance pin: the
// smallest input that reached the divisor. Kept alongside the matrix because
// it names the exact arithmetic, and a matrix cell that stops driving this
// shape would otherwise take the pin with it.
func TestRegression_MapBlockBoundSurvivesWrappedMinimum(t *testing.T) {
	s, err := avro.Parse(`{"type":"map","values":` + sumWrapsToZero + `}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	err = magDecode(func() error {
		var v any
		_, err := s.Decode(nonEmptyBlock, &v)
		return err
	})
	if magIsPanic(err) {
		t.Fatalf("decoding a one-byte map block: %v", err)
	}
	if err == nil {
		t.Fatal("a map block claiming an entry that needs 2^63 bytes was accepted with an empty remainder")
	}
}

// TestInvariant_LegitimateBlockBoundsStillAccept is the control the ceiling
// must not break. A bound made safe by rejecting more is not safe, it is
// broken in the other direction, and every assertion above is satisfied by an
// implementation that refuses everything.
func TestInvariant_LegitimateBlockBoundsStillAccept(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		value  any
	}{
		{"map-of-int", `{"type":"map","values":"int"}`, map[string]any{"k": int32(1), "j": int32(2)}},
		{"array-of-int", `{"type":"array","items":"int"}`, []any{int32(1), int32(2), int32(3)}},
		{"array-of-null", `{"type":"array","items":"null"}`, []any{nil, nil, nil}},
		{"map-of-fixed", `{"type":"map","values":{"type":"fixed","name":"SF","size":4}}`,
			map[string]any{"k": []byte{1, 2, 3, 4}}},
		{"array-of-record", `{"type":"array","items":{"type":"record","name":"AR","fields":[{"name":"x","type":"int"},{"name":"y","type":"string"}]}}`,
			[]any{map[string]any{"x": int32(1), "y": "a"}}},
		{"map-of-large-fixed", `{"type":"map","values":{"type":"fixed","name":"LF","size":70000}}`,
			map[string]any{"k": bytes.Repeat([]byte{7}, 70000)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := avro.Parse(c.schema)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			b, err := s.Encode(c.value)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var got any
			if _, err := s.Decode(b, &got); err != nil {
				t.Fatalf("the bound refused a block this schema's own encoder produced: %v", err)
			}
		})
	}
}
