package avro_test

import (
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// The CustomType-resurrected-logical parity battery — the WHOLE matrix in one
// test, so this class can't dribble a cell at a time.
//
// Background. A logical type placed on an Avro underlying it is not spec-valid
// for (uuid on bytes, duration on a size!=12 fixed, a date/timestamp on string,
// big-decimal on fixed, …) is SOFT-DROPPED by validateLogical, leaving the bare
// underlying — matching Java/fastavro/hamba. A registered CustomType whose
// LogicalType matches RESURRECTS the dropped logical (schema.go buildComplex).
// The contract: a resurrected wrong-kind / wrong-size logical must fall through
// to the RAW size/kind-checked path on EVERY axis, applying logicalUnderlyingAccept
// (validateLogical's own predicate) uniformly. Prior rounds closed this one cell
// per round — JSON typed decode (25a6e66), binary primitive encode (8236008),
// fixed-encode wrong size (ff86592). This battery drives every cell at once:
//
//	  logical × {wrong-kind, wrong-size} × {encode, decode}
//	         × {binary, JSON} × {natural, resolved deser}
//	         × {decode-into-any, decode-into-logical-typed-target}
//	         × {wildcard custom, AvroType-match custom, AvroType-mismatch custom}
//
// Oracle (independent of the code under test): the PLAIN schema — the same
// schema string parsed with NO CustomType — which soft-drops the logical to its
// bare underlying. Invariant: for EVERY resurrecting CustomType shape, the
// custom schema must be byte/value/accept-identical to the plain schema on every
// axis, and every wire it emits must round-trip through its own (natural AND
// resolved) reader. A logical serializer or deserializer wrongly applied to the
// wrong kind/size shows up as a wire-byte, accept, or value divergence from the
// plain schema, or a wire its own reader can't read.
//
// The three resurrecting custom shapes matter independently:
//   - wildcard  ({LogicalType}):        resurrects AND suppresses (any kind).
//   - AvroType-match ({LogicalType,kind}): resurrects AND suppresses (kind ==).
//   - AvroType-mismatch ({LogicalType,"boolean"}): resurrects (LogicalType-keyed)
//     but does NOT suppress (kind !=) — the decode-side gap, where the bare
//     hasMatchingCustomType suppression doesn't fire but logicalUnderlyingAccept
//     must still keep the codec raw.
//
// Non-vacuity: each cell feeds BOTH a raw underlying value AND a "canary" value
// the wrongly-applied logical serializer would accept (a UUID string, an
// avro.Duration, a time.Time, a *big.Rat). A neutered gate (logical ser/deser
// applied kind/size-blind) makes the custom schema diverge from the plain one on
// the canary (encode) or on the typed target (decode) — verified by reverting any
// single fix in schema.go / json_decode.go.

func TestRegression_CustomResurrectedLogicalFullMatrixParity(t *testing.T) {
	for _, c := range resurrectionCells() {
		t.Run(c.name, func(t *testing.T) {
			runResurrectionCell(t, c)
		})
	}
}

// resurrectionCell is one (logical, underlying) placement the logical is NOT
// spec-valid for — a soft-droppable cell a CustomType can resurrect.
type resurrectionCell struct {
	name    string
	logical string
	kind    string // bytes/int/long/string/fixed
	size    int    // fixed size; 0 otherwise
	schema  string
	// inputs is raw underlying values plus logical-shaped canaries. The plain
	// (soft-dropped) schema is the oracle for how each is encoded/rejected.
	inputs []any
	// targets are the logical-typed decode targets (besides *any) the wrongly-
	// applied logical deserializer would transform into. Each returns a fresh
	// pointer target.
	targets []func() any
}

func resurrectionCells() []resurrectionCell {
	// validOn enumerates the spec-valid placements per logical; every OTHER
	// placement in the probe set below soft-drops and is therefore a cell.
	// decimal hard-errors on a fixed underlying without precision (handled
	// inline by validateLogical, never soft-dropped→resurrected), so it is
	// probed on the non-bytes/fixed primitives only.
	type lspec struct {
		name    string
		validOn func(kind string, size int) bool
		probe   []struct {
			kind string
			size int
		}
		inputs  []any
		targets []func() any
	}

	tm := time.Date(2023, 11, 14, 22, 13, 20, 0, time.UTC)
	uuidStr := "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	uuid16 := [16]byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}
	dur := avro.Duration{Months: 1, Days: 2, Milliseconds: 3}
	rat := big.NewRat(12345, 100)

	timeTarget := func() any { return new(time.Time) }
	durTarget := func() any { return new(time.Duration) }
	ratTarget := func() any { return new(big.Rat) }
	strTarget := func() any { return new(string) }
	u16Target := func() any { return new([16]byte) }
	avroDurTarget := func() any { return new(avro.Duration) }

	// The full probe grid of underlyings. Each logical takes the subset that
	// soft-drops (i.e. !validOn), which is exactly the wrong-kind + wrong-size
	// matrix.
	grid := []struct {
		kind string
		size int
	}{
		{"int", 0}, {"long", 0}, {"string", 0}, {"bytes", 0},
		{"fixed", 8}, {"fixed", 12}, {"fixed", 16}, {"fixed", 20},
	}

	timeInputs := []any{tm}
	timeTargets := []func() any{timeTarget}
	durInputsLong := []func() any{timeTarget, durTarget} // time-millis/micros also map a duration

	specs := []lspec{
		{
			name:    "uuid",
			validOn: func(k string, s int) bool { return k == "string" || (k == "fixed" && s == 16) },
			inputs:  []any{uuidStr, uuid16},
			targets: []func() any{strTarget, u16Target},
		},
		{
			name:    "duration",
			validOn: func(k string, s int) bool { return k == "fixed" && s == 12 },
			inputs:  []any{dur},
			targets: []func() any{avroDurTarget},
		},
		{
			name:    "big-decimal",
			validOn: func(k string, s int) bool { return k == "bytes" },
			inputs:  []any{rat},
			targets: []func() any{ratTarget},
		},
		{
			// decimal soft-drops only off bytes/fixed; on fixed without
			// precision it hard-errors, so it is never resurrected there.
			name:    "decimal",
			validOn: func(k string, s int) bool { return k == "bytes" || k == "fixed" },
			inputs:  []any{rat},
			targets: []func() any{ratTarget},
		},
		{"date", func(k string, s int) bool { return k == "int" }, nil, timeInputs, timeTargets},
		{"time-millis", func(k string, s int) bool { return k == "int" }, nil, timeInputs, durInputsLong},
		{"time-micros", func(k string, s int) bool { return k == "long" }, nil, timeInputs, durInputsLong},
		{"timestamp-millis", func(k string, s int) bool { return k == "long" }, nil, timeInputs, timeTargets},
		{"timestamp-micros", func(k string, s int) bool { return k == "long" }, nil, timeInputs, timeTargets},
		{"timestamp-nanos", func(k string, s int) bool { return k == "long" }, nil, timeInputs, timeTargets},
		{"local-timestamp-millis", func(k string, s int) bool { return k == "long" }, nil, timeInputs, timeTargets},
		{"local-timestamp-micros", func(k string, s int) bool { return k == "long" }, nil, timeInputs, timeTargets},
		{"local-timestamp-nanos", func(k string, s int) bool { return k == "long" }, nil, timeInputs, timeTargets},
	}

	var cells []resurrectionCell
	for _, sp := range specs {
		probe := sp.probe
		if probe == nil {
			probe = grid
		}
		for _, g := range probe {
			if sp.validOn(g.kind, g.size) {
				continue // spec-valid placement; not a resurrection cell
			}
			// decimal on a fixed underlying without precision hard-errors at
			// Parse — never a soft-drop. Skip (it is not resurrectable).
			if sp.name == "decimal" && g.kind == "fixed" {
				continue
			}
			cells = append(cells, makeCell(sp.name, g.kind, g.size, sp.inputs, sp.targets))
		}
	}
	return cells
}

func makeCell(logical, kind string, size int, inputs []any, targets []func() any) resurrectionCell {
	name := fmt.Sprintf("%s_on_%s", logical, kind)
	var schema string
	if kind == "fixed" {
		name = fmt.Sprintf("%s_on_fixed%d", logical, size)
		nm := "F_" + strings.ReplaceAll(logical, "-", "_") + fmt.Sprintf("_%d", size)
		schema = fmt.Sprintf(`{"type":"fixed","name":%q,"size":%d,"logicalType":%q}`, nm, size, logical)
	} else {
		schema = fmt.Sprintf(`{"type":%q,"logicalType":%q}`, kind, logical)
	}
	// Add a raw underlying value the BARE type accepts, so every cell encodes at
	// least one input through the plain (soft-dropped) path.
	raw := rawUnderlyingValue(kind, size)
	return resurrectionCell{
		name:    name,
		logical: logical,
		kind:    kind,
		size:    size,
		schema:  schema,
		inputs:  append([]any{raw}, inputs...),
		targets: targets,
	}
}

func rawUnderlyingValue(kind string, size int) any {
	switch kind {
	case "int":
		return int32(7)
	case "long":
		return int64(7)
	case "string":
		return "raw-underlying-text"
	case "bytes":
		return []byte{1, 2, 3, 4, 5, 6, 7, 8}
	case "fixed":
		b := make([]byte, size)
		for i := range b {
			b[i] = byte(i + 1)
		}
		return b
	}
	panic("unknown kind " + kind)
}

func runResurrectionCell(t *testing.T, c resurrectionCell) {
	plain := avro.MustParse(c.schema)
	plainR := mustIdentityResolve(t, plain)

	// All three resurrecting CustomType shapes must reduce to the plain schema.
	shapes := []struct {
		name string
		opt  avro.SchemaOpt
	}{
		{"wildcard", avro.CustomType{LogicalType: c.logical}},
		{"avrotype-match", avro.CustomType{LogicalType: c.logical, AvroType: c.kind}},
		{"avrotype-mismatch", avro.CustomType{LogicalType: c.logical, AvroType: "boolean"}},
	}

	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			cs := avro.MustParse(c.schema, sh.opt)
			csR := mustIdentityResolve(t, cs)

			// Targets to decode into: *any plus every logical-typed target.
			targets := append([]func() any{func() any { return new(any) }}, c.targets...)

			for _, in := range c.inputs {
				// --- ENCODE parity: byte-identical wire (or identical reject). ---
				pbin, peb := plain.Encode(in)
				cbin, ceb := cs.Encode(in)
				if got, want := encResult(cbin, ceb), encResult(pbin, peb); got != want {
					t.Errorf("binary encode %T: custom=%s plain=%s — logical serializer applied to wrong kind/size", in, got, want)
				}
				pjsn, pej := plain.EncodeJSON(in)
				cjsn, cej := cs.EncodeJSON(in)
				if got, want := encResult(cjsn, cej), encResult(pjsn, pej); got != want {
					t.Errorf("JSON encode %T: custom=%q plain=%q — logical serializer applied to wrong kind/size", in, got, want)
				}

				// --- DECODE parity over the schema's own wire, into every
				// target, on binary+JSON x natural+resolved. ---
				if peb == nil && ceb == nil {
					for ti, mk := range targets {
						if got, want := decBin(cs, cbin, mk), decBin(plain, pbin, mk); got != want {
							t.Errorf("binary decode natural %T target#%d: custom=%s plain=%s — logical deser applied to wrong kind/size", in, ti, got, want)
						}
						if got, want := decBin(csR, cbin, mk), decBin(plainR, pbin, mk); got != want {
							t.Errorf("binary decode RESOLVED %T target#%d: custom=%s plain=%s — resolved deser diverged from plain", in, ti, got, want)
						}
					}
					// Self-readability: cs's own binary wire reads back via cs.
					assertSelfReadableBin(t, cs, csR, cbin, in)
				}
				if pej == nil && cej == nil {
					for ti, mk := range targets {
						if got, want := decJSON(cs, cjsn, mk), decJSON(plain, pjsn, mk); got != want {
							t.Errorf("JSON decode natural %T target#%d: custom=%s plain=%s — logical deser applied to wrong kind/size", in, ti, got, want)
						}
						if got, want := decJSON(csR, cjsn, mk), decJSON(plainR, pjsn, mk); got != want {
							t.Errorf("JSON decode RESOLVED %T target#%d: custom=%s plain=%s — resolved deser diverged from plain", in, ti, got, want)
						}
					}
					assertSelfReadableJSON(t, cs, csR, cjsn, in)
				}
			}

			// --- RESOLVED via TYPE PROMOTION. When the reader kind is a
			// promotion target (writer int→long, string→bytes, bytes→string),
			// doResolve wraps the widening deser with promotionDeserForLogical to
			// re-apply the reader's logical. A resurrected wrong-kind logical
			// must NOT be re-applied there either: the promoted decode must equal
			// the plain (soft-dropped) reader's promoted decode. This is the
			// resolved-deser axis the identity resolve above does not reach (it
			// hits the promotion branch, not maybeWrapResolvedNode). ---
			if src, ok := promotionSourceFor(c.kind); ok {
				w := avro.MustParse(src.schema)
				wire, werr := w.Encode(src.value)
				plainProm, e1 := avro.Resolve(w, plain)
				csProm, e2 := avro.Resolve(w, cs)
				if werr == nil && e1 == nil && e2 == nil {
					targets := append([]func() any{func() any { return new(any) }}, c.targets...)
					for ti, mk := range targets {
						if got, want := decBin(csProm, wire, mk), decBin(plainProm, wire, mk); got != want {
							t.Errorf("binary decode RESOLVED-PROMOTION %s->%s target#%d: custom=%s plain=%s — promotion re-applied a wrong-kind logical", src.kind, c.kind, ti, got, want)
						}
						if got, want := decJSON(csProm, mustEncodeJSON(w, src.value), mk), decJSON(plainProm, mustEncodeJSON(w, src.value), mk); got != want {
							t.Errorf("JSON decode RESOLVED-PROMOTION %s->%s target#%d: custom=%s plain=%s — promotion re-applied a wrong-kind logical", src.kind, c.kind, ti, got, want)
						}
					}
				}
			}
		})
	}
}

// promotionSourceFor returns the bare writer type that PROMOTES to readerKind,
// per the Avro promotion set (int→long, string→bytes, bytes→string), and a
// value to encode through it. Returns ok=false for reader kinds nothing promotes
// into (int, fixed). float/double readers carry no logical, so are skipped.
func promotionSourceFor(readerKind string) (struct {
	kind, schema string
	value        any
}, bool) {
	switch readerKind {
	case "long":
		return struct {
			kind, schema string
			value        any
		}{"int", `"int"`, int32(5)}, true
	case "bytes":
		return struct {
			kind, schema string
			value        any
		}{"string", `"string"`, "promo-text"}, true
	case "string":
		return struct {
			kind, schema string
			value        any
		}{"bytes", `"bytes"`, []byte{9, 8, 7}}, true
	}
	return struct {
		kind, schema string
		value        any
	}{}, false
}

func mustEncodeJSON(s *avro.Schema, v any) []byte {
	b, err := s.EncodeJSON(v)
	if err != nil {
		return nil
	}
	return b
}

func mustIdentityResolve(t *testing.T, s *avro.Schema) *avro.Schema {
	t.Helper()
	r, err := avro.Resolve(s, s)
	if err != nil {
		t.Fatalf("identity Resolve failed: %v", err)
	}
	return r
}

// encResult renders an encode outcome for comparison: "<rejected>" or the hex
// wire. Comparing renders asserts identical accept AND identical bytes.
func encResult(b []byte, err error) string {
	if err != nil {
		return "<rejected>"
	}
	return fmt.Sprintf("%x", b)
}

// decResult renders a decode outcome: "<rejected>" or the %#v of the decoded
// value. Error MESSAGES are not compared (plain and custom may word a reject
// differently); accept/reject and decoded value are.
func decResult(target any, err error) string {
	if err != nil {
		return "<rejected>"
	}
	return fmt.Sprintf("%#v", reflect.ValueOf(target).Elem().Interface())
}

func decBin(s *avro.Schema, wire []byte, mk func() any) string {
	tgt := mk()
	_, err := s.Decode(wire, tgt)
	return decResult(tgt, err)
}

func decJSON(s *avro.Schema, wire []byte, mk func() any) string {
	tgt := mk()
	err := s.DecodeJSON(wire, tgt)
	return decResult(tgt, err)
}

func assertSelfReadableBin(t *testing.T, cs, csR *avro.Schema, wire []byte, in any) {
	t.Helper()
	var v any
	if _, err := cs.Decode(wire, &v); err != nil {
		t.Errorf("custom binary wire (input %T) not self-readable: %v", in, err)
	}
	var v2 any
	if _, err := csR.Decode(wire, &v2); err != nil {
		t.Errorf("custom binary wire (input %T) not self-readable via RESOLVED reader: %v", in, err)
	}
}

func assertSelfReadableJSON(t *testing.T, cs, csR *avro.Schema, wire []byte, in any) {
	t.Helper()
	var v any
	if err := cs.DecodeJSON(wire, &v); err != nil {
		t.Errorf("custom JSON wire (input %T) not self-readable: %v", in, err)
	}
	var v2 any
	if err := csR.DecodeJSON(wire, &v2); err != nil {
		t.Errorf("custom JSON wire (input %T) not self-readable via RESOLVED reader: %v", in, err)
	}
}
