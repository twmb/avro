package avro_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Reader-grammar boundary matrices: hand-framed wire for the index and
// block-header productions, driven through every consuming path — natural
// decode, resolved decode, and the resolution SKIP path (a writer field the
// reader drops). The framing matrix (matrix_framing_test.go) covers the
// spec's legal container framings; these matrices cover the index VALUE
// space (branch-count boundary, negative, overlong, width-overflow) and the
// hostile block-header values (negative/lying byte sizes, count overflow,
// zero-byte-item caps) that no twmb writer produces.
//
// The invariant per cell: a loud error or a value-faithful accept — never a
// silent truncation, silent wrong value, or panic. Accept cells re-encode
// canonically. The skip path may be MORE lenient than the value path only
// where the discarded content does not affect framing (an enum's index is a
// self-contained varint), and that leniency must match the references:
// Java's ResolvingDecoder skips an enum via readEnum() = readInt() with no
// symbol check, and fastavro's skip_enum is a bare read_long() (compiled
// _read.pyx skip_enum; the pure-Python _read_py.py fallback is read_enum()
// = read_long() likewise) — neither validates discarded enum indices.
// Union indices DO affect framing (they select the branch skipper),
// so the skip path validates them exactly like the value path.
// TestDifferentialFastavroReaderGrammar executes the fastavro side of each
// calibrated claim.
// ---------------------------------------------------------------------------

// censusKeep is the trailing "keep" field value every skip cell asserts
// survived the skipped hostile field. zigzag(21) = 0x2A, one wire byte.
const censusKeep = int32(21)

// censusSkipWire frames a writer-record wire: the hostile payload for the
// dropped field, then the keep field.
func censusSkipWire(dropPayload []byte) []byte {
	wire := append([]byte{}, dropPayload...)
	return putZigzag(wire, int64(censusKeep))
}

// censusResolve builds Resolve(writer{drop,keep}, reader{keep}) for a given
// dropped-field schema.
func censusResolve(t *testing.T, dropSchema string) *avro.Schema {
	t.Helper()
	w := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"drop","type":` + dropSchema + `},
		{"name":"keep","type":"int"}]}`)
	r := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"keep","type":"int"}]}`)
	res, err := avro.Resolve(w, r)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	return res
}

// censusAssertSkip decodes a resolved skip wire and asserts the consistent-
// skip contract: no error, keep intact, nothing left over.
func censusAssertSkip(t *testing.T, res *avro.Schema, wire []byte) {
	t.Helper()
	var got map[string]any
	rest, err := res.Decode(wire, &got)
	if err != nil {
		t.Fatalf("skip decode: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("skip decode left %d bytes", len(rest))
	}
	if got["keep"] != censusKeep {
		t.Fatalf("keep after skip: %#v", got["keep"])
	}
}

// censusAssertSkipErr decodes a resolved skip wire and asserts a loud error
// containing wantErr.
func censusAssertSkipErr(t *testing.T, res *avro.Schema, wire []byte, wantErr string) {
	t.Helper()
	var got map[string]any
	_, err := res.Decode(wire, &got)
	if err == nil {
		t.Fatalf("skip decode accepted hostile wire (got %#v), want error containing %q", got, wantErr)
	}
	if !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("skip decode error %q does not contain %q", err, wantErr)
	}
}

// putVarintWidthOverflow returns a 5-byte varint whose final byte carries
// value bits beyond 32 (0x10 > 0x0f) — the width-overflow form readUvarint
// rejects ("uvarint overflows 32 bits"). Both int-typed productions (enum
// index, union index) read through readVarint, so the same bytes probe both.
func putVarintWidthOverflow() []byte {
	return []byte{0x80, 0x80, 0x80, 0x80, 0x10}
}

// TestMatrix_EnumIndexWireGrammar drives the enum-index production's value
// space through natural decode, resolved-identity decode, and the skip path.
//
// The value paths validate the index against the symbol table (deserEnum:
// idx in [0, len)). The SKIP path deliberately does not: a discarded enum's
// index is a self-contained varint that cannot affect framing, and neither
// reference validates it on skip (Java ResolvingDecoder skip → readEnum() =
// readInt(); fastavro skip_enum → read_long()) — so out-of-range and negative
// indices skip consistently. Width-overflow and truncated varints reject on
// EVERY path: they are varint-grammar errors, not value errors.
//
// fastavro calibration (executed in TestDifferentialFastavroReaderGrammar,
// observed 1.12.2): its VALUE path rejects out-of-range (symbols[idx]
// IndexError) but silently ACCEPTS a negative index via Python list
// wraparound (symbols[-1] is the last symbol) — an accidental leniency twmb
// does not copy (Java rejects; wraparound is silent wrong output,
// cross-impl rule 1).
func TestMatrix_EnumIndexWireGrammar(t *testing.T) {
	const enumSchema = `{"type":"enum","name":"E","symbols":["A","B","C"]}`
	s := avro.MustParse(enumSchema)
	resIdentity, err := avro.Resolve(s, avro.MustParse(enumSchema))
	if err != nil {
		t.Fatalf("Resolve identity: %v", err)
	}
	canonicalLast, err := s.AppendEncode(nil, "C")
	if err != nil {
		t.Fatalf("encode C: %v", err)
	}

	cells := []struct {
		name string
		wire []byte
		// want: expected symbol for accept cells; "" means reject.
		want    string
		wantErr string
		// skipConsistent: the skip path discards the index without
		// validating it, so the cell's reject is value-path-only.
		skipConsistent bool
	}{
		{name: "canonical-last-symbol", wire: putZigzag(nil, 2), want: "C"},
		{name: "index-eq-symbol-count", wire: putZigzag(nil, 3),
			wantErr: "enum index 3 out of range [0, 3)", skipConsistent: true},
		{name: "negative-index", wire: putZigzag(nil, -1),
			wantErr: "out of range", skipConsistent: true},
		{name: "overlong-varint-of-valid", wire: putZigzagOverlong(nil, 2), want: "C"},
		{name: "width-overflow-varint", wire: putVarintWidthOverflow(),
			wantErr: "overflows 32 bits"},
		{name: "truncated-varint", wire: []byte{0x80}, wantErr: "ShortBuffer"},
	}

	resSkip := censusResolve(t, enumSchema)
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			// Natural and resolved-identity value paths must agree.
			for _, path := range []struct {
				label string
				s     *avro.Schema
			}{{"natural", s}, {"resolved-identity", resIdentity}} {
				var got any
				_, err := path.s.Decode(c.wire, &got)
				if c.want != "" {
					if err != nil {
						t.Fatalf("%s decode: %v", path.label, err)
					}
					if got != c.want {
						t.Fatalf("%s decode: got %#v want %q", path.label, got, c.want)
					}
					re, err := path.s.AppendEncode(nil, got)
					if err != nil || !bytes.Equal(re, canonicalLast) {
						t.Fatalf("%s re-encode not canonical: err=%v re=%x want=%x", path.label, err, re, canonicalLast)
					}
					continue
				}
				if err == nil {
					t.Fatalf("%s decode accepted %x (got %#v), want error", path.label, c.wire, got)
				}
				if c.wantErr == "ShortBuffer" {
					var sbe *avro.ShortBufferError
					if !errors.As(err, &sbe) {
						t.Fatalf("%s decode error %q, want ShortBufferError", path.label, err)
					}
				} else if !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("%s decode error %q does not contain %q", path.label, err, c.wantErr)
				}
			}

			// Skip path.
			skipWire := censusSkipWire(c.wire)
			switch {
			case c.want != "" || c.skipConsistent:
				// Valid indices and value-only rejects both skip consistently.
				censusAssertSkip(t, resSkip, skipWire)
			default:
				// Varint-grammar errors stay loud through skip. The
				// "ShortBuffer" sentinel asserts the typed error on the value
				// paths; on the skip path match its rendered message.
				wantErr := c.wantErr
				if wantErr == "ShortBuffer" {
					wantErr = "short buffer"
				}
				censusAssertSkipErr(t, resSkip, skipWire, wantErr)
			}
		})
	}
}

// TestMatrix_UnionIndexWireGrammar drives the union-index production's value
// space through natural decode, resolved decode (writer union resolved
// against a wider reader union, so the resolved deserializer indexes the
// WRITER's branch table), and the skip path. Unlike enum indices, a union
// index selects the branch (de)serializer — it affects framing — so ALL
// three paths validate it identically (skipUnion carries the same
// [0, branches) guard as deserUnion; fastavro's skip_union likewise indexes
// writer_schema[index] and rejects out-of-range).
func TestMatrix_UnionIndexWireGrammar(t *testing.T) {
	const unionSchema = `["int","string","boolean"]`
	s := avro.MustParse(unionSchema)
	// Wider reader: every writer branch resolves, and the resolved decoder's
	// branch table has the writer's arity (3), putting the boundary at 3.
	resWider, err := avro.Resolve(s, avro.MustParse(`["int","string","boolean","long"]`))
	if err != nil {
		t.Fatalf("Resolve wider: %v", err)
	}

	boolPayload := []byte{0x01}
	strPayload := func() []byte {
		p := putZigzag(nil, 2)
		return append(p, "hi"...)
	}()

	canonicalBool, err := s.AppendEncode(nil, true)
	if err != nil {
		t.Fatalf("encode bool: %v", err)
	}
	canonicalStr, err := s.AppendEncode(nil, "hi")
	if err != nil {
		t.Fatalf("encode string: %v", err)
	}

	cells := []struct {
		name      string
		wire      []byte
		want      any    // non-nil for accept cells
		canonical []byte // expected re-encode for accept cells
		wantErr   string
	}{
		{name: "canonical-last-branch", wire: append(putZigzag(nil, 2), boolPayload...),
			want: true, canonical: canonicalBool},
		{name: "index-eq-branch-count", wire: putZigzag(nil, 3),
			wantErr: "union index 3 out of range [0, 3)"},
		{name: "negative-index", wire: putZigzag(nil, -1),
			wantErr: "out of range"},
		{name: "overlong-varint-of-valid", wire: append(putZigzagOverlong(nil, 1), strPayload...),
			want: "hi", canonical: canonicalStr},
		{name: "width-overflow-varint", wire: putVarintWidthOverflow(),
			wantErr: "overflows 32 bits"},
		{name: "truncated-varint", wire: []byte{0x80}, wantErr: ""},
	}

	resSkip := censusResolve(t, unionSchema)
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			for _, path := range []struct {
				label string
				s     *avro.Schema
			}{{"natural", s}, {"resolved-wider", resWider}} {
				var got any
				_, err := path.s.Decode(c.wire, &got)
				if c.want != nil {
					if err != nil {
						t.Fatalf("%s decode: %v", path.label, err)
					}
					if got != c.want {
						t.Fatalf("%s decode: got %#v want %#v", path.label, got, c.want)
					}
					if path.label == "natural" {
						re, err := path.s.AppendEncode(nil, got)
						if err != nil || !bytes.Equal(re, c.canonical) {
							t.Fatalf("re-encode not canonical: err=%v re=%x want=%x", err, re, c.canonical)
						}
					}
					continue
				}
				if err == nil {
					t.Fatalf("%s decode accepted %x (got %#v), want error", path.label, c.wire, got)
				}
				if c.wantErr != "" && !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("%s decode error %q does not contain %q", path.label, err, c.wantErr)
				}
			}

			// Skip path: union indices are validated identically (they select
			// the branch skipper), so verdicts mirror the value path.
			skipWire := censusSkipWire(c.wire)
			if c.want != nil {
				censusAssertSkip(t, resSkip, skipWire)
			} else {
				censusAssertSkipErr(t, resSkip, skipWire, c.wantErr)
			}
		})
	}
}

// TestMatrix_SkipHostileBlockFraming drives hostile array/map block-header
// values through the resolution SKIP path (and, where the natural path has
// no cell of its own, through natural decode too). The skip path has its own
// block walker (skipBlocks) with two arms the framing matrix's legal
// variants never stress: the validateByteSize guard on size-prefixed blocks
// (the skip walker jumps by the wire's byte size, so a negative or
// over-buffer size must reject loudly BEFORE the jump) and the shared
// count-vs-buffer / zero-byte-item bounds (checkArrayBlockBounds /
// checkMapBlockBounds, the same helpers the value path uses).
func TestMatrix_SkipHostileBlockFraming(t *testing.T) {
	minInt64 := int64(-1) << 63

	t.Run("array-of-string", func(t *testing.T) {
		res := censusResolve(t, `{"type":"array","items":"string"}`)
		natural := avro.MustParse(`{"type":"array","items":"string"}`)

		cells := []struct {
			name    string
			payload []byte
			wantErr string
			// alsoNatural: drive the bare payload through natural decode
			// too and require the same reject.
			alsoNatural bool
		}{
			{
				// MinInt64's negation is itself: readBlockHeader's double-
				// negative guard must reject before the count is used. The
				// negative-count grammar says a byte size follows, but the
				// reject fires first — the payload deliberately ends here.
				name:        "minint64-count",
				payload:     putZigzag(nil, minInt64),
				wantErr:     "invalid array block count",
				alsoNatural: true,
			},
			{
				// A positive count far beyond the remaining bytes must
				// reject via the buffer-relative bound (string items take
				// >=1 wire byte each), not iterate.
				name:    "count-over-buffer",
				payload: putZigzag(nil, 100000),
				wantErr: "exceeds remaining buffer",
			},
			{
				// Size-prefixed block with a negative byte size: the skip
				// walker would jump by it; validateByteSize rejects first.
				name:    "negative-bytesize",
				payload: putZigzag(putZigzag(nil, -2), -5),
				wantErr: "short buffer for array",
			},
			{
				// Size-prefixed block whose byte size exceeds the buffer.
				name:    "bytesize-over-buffer",
				payload: putZigzag(putZigzag(nil, -1), 100000),
				wantErr: "short buffer for array",
			},
			{
				// A negative string length INSIDE a skipped block: the
				// per-item skip shares readLength with the value path.
				name:    "negative-item-length-in-block",
				payload: putZigzag(putZigzag(nil, 1), -1),
				wantErr: "invalid negative",
			},
		}
		for _, c := range cells {
			t.Run(c.name, func(t *testing.T) {
				censusAssertSkipErr(t, res, censusSkipWire(c.payload), c.wantErr)
				if c.alsoNatural {
					var got any
					if _, err := natural.Decode(c.payload, &got); err == nil ||
						!strings.Contains(err.Error(), c.wantErr) {
						t.Fatalf("natural decode: err=%v, want error containing %q", err, c.wantErr)
					}
				}
			})
		}
	})

	t.Run("map-of-string", func(t *testing.T) {
		res := censusResolve(t, `{"type":"map","values":"string"}`)
		cells := []struct {
			name    string
			payload []byte
			wantErr string
		}{
			{name: "minint64-count", payload: putZigzag(nil, minInt64),
				wantErr: "invalid map block count"},
			{name: "count-over-buffer", payload: putZigzag(nil, 100000),
				wantErr: "exceeds remaining buffer"},
			{name: "negative-bytesize", payload: putZigzag(putZigzag(nil, -2), -5),
				wantErr: "short buffer for map"},
			{name: "bytesize-over-buffer", payload: putZigzag(putZigzag(nil, -1), 100000),
				wantErr: "short buffer for map"},
			{name: "negative-key-length-in-block", payload: putZigzag(putZigzag(nil, 1), -1),
				wantErr: "invalid negative"},
		}
		for _, c := range cells {
			t.Run(c.name, func(t *testing.T) {
				censusAssertSkipErr(t, res, censusSkipWire(c.payload), c.wantErr)
			})
		}
	})

	t.Run("array-of-null-zero-byte-cap", func(t *testing.T) {
		// Zero-byte items make the count the only cost driver; the absolute
		// cap (maxZeroByteItems = 4096) must hold on the skip path exactly
		// as on the value path — a foreign writer's wire, since twmb's own
		// encoder refuses to produce an over-cap array.
		res := censusResolve(t, `{"type":"array","items":"null"}`)
		natural := avro.MustParse(`{"type":"array","items":"null"}`)

		atCap := putZigzag(putZigzag(nil, 4096), 0)
		censusAssertSkip(t, res, censusSkipWire(atCap))
		var got []any
		if _, err := natural.Decode(atCap, &got); err != nil || len(got) != 4096 {
			t.Fatalf("natural at-cap: err=%v len=%d", err, len(got))
		}

		overCap := putZigzag(putZigzag(nil, 4097), 0)
		censusAssertSkipErr(t, res, censusSkipWire(overCap), "zero-byte items exceeds")
		if _, err := natural.Decode(overCap, &got); err == nil ||
			!strings.Contains(err.Error(), "zero-byte items exceeds") {
			t.Fatalf("natural over-cap: err=%v", err)
		}

		// The cap is CUMULATIVE across blocks: 4096 in one block plus 1 in
		// the next must reject (a per-block check would pass both).
		cumulative := putZigzag(putZigzag(putZigzag(nil, 4096), 1), 0)
		censusAssertSkipErr(t, res, censusSkipWire(cumulative), "zero-byte items exceeds")
	})
}

// TestMatrix_SkipByteSizeAuthority pins which authority each path trusts
// when a size-prefixed block's byte size DISAGREES with its items — an
// inconsistency only a corrupt or adversarial writer produces, where the
// spec's dual framing (count for the value path, size for the skip path)
// genuinely diverges:
//
//   - twmb's VALUE path decodes |count| items and ignores the size — like
//     Java's readArrayStart/arrayNext and fastavro's read path.
//   - twmb's SKIP path jumps the declared size and ignores the items — like
//     Java's skip (BinaryDecoder.doSkipItems: `final long bytecount =
//     readLong(); doSkipBytes(bytecount);`, BinaryDecoder.java:436-444) and
//     like fastavro's compiled skip (_read.pyx skip_array: `block_size =
//     read_long(fo); fo.read(block_size)`; observed 1.12.2 — note its
//     PURE-PYTHON fallback, _read_py.py's _iter_array_or_map, is instead
//     item-driven, "# Read block size, unused", so a no-C-extension
//     fastavro install lands where the items end).
//
// On a lying-size wire each authority lands at a different offset, so the
// two twmb paths (and Java's, and compiled-fastavro's) read DIFFERENT
// trailing fields — pinned here so a future "fix" aligning one path onto
// the other trips this cell and forces the cross-impl discussion (aligning
// skip onto items would diverge from every reference's skip; read onto
// size, from every reference's read).
// TestDifferentialFastavroReaderGrammar executes fastavro's size-driven
// skip verdict on this exact wire.
func TestMatrix_SkipByteSizeAuthority(t *testing.T) {
	item, err := avro.MustParse(`"string"`).AppendEncode(nil, "x")
	if err != nil {
		t.Fatal(err)
	}
	const keepA, keepB = int32(7), int32(9)

	// Block: count -1, size = len(item)+1+1 — the size annexes the item,
	// the array terminator, and keepA's byte.
	//
	//   [-1][size][item "x"][0x00 terminator][keepA][0x00 terminator][keepB]
	//   value path: 1 item ─┘  end of array ─┘ keep=A
	//   skip path:  ───────── jump size ──────────┘ end of array ─┘ keep=B
	var payload []byte
	payload = putZigzag(payload, -1)
	payload = putZigzag(payload, int64(len(item))+2)
	payload = append(payload, item...)
	payload = append(payload, 0x00)
	payload = putZigzag(payload, int64(keepA))
	payload = append(payload, 0x00)
	payload = putZigzag(payload, int64(keepB))

	w := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"drop","type":{"type":"array","items":"string"}},
		{"name":"keep","type":"int"}]}`)

	// Value path (writer schema): items are the authority → keep = keepA.
	var full map[string]any
	rest, err := w.Decode(payload, &full)
	if err != nil {
		t.Fatalf("value-path decode: %v", err)
	}
	if full["keep"] != keepA {
		t.Fatalf("value path read keep=%#v, want %d (items are the authority)", full["keep"], keepA)
	}
	if want := []any{"x"}; len(full["drop"].([]any)) != 1 || full["drop"].([]any)[0] != want[0] {
		t.Fatalf("value path drop=%#v", full["drop"])
	}
	// The value path stops where the items end; keepB's trailing bytes are
	// honest leftover.
	if len(rest) == 0 {
		t.Fatal("value path consumed the skip-authority tail")
	}

	// Skip path (resolved, drop dropped): the size is the authority → the
	// jump lands past keepA, and the walker reads the SECOND terminator and
	// keepB.
	r := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"keep","type":"int"}]}`)
	res, err := avro.Resolve(w, r)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	var skipped map[string]any
	if _, err := res.Decode(payload, &skipped); err != nil {
		t.Fatalf("skip-path decode: %v", err)
	}
	if skipped["keep"] != keepB {
		t.Fatalf("skip path read keep=%#v, want %d (the declared size is the authority, matching Java's doSkipItems)", skipped["keep"], keepB)
	}
}

// TestMatrix_SkipNestedContainerFraming extends the flat skip-framing net
// (TestMatrix_ForeignFramingThroughSkip) to NESTED containers: every legal
// outer framing of an array<array<map<string>>> the reader drops must be
// consumed exactly, with the trailing field intact. The nested walkers
// (skipArray → skipArray → skipMap → skipString) each re-enter the shared
// block grammar, so a framing mishandled at any depth mis-positions every
// byte after it.
func TestMatrix_SkipNestedContainerFraming(t *testing.T) {
	inner := avro.MustParse(`{"type":"array","items":{"type":"map","values":"string"}}`)
	items := make([][]byte, 3)
	for i, v := range []any{
		[]any{map[string]any{"a": "x"}},
		[]any{map[string]any{"b": "yy"}, map[string]any{}},
		[]any{},
	} {
		b, err := inner.AppendEncode(nil, v)
		if err != nil {
			t.Fatalf("inner encode %d: %v", i, err)
		}
		items[i] = b
	}

	res := censusResolve(t, `{"type":"array","items":{"type":"array","items":{"type":"map","values":"string"}}}`)
	for name, outer := range frameVariants(items) {
		t.Run(name, func(t *testing.T) {
			censusAssertSkip(t, res, censusSkipWire(outer))
		})
	}
}

// TestDifferentialFastavroReaderGrammar executes the fastavro side of the
// census matrices' calibrated claims — each cell pins fastavro's OBSERVED
// verdict (1.12.2) so an upgrade that changes it flips the cell and forces a
// deliberate recalibration rather than a silently rotting comment.
func TestDifferentialFastavroReaderGrammar(t *testing.T) {
	o := startOracle(t)

	const enumSchema = `{"type":"enum","name":"E","symbols":["A","B","C"]}`
	writerRec := `{"type":"record","name":"R","fields":[
		{"name":"drop","type":` + enumSchema + `},
		{"name":"keep","type":"int"}]}`
	readerRec := `{"type":"record","name":"R","fields":[{"name":"keep","type":"int"}]}`

	hexOf := func(b []byte) string {
		const digits = "0123456789abcdef"
		out := make([]byte, 0, len(b)*2)
		for _, x := range b {
			out = append(out, digits[x>>4], digits[x&0xf])
		}
		return string(out)
	}

	t.Run("enum value path rejects out-of-range", func(t *testing.T) {
		resp := o.call(oracleJob{Op: "decode", Schema: json.RawMessage(enumSchema),
			Hex: hexOf(putZigzag(nil, 3))})
		if resp.OK {
			t.Errorf("fastavro accepted enum index 3 of 3 symbols: %v", resp.Values)
		}
	})

	t.Run("enum value path wraps negative index (calibration)", func(t *testing.T) {
		// fastavro's read_enum indexes the Python symbol list, so -1 WRAPS to
		// the last symbol — an accidental leniency twmb does not copy (Java
		// rejects; silent wraparound is wrong output under cross-impl rule 1).
		// Pinned at the observed verdict: if a fastavro release starts
		// rejecting, this cell flips and the census comments recalibrate.
		resp := o.call(oracleJob{Op: "decode", Schema: json.RawMessage(enumSchema),
			Hex: hexOf(putZigzag(nil, -1))})
		if !resp.OK {
			t.Errorf("fastavro now REJECTS a negative enum index (%s) — recalibrate the census's wraparound note", resp.Err)
		} else if len(resp.Values) != 1 || resp.Values[0] != "C" {
			t.Errorf("fastavro negative-index enum decoded %v, want wraparound to \"C\"", resp.Values)
		}
	})

	t.Run("enum skip path discards out-of-range and negative", func(t *testing.T) {
		// fastavro's skip_enum is read_long with no symbol lookup — the
		// reference behavior twmb's skipEnum mirrors.
		for _, idx := range []int64{3, -1} {
			wire := censusSkipWire(putZigzag(nil, idx))
			resp := o.call(oracleJob{Op: "readresolve", Schema: json.RawMessage(writerRec),
				Reader: json.RawMessage(readerRec), Hex: hexOf(wire)})
			if !resp.OK {
				t.Errorf("fastavro skip of enum index %d: %s", idx, resp.Err)
				continue
			}
			m, _ := resp.Values[0].(map[string]any)
			if got, _ := m["keep"].(float64); got != float64(censusKeep) {
				t.Errorf("fastavro keep after skipping enum index %d: %v", idx, resp.Values[0])
			}
		}
	})

	t.Run("union skip path rejects out-of-range", func(t *testing.T) {
		// fastavro's skip_union indexes writer_schema[index] — same loud
		// reject as twmb's skipUnion.
		writerU := `{"type":"record","name":"R","fields":[
			{"name":"drop","type":["int","string","boolean"]},
			{"name":"keep","type":"int"}]}`
		wire := censusSkipWire(putZigzag(nil, 3))
		resp := o.call(oracleJob{Op: "readresolve", Schema: json.RawMessage(writerU),
			Reader: json.RawMessage(readerRec), Hex: hexOf(wire)})
		if resp.OK {
			t.Errorf("fastavro skipped union index 3 of 3 branches: %v", resp.Values)
		}
	})

	t.Run("bytesize-lie: fastavro compiled skip is size-driven (calibration)", func(t *testing.T) {
		// The TestMatrix_SkipByteSizeAuthority wire: fastavro's COMPILED
		// skip (_read.pyx skip_array) jumps the declared size exactly like
		// twmb's skip and Java's doSkipItems, landing on keep=9. (Its
		// pure-Python fallback would read keep=7 — item-driven; this cell
		// pins the compiled implementation every normal install runs.)
		item, _ := avro.MustParse(`"string"`).AppendEncode(nil, "x")
		var payload []byte
		payload = putZigzag(payload, -1)
		payload = putZigzag(payload, int64(len(item))+2)
		payload = append(payload, item...)
		payload = append(payload, 0x00)
		payload = putZigzag(payload, 7)
		payload = append(payload, 0x00)
		payload = putZigzag(payload, 9)
		writerA := `{"type":"record","name":"R","fields":[
			{"name":"drop","type":{"type":"array","items":"string"}},
			{"name":"keep","type":"int"}]}`
		resp := o.call(oracleJob{Op: "readresolve", Schema: json.RawMessage(writerA),
			Reader: json.RawMessage(readerRec), Hex: hexOf(payload)})
		if !resp.OK {
			t.Errorf("fastavro rejected the size-lie wire: %s (recalibrate the authority table)", resp.Err)
			return
		}
		m, _ := resp.Values[0].(map[string]any)
		if got, _ := m["keep"].(float64); got != 9 {
			t.Errorf("fastavro read keep=%v on the size-lie wire, want 9 (size-driven compiled skip, matching Java and twmb); recalibrate the authority table", resp.Values[0])
		}
	})

	t.Run("zero-byte over-cap: fastavro reads uncapped (calibration)", func(t *testing.T) {
		// fastavro has no zero-byte-item cap — it reads 4097 nulls happily.
		// twmb's reject is the documented DOS-resistance divergence
		// (maxZeroByteItems); this cell witnesses the reference's accept so
		// the divergence stays an executed fact, not a stale claim.
		writerN := `{"type":"record","name":"R","fields":[
			{"name":"drop","type":{"type":"array","items":"null"}},
			{"name":"keep","type":"int"}]}`
		wire := censusSkipWire(putZigzag(putZigzag(nil, 4097), 0))
		resp := o.call(oracleJob{Op: "readresolve", Schema: json.RawMessage(writerN),
			Reader: json.RawMessage(readerRec), Hex: hexOf(wire)})
		if !resp.OK {
			t.Errorf("fastavro rejected 4097 zero-byte items (%s) — it may have grown a cap; recalibrate the divergence note", resp.Err)
			return
		}
		m, _ := resp.Values[0].(map[string]any)
		if got, _ := m["keep"].(float64); got != float64(censusKeep) {
			t.Errorf("fastavro keep after 4097-null skip: %v", resp.Values[0])
		}
	})
}
