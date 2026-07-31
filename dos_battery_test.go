package avro

// DoS entry-point battery.
//
// This file is the single executable matrix of every PUBLIC entry point ×
// every hostile-input class. It exists to end the one-DoS-fix-per-round
// dribble: a resource-bound (DoS) finding is correct output at unbounded cost
// on hostile input, and those are closed WHOLESALE here, not one at a time.
//
// Rows (entry points): Parse / MustParse / SchemaCache.Parse / SchemaFor /
// Decode / DecodeJSON / DecodeSingleObject (safe + unsafe targets) / Encode /
// EncodeJSON / AppendSingleObject / Root / Canonical / String / Fingerprint /
// SchemaNode.Schema / Resolve / CheckCompatibility / RatFromBytes /
// DurationFromBytes / SingleObjectFingerprint.
//
// Columns (hostile-input classes):
//   C1 deep nesting          — schema JSON brackets, wire value, Go encode
//                              value, JSON value: stack overflow / O(depth^2).
//   C2 large count / length  — array/map block count, bytes/string/fixed
//                              length prefix: pre-bound memory allocation,
//                              zero-byte-item loops, count wraparound.
//   C3 number CPU amplif.    — decimal/json.Number/float strings driving
//                              big.Rat/big.Int/big.Float: O(n^2) / 10^scale.
//   C4 decompression amplif. — OCF codecs (lives in ocf/dos_battery_test.go).
//   C5 error-message echo    — hostile input echoed verbatim into an error
//                              string: 1:1 log/RPC/metric-label amplification.
//   C6 metadata DAG / value  — SchemaNode->JSON walk: shared-reference fan-out,
//                              deep per-node Props/Default value.
//   C7 cyclic Go type        — decode target / SchemaFor field type whose
//                              reflect graph is cyclic: unbounded recursion.
//   C9 registration-scaled   — a registered CustomType must not change
//      parse cost              Parse's complexity class: the custom-match
//                              subtree walks (finalize stamping, the cache
//                              boundary guard, the inherited-overlay
//                              completion) share one per-parse memo, else a
//                              backward-reference chain or a many-refs cache
//                              parse goes quadratic. Absolute wall-clock
//                              bounds, not ratios.
//
// Each cell drives the real public API with a hostile input and asserts the
// bound holds: it returns (an error, or terminates) FAST, never hangs, never
// panics, never crashes the process. Where a dedicated regression test already
// pins the extreme case, the cell's comment cites it — the battery is the
// consolidated, runnable index of the whole posture, not a replacement for the
// targeted pins.
//
// RULE: nothing here is ever "closed". A later DoS find does not invalidate the
// sweep; it EXTENDS this battery with the missed cell (and the bound that fixes
// it). Add the row/column, never delete one.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"
)

// dosBudget is the per-cell ceiling separating a working bound (rejects in
// single-digit milliseconds) from a missing one (seconds-to-forever). It is
// deliberately generous so a loaded host never false-fails a real bound, while
// still catching any unbounded path — the gap between the two is orders of
// magnitude, so the exact value is not the point.
const dosBudget = 4 * time.Second

// dosRun executes fn on hostile input under a watchdog. It fails the test if fn
// hangs past dosBudget (a missing bound on a non-allocating loop) or panics (a
// hostile input must surface as an error, never a panic). It returns fn's error
// and whether fn completed. A genuinely unbounded ALLOCATING path will OOM-kill
// the process rather than hang — that is still a loud, correct failure signal.
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
		if d := time.Since(start); d > dosBudget {
			t.Errorf("%s: completed but took %v (> %v) — cost not bounded for hostile input", name, d, dosBudget)
		}
		return r.err, true
	case <-time.After(dosBudget):
		t.Errorf("%s: did not return within %v — bound missing (hang/unbounded loop on hostile input)", name, dosBudget)
		return nil, false
	}
}

// wantReject asserts fn rejects hostile input fast (non-nil error, no hang/panic).
func wantReject(t *testing.T, name string, fn func() error) {
	t.Helper()
	if err, ok := dosRun(t, name, fn); ok && err == nil {
		t.Errorf("%s: hostile input was accepted (want a fast rejection)", name)
	}
}

// wantRejectIs asserts fn rejects fast with an error matching target.
func wantRejectIs(t *testing.T, name string, target error, fn func() error) {
	t.Helper()
	if err, ok := dosRun(t, name, fn); ok {
		if err == nil {
			t.Errorf("%s: hostile input was accepted (want %v)", name, target)
		} else if !errors.Is(err, target) {
			t.Errorf("%s: got %v, want errors.Is(_, %v)", name, err, target)
		}
	}
}

// wantTerminate asserts fn returns fast without hang/panic; accept-or-reject is
// not the DoS question here, only that the COST is bounded.
func wantTerminate(t *testing.T, name string, fn func() error) {
	t.Helper()
	dosRun(t, name, fn)
}

// dosMaxErrLen bounds an error string built from a 1 MiB hostile input. The
// content-truncating helpers cap user fragments at 40/80 chars, so even a
// message stitching several of them plus structural framing stays far below
// this — yet it is orders of magnitude under a 1:1 (1 MiB) amplification.
const dosMaxErrLen = 4096

// wantBoundedErr asserts fn errors and the error string is bounded (not a 1:1
// echo of the megabyte input).
func wantBoundedErr(t *testing.T, name string, fn func() error) {
	t.Helper()
	if err, ok := dosRun(t, name, fn); ok {
		if err == nil {
			t.Errorf("%s: want a (bounded) error, got nil", name)
		} else if n := len(err.Error()); n > dosMaxErrLen {
			t.Errorf("%s: error message is %d bytes (> %d) — hostile input echoed unbounded", name, n, dosMaxErrLen)
		}
	}
}

// ---- shared hostile wire builders ----------------------------------------

// dosVarlong zigzag-varlong-encodes i exactly as the encoder writes a count /
// length prefix (the package's own appendVarlong, so the battery's wire matches
// real producer wire).
func dosVarlong(i int64) []byte { return appendVarlong(nil, i) }

// avroBytesField length-prefixes b as an Avro `bytes`/`string` field.
func avroBytesField(b []byte) []byte { return append(dosVarlong(int64(len(b))), b...) }

// hugeBlockCount is a block count large enough that no buffer could legitimately
// back it, encoded as the Avro varlong an array/map block carries.
func hugeBlockCount() []byte { return dosVarlong(1 << 40) }

// recursiveNodeSchema is `record Node { value:int, next:["null",Node] }` — the
// canonical self-recursive shape for the deep-wire / cyclic-encode cells.
const recursiveNodeSchema = `{"type":"record","name":"Node","fields":[
	{"name":"value","type":"int"},
	{"name":"next","type":["null","Node"]}
]}`

// deepRecursiveWire is the binary encoding of `depth` nested Node records,
// terminated by a null. Decoding it must trip errTooDeep, not recurse the
// goroutine stack to death.
func deepRecursiveWire(depth int) []byte {
	var src []byte
	for range depth {
		src = append(src, 0)    // value = zigzag(0)
		src = append(src, 0x02) // union idx 1 = "Node"
	}
	return append(src, 0) // innermost union idx 0 = null
}

//////////////////////////////////////////////////////////////////////////////
// C1 — DEEP NESTING (stack overflow / O(depth^2))
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_C1_DeepNesting(t *testing.T) {
	// Schema-JSON bracket depth past the parse pre-scan cap. Bound:
	// checkSchemaNestingDepth / maxSchemaJSONDepth (schema.go), an O(input)
	// linear pre-scan run before any build. Extreme case + linear-time proof:
	// TestRegression_DeepSchemaNestingRejectedInBoundedTime,
	// TestRegression_DeepSchemaParseRunsInBoundedTime, _DeepValidSchemaParsesLinear.
	deepArraySchema := strings.Repeat(`{"type":"array","items":`, 6000) + `"int"` + strings.Repeat("}", 6000)
	wantReject(t, "Parse/schema-bracket-depth", func() error {
		_, err := Parse(deepArraySchema)
		return err
	})
	wantReject(t, "SchemaCache.Parse/schema-bracket-depth", func() error {
		var c SchemaCache
		_, err := c.Parse(deepArraySchema)
		return err
	})
	// A deeply-nested DEFAULT VALUE inflates the same bracket count, so the
	// pre-scan covers the value channel at Parse time too.
	deepDefaultSchema := `{"type":"record","name":"R","fields":[{"name":"f","type":` +
		`{"type":"array","items":"int"},"default":` +
		strings.Repeat("[", 6000) + strings.Repeat("]", 6000) + `}]}`
	wantReject(t, "Parse/deep-default-value", func() error {
		_, err := Parse(deepDefaultSchema)
		return err
	})

	s := MustParse(recursiveNodeSchema)
	wire := deepRecursiveWire(20000)

	// Binary decode of a deeply-nested value. Bound: errTooDeep via the
	// decoder's sl.depth (deser.go). Extreme: TestDecodeDeepInputDoesntPanic.
	wantRejectIs(t, "Decode/recursive-wire", errTooDeep, func() error {
		var n any
		_, err := s.Decode(wire, &n)
		return err
	})
	// Resolved-decode path carries its own depth bump (resolve.go:400).
	resolved, err := Resolve(s, s)
	if err != nil {
		t.Fatal(err)
	}
	wantRejectIs(t, "Decode/resolved/recursive-wire", errTooDeep, func() error {
		var n any
		_, err := resolved.Decode(wire, &n)
		return err
	})
	// Skip path: a reader that drops `next` must still bound the skip of the
	// writer's deep subtree (skipRecord/skipUnion via the same sl.depth).
	reader := MustParse(`{"type":"record","name":"Node","fields":[{"name":"value","type":"int"}]}`)
	skipResolved, err := Resolve(s, reader)
	if err != nil {
		t.Fatal(err)
	}
	wantRejectIs(t, "Decode/skip/recursive-wire", errTooDeep, func() error {
		var n struct {
			Value int32 `avro:"value"`
		}
		_, err := skipResolved.Decode(wire, &n)
		return err
	})

	// JSON decode of a deeply-nested matching value. Bound: decodeValue's
	// sl.depth check + the scanner's skipValueDepth (json_scan.go), both at
	// maxDepth. Extreme: TestDecodeDeepInputDoesntPanic/json_union_trial.
	var jsonDeep []byte
	for range 20000 {
		jsonDeep = append(jsonDeep, []byte(`{"value":0,"next":{"Node":`)...)
	}
	jsonDeep = append(jsonDeep, []byte(`{"value":0,"next":null}`)...)
	for range 20000 {
		jsonDeep = append(jsonDeep, []byte(`}}`)...)
	}
	wantRejectIs(t, "DecodeJSON/recursive-json", errTooDeep, func() error {
		var out any
		return s.DecodeJSON(jsonDeep, &out)
	})
	// Scanner skip of a deeply-nested UNKNOWN field value (json_scan.go's
	// skipValueDepth, a separate recursion from decodeValue).
	jsonUnknownDeep := []byte(`{"value":0,"next":null,"x":` + strings.Repeat("[", 20000) + strings.Repeat("]", 20000) + `}`)
	wantTerminate(t, "DecodeJSON/skip-unknown-deep", func() error {
		var out any
		return s.DecodeJSON(jsonUnknownDeep, &out)
	})

	// Cyclic Go value at encode. Bound: errTooDeep via the encoder's depth
	// parameter (ser.go). Extreme: TestEncodeCyclicInput.
	cyc := map[string]any{"value": int32(1)}
	cyc["next"] = cyc
	wantRejectIs(t, "Encode/cyclic-value", errTooDeep, func() error {
		_, err := s.AppendEncode(nil, cyc)
		return err
	})
	wantRejectIs(t, "EncodeJSON/cyclic-value", errTooDeep, func() error {
		_, err := s.AppendEncodeJSON(nil, cyc)
		return err
	})
	// Struct fast-path encode bypasses serRecord.ser; the bound lives on
	// serRecordFastPtr itself (unsafe.go). Extreme: TestEncodeCyclicInput/struct.
	type cyclicStructNode struct {
		Value int32             `avro:"value"`
		Next  *cyclicStructNode `avro:"next"`
	}
	cn := &cyclicStructNode{Value: 1}
	cn.Next = cn
	wantRejectIs(t, "Encode/cyclic-struct-fastpath", errTooDeep, func() error {
		_, err := s.AppendEncode(nil, cn)
		return err
	})

	// Single Object Encoding wraps the same body codec, so both directions
	// inherit the depth bound.
	wantRejectIs(t, "AppendSingleObject/cyclic-value", errTooDeep, func() error {
		_, err := s.AppendSingleObject(nil, cyc)
		return err
	})
	soeHdr, err := s.AppendSingleObject(nil, map[string]any{"value": int32(0), "next": nil})
	if err != nil {
		t.Fatal(err)
	}
	soeDeep := append(soeHdr[:10:10], wire...) // 2-byte magic + 8-byte fingerprint, then deep body
	wantRejectIs(t, "DecodeSingleObject/recursive-wire", errTooDeep, func() error {
		var n any
		_, err := s.DecodeSingleObject(soeDeep, &n)
		return err
	})
	// The unsafe (struct fast-path) DecodeSingleObject target shares the body
	// codec, so it inherits the same depth bound — the header claims "safe +
	// unsafe targets"; this drives the unsafe arm too.
	wantRejectIs(t, "DecodeSingleObject/recursive-wire(unsafe)", errTooDeep, func() error {
		var n cyclicStructNode
		_, err := s.DecodeSingleObject(soeDeep, &n)
		return err
	})

	// A reserved structural key on a kind that does not bind it (a stray
	// "items"/"values"/"fields" on an "int") is inert metadata the parser
	// ACCEPTS, so it never reaches the bracket-depth reject above — but it is
	// still decoded to decide whether it surfaces as-written or rides in
	// Props. That decode must happen ONCE per level; a routing pass that
	// re-decodes it re-enters the recursive schema decode, and two decodes per
	// level compound to O(2^depth) — a sub-KB stray schema that hangs Parse.
	// The bracket-depth arms above use the BINDING form (array/items), which
	// never routes a stray, so they cannot see this; these arms exercise the
	// stray form at every parse entry point and the metadata rebuild. Depth
	// 1000 stays under the bracket pre-scan for all three keys (fields nests 3
	// brackets/level); the bound is far above the linear cost (single-digit ms)
	// and far below the exponential (unbounded) and quadratic (multi-hundred-ms
	// at this depth) it exists to catch.
	const strayDepth = 1000
	for _, key := range []string{"items", "values", "fields"} {
		open, closeStr := `{"type":"int","`+key+`":`, `}`
		if key == "fields" {
			open, closeStr = `{"type":"int","fields":[{"name":"f","type":`, `}]}`
		}
		straySchema := strings.Repeat(open, strayDepth) + `"int"` + strings.Repeat(closeStr, strayDepth)
		wantAcceptUnder(t, "Parse/nested-stray-"+key, 300*time.Millisecond, func() error {
			_, err := Parse(straySchema)
			return err
		})
		wantAcceptUnder(t, "SchemaCache.Parse/nested-stray-"+key, 300*time.Millisecond, func() error {
			var c SchemaCache
			_, err := c.Parse(straySchema)
			return err
		})
		wantAcceptUnder(t, "Root().Schema()/nested-stray-"+key, 300*time.Millisecond, func() error {
			ss, err := Parse(straySchema)
			if err != nil {
				return err
			}
			root := ss.Root()
			_, err = root.Schema()
			return err
		})
	}
}

//////////////////////////////////////////////////////////////////////////////
// C2 — LARGE COUNT / LENGTH PREFIX (pre-bound allocation, zero-byte loops)
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_C2_LargeCountLength(t *testing.T) {
	// array<null>: zero-byte elements, so the buffer-relative bound is vacuous
	// and the absolute cap maxZeroByteItems applies (pre-add form, overflow
	// safe). Extreme: TestRegression_DecodeArrayOfNullLargeCount/_Capped,
	// TestRegression_ArrayZeroByteProducerCompliance.
	arrNull := MustParse(`{"type":"array","items":"null"}`)
	arrNullWire := append(hugeBlockCount(), 0x00)
	wantReject(t, "Decode/array<null>-huge-count(any)", func() error {
		var got []any
		_, err := arrNull.Decode(arrNullWire, &got)
		return err
	})
	wantReject(t, "Decode/array<null>-huge-count(typed)", func() error {
		var got []struct{}
		_, err := arrNull.Decode(arrNullWire, &got)
		return err
	})

	// array<int>: minItemBytes=1, so checkArrayBlockBounds rejects any count
	// past the remaining buffer (overflow-safe division form). Extreme:
	// TestRegression_DeserArraySliceBlockCountOverflow and siblings.
	arrInt := MustParse(`{"type":"array","items":"int"}`)
	arrIntWire := append(hugeBlockCount(), 0x02, 0x00)
	for _, tgt := range []struct {
		name string
		dst  func() any
	}{
		{"any", func() any { var v []any; return &v }},
		{"typed", func() any { var v []int32; return &v }},
	} {
		wantReject(t, "Decode/array<int>-huge-count("+tgt.name+")", func() error {
			_, err := arrInt.Decode(arrIntWire, tgt.dst())
			return err
		})
	}
	// Skip path (reader drops a writer array field) routes through the same
	// checkArrayBlockBounds.
	arrRec := MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":"int"}},{"name":"keep","type":"int"}]}`)
	arrRecReader := MustParse(`{"type":"record","name":"R","fields":[{"name":"keep","type":"int"}]}`)
	skipArr, err := Resolve(arrRec, arrRecReader)
	if err != nil {
		t.Fatal(err)
	}
	wantReject(t, "Decode/skip-array-huge-count", func() error {
		var v struct {
			Keep int32 `avro:"keep"`
		}
		_, err := skipArr.Decode(append(hugeBlockCount(), 0x02, 0x00), &v)
		return err
	})

	// map<int>: minEntryBytes>=1 (a key is at least 1 byte), checkMapBlockBounds
	// is always buffer-relative. Extreme: TestRegression_MapDecodeBucketAmplificationDoS.
	mapInt := MustParse(`{"type":"map","values":"int"}`)
	mapWire := append(hugeBlockCount(), 0x02, 0x00)
	for _, tgt := range []struct {
		name string
		dst  func() any
	}{
		{"any", func() any { var v map[string]any; return &v }},
		{"typed", func() any { var v map[string]int32; return &v }},
	} {
		wantReject(t, "Decode/map<int>-huge-count("+tgt.name+")", func() error {
			_, err := mapInt.Decode(mapWire, tgt.dst())
			return err
		})
	}

	// bytes / string length prefix: readLength rejects length > remaining
	// buffer BEFORE make([]byte, length), so the alloc can never exceed the
	// bytes actually supplied (1:1, never amplified).
	wantReject(t, "Decode/bytes-huge-length", func() error {
		var v []byte
		_, err := MustParse(`"bytes"`).Decode(dosVarlong(1<<40), &v)
		return err
	})
	wantReject(t, "Decode/string-huge-length", func() error {
		var v string
		_, err := MustParse(`"string"`).Decode(dosVarlong(1<<40), &v)
		return err
	})

	// fixed: size is a schema integer with no upper bound at parse — only
	// negatives reject, as avro-rs does (its size parse is as_u64, rejecting
	// negatives with no maximum; fastavro 1.12.2 is laxer still and parses
	// even a negative size, observed) — but deserFixed.deser calls
	// needLen before make([]byte, size), so a 2e9-size fixed against an empty
	// wire rejects without allocating. Sibling parse-time alloc bounds:
	// TestRegression_DecimalFixedSizeCapacityNoOverflow, _FixedLogicalProbeSizeBounded.
	fixedHuge := MustParse(`{"type":"fixed","name":"F","size":2000000000}`)
	wantReject(t, "Decode/fixed-huge-size-short-wire", func() error {
		var v []byte
		_, err := fixedHuge.Decode(nil, &v)
		return err
	})
}

//////////////////////////////////////////////////////////////////////////////
// C3 — NUMBER CPU AMPLIFICATION (big.Rat/big.Int/big.Float on compact strings)
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_C3_NumberCPU(t *testing.T) {
	hostile1MiB := strings.Repeat("9", 1<<20)

	// Decimal unscaled value on the wire: bounded by maxDecimalUnscaledBytes
	// before the big.Int materialization / base conversion. Extreme:
	// TestRegression_DecimalUnscaledLengthDoS, TestCoverage_RatFromBytesHostileScale.
	bytesDec := MustParse(`{"type":"bytes","logicalType":"decimal","precision":65536,"scale":0}`)
	hostileUnscaled := avroBytesField(bytes.Repeat([]byte{0x55}, 1<<20)) // ~1 MiB unscaled
	for _, tgt := range []struct {
		name string
		dst  func() any
	}{
		{"any", func() any { var v any; return &v }},
		{"bigRat", func() any { var v big.Rat; return &v }},
	} {
		wantReject(t, "Decode/bytes-decimal-huge-unscaled("+tgt.name+")", func() error {
			_, err := bytesDec.Decode(hostileUnscaled, tgt.dst())
			return err
		})
	}

	// JSON-decode of a megabyte float number: bounded by maxParseFloatLen /
	// boundedRatFromString. Extreme: TestRegression_DecodeJSONFloatLengthCapDoS,
	// TestRegression_DecimalJSONExpDoS.
	wantTerminate(t, "DecodeJSON/double-megabyte-digits", func() error {
		var v float64
		return MustParse(`"double"`).DecodeJSON([]byte(hostile1MiB), &v)
	})

	// Encode of a megabyte json.Number against numeric/decimal schemas:
	// bounded by boundedRatFromString (maxRatInputLen) / parseInt64Lenient's
	// length cap. Extreme: TestSerJSONNumberOverflowInCollections,
	// TestRegression_FiniteScaleCPUBound, TestRegression_JsonNumberToFloatErrorBounded.
	jn := json.Number(hostile1MiB)
	wantReject(t, "Encode/json.Number-megabyte->decimal", func() error {
		_, err := bytesDec.AppendEncode(nil, jn)
		return err
	})
	wantReject(t, "Encode/json.Number-megabyte->long", func() error {
		_, err := MustParse(`"long"`).AppendEncode(nil, jn)
		return err
	})
	wantTerminate(t, "Encode/json.Number-megabyte->double", func() error {
		_, err := MustParse(`"double"`).AppendEncode(nil, jn)
		return err
	})

	// Parse-time field-default number: a megabyte integer/float default is the
	// third validation axis (defaultAsInt64/defaultAsFloat via the same length
	// caps). Extreme: TestRegression_IntDefaultLengthCapBounded,
	// TestRegression_ParseFloatLengthCapDoS.
	wantTerminate(t, "Parse/long-default-megabyte-int", func() error {
		_, err := Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"long","default":` + hostile1MiB + `}]}`)
		return err
	})
	wantTerminate(t, "Parse/double-default-megabyte-float", func() error {
		_, err := Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"double","default":` + hostile1MiB + `}]}`)
		return err
	})

	// Parse-time decimal scale/precision: 10^scale is materialized at decode,
	// so the schema integer is capped at decimalScaleLimit at parse. Extreme:
	// TestRegression_DecimalScaleAllocBound, TestRegression_DecimalExponentOverflowRejectsAcrossArms.
	wantReject(t, "Parse/decimal-scale-over-limit", func() error {
		_, err := Parse(`{"type":"bytes","logicalType":"decimal","precision":2000000000,"scale":1000000000}`)
		return err
	})

	// Metadata-API number observability (FOURTH axis): a megabyte Props number
	// survives Parse fast (caps reject the conversion, json.Number is preserved)
	// and Root()/String()/Canonical() serialize it under the maxSchemaJSONBytes
	// budget. Extreme: TestRegression_ParseFloatLengthCapDoS (Props case),
	// TestRegression_SchemaMetadataExponentOverflowNormalizesToInf.
	wantTerminate(t, "Parse+Root+String/metadata-megabyte-number", func() error {
		s, err := Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"int"}],"x":` + hostile1MiB + `}`)
		if err != nil {
			return err
		}
		_ = s.Root()
		_ = s.String()
		_ = s.Canonical()
		return nil
	})
}

//////////////////////////////////////////////////////////////////////////////
// C5 — ERROR-MESSAGE ECHO (string-size amplification, not CPU)
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_C5_ErrorEcho(t *testing.T) {
	huge := strings.Repeat("a", 1<<20)

	// Schema-parse error echoing a megabyte type token: bounded by
	// boundJSONErrorEcho + boundErrorLen (maxParseErrorLen). Extreme:
	// TestRegression_SchemaParseErrorBoundedForHostileInput.
	wantBoundedErr(t, "Parse/unknown-type-megabyte-name", func() error {
		_, err := Parse(`{"type":"` + huge + `"}`)
		return err
	})

	// Decode error echoing megabyte wire content: a json.Number target fed a
	// megabyte string-typed wire value is rejected with truncForError(content).
	// Extreme: TestRegression_ErrorMessageBoundedForHostileInput.
	wantBoundedErr(t, "Decode/json.Number<-megabyte-string-error", func() error {
		var v json.Number
		_, err := MustParse(`"string"`).Decode(avroBytesField([]byte(huge)), &v)
		return err
	})

	// Encode error echoing a megabyte json.Number against a decimal schema:
	// truncForError / truncRatForError keep the message bounded even though the
	// source value is a megabyte. Extreme: TestRegression_BigDecimalRatErrorMessageBounded.
	bytesDec := MustParse(`{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`)
	wantBoundedErr(t, "Encode/decimal<-megabyte-json.Number-error", func() error {
		_, err := bytesDec.AppendEncode(nil, json.Number(strings.Repeat("9", 1<<20)))
		return err
	})

	// CheckCompatibility error echoing a megabyte FIELD NAME: the dotted path
	// is render-truncated (truncForError on CompatibilityError.Path). The
	// per-datum SemanticError.Error() render path is the same shape, pinned by
	// TestRegression_SemanticErrorFieldRenderBounded; CompatibilityError by
	// TestRegression_CompatibilityErrorRenderingBounded.
	writer := MustParse(`{"type":"record","name":"R","fields":[{"name":"` + huge + `","type":"int"}]}`)
	reader := MustParse(`{"type":"record","name":"R","fields":[{"name":"` + huge + `","type":"string"}]}`)
	wantBoundedErr(t, "CheckCompatibility/megabyte-field-name", func() error {
		return CheckCompatibility(writer, reader)
	})
}

//////////////////////////////////////////////////////////////////////////////
// C6 — METADATA DAG / DEEP VALUE (SchemaNode->JSON walk)
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_C6_MetadataWalk(t *testing.T) {
	// The deep-value, deep-structure, shared-DAG, and duplicate-named-def
	// expansion axes of the SchemaNode->JSON walk are reachable only by HAND-
	// BUILDING a SchemaNode (Parse's bracket pre-scan rejects the deep-JSON
	// route up front — see C1). The intricate hand-built constructions are
	// pinned by the dedicated battery in schema_node_test.go:
	//   - TestRegression_SchemaNodeSchemaDeepValueBounded   (deep Props/Default value)
	//   - TestRegression_SchemaNodeWalkDepthAllChannels     (all 4 structural channels + value sites)
	//   - TestRegression_SchemaNodeSharedDAGExpansionBounded (shared-reference 2^depth fan-out)
	//   - TestRegression_SchemaNodeDuplicateNamedDefinitionBounded (nested-marshal product blowup)
	// Bounds: maxSchemaJSONNodes + maxSchemaJSONBytes (one shared walkBudget),
	// valueWalkLimit, toJSONShared.
	//
	// This cell exercises the PUBLIC round-trip a normal caller reaches: Parse a
	// legal schema carrying real Props/defaults, then re-serialize via every
	// metadata surface, confirming the walk runs and terminates fast.
	s := MustParse(`{"type":"record","name":"R","doc":"d","x":{"a":[1,2,3],"b":"y"},"fields":[
		{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B","C"]},"default":"A"},
		{"name":"f","type":"int","default":7}
	]}`)
	wantTerminate(t, "Root+Schema/round-trip", func() error {
		root := s.Root() // addressable: Schema() has a pointer receiver
		_, _ = root.Schema()
		return nil
	})
	wantTerminate(t, "String/metadata-walk", func() error {
		_ = s.String()
		return nil
	})
	// A recursive schema's canonical/String emitter must REFERENCE (not
	// re-expand) the named type, so the walk over a cyclic node tree terminates.
	wantTerminate(t, "Canonical+String/recursive-schema", func() error {
		rec := MustParse(recursiveNodeSchema)
		_ = rec.Canonical()
		_ = rec.String()
		return nil
	})
}

//////////////////////////////////////////////////////////////////////////////
// C7 — CYCLIC GO TYPE (decode target / SchemaFor field type)
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_C7_CyclicGoType(t *testing.T) {
	// NEW CELL. The custom-decode and encode-field cyclic-pointer paths are
	// pinned (TestRegression_CustomDecodeBoundsRecursivePointerTarget,
	// _EncodeStructCyclicPointerFieldTerminates, _StructFieldPointerChainMatchesReflect),
	// but the NON-custom binary Decode into a cyclic-pointer TARGET had no
	// direct pin — only the shared indirectAlloc bound (reflect.go,
	// maxIndirectDepth) inferred from the custom path's comment. A user can
	// write `type P *P; s.Decode(wire, &p)`; the target is infinitely indirect.
	// indirectAlloc peels at most maxIndirectDepth levels, never reaches a
	// concrete kind, and the setter returns a SemanticError — bounded, no hang.
	long := MustParse(`"long"`)
	wire := dosVarlong(42)

	// type P *P as a decode target: infinitely indirect, must reject fast.
	wantTerminate(t, "Decode/cyclic-pointer-target", func() error {
		type P *P
		var p P
		_, err := long.Decode(wire, &p)
		if err == nil {
			return errors.New("cyclic pointer target unexpectedly accepted")
		}
		return nil
	})

	// A deep-but-finite pointer chain past maxIndirectDepth must also terminate
	// (the bound is depth, not cyclicity) rather than walking/allocating the
	// whole chain.
	wantTerminate(t, "Decode/deep-pointer-chain-target", func() error {
		var p7 *******int64 // 7 levels > maxIndirectDepth
		_, err := long.Decode(wire, &p7)
		_ = err // accept or reject; only the bounded cost is asserted
		return nil
	})

	// SchemaFor over a cyclic NON-struct Go field type: inferType's depth bound
	// + inferRecord's seen[] break the type-graph cycle. Extreme:
	// TestRegression_SchemaForRecursiveNonStructTypeErrors,
	// _SchemaForRecursivePtrDefaultTerminates.
	wantTerminate(t, "SchemaFor/cyclic-pointer-field-type", func() error {
		type P *P
		_, err := SchemaFor[struct {
			F P `avro:"f"`
		}]()
		_ = err
		return nil
	})
}

//////////////////////////////////////////////////////////////////////////////
// C8 — DIRECT byte-slice / hash PUBLIC entry points the row list above omitted.
//////////////////////////////////////////////////////////////////////////////

// TestDoSBattery_C8_DirectByteAPIs covers the public entry points that take a
// caller-supplied byte slice (or hostile schema) directly, bypassing Decode's
// length-prefix bounds: RatFromBytes, DurationFromBytes, SingleObjectFingerprint,
// and Fingerprint. Each must bound its cost on a megabyte / over-limit input
// and never panic on a short one. These were missing from the battery's row
// list even though two of them (the number-CPU and the metadata-hash surfaces)
// are exactly the amplification shapes C3/C6 guard elsewhere.
func TestDoSBattery_C8_DirectByteAPIs(t *testing.T) {
	hostile1MiB := bytes.Repeat([]byte{0x55}, 1<<20)

	// RatFromBytes (C3 number-CPU, DIRECT surface): a megabyte unscaled value or
	// an attacker scale would drive an unbounded big.Int base conversion / 10^scale
	// without the public-API guards (maxDecimalUnscaledBytes / decimalScaleLimit),
	// which return a zero *big.Rat instead. Extreme: TestCoverage_RatFromBytesHostileScale.
	wantTerminate(t, "RatFromBytes/megabyte-unscaled", func() error {
		got := RatFromBytes(hostile1MiB, 2)
		if got.Sign() != 0 {
			return errors.New("over-length unscaled not bounded to zero rat")
		}
		return nil
	})
	wantTerminate(t, "RatFromBytes/hostile-scale", func() error {
		got := RatFromBytes([]byte{0x01}, decimalScaleLimit+1)
		if got.Sign() != 0 {
			return errors.New("over-limit scale not bounded to zero rat")
		}
		return nil
	})
	wantTerminate(t, "RatFromBytes/hostile-negative-scale", func() error {
		_ = RatFromBytes([]byte{0x01}, -(decimalScaleLimit + 1))
		return nil
	})

	// DurationFromBytes (C2 length): reads exactly 12 bytes, so a megabyte input
	// is read 12-bounded and a short input returns the zero Duration, never panics.
	wantTerminate(t, "DurationFromBytes/megabyte", func() error {
		_ = DurationFromBytes(hostile1MiB)
		return nil
	})
	wantTerminate(t, "DurationFromBytes/short", func() error {
		_ = DurationFromBytes([]byte{1, 2, 3})
		return nil
	})

	// SingleObjectFingerprint (C2 length): validates the 10-byte header then reads
	// it; a megabyte input is header-bounded and a short input errors, never panics.
	wantTerminate(t, "SingleObjectFingerprint/megabyte", func() error {
		_, _, err := SingleObjectFingerprint(hostile1MiB)
		return err
	})
	wantTerminate(t, "SingleObjectFingerprint/short", func() error {
		_, _, err := SingleObjectFingerprint([]byte{0xC3, 0x01})
		return err
	})

	// Fingerprint (C6 metadata-hash): hashes Canonical(), so it inherits the
	// maxSchemaJSONBytes budget — a megabyte Props number (stripped by PCF) and a
	// recursive (cyclic) schema must both fingerprint fast without re-expansion.
	wantTerminate(t, "Fingerprint/metadata-megabyte-number", func() error {
		s, err := Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"int"}],"x":` + strings.Repeat("9", 1<<20) + `}`)
		if err != nil {
			return err
		}
		_ = s.Fingerprint(NewRabin())
		return nil
	})
	wantTerminate(t, "Fingerprint/recursive-schema", func() error {
		_ = MustParse(recursiveNodeSchema).Fingerprint(NewRabin())
		return nil
	})
}

// wantAcceptUnder asserts fn ACCEPTS (nil error) within bound. The C9 cells
// pin a complexity CLASS on the accept path, so unlike the reject cells they
// carry per-cell absolute bounds: generous multiples of the healthy cost
// (linear parse lands well under a tenth of each bound on a laden host) yet
// far below the quadratic cost the bound exists to catch — the two sit an
// order of magnitude apart, so the exact value is not the point. Under -race
// the bound gets the same ~3s ceiling every other wall-clock cell takes
// (see avro_test's raceRelaxed for the canonical rationale):
// instrumentation puts the healthy linear cost past the tight bound
// (~350ms observed for the 200ms chain cells), while the quadratic classes
// these cells catch are multi-second under -race; the tight bound stays in
// force for normal runs.
func wantAcceptUnder(t *testing.T, name string, bound time.Duration, fn func() error) {
	t.Helper()
	if raceEnabled && bound < 3*time.Second {
		bound = 3 * time.Second
	}
	start := time.Now()
	err := fn()
	elapsed := time.Since(start)
	if err != nil {
		t.Errorf("%s: %v", name, err)
		return
	}
	if elapsed > bound {
		t.Errorf("%s: took %v (> %v) — the pass lost its complexity bound at this size", name, elapsed, bound)
	}
}

// dosChainSchema builds a backward-reference chain: Top's field i defines
// record Ri whose single field references R(i-1). Every custom-match subtree
// walk from Ri reaches all of R0..R(i-1), so without a shared per-parse memo
// the finalize stamping loop costs O(n^2) node visits over ~60n bytes of
// schema text.
func dosChainSchema(n int) string {
	var sb strings.Builder
	sb.WriteString(`{"type":"record","name":"Top","fields":[`)
	for i := range n {
		if i > 0 {
			sb.WriteString(",")
		}
		if i == 0 {
			sb.WriteString(`{"name":"f0","type":{"type":"record","name":"R0","fields":[{"name":"v","type":"long"}]}}`)
		} else {
			fmt.Fprintf(&sb, `{"name":"f%d","type":{"type":"record","name":"R%d","fields":[{"name":"r","type":"R%d"}]}}`, i, i, i-1)
		}
	}
	sb.WriteString(`]}`)
	return sb.String()
}

func TestDoSBattery_C9_CustomTypeParseCost(t *testing.T) {
	chain := dosChainSchema(3000) // ~200KB of well-formed schema text
	noMatch := CustomType{LogicalType: "no-such-logical", AvroType: "string"}
	match := CustomType{AvroType: "long"} // matches every chain leaf

	// Parse × non-matching CustomType: every stamp walk completes clean, the
	// worst case for a memo (nothing to short-circuit on). Healthy cost is
	// the plain-parse neighborhood (tens of ms); quadratic is ~500ms+ here
	// and grows 4x per doubling.
	wantAcceptUnder(t, "Parse/chain-noMatch-custom", 200*time.Millisecond, func() error {
		_, err := Parse(chain, noMatch)
		return err
	})

	// Parse × MATCHING CustomType: walks short-circuit at the chain's leaf,
	// which without a memo is still O(n) per walk = O(n^2) total. The memo
	// must bound the match case too, not only the clean case.
	wantAcceptUnder(t, "Parse/chain-matching-custom", 200*time.Millisecond, func() error {
		_, err := Parse(chain, match)
		return err
	})

	// SchemaCache × many references to a large inherited type: the boundary
	// guard and the overlay completion each walk the inherited subtree per
	// reference — without per-parse sharing that is O(refs × nodes) (seconds
	// at 1000 × 5000); with it, one walk total.
	var big strings.Builder
	big.WriteString(`{"type":"record","name":"Big","fields":[`)
	for i := range 5000 {
		if i > 0 {
			big.WriteString(",")
		}
		fmt.Fprintf(&big, `{"name":"f%d","type":"long"}`, i)
	}
	big.WriteString(`]}`)
	var wide strings.Builder
	wide.WriteString(`{"type":"record","name":"Wide","fields":[`)
	for i := range 1000 {
		if i > 0 {
			wide.WriteString(",")
		}
		fmt.Fprintf(&wide, `{"name":"r%d","type":"Big"}`, i)
	}
	wide.WriteString(`]}`)

	c := new(SchemaCache)
	if _, err := c.Parse(big.String(), noMatch); err != nil {
		t.Fatalf("cache parse of the inherited type: %v", err)
	}
	wantAcceptUnder(t, "SchemaCache.Parse/many-refs-custom", 400*time.Millisecond, func() error {
		_, err := c.Parse(wide.String(), noMatch)
		return err
	})
}

//////////////////////////////////////////////////////////////////////////////
// C11 — SCHEMA-DECLARED MAGNITUDE at the top of the integer range.
//////////////////////////////////////////////////////////////////////////////

// TestDoSBattery_C11_SchemaDeclaredMagnitude is the column this battery was
// missing. C2 drives hostile counts and lengths off the WIRE; nothing drove a
// magnitude the SCHEMA TEXT declares. A `fixed` size is the one parse-time
// quantity whose value is not bounded by the length of the text declaring it —
// nineteen characters name 2^63, and the parser deliberately leaves the upper
// bound open to match the lenient majority — so any entry point that does
// arithmetic on it has to survive the top of the range.
//
// The bound is saturation at the producer (saturateSchemaMagnitude, deser.go),
// and the shapes below are chosen so the arithmetic is reached in each of the
// ways it can be: a magnitude standing alone, a SUM over record fields that
// carries past the range, and a union whose smallest branch is one. dosRun's
// "never panics" assertion is the operative one here — an unsaturated sum
// reaches a divisor as zero.
func TestDoSBattery_C11_SchemaDeclaredMagnitude(t *testing.T) {
	const huge = `{"type":"fixed","name":"BigF","size":9223372036854775807}`
	// A record whose field minimums sum past the range and land on -1.
	const sumWrap = `{"type":"record","name":"SumWrap","fields":[
		{"name":"lead","type":"long"},
		{"name":"a","type":{"type":"fixed","name":"SWA","size":9223372036854775807}},
		{"name":"b","type":{"type":"fixed","name":"SWB","size":9223372036854775807}}]}`

	shapes := []struct{ name, schema string }{
		{"fixed-alone", huge},
		{"sum-wrap-record", sumWrap},
		{"union-of-huge", `[` + huge + `]`},
		{"map-of-sum-wrap", `{"type":"map","values":` + sumWrap + `}`},
		{"array-of-sum-wrap", `{"type":"array","items":` + sumWrap + `}`},
		{"map-of-huge", `{"type":"map","values":` + huge + `}`},
		{"decimal-on-huge-fixed", `{"type":"fixed","name":"BigD","size":9223372036854775807,"logicalType":"decimal","precision":4,"scale":2}`},
	}

	// One block header claiming a single element, and nothing after it. Every
	// shape above needs more bytes for one element than any wire can hold.
	claimsOne := []byte{0x02}

	for _, sh := range shapes {
		// Parse must accept these — an open upper bound is the documented
		// posture, so rejecting here would be the other kind of bug.
		s, err := Parse(sh.schema)
		if err != nil {
			t.Errorf("%s: parse rejected a schema the open-size posture accepts: %v", sh.name, err)
			continue
		}

		wantTerminate(t, "Decode/"+sh.name, func() error {
			var v any
			_, err := s.Decode(claimsOne, &v)
			return err
		})
		wantTerminate(t, "DecodeJSON/"+sh.name, func() error {
			var v any
			return s.DecodeJSON([]byte(`{}`), &v)
		})
		wantTerminate(t, "Encode/"+sh.name, func() error {
			_, err := s.Encode(map[string]any{})
			return err
		})
		wantTerminate(t, "EncodeJSON/"+sh.name, func() error {
			_, err := s.EncodeJSON(map[string]any{})
			return err
		})
		// The metadata surfaces read the same magnitude back out.
		wantTerminate(t, "Root/"+sh.name, func() error {
			r := s.Root()
			_, err := r.Schema()
			return err
		})
		wantTerminate(t, "Canonical/"+sh.name, func() error {
			_ = s.Canonical()
			_ = s.String()
			_ = s.Fingerprint(NewRabin())
			return nil
		})
		// Resolution derives the same bounds a second and third time — once
		// for the resolved reader tree, once for the skip of a dropped field.
		wantTerminate(t, "Resolve/"+sh.name, func() error {
			_, err := Resolve(s, s)
			return err
		})
		wantTerminate(t, "CheckCompatibility/"+sh.name, func() error {
			return CheckCompatibility(s, s)
		})

		dropW, errW := Parse(`{"type":"record","name":"DropOuter","fields":[
			{"name":"c","type":` + sh.schema + `},{"name":"keep","type":"int"}]}`)
		dropR, errR := Parse(`{"type":"record","name":"DropOuter","fields":[{"name":"keep","type":"int"}]}`)
		if errW == nil && errR == nil {
			wantTerminate(t, "Resolve+Decode/"+sh.name, func() error {
				res, err := Resolve(dropW, dropR)
				if err != nil {
					return err
				}
				var v any
				_, err = res.Decode(append(append([]byte{}, claimsOne...), 0x02), &v)
				return err
			})
		}
	}

	// SchemaCache.Parse derives the same bounds through the cache's own
	// build path, and a second parse re-derives them over inherited nodes.
	wantTerminate(t, "SchemaCache.Parse/sum-wrap", func() error {
		var c SchemaCache
		if _, err := c.Parse(sumWrap); err != nil {
			return err
		}
		s, err := c.Parse(`{"type":"map","values":"SumWrap"}`)
		if err != nil {
			return err
		}
		var v any
		_, err = s.Decode(claimsOne, &v)
		return err
	})
}
