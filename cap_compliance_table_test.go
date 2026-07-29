package avro_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

// ---------------------------------------------------------------------------
// The DoS-cap PRODUCER-COMPLIANCE table.
//
// The rule this drives is one sentence: every reader-side cap needs a
// producer-side compliance check, with one documented exception. What the table
// adds is that the rule is asked of every cap and of every CARRIER, because a
// cap is not one question — the same bound is reachable through a wire VALUE
// and through a schema DEFAULT, and defaults are pre-encoded by a separate walk
// that shares no code with the serializers. Four rounds in a row found the
// value-carrier face of a cap, fixed it, and left the default-carrier face for
// the next round to rediscover.
//
// Three things make this a table rather than a pile of pins:
//
//   - Every cap carries an APPLICABILITY verdict with its reason. Not every cap
//     in the family bounds a wire value: one bounds a Go target type, another
//     bounds a pre-allocation hint and refuses nothing. "Not applicable,
//     because X" is a real cell; a forced cell would be vacuous.
//   - Expectations come from the RULE, not from what the code does today. The
//     invariant is PER-WIRE self-consistency — if Encode on a wire succeeds,
//     Decode on that same wire must succeed — which is exactly what the rule
//     says and no more. It is deliberately NOT "the cap rejects on every wire":
//     maxZeroByteItems is binary-only, because JSON text cannot amplify, and a
//     net demanding uniform rejection would encode a false invariant and fail a
//     correct implementation.
//   - A cap added later lands with no row and FAILS until someone classifies
//     it (TestInvariant_EveryCapIsClassified). Without that the table is a
//     snapshot of today's caps and the next one repeats the last four rounds.
// ---------------------------------------------------------------------------

type capApplicability string

const (
	// capWireValue: the cap refuses wire content, so both carriers must comply.
	capWireValue capApplicability = "wire-value"
	// capReaderOnly: reader-only BY DESIGN — the documented exception. The
	// table asserts the exception rather than closing it.
	capReaderOnly capApplicability = "reader-only-by-design"
	// capNotApplicable: the cap has no wire-value face at all.
	capNotApplicable capApplicability = "not-applicable"
	// capUnruled: nothing settles this cell yet. Reported, never guessed.
	capUnruled capApplicability = "UNRULED"
)

// capDriver builds an over-cap and an at-cap case for one cap at one nesting,
// for one carrier. A nil driver means the row is classified but not driven.
type capDriver struct {
	// inner is the leaf schema the cap lives on.
	inner string
	// overCapDefault / atCapDefault render the leaf's JSON default literal.
	overCapDefault func() string
	atCapDefault   func() string
	// overCapValue / atCapValue render the leaf's Go value.
	overCapValue func() any
	atCapValue   func() any
}

type capRow struct {
	// konst is the identifier in the package sources. The completeness guard
	// matches on this, so a renamed constant fails loudly rather than silently
	// leaving the cap unwatched.
	konst         string
	applicability capApplicability
	reason        string
	driver        *capDriver
}

func codepointDefault(b []byte) string {
	var sb strings.Builder
	sb.Grow(len(b)*6 + 2)
	sb.WriteByte('"')
	for _, c := range b {
		fmt.Fprintf(&sb, "\\u%04x", c)
	}
	sb.WriteByte('"')
	return sb.String()
}

func capRows() []capRow {
	const zeroByteCap = 4 << 10
	const unscaledCap = 32 << 10
	raw := func(n int) []byte { return bytes.Repeat([]byte{0x01}, n) }
	nulls := func(n int) string {
		return "[" + strings.TrimSuffix(strings.Repeat("null,", n), ",") + "]"
	}
	anyNulls := func(n int) any {
		out := make([]any, n)
		return out
	}

	return []capRow{
		{
			konst:         "maxDecimalUnscaledBytes",
			applicability: capWireValue,
			reason:        "bounds the unscaled byte length the decoder will base-convert; refuses wire content, so both carriers must comply",
			driver: &capDriver{
				inner:          `{"type":"bytes","logicalType":"decimal","precision":65536,"scale":0}`,
				overCapDefault: func() string { return codepointDefault(raw(unscaledCap + 1)) },
				atCapDefault:   func() string { return codepointDefault(raw(unscaledCap)) },
				overCapValue:   func() any { return raw(unscaledCap + 1) },
				atCapValue:     func() any { return raw(unscaledCap) },
			},
		},
		{
			konst:         "maxZeroByteItems",
			applicability: capWireValue,
			reason:        "bounds the cumulative count of zero-byte array items the decoder accepts; refuses wire content, so both carriers must comply",
			driver: &capDriver{
				inner:          `{"type":"array","items":"null"}`,
				overCapDefault: func() string { return nulls(zeroByteCap + 1) },
				atCapDefault:   func() string { return nulls(zeroByteCap) },
				overCapValue:   func() any { return anyNulls(zeroByteCap + 1) },
				atCapValue:     func() any { return anyNulls(zeroByteCap) },
			},
		},
		{
			konst:         "maxOCFZeroByteSlack",
			applicability: capWireValue,
			reason: "bounds consecutive zero-byte datums per OCF block; the WRITER enforces the same bound through its flush discipline, " +
				"so a default filling a zero-byte field reaches it through the ordinary encode path the OCF wire column already drives",
		},
		{
			konst:         "decimalScaleLimit",
			applicability: capWireValue,
			reason: "bounds decimal scale/precision at BOTH parse and decode, and a default's scale is the schema's declared scale — " +
				"so the producer-side check is the parse-time one, and no default can carry a scale the parse did not already admit",
		},
		{
			konst:         "maxRatInputLen",
			applicability: capWireValue,
			reason: "bounds the string->big.Rat parse; the producer side is boundedRatFromString itself, which every encode-side " +
				"numeric-text carrier routes through. A stored decimal default is []byte, not text, so it does not reach this parse",
		},
		{
			konst:         "maxDepth",
			applicability: capWireValue,
			reason: "bounds recursion on every walk; the default pre-encode carries its own depth counter against this same constant " +
				"(encodeDefaultDepth), which is what makes a self-referential default an errTooDeep parse error rather than a stack overflow",
		},
		{
			konst:         "maxMapPreAllocSize",
			applicability: capNotApplicable,
			reason: "NOT a reject: it caps the size hint passed to reflect.MakeMapWithSize, and larger maps still grow dynamically. " +
				"No wire is refused, so there is nothing for a producer to comply with",
		},
		{
			konst:         "maxIndirectDepth",
			applicability: capNotApplicable,
			reason: "bounds Go POINTER depth on a target type, not wire content. It has no default-carrier face at all — a schema " +
				"default is Avro-native data and names no Go type",
		},
		{
			konst:         "ocfMetadataSafetyLimit",
			applicability: capWireValue,
			reason: "bounds OCF user-metadata size, and the writer carries the matching producer check. No default-carrier face: " +
				"container metadata is not schema-default data",
		},
		{
			konst:         "ocfSchemaSafetyLimit",
			applicability: capWireValue,
			reason: "bounds the OCF header's avro.schema size, writer-checked. No default-carrier face, for the same reason as the " +
				"metadata limit",
		},
		{
			konst:         "maxSchemaJSONNodes",
			applicability: capWireValue,
			reason: "the node-count half of the schema-tree walk budget, paired with maxSchemaJSONBytes — producer-side by " +
				"construction for the same reason: it bounds what the walk EMITS",
		},
		{
			konst:         "maxLaxIntDataLen",
			applicability: capWireValue,
			reason: "bounds lax integer TEXT at parse; same shape as maxInt64LenientLen and maxParseFloatLen — the producer side " +
				"is bounded integer formatting",
		},
		{
			konst:         "maxParseErrorLen",
			applicability: capNotApplicable,
			reason: "bounds an ERROR MESSAGE's length (the error-echo amplification family), not accepted input. It refuses no " +
				"wire, so the producer-compliance rule has nothing to say about it",
		},
		{
			konst:         "maxConsecutiveEmptyReads",
			applicability: capNotApplicable,
			reason: "bounds a misbehaving io.Reader's empty-read livelock, a trust-boundary guard on the CALLER's reader rather " +
				"than on wire content; there is no producer of it to comply",
		},
		{
			konst:         "defaultMaxDecompressedBytes",
			applicability: capReaderOnly,
			reason: "the OCF block-size pair is READER-ONLY BY DESIGN — the documented exception to the producer-compliance rule. " +
				"Producer enforcement was implemented once and REVERTED: it traps data at flush and leaves an unclosable " +
				"compressed-size residual. The table asserts the exception; it must not be closed",
		},
		{
			konst:         "defaultMaxBlockBytes",
			applicability: capReaderOnly,
			reason: "the COMPRESSED half of the same reader-only pair. It was an inline literal until this table asked for it: a " +
				"bound with no name cannot be classified by a guard keyed on names, so the row recorded a hole rather than " +
				"covering one. Naming it also made ocfEagerBlockAllocLimit DERIVE from it instead of restating the same number " +
				"in a second spelling under a comment asserting the two were equal",
		},

		// Below: bounds the #11 entry does not enumerate. The completeness
		// guard surfaced them, which is the guard doing its job — the written
		// list was never the set of caps.
		{
			konst:         "maxSchemaJSONBytes",
			applicability: capWireValue,
			reason: "bounds what the schema-tree walk EMITS. It is itself the producer-side check for the schema-TEXT channel, " +
				"paired with maxSchemaJSONDepth on the reading side; the rule is satisfied by construction here rather than by a separate charge",
		},
		{
			konst:         "maxSchemaJSONDepth",
			applicability: capWireValue,
			reason: "bounds schema-JSON nesting at parse; its producer counterpart is the walk depth budget that governs emission, " +
				"so a tree this package renders cannot exceed what it will re-read",
		},
		{
			konst:         "maxParseFloatLen",
			applicability: capWireValue,
			reason: "bounds float TEXT at parse; the producer counterpart is strconv formatting, whose output is bounded by the " +
				"float format itself, so no value this package emits can exceed it",
		},
		{
			konst:         "maxInt64LenientLen",
			applicability: capWireValue,
			reason: "bounds lenient int64 TEXT at parse; same shape as maxParseFloatLen — the producer side is bounded integer " +
				"formatting, so the text this package emits is always inside it",
		},
		{
			konst:         "maxFixedLogicalLen",
			applicability: capNotApplicable,
			reason: "NOT a reject: it bounds a parse-time PROBE BUFFER so a hostile fixed size cannot drive a large allocation. " +
				"No wire is refused, so there is nothing for a producer to comply with",
		},
		{
			konst:         "defaultBlockBytes",
			applicability: capNotApplicable,
			reason: "the OCF writer's FLUSH THRESHOLD, not a bound on accepted input — it decides when a block is cut, and refuses " +
				"nothing. Distinct from the reader-only block-size pair despite the similar name",
		},
	}
}

// nestings are the shapes the leaf can sit at. This is the axis the
// default-carrier bug turned on: the charge was asked of the FIELD's kind, so
// every nested leaf went unasked, and no existing generator varied it.
var capNestings = []struct {
	name string
	// arm is the encodeDefaultDepth case this nesting drives, or "" for the
	// flat shape which drives no composite arm. The completeness guard matches
	// on it, so a composite arm with no nesting here fails loudly.
	arm string
	// field renders the record field holding the leaf, given the leaf schema
	// and a leaf default literal (empty for the value carrier).
	field func(inner, deflt string) string
	// value renders the Go value for the leaf at this nesting.
	value func(leaf any) any
}{
	{"flat", "", func(inner, d string) string { return fieldOf("d", inner, d) },
		func(leaf any) any { return leaf }},
	{"in-record", "record", func(inner, d string) string {
		// The default has to sit on the OUTER field: a record-typed field
		// defaults as a whole object. Hanging it on the inner field instead
		// leaves the outer one defaultless, and the cell then measures
		// "missing key" rather than the cap.
		return fieldOf("d", `{"type":"record","name":"Inner","fields":[`+fieldOf("x", inner, "")+`]}`, objKeyOf("x", d))
	}, func(leaf any) any { return map[string]any{"x": leaf} }},
	{"in-array", "array", func(inner, d string) string {
		return fieldOf("d", `{"type":"array","items":`+inner+`}`, arrOf(d))
	}, func(leaf any) any { return []any{leaf} }},
	{"in-map", "map", func(inner, d string) string {
		return fieldOf("d", `{"type":"map","values":`+inner+`}`, objOf(d))
	}, func(leaf any) any { return map[string]any{"k": leaf} }},
	// A union default corresponds to the FIRST branch, so the leaf's own
	// literal is the union's literal. This arm hid every cap until it was
	// driven: it selects a branch by trying each and keeping the first that
	// encodes, and it charged nothing at all.
	{"in-union", "union", func(inner, d string) string {
		return fieldOf("d", `[`+inner+`,"null"]`, d)
	}, func(leaf any) any { return leaf }},
}

func fieldOf(name, typ, deflt string) string {
	if deflt == "" {
		return fmt.Sprintf(`{"name":%q,"type":%s}`, name, typ)
	}
	return fmt.Sprintf(`{"name":%q,"type":%s,"default":%s}`, name, typ, deflt)
}
func arrOf(d string) string {
	if d == "" {
		return ""
	}
	return "[" + d + "]"
}
func objKeyOf(k, d string) string {
	if d == "" {
		return ""
	}
	return `{"` + k + `":` + d + `}`
}
func objOf(d string) string {
	if d == "" {
		return ""
	}
	return `{"k":` + d + `}`
}

// TestMatrix_CapProducerCompliance drives cap x carrier x nesting x wire.
func TestMatrix_CapProducerCompliance(t *testing.T) {
	var driven, unruled int
	for _, row := range capRows() {
		if row.applicability == capUnruled {
			t.Logf("UNRULED: %s — %s", row.konst, row.reason)
			unruled++
			continue
		}
		if row.driver == nil {
			t.Logf("classified, not driven: %s [%s] — %s", row.konst, row.applicability, row.reason)
			continue
		}
		d := row.driver
		for _, nest := range capNestings {
			for _, carrier := range []string{"value", "default"} {
				for _, over := range []bool{false, true} {
					label := fmt.Sprintf("%s/%s/%s/%s", row.konst, carrier, nest.name, map[bool]string{false: "at-cap", true: "over-cap"}[over])
					t.Run(label, func(t *testing.T) {
						driven++
						var schema string
						var val any
						if carrier == "default" {
							lit := d.atCapDefault
							if over {
								lit = d.overCapDefault
							}
							schema = recordOf(nest.field(d.inner, lit()))
							val = map[string]any{"keep": int32(7)}
						} else {
							schema = recordOf(nest.field(d.inner, ""))
							leaf := d.atCapValue
							if over {
								leaf = d.overCapValue
							}
							val = map[string]any{"d": nest.value(leaf()), "keep": int32(7)}
						}
						s, err := avro.Parse(schema)
						if err != nil {
							// The rule forbids a PARSE reject for an
							// unwritable default: a reader that drops the
							// field must still be able to read its data.
							t.Fatalf("schema must parse; the bound belongs on encode: %v", err)
						}
						capCheckWires(t, s, val, over)
					})
				}
			}
		}
	}
	t.Logf("driven cells: %d, unruled rows: %d", driven, unruled)
}

func recordOf(field string) string {
	return `{"type":"record","name":"R","fields":[` + field + `,{"name":"keep","type":"int"}]}`
}

// capCheckWires asserts PER-WIRE self-consistency: on each wire independently,
// if Encode succeeded then Decode of that same output must succeed. It does NOT
// require the cap to reject on every wire — some caps are binary-only because
// the JSON representation cannot amplify, and demanding uniform rejection would
// encode an invariant the rule does not state.
func capCheckWires(t *testing.T, s *avro.Schema, val any, over bool) {
	t.Helper()
	if b, err := s.Encode(val); err == nil {
		var sink any
		if _, derr := s.Decode(b, &sink); derr != nil {
			t.Errorf("binary: Encode produced %d bytes its own Decode refuses: %v", len(b), derr)
		}
	}
	if j, err := s.EncodeJSON(val); err == nil {
		var sink any
		if derr := s.DecodeJSON(j, &sink); derr != nil {
			t.Errorf("json: EncodeJSON produced %d bytes its own DecodeJSON refuses: %v", len(j), derr)
		}
	}
	if so, err := s.AppendSingleObject(nil, val); err == nil {
		var sink any
		if _, derr := s.DecodeSingleObject(so, &sink); derr != nil {
			t.Errorf("single-object: Encode produced %d bytes its own Decode refuses: %v", len(so), derr)
		}
	}
	var buf bytes.Buffer
	if w, err := ocf.NewWriter(&buf, s); err == nil {
		if err := w.Encode(val); err == nil && w.Close() == nil {
			size := buf.Len()
			if r, rerr := ocf.NewReader(&buf); rerr != nil {
				t.Errorf("ocf: writer produced a %d-byte file NewReader refuses: %v", size, rerr)
			} else {
				var sink any
				if derr := r.Decode(&sink); derr != nil {
					t.Errorf("ocf: writer produced a %d-byte file the reader refuses: %v", size, derr)
				}
				r.Close()
			}
		}
	}
	// At or under the bound the value must actually make it onto the wire, or
	// the check moved the boundary inward and the cells above pass vacuously.
	if !over {
		if _, err := s.Encode(val); err != nil {
			t.Fatalf("at-cap must still encode: %v", err)
		}
	}
}

// TestInvariant_OCFBlockCapsStayReaderOnly asserts the EXCEPTION as a cell.
//
// The block-size pair is reader-only BY DESIGN, and the type system already
// says so: WithMaxBlockBytes is a ReaderOpt, so it cannot even be handed to
// NewWriter. That is a stronger statement of the exception than any behavioral
// probe — producer enforcement is not merely absent, it is unexpressible.
// Producer enforcement was implemented once and reverted (it traps data at
// flush and leaves an unclosable compressed-size residual), so this cell exists
// to fail if a later round re-adds it.
func TestInvariant_OCFBlockCapsStayReaderOnly(t *testing.T) {
	var _ ocf.ReaderOpt = ocf.WithMaxBlockBytes(1 << 10)
	var _ ocf.ReaderOpt = ocf.WithMaxDecompressedBlockBytes(1 << 10)

	// And behaviorally: a writer given a datum far larger than any reader
	// bound still WRITES it, because the bound governs reading.
	s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"b","type":"bytes"}]}`)
	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, s)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := w.Encode(map[string]any{"b": bytes.Repeat([]byte{0x01}, 1<<20)}); err != nil {
		t.Fatalf("the block-size cap is reader-only by design; the writer must not enforce it: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// The matching reader bound then refuses that file — the exception is a
	// working reader-side bound, not an absent one.
	if _, err := ocf.NewReader(bytes.NewReader(buf.Bytes()), ocf.WithMaxBlockBytes(1<<10)); err == nil {
		var sink any
		r, _ := ocf.NewReader(bytes.NewReader(buf.Bytes()), ocf.WithMaxBlockBytes(1<<10))
		if r != nil && r.Decode(&sink) == nil {
			t.Error("the reader-side block bound accepted a block far above it; the exception rests on that bound working")
		}
	}
}

// capNamePattern matches the identifiers this family names its bounds with.
// It is applied only to CONST declarations — an assignment like
// `maxBlockBytes = o.n` inside a constructor is a local binding of a bound
// declared elsewhere, not a new bound, and matching it would make the guard
// demand rows for things that are not caps.
var capNamePattern = regexp.MustCompile(`^(max[A-Z]\w*|default[A-Z]\w*Bytes|\w*SafetyLimit|\w*ScaleLimit)$`)

var capConstDecl = regexp.MustCompile(`^\s*(?:const\s+)?([A-Za-z_]\w*)\s+=`)

// scanCapConsts returns every cap-shaped CONSTANT declared in path.
func scanCapConsts(src string) []string {
	var out []string
	inBlock := false
	for _, line := range strings.Split(src, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "const ("):
			inBlock = true
			continue
		case inBlock && trimmed == ")":
			inBlock = false
			continue
		}
		isConst := strings.HasPrefix(trimmed, "const ") || inBlock
		if !isConst {
			continue
		}
		m := capConstDecl.FindStringSubmatch(line)
		if m == nil || !capNamePattern.MatchString(m[1]) {
			continue
		}
		out = append(out, m[1])
	}
	return out
}

// capNotABound lists identifiers the pattern catches that are not reader-side
// DoS caps, each with the reason. An entry here is a classification too: it
// says someone looked.
var capNotABound = map[string]string{
	"maxVarintLen":  "an encoding width, not a bound on accepted input",
	"maxVarlongLen": "an encoding width, not a bound on accepted input",
}

// TestInvariant_EveryCapIsClassified is the completeness half: a cap added
// later lands with no row and fails here until someone classifies it. Without
// this the table is a snapshot, and the next cap repeats the rounds that found
// this one.
func TestInvariant_EveryCapIsClassified(t *testing.T) {
	classified := map[string]bool{}
	for _, r := range capRows() {
		classified[r.konst] = true
	}
	roots := []string{".", "ocf"}
	found := map[string]string{}
	for _, root := range roots {
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatalf("read %s: %v", root, err)
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(root, e.Name())
			src, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			for _, name := range scanCapConsts(string(src)) {
				found[name] = path
			}
		}
	}
	if len(found) == 0 {
		t.Fatal("the scan matched no cap constants at all — the pattern has rotted and this guard is watching nothing")
	}
	for name, path := range found {
		if classified[name] {
			continue
		}
		if why, ok := capNotABound[name]; ok {
			t.Logf("not a bound: %s (%s) — %s", name, path, why)
			continue
		}
		t.Errorf("cap %s (%s) has no row in the producer-compliance table.\n"+
			"  Classify it: wire-value (and drive both carriers), reader-only-by-design (assert the exception),\n"+
			"  not-applicable (with the reason), or UNRULED — or add it to capNotABound if it is not a reader-side bound.",
			name, path)
	}
	for name := range classified {
		if _, ok := found[name]; !ok {
			t.Errorf("row %s names no constant in the sources — it was renamed or deleted, and this row now watches nothing", name)
		}
	}
}

// scanDefaultWalkCompositeArms returns the kinds encodeDefaultDepth RECURSES
// through — the arms that can nest a cap's carrier below the field's own node.
// It is derived from the source rather than listed, because a hand-listed axis
// is what let the union arm hide: it was a composite the table never drove, and
// a longer hand list would only move the blind spot to the next arm.
func scanDefaultWalkCompositeArms(src string) []string {
	start := strings.Index(src, "func encodeDefaultDepth(")
	if start < 0 {
		return nil
	}
	body := src[start:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}
	caseLine := regexp.MustCompile(`^\tcase ("[a-z]+"(?:, "[a-z]+")*):`)
	quoted := regexp.MustCompile(`"([a-z]+)"`)
	var out []string
	var current []string
	recursed := false
	flush := func() {
		if recursed {
			out = append(out, current...)
		}
	}
	for _, line := range strings.Split(body, "\n") {
		if m := caseLine.FindStringSubmatch(line); m != nil {
			flush()
			current, recursed = nil, false
			for _, q := range quoted.FindAllStringSubmatch(m[1], -1) {
				current = append(current, q[1])
			}
			continue
		}
		if current != nil && strings.Contains(line, "encodeDefaultDepth(") {
			recursed = true
		}
	}
	flush()
	return out
}

// TestInvariant_EveryDefaultWalkArmHasANestingCell is the completeness half of
// the NESTING axis, mirroring the cap classifier: every composite arm of the
// default walk must be driven by a nesting, or land with no cell and FAIL.
//
// The union arm is why this exists. It recursed like the other three, behaved
// differently from all of them (it selects a branch by trying each), and was
// simply absent from a hand-written axis — so the table stayed green over an
// open hole. An axis that exists but omits the shape a bug lives in is worse
// than no axis, because it reads as coverage.
func TestInvariant_EveryDefaultWalkArmHasANestingCell(t *testing.T) {
	src, err := os.ReadFile("resolve.go")
	if err != nil {
		t.Fatalf("read resolve.go: %v", err)
	}
	arms := scanDefaultWalkCompositeArms(string(src))
	if len(arms) == 0 {
		t.Fatal("the scan found no recursing arms at all — encodeDefaultDepth moved or was renamed, and this guard is watching nothing")
	}
	driven := map[string]bool{}
	for _, n := range capNestings {
		if n.arm != "" {
			driven[n.arm] = true
		}
	}
	for _, arm := range arms {
		if !driven[arm] {
			t.Errorf("encodeDefaultDepth recurses through the %q arm, but no nesting in capNestings drives it.\n"+
				"  A composite arm can nest a cap's carrier below the field's own node, which is exactly where the\n"+
				"  charge is asked. Add a nesting with arm: %q, or this axis reads as coverage it does not have.", arm, arm)
		}
	}
	for arm := range driven {
		if !slices.Contains(arms, arm) {
			t.Errorf("nesting drives the %q arm, but encodeDefaultDepth no longer recurses through it — the cell is stale", arm)
		}
	}
	t.Logf("composite arms derived from source: %v", arms)
}
