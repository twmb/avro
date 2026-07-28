package avro_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// An attribute WRITTEN AS ITS DESTINATION'S ZERO is written.
//
// A schema attribute read into a plain string or int has no companion saying
// whether the key appeared, so `x != ""` means two things at once: "the caller
// chose a value" and "the caller chose the value that happens to be my zero".
// Every reader guarded that way is silently wrong for exactly one input, and
// which way it is wrong depends on what the reader does with the answer — a
// validator SKIPS the zero, an emitter DROPS it.
//
// Apache Avro has no such gap, because it decides on the JSON NODE rather than
// on the parsed value, and its emission condition DIFFERS PER ATTRIBUTE:
//
//   - doc emits when non-NULL (Schema.java:1039 record, :1154 enum, :1367
//     fixed, :1062 field), so a doc written as "" survives;
//   - aliases emits when non-EMPTY (:886 named, :1070 field), so an alias
//     list written as [] is dropped;
//   - order is decided on the node — `if (orderNode != null)
//     Order.valueOf(node.textValue().toUpperCase())` (:1895-1897) — so an
//     order written as "" reaches valueOf and throws;
//   - logicalType is not reserved at all (:175-176), so parseProperties keeps
//     it as an ordinary schema property whatever its content, "" included.
//
// One blanket "was it written" rule for every attribute would therefore ship a
// divergence rather than fix one. These tests pin the four answers separately,
// each against the reference behavior that decides it.
//
// fastavro 1.12.2 preserves every one of these bodies verbatim (executed), so
// where Java and fastavro disagree the entry naming the deciding rule is the
// authority, and the cases this package deliberately keeps dropping are pinned
// here as such rather than left to look like oversights.
// ---------------------------------------------------------------------------

// TestRegression_EmptyOrderRejected pins the validator half: presence and
// validity are one question, so an order written as the empty string is a
// written order that is not one of the three the spec defines.
func TestRegression_EmptyOrderRejected(t *testing.T) {
	const host = `{"type":"record","name":"R","fields":[{"name":"f","type":"int"%s}]}`
	if _, err := avro.Parse(strings.Replace(host, "%s", `,"order":""`, 1)); err == nil {
		t.Error(`"order":"" parsed; it is a written order and not one of ascending/descending/ignore`)
	} else if !strings.Contains(err.Error(), "order") {
		t.Errorf("the reject does not name the offending attribute: %v", err)
	}

	// The three legal orders keep parsing, and an ABSENT order stays legal —
	// the check must key on written-ness, not on non-emptiness, or it would
	// reject every field that does not spell one.
	for _, ok := range []string{`,"order":"ascending"`, `,"order":"descending"`, `,"order":"ignore"`, ``} {
		if _, err := avro.Parse(strings.Replace(host, "%s", ok, 1)); err != nil {
			t.Errorf("%q must stay legal: %v", ok, err)
		}
	}

	// Case variants stay rejected. Apache Avro upper-cases before its own
	// lookup, but reserved attribute VALUES are matched by exact spelling
	// here: a variant is a different string, not a different case of the
	// same one.
	for _, bad := range []string{`,"order":"ASCENDING"`, `,"order":"Ignore"`, `,"order":"asc"`} {
		if _, err := avro.Parse(strings.Replace(host, "%s", bad, 1)); err == nil {
			t.Errorf("%q parsed; the order comparison is exact-case", bad)
		}
	}
}

// TestRegression_EmptyDocSurvivesWhereJavaEmitsOne pins the five placements
// Apache Avro carries a doc on — the four named kinds and the record field —
// where an empty doc is a doc and is emitted.
func TestRegression_EmptyDocSurvivesWhereJavaEmitsOne(t *testing.T) {
	cases := []struct{ name, src string }{
		{"record", `{"type":"record","name":"R","doc":"","fields":[]}`},
		{"error", `{"type":"error","name":"E","doc":"","fields":[]}`},
		{"enum", `{"type":"enum","name":"En","doc":"","symbols":["A"]}`},
		{"fixed", `{"type":"fixed","name":"F","doc":"","size":1}`},
		{"field", `{"type":"record","name":"R","fields":[{"name":"f","type":"int","doc":""}]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := avro.Parse(c.src)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			n := s.Root()
			rb, err := n.Schema()
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if !strings.Contains(rb.String(), `"doc":""`) {
				t.Errorf("the rebuild dropped a written empty doc: %s", rb)
			}
			// A second pass must keep it, or the attribute survives one
			// rebuild and dies on the next.
			rbRoot := rb.Root()
			rb2, err := rbRoot.Schema()
			if err != nil {
				t.Fatalf("second rebuild: %v", err)
			}
			if rb2.String() != rb.String() {
				t.Errorf("emission is not a fixpoint:\n first %s\nsecond %s", rb, rb2)
			}
			// Nothing on the wire side may move: neither the canonical form
			// nor the fingerprint carries doc at all.
			twin := avro.MustParse(strings.Replace(c.src, `"doc":"",`, "", 1))
			if !bytes.Equal(s.Canonical(), twin.Canonical()) {
				t.Errorf("canonical form differs from the doc-free twin: %s vs %s", s.Canonical(), twin.Canonical())
			}
			if !bytes.Equal(s.Fingerprint(avro.NewRabin()), twin.Fingerprint(avro.NewRabin())) {
				t.Error("fingerprint differs from the doc-free twin")
			}
		})
	}
}

// TestRegression_EmptyLogicalTypeSurvives pins the other preserved attribute.
// logicalType is absent from Apache Avro's reserved set, so it is an ordinary
// schema property there and survives on every kind whatever its content —
// including on a primitive, which is the node the bare-emission shortcut
// collapses and therefore the one that has to consult presence.
func TestRegression_EmptyLogicalTypeSurvives(t *testing.T) {
	for _, src := range []string{
		`{"type":"int","logicalType":""}`,
		`{"type":"string","logicalType":""}`,
		`{"type":"record","name":"R","logicalType":"","fields":[]}`,
		`{"type":"array","items":"int","logicalType":""}`,
	} {
		s, err := avro.Parse(src)
		if err != nil {
			t.Fatalf("Parse(%s): %v", src, err)
		}
		n := s.Root()
		if n.LogicalType != "" {
			t.Errorf("LogicalType = %q, want the written empty string (%s)", n.LogicalType, src)
		}
		rb, err := n.Schema()
		if err != nil {
			t.Fatalf("rebuild(%s): %v", src, err)
		}
		if !strings.Contains(rb.String(), `"logicalType":""`) {
			t.Errorf("the rebuild dropped a written empty logicalType: %s (from %s)", rb, src)
		}
		rbRoot := rb.Root()
		rb2, err := rbRoot.Schema()
		if err != nil {
			t.Fatalf("second rebuild: %v", err)
		}
		if rb2.String() != rb.String() {
			t.Errorf("emission is not a fixpoint:\n first %s\nsecond %s", rb, rb2)
		}
	}
}

// TestRegression_EmptyAliasesStayDropped is the other side of the
// per-attribute rule, and the reason a blanket presence mechanism would be
// wrong. An alias list written as [] is EMPTY, and Apache Avro's emission
// condition for aliases is non-EMPTY rather than non-null (Schema.java:886
// for a named type, :1070 for a field) — so where a kind BINDS the key, this
// package and Apache Avro agree on dropping it. fastavro 1.12.2 preserves it;
// two of three drop.
//
// The scope is the BINDING placement. On a kind that does not bind aliases
// there is no Apache Avro condition to follow, and the stray-routing posture
// governs instead — TestRegression_StrayZeroBodySurvivesTheRebuild covers it.
func TestRegression_EmptyAliasesStayDropped(t *testing.T) {
	drops := []struct{ name, src string }{
		{"type-level", `{"type":"record","name":"R","aliases":[],"fields":[]}`},
		{"field-level", `{"type":"record","name":"R","fields":[{"name":"f","type":"int","aliases":[]}]}`},
	}
	for _, c := range drops {
		t.Run(c.name, func(t *testing.T) {
			s := avro.MustParse(c.src)
			root := s.Root()
			rb, err := root.Schema()
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if strings.Contains(rb.String(), `"aliases"`) {
				t.Errorf("an empty alias list survived where the kind BINDS the key; Apache Avro's condition there is non-empty: %s", rb)
			}
		})
	}
	// The control: a non-empty list survives, so the drop is about the BODY
	// and not about the attribute being unsupported.
	keeps := []struct{ name, src string }{
		{"type-level", `{"type":"record","name":"R","aliases":["X"],"fields":[]}`},
		{"field-level", `{"type":"record","name":"R","fields":[{"name":"f","type":"int","aliases":["x"]}]}`},
	}
	for _, c := range keeps {
		t.Run("control/"+c.name, func(t *testing.T) {
			s := avro.MustParse(c.src)
			root := s.Root()
			rb, err := root.Schema()
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if !strings.Contains(rb.String(), `"aliases"`) {
				t.Errorf("the non-empty control lost the aliases too, so the drop above is not about the body: %s", rb)
			}
		})
	}
}

// TestRegression_PrimitiveDocSurvivesEitherWay pins the placement-authority
// rule, which is what decides a cell no single reference can.
//
// Apache Avro has no doc slot on a primitive or a container at all: parseDoc
// is called from parseRecord/parseEnum/parseFixed and parseField and nowhere
// else, so it neither keeps nor drops one — it has no opinion. fastavro does
// have the placement and preserves the attribute. This package already
// followed fastavro there for a NON-EMPTY doc, so the empty twin follows the
// same authority: deriving it from Apache Avro's absence while its non-empty
// sibling follows fastavro's presence would split one placement between two
// references and make the two bodies of one attribute disagree for no reason
// a caller could name.
func TestRegression_PrimitiveDocSurvivesEitherWay(t *testing.T) {
	for _, kind := range []string{`{"type":"int"%s}`, `{"type":"string"%s}`,
		`{"type":"array","items":"int"%s}`, `{"type":"map","values":"int"%s}`} {
		for _, doc := range []string{`,"doc":""`, `,"doc":"d"`} {
			src := strings.Replace(kind, "%s", doc, 1)
			s, err := avro.Parse(src)
			if err != nil {
				t.Fatalf("Parse(%s): %v", src, err)
			}
			root := s.Root()
			rb, err := root.Schema()
			if err != nil {
				t.Fatalf("rebuild(%s): %v", src, err)
			}
			if !strings.Contains(rb.String(), `"doc"`) {
				t.Errorf("the rebuild dropped the doc: %s (from %s)", rb, src)
			}
			// Fixpoint, and the wire side untouched.
			rbRoot := rb.Root()
			rb2, err := rbRoot.Schema()
			if err != nil {
				t.Fatalf("second rebuild: %v", err)
			}
			if rb2.String() != rb.String() {
				t.Errorf("emission is not a fixpoint:\n first %s\nsecond %s", rb, rb2)
			}
			twin := avro.MustParse(strings.Replace(kind, "%s", "", 1))
			if !bytes.Equal(s.Canonical(), twin.Canonical()) {
				t.Errorf("canonical form differs from the doc-free twin: %s vs %s", s.Canonical(), twin.Canonical())
			}
		}
	}
}

// TestRegression_StrayZeroBodySurvivesTheRebuild covers the placements no
// reference can adjudicate: a structural key written as its destination's
// ZERO on a kind that does not bind it.
//
// Apache Avro skips every stray key as reserved and keeps none of them;
// fastavro keeps every stray key as a property and drops none. Neither is
// answering the question this package's metadata tree poses, so its own
// stray-routing posture governs — the same basis the field-lift and
// consumed-parameter rulings rest on — and that posture says as-written is
// the key's ONLY surface. Reaching neither surface is what it forbids.
//
// The trap this pins, and the reason each cell asserts the EMITTED FORM's
// round trip rather than only the field's survival: the exclusivity rule
// (a kind carrying another kind's defining key) is decided on the VALUE, so
// `symbols:["A"]` on an array rejects while `symbols:[]` is accepted.
// Preserving the empty body therefore emits a schema whose own re-parse has
// to be checked, not assumed — if exclusivity ever became presence-decided,
// the rebuild would start emitting schemas this package rejects.
func TestRegression_StrayZeroBodySurvivesTheRebuild(t *testing.T) {
	// Each cell: a kind that does NOT bind the key, and the key written as
	// its destination's zero.
	cells := []struct{ name, src, key string }{
		{"name/int", `{"type":"int","name":""}`, "name"},
		{"name/array", `{"type":"array","items":"int","name":""}`, "name"},
		{"namespace/int", `{"type":"int","namespace":""}`, "namespace"},
		{"namespace/map", `{"type":"map","values":"int","namespace":""}`, "namespace"},
		{"aliases/int", `{"type":"int","aliases":[]}`, "aliases"},
		{"aliases/array", `{"type":"array","items":"int","aliases":[]}`, "aliases"},
		{"symbols/int", `{"type":"int","symbols":[]}`, "symbols"},
		{"symbols/array", `{"type":"array","items":"int","symbols":[]}`, "symbols"},
		{"symbols/record", `{"type":"record","name":"R","fields":[],"symbols":[]}`, "symbols"},
		{"size/int", `{"type":"int","size":0}`, "size"},
		{"size/string", `{"type":"string","size":0}`, "size"},
		{"fields/int", `{"type":"int","fields":[]}`, "fields"},
		{"fields/enum", `{"type":"enum","name":"E","symbols":["A"],"fields":[]}`, "fields"},
		{"fields/map", `{"type":"map","values":"int","fields":[]}`, "fields"},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			s, err := avro.Parse(c.src)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			root := s.Root()
			rb, err := root.Schema()
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if !strings.Contains(rb.String(), `"`+c.key+`"`) {
				t.Errorf("the as-written %q reached NEITHER surface — it is not in Props (the kind consumed it) and the rebuild dropped it: %s",
					c.key, rb)
			}
			// The trap, in three legs. SchemaNode.Schema() parses the tree
			// it marshals, so an emitted body the parser rejects already
			// fails ABOVE, at the rebuild — that leg is why the error check
			// there is a Fatalf and not a skip. What the re-parse adds is
			// the RENDERED TEXT: String() is written independently of the
			// tree Schema() handed the parser, so a divergence between them
			// shows up only here.
			back, err := avro.Parse(rb.String())
			if err != nil {
				t.Fatalf("the rendered text fails its own re-parse: %v\n  emitted: %s", err, rb)
			}
			backRoot := back.Root()
			rb2, err := backRoot.Schema()
			if err != nil {
				t.Fatalf("second rebuild: %v", err)
			}
			if rb2.String() != rb.String() {
				t.Errorf("emission is not a fixpoint:\n first %s\nsecond %s", rb, rb2)
			}
			// The wire side must not move: none of these keys reaches the
			// canonical form on a kind that does not bind it.
			twin := avro.MustParse(stripKey(c.src, c.key))
			if !bytes.Equal(s.Canonical(), twin.Canonical()) {
				t.Errorf("canonical form differs from the attribute-free twin: %s vs %s",
					s.Canonical(), twin.Canonical())
			}
		})
	}
}

// TestRegression_StrayReadableBodyStillRejectsOnExclusivity is the boundary
// the preservation above must not cross: a stray defining key that parsed as
// a REAL definition still hard-rejects on a kind that binds another one. The
// exclusivity rule is about a key that defines something, and an empty body
// defines nothing — which is exactly why the two verdicts differ.
func TestRegression_StrayReadableBodyStillRejectsOnExclusivity(t *testing.T) {
	for _, src := range []string{
		`{"type":"array","items":"int","symbols":["A"]}`,
		`{"type":"record","name":"R","fields":[],"symbols":["A"]}`,
		`{"type":"enum","name":"E","symbols":["A"],"fields":[{"name":"f","type":"int"}]}`,
		`{"type":"map","values":"int","fields":[{"name":"f","type":"int"}]}`,
		`{"type":"record","name":"R","fields":[],"size":2}`,
	} {
		if _, err := avro.Parse(src); err == nil {
			t.Errorf("a readable defining key on a kind that binds another one stopped rejecting: %s", src)
		}
	}
}

// stripKey removes `,"key":<body>` from a compact schema literal, giving the
// attribute-free twin the wire comparisons above need.
func stripKey(src, key string) string {
	i := strings.Index(src, `,"`+key+`":`)
	if i < 0 {
		return src
	}
	j := i + 1
	depth := 0
	for ; j < len(src); j++ {
		switch src[j] {
		case '[', '{':
			depth++
		case ']', '}':
			if depth == 0 {
				return src[:i] + src[j:]
			}
			depth--
		case ',':
			if depth == 0 {
				return src[:i] + src[j:]
			}
		}
	}
	return src[:i]
}

// TestRegression_NamespaceStrictnessIsUniform records why a non-string
// namespace rejecting here — where Apache Avro silently ignores it and
// fastavro keeps it — is coherence rather than an accidental third answer.
//
// The tempting analogy is the non-string logicalType, which this package
// routes to Props instead of rejecting. That analogy fails, and the reason is
// visible in one probe: this package also rejects garbage STRING namespaces.
// A rule that accepted every string and rejected every non-string would be
// judging the JSON token type while ignoring the content, which is the shape
// that made the logicalType case incoherent to reject. Here the content is
// judged too, so rejecting a body that cannot be a namespace at all is the
// same rule applied one step earlier.
//
// Names and namespaces are the one part of the grammar that stays strictly
// validated; only ALIASES relax, because a reader has to be able to alias a
// writer's illegal legacy name. Nothing about a namespace needs that
// latitude — it is this schema's own scope, not a foreign one being matched.
func TestRegression_NamespaceStrictnessIsUniform(t *testing.T) {
	const host = `{"type":"record","name":"R","namespace":%s,"fields":[]}`

	// The coherence proof: garbage STRINGS reject too, so the strictness is
	// about what a namespace can be, not about the JSON token class.
	for _, ns := range []string{`"123bad"`, `"has space"`, `"a..b"`, `"weird!"`} {
		if _, err := avro.Parse(fmt.Sprintf(host, ns)); err == nil {
			t.Errorf("namespace %s parsed; if garbage strings were accepted, rejecting non-strings would be judging the token class alone", ns)
		}
	}

	// The cells this pins: a non-string namespace rejects, on the same rule.
	for _, ns := range []string{`null`, `5`, `[]`, `{}`, `true`} {
		if _, err := avro.Parse(fmt.Sprintf(host, ns)); err == nil {
			t.Errorf("namespace %s parsed", ns)
		}
	}

	// The controls: legal namespaces parse, including the explicit-empty
	// null-namespace escape and the dotted form.
	for _, ns := range []string{`"a"`, `"a.b"`, `"a.b.c"`, `""`} {
		if _, err := avro.Parse(fmt.Sprintf(host, ns)); err != nil {
			t.Errorf("namespace %s must stay legal: %v", ns, err)
		}
	}

	// And the contrast that makes the rule a rule: an ALIAS accepts any
	// string, so the strictness is scoped to names and namespaces rather
	// than applied to every string attribute.
	if _, err := avro.Parse(`{"type":"record","name":"R","aliases":["123bad","has space"],"fields":[]}`); err != nil {
		t.Errorf("aliases must stay unvalidated — a reader has to be able to alias a writer's illegal name: %v", err)
	}
}
