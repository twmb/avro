package avro_test

import (
	"bytes"
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

// TestRegression_EmptyAliasesAndPrimitiveDocStayDropped is the other side of
// the per-attribute rule, and the reason a blanket presence mechanism would be
// wrong. Both bodies below are preserved by fastavro and dropped by Apache
// Avro, and this package follows Apache Avro:
//
//   - an alias list written as [] is empty, and the emission condition for
//     aliases is non-EMPTY, not non-null (Schema.java:886, :1070);
//   - a doc on a primitive or a container has no Apache Avro analogue at all,
//     because parseDoc is called only from parseRecord/parseEnum/parseFixed
//     and parseField — so there is no emission condition to satisfy.
func TestRegression_EmptyAliasesAndPrimitiveDocStayDropped(t *testing.T) {
	drops := []struct{ name, src, key string }{
		{"type-level aliases", `{"type":"record","name":"R","aliases":[],"fields":[]}`, `"aliases"`},
		{"field-level aliases", `{"type":"record","name":"R","fields":[{"name":"f","type":"int","aliases":[]}]}`, `"aliases"`},
		{"primitive doc", `{"type":"int","doc":""}`, `"doc"`},
		{"array doc", `{"type":"array","items":"int","doc":""}`, `"doc"`},
		{"map doc", `{"type":"map","values":"int","doc":""}`, `"doc"`},
	}
	for _, c := range drops {
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
			if strings.Contains(rb.String(), c.key) {
				t.Errorf("%s survived the rebuild; this package follows Apache Avro in dropping it: %s", c.key, rb)
			}
		})
	}

	// The controls: the same attributes with a NON-empty body survive, so the
	// drops above are about the body and not about the attribute being
	// unsupported.
	keeps := []struct{ name, src, key string }{
		{"type-level aliases", `{"type":"record","name":"R","aliases":["X"],"fields":[]}`, `"aliases"`},
		{"field-level aliases", `{"type":"record","name":"R","fields":[{"name":"f","type":"int","aliases":["x"]}]}`, `"aliases"`},
		{"primitive doc", `{"type":"int","doc":"d"}`, `"doc"`},
	}
	for _, c := range keeps {
		t.Run("control/"+c.name, func(t *testing.T) {
			s := avro.MustParse(c.src)
			root := s.Root()
			rb, err := root.Schema()
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if !strings.Contains(rb.String(), c.key) {
				t.Errorf("the non-empty control lost %s too, so the drop above is not about the body: %s", c.key, rb)
			}
		})
	}
}
