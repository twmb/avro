package avro_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// The Avro-JSON scanner is hand-rolled (json_scan.go), not encoding/json, so
// its string grammar is a second implementation of one thing and can drift from
// the stdlib's in either direction. The escape half of that grammar was pinned
// by nothing: the rejection table in the conformance suite carried a COMMENT
// saying this package accepted unknown escapes (`\q` as `q`) and left the row
// out on that basis. The comment was false — every unknown escape is rejected —
// so the table had no row for correct behavior, and a regression that started
// accepting them would have passed.
//
// This is the class rather than the instance. The axes are the ESCAPE and the
// POSITION the string occupies, because the scanner is entered from many places
// (a value, a map key, a record field NAME, the skip path for an unknown field)
// and a single top-level probe cannot tell whether they share one string
// reader. The expectation per cell comes from encoding/json — an oracle outside
// this package — on the same escape in the same JSON string, so no cell's
// verdict is read off current behavior.
type jsonEscapeCase struct {
	name   string
	escape string // the two-character escape as it appears in the JSON text
}

var jsonEscapeCases = []jsonEscapeCase{
	// Unknown escapes: encoding/json rejects each.
	{"unknown-letter", `\q`},
	{"hex-style", `\x41`},
	{"c-style-bell", `\a`},
	{"c-style-vtab", `\v`},
	{"c-style-nul", `\0`},
	{"escaped-space", `\ `},
	// Legal escapes, so the matrix pins the boundary in BOTH directions: a
	// scanner made strict enough to reject the six above must still accept
	// these, or the fix is a new rejection bug.
	{"newline", `\n`},
	{"quote", `\"`},
	{"backslash", `\\`},
	{"solidus", `\/`},
	{"unicode", `A`},
}

// jsonEscapePosition places a string carrying the escape somewhere a JSON
// document can hold one, and says which schema reads that document.
type jsonEscapePosition struct {
	name   string
	schema string
	// doc builds the JSON document with esc embedded in a string.
	doc func(esc string) string
}

var jsonEscapePositions = []jsonEscapePosition{
	{"top-level string", `"string"`,
		func(e string) string { return `"a` + e + `b"` }},
	{"record field value", `{"type":"record","name":"R","fields":[{"name":"x","type":"string"}]}`,
		func(e string) string { return `{"x":"a` + e + `b"}` }},
	{"record field NAME", `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`,
		func(e string) string { return `{"a` + e + `b":1}` }},
	{"map key", `{"type":"map","values":"int"}`,
		func(e string) string { return `{"a` + e + `b":1}` }},
	{"map value", `{"type":"map","values":"string"}`,
		func(e string) string { return `{"k":"a` + e + `b"}` }},
	{"array item", `{"type":"array","items":"string"}`,
		func(e string) string { return `["a` + e + `b"]` }},
	{"enum symbol", `{"type":"enum","name":"E","symbols":["A"]}`,
		func(e string) string { return `"a` + e + `b"` }},
	{"bytes", `"bytes"`,
		func(e string) string { return `"a` + e + `b"` }},
	{"union branch", `["null","string"]`,
		func(e string) string { return `{"string":"a` + e + `b"}` }},
	{"skipped unknown field", `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`,
		func(e string) string { return `{"x":1,"zz":"a` + e + `b"}` }},
	{"nested record field value", `{"type":"record","name":"R","fields":[{"name":"in","type":{"type":"record","name":"I","fields":[{"name":"y","type":"string"}]}}]}`,
		func(e string) string { return `{"in":{"y":"a` + e + `b"}}` }},
}

func TestMatrix_JSONEscapeRejectedAtEveryStringPosition(t *testing.T) {
	for _, pos := range jsonEscapePositions {
		s, err := avro.Parse(pos.schema)
		if err != nil {
			t.Fatalf("%s: schema does not parse: %v", pos.name, err)
		}
		for _, esc := range jsonEscapeCases {
			t.Run(pos.name+"/"+esc.name, func(t *testing.T) {
				doc := pos.doc(esc.escape)

				// The oracle: encoding/json's verdict on the SAME escape in a
				// string, decided outside this package. A bare string literal
				// is used rather than the whole document so the oracle answers
				// only the grammar question and not a schema question.
				var probe string
				wantReject := json.Unmarshal([]byte(`"a`+esc.escape+`b"`), &probe) != nil

				var out any
				err := s.DecodeJSON([]byte(doc), &out)

				// The verdict compared is the ESCAPE GRAMMAR's, not the
				// document's. Some positions reject a perfectly well-escaped
				// string for a SCHEMA reason — no enum symbol may contain an
				// escape at all, a field named `a\nb` is not the field the
				// record declares — so comparing overall accept/reject would
				// make those cells measure the schema and score a correct
				// grammar as a divergence. What the oracle knows is whether
				// the escape is legal, so that is what is asked of both sides:
				// an illegal escape must produce an ESCAPE error, and a legal
				// one must not, whatever the schema then decides.
				gotEscapeErr := err != nil && strings.Contains(err.Error(), "escape")
				if gotEscapeErr != wantReject {
					verb := map[bool]string{true: "an escape error", false: "no escape error"}
					t.Errorf("%s at %s: this package gives %s, encoding/json calls the escape %s.\ndoc=%s err=%v",
						esc.escape, pos.name, verb[gotEscapeErr],
						map[bool]string{true: "invalid", false: "valid"}[wantReject], doc, err)
				}
			})
		}
	}
}

// The matrix above asks whether the verdicts AGREE. Agreement alone is blind to
// a change that moves both sides, and one side is a stdlib this package does not
// control — so the count of rejecting escapes is pinned absolutely as well. If
// the scanner started accepting the unknown six, agreement would break; if some
// future stdlib accepted them too, agreement would hold and this would not.
func TestInvariant_JSONEscapeRejectionCountIsAbsolute(t *testing.T) {
	const wantRejected = 6
	s := avro.MustParse(`"string"`)
	rejected := 0
	for _, esc := range jsonEscapeCases {
		if s.DecodeJSON([]byte(`"a`+esc.escape+`b"`), new(string)) != nil {
			rejected++
		}
	}
	if rejected != wantRejected {
		var got []string
		for _, esc := range jsonEscapeCases {
			if s.DecodeJSON([]byte(`"a`+esc.escape+`b"`), new(string)) != nil {
				got = append(got, esc.escape)
			}
		}
		t.Errorf("%d of %d escapes rejected, want %d (%s)", rejected, len(jsonEscapeCases), wantRejected, fmt.Sprint(got))
	}
}
