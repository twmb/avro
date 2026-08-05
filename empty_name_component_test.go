package avro_test

import (
	"bytes"
	"fmt"
	"hash/crc64"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// The empty string is a legal NAME COMPONENT.
//
// [avro.WithLaxNames] takes a caller-supplied validator and calls it per name
// component; a validator that accepts "" makes the empty string a legal
// record name, field name, namespace component, enum symbol, or alias. The
// package relies on that being carriable: its own internal re-parses of
// library-produced schema text use an accept-everything validator precisely so
// a name the caller's validator accepted cannot be rejected later.
//
// That makes "" a value like any other, and it forbids a common shortcut:
// using a name string's ZERO VALUE as an absence sentinel. A guard written as
// `claimedName[i] != ""` cannot tell "no one claimed slot i" from "the field
// named "" claimed slot i", so it silently skips its own check. Presence and
// identity have to be separate variables.
//
// The nets below put "" in every name-shaped position and hold the package to
// invariants that need no reference implementation:
//
//   - CheckCompatibility and Resolve must AGREE. They are two independent
//     implementations of the same compatibility rules, so they are each
//     other's oracle; a disagreement is a defect regardless of which one is
//     right. Resolve calls CheckCompatibility first, so the only reachable
//     disagreement is "CheckCompatibility accepts, Resolve rejects" — a caller
//     using CheckCompatibility as an admission gate is told a pair is fine and
//     then fails at Resolve.
//   - A schema's own String() must re-parse, and Root().Schema() must rebuild,
//     to the same canonical form and fingerprint.
//
// Every schema here needs the lax validator to parse at all, which is exactly
// the axis the ordinary compatibility corpora hold constant.
// ---------------------------------------------------------------------------

var laxAny = avro.WithLaxNames(func(string) error { return nil })

func fingerprintOf(t *testing.T, s *avro.Schema) []byte {
	t.Helper()
	return s.Fingerprint(crc64.New(crc64.MakeTable(crc64.ECMA)))
}

// emptyNamePositions places "" in one name-shaped position each.
func emptyNamePositions() []struct{ name, schema string } {
	return []struct{ name, schema string }{
		{"record name", `{"type":"record","name":"","fields":[{"name":"f","type":"int"}]}`},
		{"field name", `{"type":"record","name":"R","fields":[{"name":"","type":"int"}]}`},
		{"enum name", `{"type":"enum","name":"","symbols":["A"]}`},
		{"fixed name", `{"type":"fixed","name":"","size":4}`},
		{"namespace component", `{"type":"record","name":"R","namespace":"a..b","fields":[{"name":"f","type":"int"}]}`},
		{"trailing dot fullname", `{"type":"record","name":"a.","fields":[{"name":"f","type":"int"}]}`},
		{"nested empty-named record", `{"type":"record","name":"Outer","fields":[{"name":"f","type":{"type":"record","name":"","fields":[{"name":"g","type":"int"}]}}]}`},
		{"enum symbol", `{"type":"enum","name":"E","symbols":[""]}`},
		{"field alias", `{"type":"record","name":"R","fields":[{"name":"f","type":"int","aliases":[""]}]}`},
		{"type alias", `{"type":"record","name":"R","aliases":[""],"fields":[{"name":"f","type":"int"}]}`},
		{"empty name and empty namespace", `{"type":"record","name":"","namespace":"","fields":[{"name":"f","type":"int"}]}`},
		{"empty-named record inside a union", `["null",{"type":"record","name":"","fields":[{"name":"f","type":"int"}]}]`},
		{"empty-named field inside a union branch", `["null",{"type":"record","name":"R","fields":[{"name":"","type":"int"}]}]`},
	}
}

// TestMatrix_EmptyNameComponentSelfConsistency holds each position to the two
// self-consistency invariants: the emitted text and the rebuilt metadata tree
// are the schema's own claims about itself, so both must reproduce it.
func TestMatrix_EmptyNameComponentSelfConsistency(t *testing.T) {
	for _, p := range emptyNamePositions() {
		t.Run(p.name, func(t *testing.T) {
			s, err := avro.Parse(p.schema, laxAny)
			if err != nil {
				t.Fatalf("a permissive validator must accept this name: %v", err)
			}
			text, canon, finger := s.String(), s.Canonical(), fingerprintOf(t, s)

			re, err := avro.Parse(text, laxAny)
			if err != nil {
				t.Fatalf("String() emitted text that does not re-parse: %v\n  emitted %s", err, text)
			}
			if !bytes.Equal(re.Canonical(), canon) {
				t.Errorf("canonical form drifts across the String() round trip:\n  before %s\n  after  %s", canon, re.Canonical())
			}
			if !bytes.Equal(fingerprintOf(t, re), finger) {
				t.Errorf("fingerprint drifts across the String() round trip (emitted %s)", text)
			}

			// SchemaNode.Schema documents that the options the original parse
			// used must be supplied again, so the validator is passed here too.
			root := s.Root()
			rb, err := root.Schema(laxAny)
			if err != nil {
				t.Fatalf("Root().Schema() cannot rebuild the tree it produced: %v", err)
			}
			if !bytes.Equal(rb.Canonical(), canon) {
				t.Errorf("canonical form drifts across the metadata rebuild:\n  before %s\n  after  %s", canon, rb.Canonical())
			}
			if !bytes.Equal(fingerprintOf(t, rb), finger) {
				t.Errorf("fingerprint drifts across the metadata rebuild")
			}
		})
	}
}

// TestMatrix_EmptyNameComponentCompatResolveAgree crosses every position with
// the evolution shapes, asserting the cross-path agreement. The corpora that
// already assert this agreement all parse with the default validator, so no
// cell of theirs can carry an empty name component.
func TestMatrix_EmptyNameComponentCompatResolveAgree(t *testing.T) {
	var cells int
	for _, p := range emptyNamePositions() {
		s, err := avro.Parse(p.schema, laxAny)
		if err != nil {
			t.Fatalf("%s: %v", p.name, err)
		}
		for _, pair := range []struct {
			name           string
			writer, reader *avro.Schema
		}{
			{"identity", s, s},
		} {
			cells++
			compatErr := avro.CheckCompatibility(pair.writer, pair.reader)
			_, resolveErr := avro.Resolve(pair.writer, pair.reader)
			if (compatErr == nil) != (resolveErr == nil) {
				t.Errorf("%s/%s: CheckCompatibility and Resolve disagree\n  CheckCompatibility: %v\n  Resolve:            %v",
					p.name, pair.name, compatErr, resolveErr)
			}
			if compatErr != nil {
				t.Errorf("%s/%s: a schema is not compatible with itself: %v", p.name, pair.name, compatErr)
			}
		}
	}
	t.Logf("cells: %d", cells)
}

// TestMatrix_EmptyFieldNameAliasClaimAgreement is the shape that a name-valued
// absence sentinel hides. Two writer fields resolve to ONE reader slot — one
// by the reader field's name, one by its alias — which both the resolver and
// the compatibility check must refuse. Crossing the reader field's name with
// the empty string, and crossing the DECLARATION ORDER of the two writer
// fields, is what separates a real presence flag from a sentinel: with the
// sentinel, the collision is detected only when the ""-named writer field
// comes second, because by then some other name has overwritten the slot.
func TestMatrix_EmptyFieldNameAliasClaimAgreement(t *testing.T) {
	var cells int
	for _, readerName := range []string{"", "a"} {
		for _, aliasName := range []string{"b", ""} {
			if readerName == aliasName {
				continue // a field name colliding with its own alias is rejected at parse
			}
			reader := fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":%q,"type":"int","aliases":[%q]}]}`,
				readerName, aliasName)
			r, err := avro.Parse(reader, laxAny)
			if err != nil {
				t.Fatalf("reader %s: %v", reader, err)
			}
			for _, order := range []struct {
				name  string
				first string
				last  string
			}{
				{"name-then-alias", readerName, aliasName},
				{"alias-then-name", aliasName, readerName},
			} {
				cells++
				writer := fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":%q,"type":"int"},{"name":%q,"type":"int"}]}`,
					order.first, order.last)
				w, err := avro.Parse(writer, laxAny)
				if err != nil {
					t.Fatalf("writer %s: %v", writer, err)
				}
				compatErr := avro.CheckCompatibility(w, r)
				_, resolveErr := avro.Resolve(w, r)
				where := fmt.Sprintf("reader field %q alias %q / writer %s", readerName, aliasName, order.name)
				if (compatErr == nil) != (resolveErr == nil) {
					t.Errorf("%s: CheckCompatibility and Resolve disagree\n  CheckCompatibility: %v\n  Resolve:            %v",
						where, compatErr, resolveErr)
				}
				if compatErr == nil {
					t.Errorf("%s: two writer fields resolve to one reader field and neither API refused it", where)
				}
			}
		}
	}
	t.Logf("cells: %d", cells)
}

// TestMatrix_EmptyFieldNameNonCollidingControl is the boundary: the same
// shapes with only ONE writer field claiming the reader slot must be ACCEPTED,
// by both APIs. Without this, refusing every writer that mentions an empty
// name would satisfy the matrix above.
func TestMatrix_EmptyFieldNameNonCollidingControl(t *testing.T) {
	for _, readerName := range []string{"", "a"} {
		for _, writerName := range []string{"", "a", "b"} {
			reader := fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":%q,"type":"int","aliases":["b"]}]}`, readerName)
			writer := fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":%q,"type":"int"}]}`, writerName)
			r, err := avro.Parse(reader, laxAny)
			if err != nil {
				t.Fatalf("reader %s: %v", reader, err)
			}
			w, err := avro.Parse(writer, laxAny)
			if err != nil {
				t.Fatalf("writer %s: %v", writer, err)
			}
			where := fmt.Sprintf("reader field %q alias \"b\" / single writer field %q", readerName, writerName)
			compatErr := avro.CheckCompatibility(w, r)
			_, resolveErr := avro.Resolve(w, r)
			if (compatErr == nil) != (resolveErr == nil) {
				t.Errorf("%s: CheckCompatibility and Resolve disagree\n  CheckCompatibility: %v\n  Resolve:            %v",
					where, compatErr, resolveErr)
			}
			// One writer field can claim at most one reader slot, so the only
			// reason to refuse is a genuinely absent required reader field.
			matches := writerName == readerName || writerName == "b"
			if matches && compatErr != nil {
				t.Errorf("%s: a single claiming writer field must be accepted: %v", where, compatErr)
			}
		}
	}
}
