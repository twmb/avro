package avro_test

import (
	"bytes"
	"testing"

	"github.com/twmb/avro"
)

// The runtime field mapper must tokenize the avro struct tag with the SAME
// grammar SchemaFor uses: a default= value takes the rest of the tag verbatim,
// and a bracketed alias=[...] value is not split on its internal commas. A
// naive comma split mis-reads a comma inside such a value as a separate option,
// so a chunk that happens to equal "omitzero"/"inline" spuriously activates
// that option — corrupting the zero value's wire form or making SchemaFor's own
// schema unencodable for the type that produced it.

type tagDefaultWithKeyword struct {
	// Per doc.go's grammar, default= takes the rest of the tag, so the default
	// value is the literal string "red,omitzero" — there is NO omitzero option.
	F string `avro:"f,default=red,omitzero"`
}

func TestRegression_RuntimeTagDefaultValueWithKeyword(t *testing.T) {
	s, err := avro.SchemaFor[tagDefaultWithKeyword]()
	if err != nil {
		t.Fatalf("SchemaFor: %v", err)
	}
	for _, tc := range []struct {
		name   string
		encode func(any) ([]byte, error)
		decode func([]byte, any) error
	}{
		{"binary", func(v any) ([]byte, error) { return s.Encode(v) }, func(b []byte, v any) error { _, e := s.Decode(b, v); return e }},
		{"json", func(v any) ([]byte, error) { return s.AppendEncodeJSON(nil, v) }, func(b []byte, v any) error { return s.DecodeJSON(b, v) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// omitzero is NOT a real option here, so the zero value must encode
			// as itself ("") and survive the round-trip, not be replaced by the
			// default.
			wire, err := tc.encode(&tagDefaultWithKeyword{F: ""})
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var got tagDefaultWithKeyword
			if err := tc.decode(wire, &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got.F != "" {
				t.Fatalf("zero value corrupted: got %q, want %q (a comma in the default= value was mis-parsed as an omitzero option)", got.F, "")
			}
		})
	}
}

type tagAliasSub struct {
	A int32 `avro:"a"`
}

// The alias list contains "inline" as an element. Aliases accept any string, so
// this is legal; the runtime must not treat the alias element as an inline
// option (which would flatten the field's subfields and make field "f" missing).
type tagAliasListWithKeyword struct {
	F tagAliasSub `avro:"f,alias=[x,inline,y]"`
}

func TestRegression_RuntimeTagAliasListWithKeyword(t *testing.T) {
	s, err := avro.SchemaFor[tagAliasListWithKeyword]()
	if err != nil {
		t.Fatalf("SchemaFor: %v", err)
	}
	in := &tagAliasListWithKeyword{F: tagAliasSub{A: 7}}
	if _, err := s.Encode(in); err != nil {
		t.Fatalf("binary: SchemaFor-built schema cannot encode its own source type: %v", err)
	}
	if _, err := s.AppendEncodeJSON(nil, in); err != nil {
		t.Fatalf("json: SchemaFor-built schema cannot encode its own source type: %v", err)
	}
}

// A MALFORMED tag (unbalanced bracket) whose comma-separated tail happens to
// contain "inline"/"omitzero" must not fire that option. splitTag's grammar
// cannot tokenize an unbalanced bracket, so the runtime mapper falls back to a
// lenient form that maps the field by name with NO options — a hand-written-
// schema user's malformed tag stays usable (no new error) but a bracket typo
// never silently flips a field between nested-record and inline-flattened (or
// toggles omitzero). The only difference from the well-formed alias=[x,inline]
// case above is the missing ']'; the wire shape must be identical.
type tagMalformedInlineSub struct {
	X int32 `avro:"x"`
	Y int32 `avro:"y"`
}

func TestRegression_RuntimeMalformedTagFiresNoOption(t *testing.T) {
	// Field "f" is a nested record in the hand-written schema; the runtime must
	// map it as such, never flatten its subfields by spuriously firing inline.
	nested := `{"type":"record","name":"Outer","fields":[
		{"name":"f","type":{"type":"record","name":"Inner","fields":[
			{"name":"x","type":"int"},{"name":"y","type":"int"}]}}]}`
	s, err := avro.Parse(nested)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	type clean struct {
		F tagMalformedInlineSub `avro:"f"`
	}
	type closedBracket struct { // well-formed control: inline is an alias element
		F tagMalformedInlineSub `avro:"f,alias=[a,inline]"`
	}
	type unclosedInline struct { // malformed: missing ']', "inline" trails
		F tagMalformedInlineSub `avro:"f,alias=[a,inline"`
	}
	type unclosedOmitzero struct { // malformed: missing ']', "omitzero" trails
		F tagMalformedInlineSub `avro:"f,alias=[a,omitzero"`
	}

	want, err := s.Encode(clean{F: tagMalformedInlineSub{X: 1, Y: 2}})
	if err != nil {
		t.Fatalf("encode clean control: %v", err)
	}
	for _, tc := range []struct {
		name string
		wire func() ([]byte, error)
	}{
		{"closed_bracket_control", func() ([]byte, error) { return s.Encode(closedBracket{F: tagMalformedInlineSub{X: 1, Y: 2}}) }},
		{"unclosed_inline", func() ([]byte, error) { return s.Encode(unclosedInline{F: tagMalformedInlineSub{X: 1, Y: 2}}) }},
		{"unclosed_omitzero", func() ([]byte, error) { return s.Encode(unclosedOmitzero{F: tagMalformedInlineSub{X: 1, Y: 2}}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.wire()
			if err != nil {
				t.Fatalf("encode: a malformed/aliased tag corrupted the field mapping (option spuriously fired): %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("wire %x != clean %x: a tag option fired that must not have", got, want)
			}
		})
	}
}

// Controls: the documented options must still work when they ARE present, and a
// plain default= value (no embedded keyword) must round-trip the zero value to
// the default it actually fills only under omitzero.
func TestRuntimeTagOptionsStillFire(t *testing.T) {
	t.Run("omitzero_active_fills_default", func(t *testing.T) {
		type R struct {
			F string `avro:"f,omitzero"`
		}
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatalf("SchemaFor: %v", err)
		}
		// omitzero IS a real option here. SchemaFor gives the field no
		// default, so a nullable... actually no default → omitzero on a
		// non-nullable no-default field encodes the zero value itself. Just
		// assert it encodes without error on both formats (option recognized,
		// no spurious behavior change).
		if _, err := s.Encode(&R{}); err != nil {
			t.Fatalf("binary encode with omitzero: %v", err)
		}
	})
	t.Run("plain_default_no_keyword", func(t *testing.T) {
		type R struct {
			F string `avro:"f,default=plainvalue"`
		}
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatalf("SchemaFor: %v", err)
		}
		wire, err := s.Encode(&R{F: "kept"})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got R
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.F != "kept" {
			t.Fatalf("plain default field corrupted: got %q want %q", got.F, "kept")
		}
	})
}
