package avro

import (
	"bytes"
	"reflect"
	"testing"
)

// A union may not contain the same named type twice (spec, "Unions":
// "Unions may not contain more than one schema with the same type, except
// for the named types record, fixed and enum ... two types with different
// names are permitted"). The duplicate check must be reference-order
// independent: a short-name forward reference and a later inline
// definition of the same type are the same union member, exactly as the
// backward-ordered spelling is.
func TestRegression_UnionForwardRefDuplicateOrderIndependent(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		ok     bool
		// lossyCanonical marks schemas with a null-namespace type nested
		// inside a namespaced scope: the PCF [FULLNAMES] transform writes
		// that type's fullname as a bare name, which re-reads as
		// inheriting the enclosing namespace — so the canonical form does
		// not re-parse. Java's SchemaNormalization emits the identical
		// ambiguity; PCF is a fingerprint surface, not a round-trip
		// surface.
		lossyCanonical bool
	}{
		{
			// forward short-name ref + inline definition: duplicate.
			name: "fwd ref then inline",
			schema: `{"type":"record","name":"R","namespace":"n","fields":[
				{"name":"f","type":["Inner",{"type":"fixed","name":"Inner","size":4}]}
			]}`,
			ok: false,
		},
		{
			// the branch-swapped spelling of the same union: duplicate.
			name: "inline then backward ref",
			schema: `{"type":"record","name":"R","namespace":"n","fields":[
				{"name":"f","type":[{"type":"fixed","name":"Inner","size":4},"Inner"]}
			]}`,
			ok: false,
		},
		{
			// full-name forward ref + inline definition: duplicate.
			name: "full-name fwd ref then inline",
			schema: `{"type":"record","name":"R","namespace":"n","fields":[
				{"name":"f","type":["n.Inner",{"type":"fixed","name":"Inner","size":4}]}
			]}`,
			ok: false,
		},
		{
			// a forward reference whose definition lives in a LATER field is
			// not a duplicate: the union holds the type once.
			name: "fwd ref defined in sibling field",
			schema: `{"type":"record","name":"R","namespace":"n","fields":[
				{"name":"f","type":["null","Inner"]},
				{"name":"g","type":{"type":"fixed","name":"Inner","size":4}}
			]}`,
			ok: true,
		},
		{
			// two distinct named types sharing a short name across
			// namespaces are NOT duplicates.
			name: "same short name distinct namespaces",
			schema: `{"type":"record","name":"R","namespace":"n","fields":[
				{"name":"f","type":[{"type":"fixed","name":"Inner","namespace":"a","size":4},{"type":"fixed","name":"Inner","namespace":"b","size":4}]}
			]}`,
			ok: true,
		},
		{
			// two identically-spelled forward refs resolve to the same
			// type: duplicate (caught after resolution).
			name: "two identical fwd refs",
			schema: `{"type":"record","name":"R","namespace":"n","fields":[
				{"name":"f","type":["Inner","Inner"]},
				{"name":"g","type":{"type":"fixed","name":"Inner","size":4}}
			]}`,
			ok: false,
		},
		{
			// References bind EAGERLY at the point of reference: with the
			// null-namespace Inner already defined (earlier branch) and
			// n.Inner not yet defined, the bare ref binds the null-ns
			// type — a genuine duplicate of the sibling branch. A later
			// definition does not retroactively rebind (old-Java
			// Names.get semantics; only never-resolvable refs defer to
			// finalize).
			name: "bare ref eagerly binds existing null-ns sibling",
			schema: `{"type":"record","name":"R","namespace":"n","fields":[
				{"name":"f","type":[{"type":"fixed","name":"Inner","namespace":"","size":8},"Inner"]},
				{"name":"g","type":{"type":"fixed","name":"Inner","size":4}}
			]}`,
			ok: false,
		},
		{
			// The same union with the ref FIRST: nothing named Inner
			// exists at reference time, so the ref defers to finalize,
			// where in-scope-first binding picks n.Inner (the later
			// sibling field's type) — two distinct types, not a
			// duplicate.
			name: "deferred fwd ref binds in-scope over null-ns sibling",
			schema: `{"type":"record","name":"R","namespace":"n","fields":[
				{"name":"f","type":["Inner",{"type":"fixed","name":"Inner","namespace":"","size":8}]},
				{"name":"g","type":{"type":"fixed","name":"Inner","size":4}}
			]}`,
			ok:             true,
			lossyCanonical: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := Parse(c.schema)
			if c.ok {
				if err != nil {
					t.Fatalf("Parse: %v", err)
				}
				// Every accepted schema's canonical form must re-parse
				// (Canonical() output is what registries store) — except
				// the documented PCF null-namespace lossiness class.
				if !c.lossyCanonical {
					if _, err := Parse(string(s.Canonical())); err != nil {
						t.Fatalf("Parse(s.Canonical()): %v\ncanonical: %s", err, s.Canonical())
					}
				}
				return
			}
			if err == nil {
				t.Fatalf("Parse accepted a union containing the same named type twice\ncanonical: %s", s.Canonical())
			}
		})
	}
}

// TaggedUnions branch names are the RESOLVED full names regardless of
// whether the branch was a forward reference or an in-order reference:
// a named reference is position-independent in Avro, so the tagged
// envelope key and the tagged-map encode acceptance cannot depend on
// where the definition appeared. Binary and JSON must agree.
func TestRegression_UnionForwardRefTaggedNamesResolved(t *testing.T) {
	const fwd = `{"type":"record","name":"R","namespace":"n","fields":[
		{"name":"f","type":["null","Inner"]},
		{"name":"g","type":{"type":"fixed","name":"Inner","size":4}}]}`
	const ord = `{"type":"record","name":"R","namespace":"n","fields":[
		{"name":"g","type":{"type":"fixed","name":"Inner","size":4}},
		{"name":"f","type":["null","Inner"]}]}`

	type rec struct {
		F *[4]byte `avro:"f"`
		G [4]byte  `avro:"g"`
	}
	val := rec{F: &[4]byte{1, 2, 3, 4}, G: [4]byte{9, 9, 9, 9}}

	sf := MustParse(fwd)
	so := MustParse(ord)

	// (a) binary TaggedUnions decode envelope: full name on both schemas.
	envelope := func(s *Schema) any {
		t.Helper()
		wire, err := s.AppendEncode(nil, val)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var out any
		if _, err := s.Decode(wire, &out, TaggedUnions()); err != nil {
			t.Fatalf("decode tagged: %v", err)
		}
		return out.(map[string]any)["f"]
	}
	envF, envO := envelope(sf), envelope(so)
	want := map[string]any{"n.Inner": []byte{1, 2, 3, 4}}
	if !reflect.DeepEqual(envF, want) {
		t.Errorf("forward-ref schema binary envelope: got %#v, want %#v", envF, want)
	}
	if !reflect.DeepEqual(envO, want) {
		t.Errorf("in-order schema binary envelope: got %#v, want %#v", envO, want)
	}

	// (b) JSON TaggedUnions envelope agrees with binary on the fwd schema.
	js, err := sf.EncodeJSON(val)
	if err != nil {
		t.Fatalf("encodeJSON: %v", err)
	}
	var outJ any
	if err := sf.DecodeJSON(js, &outJ, TaggedUnions()); err != nil {
		t.Fatalf("decodeJSON tagged: %v", err)
	}
	if envJ := outJ.(map[string]any)["f"]; !reflect.DeepEqual(envJ, want) {
		t.Errorf("forward-ref schema JSON envelope: got %#v, want %#v", envJ, want)
	}

	// (c) tagged-map binary encode accepts the full name AND the unique
	// short name on both schemas, producing identical wire bytes.
	type recTagged struct {
		F map[string]any `avro:"f"`
		G [4]byte        `avro:"g"`
	}
	wireTyped, err := sf.AppendEncode(nil, val)
	if err != nil {
		t.Fatalf("encode typed: %v", err)
	}
	for _, s := range []*Schema{sf, so} {
		for _, tag := range []string{"n.Inner", "Inner"} {
			got, err := s.AppendEncode(nil, recTagged{F: map[string]any{tag: [4]byte{1, 2, 3, 4}}, G: [4]byte{9, 9, 9, 9}})
			if err != nil {
				t.Errorf("tagged-map encode with key %q: %v", tag, err)
				continue
			}
			if s == sf && !bytes.Equal(got, wireTyped) {
				t.Errorf("tagged-map encode with key %q: wire differs from typed encode", tag)
			}
		}
	}
}

// TagLogicalTypes branch names resolve through forward references too:
// a logical-bearing NAMED branch (a fixed carrying a logical type) tags
// under its NAME — not "<kind>.<logical>" — regardless of reference order.
// This exercises the forward-reference path (finalizeUnionNames), which
// re-derives the branch-name tables after the fwd-ref branch resolves; the
// resolved name must match what an in-order reference produces.
func TestRegression_UnionForwardRefTagLogicalNamesResolved(t *testing.T) {
	const fwd = `{"type":"record","name":"R","namespace":"n","fields":[
		{"name":"f","type":["null","Dec"]},
		{"name":"g","type":{"type":"fixed","name":"Dec","size":4,"logicalType":"decimal","precision":9,"scale":2}}]}`
	s := MustParse(fwd)

	type rec struct {
		F *[4]byte `avro:"f"`
		G [4]byte  `avro:"g"`
	}
	val := rec{F: &[4]byte{0, 0, 0, 1}, G: [4]byte{0, 0, 0, 2}}
	wire, err := s.AppendEncode(nil, val)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out any
	if _, err := s.Decode(wire, &out, TaggedUnions(), TagLogicalTypes()); err != nil {
		t.Fatalf("decode tag-logical: %v", err)
	}
	env, ok := out.(map[string]any)["f"].(map[string]any)
	if !ok {
		t.Fatalf("envelope shape: %#v", out.(map[string]any)["f"])
	}
	// The fixed is defined in namespace "n", so its tagged-union key is the
	// fully-qualified name "n.Dec" — matching goavro's typeName.fullName and
	// Java's getFullName(), both of which qualify with the namespace.
	if _, ok := env["n.Dec"]; !ok {
		t.Errorf("logical tag: got keys %v, want n.Dec (the fixed's fullname)", reflect.ValueOf(env).MapKeys())
	}
}
