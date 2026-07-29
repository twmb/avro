package avro_test

import (
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Union tag tables — one namespace, three consumers, one precedence rule.
//
// A union branch is addressed on the tagged wires by a NAME. Two different
// spellings can produce the same name: the "<kind>.<logicalType>" qualifier
// TagLogicalTypes emits for a primitive-backed logical branch, and the
// "<namespace>.<name>" fullname of a named type. "bytes.decimal" is both the
// qualifier of a decimal-on-bytes branch and the fullname of a fixed named
// "decimal" in namespace "bytes"; every name involved is valid under the
// strict Avro name regex, and the union is legal Avro that the reference
// implementations parse.
//
// Three tables read that one namespace — the JSON emitter, the decoder's
// tagged-map wrap, and the encoder's tagged-map lookup — so all three must
// agree on which branch owns a tag. The oracle here is calibration-free and
// needs no reference implementation: A VALUE'S JSON TAGGED ROUND TRIP MUST
// LAND ON THE BRANCH IT LEFT FROM. The binary branch index is the observable,
// read straight off the wire, so a tag that resolves to a different branch
// shows up as a changed index rather than having to be inferred from a Go
// type.
//
// The second half is the over-correction guard: dropping the qualifier
// everywhere would satisfy the round trip too, and it would silently undo
// TagLogicalTypes. So the unambiguous case is pinned to still emit the
// qualified form.
// ---------------------------------------------------------------------------

// unionBranchIndexOf reads the leading zig-zag varint of an Avro union wire,
// which is the selected branch index.
func unionBranchIndexOf(t *testing.T, wire []byte) int64 {
	t.Helper()
	if len(wire) == 0 {
		t.Fatal("empty wire has no branch index")
	}
	var u uint64
	var shift uint
	for _, b := range wire {
		u |= uint64(b&0x7f) << shift
		if b < 0x80 {
			break
		}
		shift += 7
		if shift > 63 {
			t.Fatalf("branch index varint does not terminate in %x", wire)
		}
	}
	return int64(u>>1) ^ -int64(u&1)
}

// A collision family is a logical branch whose qualifier is spelled exactly
// like a named branch's fullname, given in both declaration orders because
// the tables are built by iterating branches and a last-write-wins map made
// the answer depend on that order.
type tagCollisionFamily struct {
	name      string
	logical   string // the logical-carrying branch, whose qualifier collides
	named     string // the named branch whose fullname is that same spelling
	namedFull string // that fullname, usable as an explicit tag
	values    []any  // values reaching one branch or the other
}

func tagCollisionFamilies() []tagCollisionFamily {
	return []tagCollisionFamily{
		{
			name:      "bytes.decimal",
			logical:   `{"type":"bytes","logicalType":"decimal","precision":20,"scale":2}`,
			named:     `{"type":"fixed","name":"decimal","namespace":"bytes","size":4}`,
			namedFull: "bytes.decimal",
			values:    []any{big.NewRat(1, 4), []byte{1, 2, 3, 4}},
		},
		{
			name:      "string.uuid",
			logical:   `{"type":"string","logicalType":"uuid"}`,
			named:     `{"type":"fixed","name":"uuid","namespace":"string","size":4}`,
			namedFull: "string.uuid",
			values:    []any{"6ba7b810-9dad-11d1-80b4-00c04fd430c8", []byte{1, 2, 3, 4}},
		},
		{
			name:      "int.date",
			logical:   `{"type":"int","logicalType":"date"}`,
			named:     `{"type":"fixed","name":"date","namespace":"int","size":4}`,
			namedFull: "int.date",
			values:    []any{int32(19000), []byte{1, 2, 3, 4}},
		},
	}
}

var tagOptionCombos = []struct {
	name string
	opts []avro.Opt
}{
	{"TaggedUnions", []avro.Opt{avro.TaggedUnions()}},
	{"TaggedUnions+TagLogicalTypes", []avro.Opt{avro.TaggedUnions(), avro.TagLogicalTypes()}},
}

// TestMatrix_UnionTagRoundTripPreservesBranch crosses the collision families
// with branch declaration order, the tag-affecting option combinations, and
// the values that reach each branch — including the tagged-map form, which is
// the only way a caller addresses the named branch explicitly.
func TestMatrix_UnionTagRoundTripPreservesBranch(t *testing.T) {
	var cells int
	for _, fam := range tagCollisionFamilies() {
		for _, order := range []struct {
			name   string
			schema string
		}{
			{"logical-declared-first", fmt.Sprintf(`["null",%s,%s]`, fam.logical, fam.named)},
			{"named-declared-first", fmt.Sprintf(`["null",%s,%s]`, fam.named, fam.logical)},
		} {
			s, err := avro.Parse(order.schema)
			if err != nil {
				t.Fatalf("%s/%s: this union is legal Avro and must parse: %v", fam.name, order.name, err)
			}
			// The tagged-map form addressing the NAMED branch by its exact
			// fullname is part of the value domain: it is the caller's only
			// handle on that branch, and it is the input the encoder's tag
			// lookup resolves.
			values := append([]any{}, fam.values...)
			values = append(values, map[string]any{fam.namedFull: []byte{1, 2, 3, 4}})

			for vi, v := range values {
				for _, combo := range tagOptionCombos {
					cells++
					wire, err := s.Encode(v)
					if err != nil {
						// A value no branch accepts is not a cell; the
						// round trip has nothing to preserve.
						continue
					}
					before := unionBranchIndexOf(t, wire)

					j, err := s.EncodeJSON(v, combo.opts...)
					if err != nil {
						t.Errorf("%s/%s/value#%d/%s: a value the binary wire accepts must encode as JSON: %v",
							fam.name, order.name, vi, combo.name, err)
						continue
					}
					var back any
					if err := s.DecodeJSON(j, &back, combo.opts...); err != nil {
						t.Errorf("%s/%s/value#%d/%s: the schema's OWN JSON output does not decode against it: %v\n  emitted %s",
							fam.name, order.name, vi, combo.name, err, j)
						continue
					}
					after, err := s.Encode(back)
					if err != nil {
						t.Errorf("%s/%s/value#%d/%s: the decoded value does not re-encode: %v\n  emitted %s",
							fam.name, order.name, vi, combo.name, err, j)
						continue
					}
					if got := unionBranchIndexOf(t, after); got != before {
						t.Errorf("%s/%s/value#%d/%s: the JSON tagged round trip MOVED the branch: %d -> %d\n"+
							"  emitted %s\n  the tag it emitted resolves to a different branch than the one that produced it",
							fam.name, order.name, vi, combo.name, before, got, j)
					}
				}
			}
		}
	}
	t.Logf("cells: %d", cells)
}

// TestMatrix_UnionQualifiedTagStillEmittedWhenUnambiguous is the
// over-correction guard. Dropping the "<kind>.<logicalType>" qualifier
// unconditionally would satisfy the round-trip matrix above while silently
// disabling TagLogicalTypes, so the unambiguous case is pinned: with no
// branch owning the qualified spelling as its exact name, the qualified form
// is what gets emitted, and the unqualified form is what gets emitted without
// the option.
func TestMatrix_UnionQualifiedTagStillEmittedWhenUnambiguous(t *testing.T) {
	cells := []struct {
		name       string
		schema     string
		value      any
		wantLogTag string // under TaggedUnions+TagLogicalTypes
		wantStdTag string // under TaggedUnions alone
	}{
		{
			"long timestamp-millis, nothing owns the qualifier",
			`["null",{"type":"long","logicalType":"timestamp-millis"}]`,
			int64(1600000000000), "long.timestamp-millis", "long",
		},
		{
			"bytes decimal, nothing owns the qualifier",
			`["null",{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}]`,
			big.NewRat(1, 4), "bytes.decimal", "bytes",
		},
		{
			"int date, nothing owns the qualifier",
			`["null",{"type":"int","logicalType":"date"}]`,
			int32(19000), "int.date", "int",
		},
		{
			"string uuid, a fixed with an UNRELATED fullname is present",
			`["null",{"type":"string","logicalType":"uuid"},{"type":"fixed","name":"other","namespace":"elsewhere","size":4}]`,
			"6ba7b810-9dad-11d1-80b4-00c04fd430c8", "string.uuid", "string",
		},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			s := avro.MustParse(c.schema)
			j, err := s.EncodeJSON(c.value, avro.TaggedUnions(), avro.TagLogicalTypes())
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if want := `{"` + c.wantLogTag + `":`; !strings.HasPrefix(string(j), want) {
				t.Errorf("qualified tag was dropped where nothing collides with it:\n  got  %s\n  want prefix %s", j, want)
			}
			j2, err := s.EncodeJSON(c.value, avro.TaggedUnions())
			if err != nil {
				t.Fatalf("encode without TagLogicalTypes: %v", err)
			}
			if want := `{"` + c.wantStdTag + `":`; !strings.HasPrefix(string(j2), want) {
				t.Errorf("unqualified tag is wrong:\n  got  %s\n  want prefix %s", j2, want)
			}
		})
	}
}

// TestInvariant_UnionTagOwnerIsUniquePerSchema states the property the tables
// exist to hold, over every union any other cell in this file builds plus the
// ordinary shapes: no two branches may EMIT the same tag under the same
// options. This is the table-level statement of the round-trip property, and
// it fails on a schema for which no value happens to be in the value domain
// above.
func TestInvariant_UnionTagOwnerIsUniquePerSchema(t *testing.T) {
	var schemas []string
	for _, fam := range tagCollisionFamilies() {
		schemas = append(schemas,
			fmt.Sprintf(`["null",%s,%s]`, fam.logical, fam.named),
			fmt.Sprintf(`["null",%s,%s]`, fam.named, fam.logical),
		)
	}
	schemas = append(schemas,
		`["null","int","string"]`,
		`["null",{"type":"long","logicalType":"timestamp-millis"},{"type":"int","logicalType":"date"}]`,
		`["null",{"type":"fixed","name":"F","size":16,"logicalType":"uuid"},{"type":"string","logicalType":"uuid"}]`,
		`[{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]},{"type":"enum","name":"E","symbols":["A"]}]`,
	)
	for _, sc := range schemas {
		t.Run(sc, func(t *testing.T) {
			s, err := avro.Parse(sc)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			root := s.Root()
			if root.Type != "union" {
				t.Fatalf("expected a union, got %q", root.Type)
			}
			for _, tagLogical := range []bool{false, true} {
				seen := map[string]int{}
				for i := range root.Branches {
					tag := unionEmitTagForTest(t, s, i, tagLogical)
					if tag == "" {
						continue // null branch is never wrapped
					}
					if prev, dup := seen[tag]; dup {
						t.Errorf("tagLogical=%v: branches %d and %d both emit the tag %q; a tag must name exactly one branch",
							tagLogical, prev, i, tag)
					}
					seen[tag] = i
				}
			}
		})
	}
}

// unionEmitTagForTest recovers the tag a branch emits using only the public
// API: encode a value onto that branch and read the key of the tagged
// envelope. Branches no probe value reaches report "".
func unionEmitTagForTest(t *testing.T, s *avro.Schema, branch int, tagLogical bool) string {
	t.Helper()
	opts := []avro.Opt{avro.TaggedUnions()}
	if tagLogical {
		opts = append(opts, avro.TagLogicalTypes())
	}
	probes := []any{
		big.NewRat(1, 4), []byte{1, 2, 3, 4}, int32(19000), int64(1600000000000),
		"6ba7b810-9dad-11d1-80b4-00c04fd430c8", "A",
		map[string]any{"x": int32(1)}, [16]byte{}, true, 1.5,
	}
	for _, p := range probes {
		wire, err := s.Encode(p)
		if err != nil || unionBranchIndexOf(t, wire) != int64(branch) {
			continue
		}
		j, err := s.EncodeJSON(p, opts...)
		if err != nil {
			continue
		}
		str := string(j)
		if !strings.HasPrefix(str, `{"`) {
			continue // bare null
		}
		if end := strings.Index(str[2:], `"`); end >= 0 {
			return str[2 : 2+end]
		}
	}
	return ""
}

// TestRegression_UnionNamedTypeSpelledLikeAKindIsRejected pins the sibling
// ruling: the DUPLICATE-BRANCH check keys a named branch by its fullname and
// an unnamed one by its kind, in one namespace, so a null-namespace named type
// spelled like an unnamed complex kind collides with a branch of that kind.
//
// This rejection is deliberate and is NOT the same question as the tag tables
// above. Apache Avro rejects the identical shape for the identical reason
// (UnionSchema keys indexByName by getFullName(), which for an unnamed schema
// returns the kind string), and the Avro JSON encoding would give both
// branches the same envelope name, which is unresolvable ambiguity. A logical
// qualifier colliding with a named branch's fullname is the OTHER case: legal
// Avro that stays accepted, with only the emitted tag degrading.
func TestRegression_UnionNamedTypeSpelledLikeAKindIsRejected(t *testing.T) {
	reject := []string{
		`[{"type":"record","name":"map","fields":[{"name":"x","type":"int"}]},{"type":"map","values":"int"}]`,
		`[{"type":"record","name":"array","fields":[{"name":"x","type":"int"}]},{"type":"array","items":"int"}]`,
		`[{"type":"fixed","name":"map","size":4},{"type":"map","values":"int"}]`,
		`[{"type":"enum","name":"array","symbols":["A"]},{"type":"array","items":"int"}]`,
		// Declaration order does not change the answer.
		`[{"type":"map","values":"int"},{"type":"record","name":"map","fields":[{"name":"x","type":"int"}]}]`,
	}
	for _, sc := range reject {
		if _, err := avro.Parse(sc); err == nil {
			t.Errorf("two branches would share one JSON envelope name, so this must be refused:\n  %s", sc)
		} else if !strings.Contains(err.Error(), "duplicate union type") {
			t.Errorf("refused for the wrong reason (%v):\n  %s", err, sc)
		}
	}
	// Controls. A NAMESPACE keeps the fullname off the kind's spelling, and a
	// name spelled like a kind is fine when no branch of that kind is present.
	accept := []string{
		`[{"type":"record","name":"map","namespace":"ns","fields":[{"name":"x","type":"int"}]},{"type":"map","values":"int"}]`,
		`[{"type":"record","name":"array","namespace":"ns","fields":[{"name":"x","type":"int"}]},{"type":"array","items":"int"}]`,
		`[{"type":"record","name":"map","fields":[{"name":"x","type":"int"}]},"int"]`,
		`[{"type":"record","name":"array","fields":[{"name":"x","type":"int"}]},"int"]`,
	}
	for _, sc := range accept {
		if _, err := avro.Parse(sc); err != nil {
			t.Errorf("no two branches share an envelope name here, so this must parse: %v\n  %s", err, sc)
		}
	}
}

// TestRegression_ResolvedWriterUnionEmitsNoTag is the pinned control behind
// the one site that still builds its wrap tables from the raw branch names
// rather than the collision-aware tag: a writer-only union resolved against a
// NON-union reader. Its tables are never read, because the reader has no union
// to dispatch through and the wrap is suppressed outright — so leaving them
// raw is safe only for as long as that suppression holds, which is what this
// asserts. If the wrap is ever enabled there, the decoded value gains an
// envelope and this fails, which is the signal to route that site through the
// same tag rule as the others.
func TestRegression_ResolvedWriterUnionEmitsNoTag(t *testing.T) {
	writer := avro.MustParse(`["int","long"]`)
	reader := avro.MustParse(`"long"`)
	res, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	wire, err := writer.Encode(int32(7))
	if err != nil {
		t.Fatalf("writer encode: %v", err)
	}
	for _, combo := range tagOptionCombos {
		var back any
		if _, err := res.Decode(wire, &back, combo.opts...); err != nil {
			t.Fatalf("%s: resolved decode: %v", combo.name, err)
		}
		if m, wrapped := back.(map[string]any); wrapped {
			t.Errorf("%s: a writer-only union resolved to a non-union reader must decode BARE, got the envelope %v",
				combo.name, m)
		}
	}
}
