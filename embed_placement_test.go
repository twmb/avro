package avro

// Embedded-field name collisions: WHERE the decision is made.
//
// Two implementations answer "which of two same-named promoted fields wins,
// and when is the collision ambiguous?" — collectFields, for SchemaFor, and
// typeFieldMapping, the shared field map for encode and decode. They agree on
// the RULE. What this file guards is that they agree on where the rule RUNS.
//
// The rule ranges over the whole collected field set: shallowest depth wins,
// and only a tie at the winning depth is ambiguous. A resolution step that
// ranges over the whole set but is written as the trailing block of the
// RECURSIVE collector runs once per level instead of once per type, on a
// partial set — so a collision one level below the root is decided before the
// level that resolves it has been read, and any index the step resolves is in
// the root's coordinate space while its receiver is the nested type.
//
// No verdict-comparison net can see that: at the root both placements agree.
// The discriminating observation is the SAME construct at several nesting
// depths, which is the axis this matrix drives.
//
// The oracle is Go itself. reflect.Type.FieldByName implements the language's
// promotion rule and reports an ambiguous promoted name by returning false;
// it is placement-blind by construction, so it decides every untagged cell
// here without reference to anything this package does.

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// ---------- the shapes ----------
//
// epLeaf's V is reachable through two sibling embed paths, which is what makes
// epCollide's V a genuine same-depth ambiguity. Everything below places that
// one construct at a different distance from the root.

type epLeaf struct{ V int }

type epWrapA struct{ epLeaf }
type epWrapB struct{ epLeaf }

// epCollide: V promoted from two paths at equal depth — ambiguous.
type epCollide struct {
	epWrapA
	epWrapB
}

type epCollideD1 struct{ epCollide }
type epCollideD2 struct{ epCollideD1 }
type epCollideD3 struct{ epCollideD2 }

// epResolved: the same ambiguity with a shallower V that resolves it. Go
// promotes the shallow one; encoding/json marshals it.
type epResolved struct {
	epCollide
	V int
}

type epResolvedD1 struct{ epResolved }
type epResolvedD2 struct{ epResolvedD1 }
type epResolvedD3 struct{ epResolvedD2 }

// epRootResolves is the sharpest cell: the ambiguity is three levels down and
// the field that resolves it is at the ROOT, so a decision taken at the
// collision's own level cannot possibly see it.
type epRootResolves struct {
	epCollideD2
	V int
}

// Pointer carrier: the promotion path crosses an embedded *struct.
type epWrapPA struct{ *epLeaf }
type epWrapPB struct{ *epLeaf }
type epCollideP struct {
	epWrapPA
	epWrapPB
}
type epCollidePD1 struct{ epCollideP }
type epResolvedP struct {
	epCollideP
	V int
}
type epResolvedPD1 struct{ epResolvedP }

// Tag tier: the collision exists only in AVRO name space (the Go names
// differ), so Go has no opinion and the package's documented tiebreaker
// decides — tagged beats untagged at equal depth. Placement must not change
// that either.
type epTagPlain struct{ Shared int32 }
type epTagNamed struct {
	Renamed int32 `avro:"Shared"`
}
type epTagCollide struct {
	epTagPlain
	epTagNamed
}
type epTagCollideD1 struct{ epTagCollide }
type epTagCollideD2 struct{ epTagCollideD1 }

// ---------- the matrix ----------

type epCell struct {
	name  string
	typ   reflect.Type
	depth int // how far below the root the colliding pair sits
}

// epUntagged are the cells Go's own promotion rule decides.
func epUntagged() []epCell {
	return []epCell{
		{"struct/collide/d0", reflect.TypeFor[epCollide](), 0},
		{"struct/collide/d1", reflect.TypeFor[epCollideD1](), 1},
		{"struct/collide/d2", reflect.TypeFor[epCollideD2](), 2},
		{"struct/collide/d3", reflect.TypeFor[epCollideD3](), 3},
		{"struct/resolved/d0", reflect.TypeFor[epResolved](), 0},
		{"struct/resolved/d1", reflect.TypeFor[epResolvedD1](), 1},
		{"struct/resolved/d2", reflect.TypeFor[epResolvedD2](), 2},
		{"struct/resolved/d3", reflect.TypeFor[epResolvedD3](), 3},
		{"struct/root-resolves-deep-collision", reflect.TypeFor[epRootResolves](), 3},
		{"pointer/collide/d0", reflect.TypeFor[epCollideP](), 0},
		{"pointer/collide/d1", reflect.TypeFor[epCollidePD1](), 1},
		{"pointer/resolved/d0", reflect.TypeFor[epResolvedP](), 0},
		{"pointer/resolved/d1", reflect.TypeFor[epResolvedPD1](), 1},
	}
}

// TestMatrix_EmbedCollisionVerdictIsPlacementInvariant is the class
// elimination. Every cell is the same collision at a different distance from
// the root; Go decides each one, and both of this package's answerers must
// return Go's verdict at every distance.
func TestMatrix_EmbedCollisionVerdictIsPlacementInvariant(t *testing.T) {
	for _, c := range epUntagged() {
		t.Run(c.name, func(t *testing.T) {
			// The oracle: Go's own promotion. false means "ambiguous
			// selector", which is a compile error for a program that writes
			// x.V — the exact condition this package reports as a duplicate
			// field name.
			_, goResolves := c.typ.FieldByName("V")

			cfErr := epCollectErr(t, c.typ)
			tfmErr := epMappingErr(t, c.typ)

			if goResolves {
				if cfErr != nil {
					t.Errorf("SchemaFor's collector rejects a type Go promotes unambiguously (x.V compiles): %v", cfErr)
				}
				if tfmErr != nil {
					t.Errorf("the runtime field map rejects a type Go promotes unambiguously (x.V compiles): %v", tfmErr)
				}
			} else {
				if cfErr == nil {
					t.Errorf("SchemaFor's collector accepts a type whose V is an ambiguous selector in Go")
				}
				if tfmErr == nil {
					t.Errorf("the runtime field map accepts a type whose V is an ambiguous selector in Go")
				}
			}
			// The two answerers must not merely both be right about Go; they
			// must agree with each other, which is what makes a schema built
			// by SchemaFor usable by Encode and Decode.
			if (cfErr == nil) != (tfmErr == nil) {
				t.Errorf("the two answerers disagree: collector err=%v, runtime field map err=%v", cfErr, tfmErr)
			}
		})
	}
}

// TestMatrix_EmbedCollisionErrorNamesTheCollidingFields pins the other half of
// the placement fact: the error is built by resolving field INDEX PATHS, and
// an index path accumulated from the root only denotes a field when it is
// resolved against the root. Reported against any other type it names a
// different field, or steps into a non-struct and panics.
func TestMatrix_EmbedCollisionErrorNamesTheCollidingFields(t *testing.T) {
	for _, c := range epUntagged() {
		if _, ok := c.typ.FieldByName("V"); ok {
			continue // no error to inspect
		}
		t.Run(c.name, func(t *testing.T) {
			err := epCollectErr(t, c.typ)
			if err == nil {
				t.Fatalf("want a duplicate-field error")
			}
			msg := err.Error()
			// The colliding Go fields are both named V, and the type the
			// caller asked about is the one that must be named.
			if !strings.Contains(msg, `"V" and "V"`) {
				t.Errorf("error names the wrong Go fields: %s", msg)
			}
			if !strings.Contains(msg, c.typ.String()) {
				t.Errorf("error blames %s, but the type asked about is %s: %s", "another type", c.typ, msg)
			}
		})
	}
}

// TestMatrix_EmbedTagTierIsPlacementInvariant covers the tier Go has no
// opinion about: the collision is in Avro name space only, so the package's
// documented tiebreaker decides, and it must decide the same way wherever the
// pair sits.
func TestMatrix_EmbedTagTierIsPlacementInvariant(t *testing.T) {
	cells := []epCell{
		{"tag/d0", reflect.TypeFor[epTagCollide](), 0},
		{"tag/d1", reflect.TypeFor[epTagCollideD1](), 1},
		{"tag/d2", reflect.TypeFor[epTagCollideD2](), 2},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			fields, err := collectFields(c.typ, make(map[reflect.Type]bool))
			if err != nil {
				t.Fatalf("tagged beats untagged at equal depth, so this resolves: %v", err)
			}
			var got []string
			for _, f := range fields {
				got = append(got, f.name)
			}
			if len(got) != 1 || got[0] != "Shared" {
				t.Fatalf("want exactly one field %q; got %v", "Shared", got)
			}
			// The winner must be the TAGGED field, at every depth: the
			// runtime map selects by index path, so ask it which Go field it
			// picked rather than trusting the name.
			m, err := typeFieldMapping([]string{"Shared"}, nil, c.typ)
			if err != nil {
				t.Fatalf("runtime field map: %v", err)
			}
			ft := fieldTypeByIndex(c.typ, m.indices[0])
			if ft.Kind() != reflect.Int32 {
				t.Fatalf("runtime map selected a %s field; the tagged winner is int32", ft)
			}
			sf := epFieldByIndexPath(c.typ, m.indices[0])
			if sf.Name != "Renamed" {
				t.Errorf("runtime map selected Go field %q; the tagged field %q wins at equal depth", sf.Name, "Renamed")
			}
		})
	}
}

// TestRegression_EmbedCollisionBelowRootDoesNotPanic is the public-entry
// pin. SchemaFor is generic, so these are written out rather than generated;
// the panic they lock is a reflect index path resolved against the wrong
// type, and it needs no collision at the root to fire.
func TestRegression_EmbedCollisionBelowRootDoesNotPanic(t *testing.T) {
	cases := []struct {
		name     string
		fn       func() (*Schema, error)
		wantErr  bool
		goResolv bool
	}{
		{"collide-at-root", func() (*Schema, error) { return SchemaFor[epCollide]() }, true, false},
		{"collide-one-below-root", func() (*Schema, error) { return SchemaFor[epCollideD1]() }, true, false},
		{"collide-three-below-root", func() (*Schema, error) { return SchemaFor[epCollideD3]() }, true, false},
		{"resolved-at-root", func() (*Schema, error) { return SchemaFor[epResolved]() }, false, true},
		{"resolved-one-below-root", func() (*Schema, error) { return SchemaFor[epResolvedD1]() }, false, true},
		{"root-resolves-deep-collision", func() (*Schema, error) { return SchemaFor[epRootResolves]() }, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("SchemaFor panicked: %v", r)
				}
			}()
			s, err := c.fn()
			switch {
			case c.wantErr && err == nil:
				t.Errorf("want a duplicate-field error, got schema %s", s.String())
			case !c.wantErr && err != nil:
				t.Errorf("want a schema, got error: %v", err)
			}
		})
	}
}

// TestRegression_EmbedResolvedBelowRootRoundTrips pins the consequence a
// caller sees: a type whose deep collision is resolved by a shallower field
// is one Go promotes, encoding/json marshals, and this package's own encoder
// already handles — so SchemaFor must produce a schema for it, and that
// schema must round-trip the promoted value.
func TestRegression_EmbedResolvedBelowRootRoundTrips(t *testing.T) {
	// A panic here would take the binary down and hide every other result,
	// and the failure being pinned is reachable as one.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked instead of building a schema: %v", r)
		}
	}()
	var in epRootResolves
	in.V = 7

	// Go and encoding/json both resolve V to the shallow field.
	if in.V != 7 {
		t.Fatal("unreachable: the selector must compile")
	}
	jb, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("encoding/json: %v", err)
	}
	if !strings.Contains(string(jb), `"V":7`) {
		t.Fatalf("encoding/json promoted a different V: %s", jb)
	}

	s, err := SchemaFor[epRootResolves]()
	if err != nil {
		t.Fatalf("SchemaFor rejected a type Go and encoding/json both resolve: %v", err)
	}
	b, err := s.Encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out epRootResolves
	if _, err := s.Decode(b, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.V != 7 {
		t.Errorf("round trip put the value in a different field: got V=%d, want 7", out.V)
	}
}

// ---------- helpers ----------

func epCollectErr(t *testing.T, typ reflect.Type) (err error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("PANIC: %v", r)
			t.Errorf("collectFields panicked on %s: %v", typ, r)
		}
	}()
	_, err = collectFields(typ, make(map[reflect.Type]bool))
	return err
}

func epMappingErr(t *testing.T, typ reflect.Type) (err error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("PANIC: %v", r)
			t.Errorf("typeFieldMapping panicked on %s: %v", typ, r)
		}
	}()
	_, err = typeFieldMapping([]string{"V"}, nil, typ)
	return err
}

// epFieldByIndexPath walks an index path the way the encoders do, so the test
// reads the same coordinate space they do.
func epFieldByIndexPath(t reflect.Type, index []int) reflect.StructField {
	var sf reflect.StructField
	for _, i := range index {
		if t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		sf = t.Field(i)
		t = sf.Type
	}
	return sf
}
