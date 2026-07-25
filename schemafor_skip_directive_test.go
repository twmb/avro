package avro

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// The avro struct tag is validated on two structurally distinct paths in
// collectFields: the NAMED-FIELD path (an ordinary field, and an anonymous
// non-struct field, which falls through to it) and the ANONYMOUS EMBEDDED
// STRUCT path, which handles its own tag before the named path is reached.
// A validation that lives on only one path is a hole: the same tag string
// then means different things depending on where it is written.
//
// The census below is the executable form of that claim. Every row is a tag
// whose verdict must NOT depend on which path reads it, exercised through
// both a named field and an anonymous embed of the same struct type, under
// strict AND lax name validation — lax matters because a guard that only
// appears to work by way of Avro's name grammar (a field named "-" is not a
// valid Avro name) stops working the moment a caller supplies their own
// validator via WithLaxNames.

type skipCensusInner struct{ A string }

// skipCensusStruct builds `struct { F skipCensusInner "tag"; G string }` when
// embed is false, and `struct { skipCensusInner "tag"; G string }` when true.
func skipCensusStruct(tag string, embed bool) reflect.Type {
	first := reflect.StructField{
		Name: "F",
		Type: reflect.TypeFor[skipCensusInner](),
		Tag:  reflect.StructTag(tag),
	}
	if embed {
		first.Name = "SkipCensusInner"
		first.Type = reflect.TypeFor[skipCensusInner]()
		first.Anonymous = true
	}
	return reflect.StructOf([]reflect.StructField{
		first,
		{Name: "G", Type: reflect.TypeFor[string]()},
	})
}

// skipCensusBuild runs the SchemaFor pipeline over a runtime-built struct.
func skipCensusBuild(t *testing.T, tag string, embed bool, opts ...SchemaOpt) (*Schema, error) {
	t.Helper()
	st := skipCensusStruct(tag, embed)
	fields := make([]reflect.StructField, st.NumField())
	for i := range fields {
		fields[i] = st.Field(i)
	}
	return schemaForScopeCell(t, fields, "", nil, opts...)
}

// TestMatrix_SchemaForTagGuardPathCensus is the pattern-14a census: for every
// tag validation on the named-field path, the anonymous-embed path must reach
// the same verdict. A row's wantErr is the substring the error must name; an
// empty wantErr means the tag is valid and the build must succeed.
func TestMatrix_SchemaForTagGuardPathCensus(t *testing.T) {
	census := []struct {
		guard   string
		tag     string
		wantErr string
	}{
		{"exact skip directive", `avro:"-"`, ""},
		{"skip directive is exact-match only (options)", `avro:"-,omitzero"`, "exact-match only"},
		{"skip directive is exact-match only (suffix)", `avro:"-foo"`, "exact-match only"},
		{"splitTag unclosed bracket", `avro:"X,alias=[a"`, "unclosed"},
		{"splitTag unexpected close", `avro:"X,alias=a]"`, "unexpected"},
		{"inline with an explicit name", `avro:"X,inline"`, "inline is incompatible with an explicit field name"},
		{"inline with another option", `avro:",inline,omitzero"`, "inline is incompatible with option"},
		{"alias empty brackets", `avro:"X,alias=[]"`, "empty brackets"},
		{"alias empty element", `avro:"X,alias=[a,]"`, "empty element"},
		{"type-alias empty brackets", `avro:"X,type-alias=[]"`, "empty brackets"},
		{"decimal trailing junk", `avro:"X,decimal(1,2,3)"`, "invalid decimal tag"},
		{"unknown tag option", `avro:"X,bogusopt"`, "unknown avro tag option"},
		{"uuid on an incompatible Go type", `avro:"X,uuid"`, "uuid logical type"},
		{"decimal on an incompatible Go type", `avro:"X,decimal(4,2)"`, "decimal logical type requires"},
	}

	lax := WithLaxNames(func(string) error { return nil })
	for _, mode := range []struct {
		name string
		opts []SchemaOpt
	}{
		{"strict", nil},
		{"lax", []SchemaOpt{lax}},
	} {
		for _, row := range census {
			for _, embed := range []bool{false, true} {
				path := "named"
				if embed {
					path = "embed"
				}
				t.Run(fmt.Sprintf("%s/%s/%s", mode.name, path, row.guard), func(t *testing.T) {
					_, err := skipCensusBuild(t, row.tag, embed, mode.opts...)
					switch {
					case row.wantErr == "" && err != nil:
						t.Fatalf("tag %s must build on the %s path, got: %v", row.tag, path, err)
					case row.wantErr == "":
						return
					case err == nil:
						t.Fatalf("tag %s must be rejected on the %s path naming %q, but the build succeeded",
							row.tag, path, row.wantErr)
					case !strings.Contains(err.Error(), row.wantErr):
						t.Fatalf("tag %s on the %s path rejected with %q, which does not name %q",
							row.tag, path, err, row.wantErr)
					}
				})
			}
		}
	}
}

// TestRegression_SchemaForEmbeddedSkipDirectiveExactMatch is the per-symptom
// pin for the census row that was open: the "-" skip directive is
// exact-match only, and the anonymous-embed path must say so in the same
// actionable terms as the named path rather than deferring to Avro's name
// grammar. Under WithLaxNames the grammar does not fire at all, so before
// the guard was shared the embed path emitted a field literally named "-"
// carrying the whole embedded record — the opposite of the skip the tag
// asked for.
func TestRegression_SchemaForEmbeddedSkipDirectiveExactMatch(t *testing.T) {
	lax := WithLaxNames(func(string) error { return nil })

	for _, mode := range []struct {
		name string
		opts []SchemaOpt
	}{
		{"strict", nil},
		{"lax", []SchemaOpt{lax}},
	} {
		for _, tag := range []string{`avro:"-,omitzero"`, `avro:"-,inline"`, `avro:"-foo"`} {
			t.Run(mode.name+"/"+tag, func(t *testing.T) {
				s, err := skipCensusBuild(t, tag, true, mode.opts...)
				if err == nil {
					t.Fatalf("embedded %s accepted; emitted %s", tag, s.String())
				}
				if !strings.Contains(err.Error(), "exact-match only") {
					t.Fatalf("embedded %s rejected with %q, which does not name the skip directive", tag, err)
				}
			})
		}
	}

	// Controls: the exact "-" directive still skips on BOTH paths, in both
	// name modes. The guard must not widen into the directive it protects.
	for _, mode := range []struct {
		name string
		opts []SchemaOpt
	}{
		{"strict", nil},
		{"lax", []SchemaOpt{lax}},
	} {
		for _, embed := range []bool{false, true} {
			path := "named"
			if embed {
				path = "embed"
			}
			t.Run("control/"+mode.name+"/"+path+"/exact-dash-skips", func(t *testing.T) {
				s, err := skipCensusBuild(t, `avro:"-"`, embed, mode.opts...)
				if err != nil {
					t.Fatalf("exact avro:\"-\" must skip cleanly on the %s path: %v", path, err)
				}
				root := s.Root()
				if len(root.Fields) != 1 || root.Fields[0].Name != "G" {
					t.Fatalf("exact avro:\"-\" did not skip on the %s path: %s", path, s.String())
				}
			})
		}
	}
}

// dashEmbedRuntime carries the tag whose SchemaFor build is rejected, so the
// runtime field mapper can be exercised against a HAND-WRITTEN schema that
// SchemaFor would never emit.
type dashEmbedRuntime struct {
	skipCensusInner `avro:"-,omitzero"`
	G               string
}

// TestRegression_SkipDirectiveGuardIsSchemaForScoped pins the boundary of
// the tag guard: it is a SchemaFor-side build validation, and it does not
// change how the runtime field mapper (reflect.go's typeFieldMapping) binds
// Go fields to Avro names. The mapper answers "which Go field owns this Avro
// name" for a caller-supplied schema; it has never enforced tag grammar, and
// none of collectFields' other tag rejections (unknown option, bad decimal,
// inline-on-non-struct) has a mapper counterpart either. A caller who
// hand-writes a lax schema with a field named "-" therefore keeps the exact
// encode/decode behavior they had.
func TestRegression_SkipDirectiveGuardIsSchemaForScoped(t *testing.T) {
	lax := WithLaxNames(func(string) error { return nil })
	s, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"-","type":{"type":"record","name":"Inner","fields":[{"name":"A","type":"string"}]}},
		{"name":"G","type":"string"}]}`, lax)
	if err != nil {
		t.Fatalf("hand-written lax schema must parse: %v", err)
	}
	in := dashEmbedRuntime{skipCensusInner: skipCensusInner{A: "a"}, G: "g"}
	wire, err := s.Encode(in)
	if err != nil {
		t.Fatalf("encode against the hand-written schema: %v", err)
	}
	var out dashEmbedRuntime
	if _, err := s.Decode(wire, &out); err != nil {
		t.Fatalf("decode against the hand-written schema: %v", err)
	}
	if out != in {
		t.Fatalf("runtime mapping changed: got %#v want %#v", out, in)
	}
}
