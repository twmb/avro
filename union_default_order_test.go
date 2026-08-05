package avro_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// Per Avro 1.12 (AVRO-3649) a union-field default may match ANY branch, so
// whether a schema PARSES — and which branch its default selects — must not
// depend on the textual order the branches are written in. Java's
// Schema.isValidDefault is anyMatch over an immutable JsonNode
// (lang/java/avro/src/main/java/org/apache/avro/Schema.java:1786), which is
// strictly order-independent.
//
// The branch matcher selects via validateDefault, whose container arms coerce
// record/array/map fields IN PLACE (a string "5" -> float64 against a double
// field, the documented outer-float carveout). If each branch trial observes
// the SAME value a prior FAILED branch already coerced, a later string-typed
// branch rejects a float64 it should never have seen — making acceptance
// order-dependent. These tests pin order-independence of both the parse result
// and the selected branch; each branch trial must validate the original value.
//
// Generative: cross every branch-order permutation with leak shapes whose
// earlier record branch coerces a field (directly, or through an array element
// / map value) and then fails, so a naive shared-value matcher leaks the
// coercion into the next branch.
func TestRegression_UnionDefaultBranchOrderIndependent(t *testing.T) {
	type shape struct {
		name      string
		fail      string // a record branch that coerces a field then fails the default
		match     string // the record branch the default actually matches (all-string)
		def       string // default value JSON; matches `match`, triggers `fail`'s coercion
		wantField string // a field of `match` whose decoded value is asserted
		wantVal   any    // the expected decoded value of wantField
	}
	shapes := []shape{
		{
			name:      "double-field-leak",
			fail:      `{"type":"record","name":"Fa","fields":[{"name":"f","type":"double"},{"name":"g","type":"double"}]}`,
			match:     `{"type":"record","name":"Mb","fields":[{"name":"f","type":"string"},{"name":"g","type":"string"}]}`,
			def:       `{"f":"5","g":"z"}`,
			wantField: "f", wantVal: "5",
		},
		{
			name:      "array-element-leak",
			fail:      `{"type":"record","name":"Fa","fields":[{"name":"a","type":{"type":"array","items":"double"}},{"name":"g","type":"double"}]}`,
			match:     `{"type":"record","name":"Mb","fields":[{"name":"a","type":{"type":"array","items":"string"}},{"name":"g","type":"string"}]}`,
			def:       `{"a":["5"],"g":"z"}`,
			wantField: "g", wantVal: "z",
		},
		{
			name:      "map-value-leak",
			fail:      `{"type":"record","name":"Fa","fields":[{"name":"m","type":{"type":"map","values":"double"}},{"name":"g","type":"double"}]}`,
			match:     `{"type":"record","name":"Mb","fields":[{"name":"m","type":{"type":"map","values":"string"}},{"name":"g","type":"string"}]}`,
			def:       `{"m":{"k":"5"},"g":"z"}`,
			wantField: "g", wantVal: "z",
		},
	}

	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			// Two branch sets: {fail, match} and {null, fail, match}; the
			// object default never matches null, so null is just a present
			// non-matching branch that must not perturb selection.
			for _, base := range [][]string{
				{sh.fail, sh.match},
				{`"null"`, sh.fail, sh.match},
			} {
				var firstDecoded any
				var firstMeta any
				orders := permuteStrings(base)
				for oi, order := range orders {
					schema := fmt.Sprintf(
						`{"type":"record","name":"O","fields":[{"name":"u","type":[%s],"default":%s}]}`,
						strings.Join(order, ","), sh.def)
					s, err := avro.Parse(schema)
					if err != nil {
						t.Fatalf("order %d %v: parse FAILED (order-dependent acceptance): %v\nschema=%s", oi, order, err, schema)
					}
					// Selected branch: auto-fill the absent field, decode, and
					// compare the value across every ordering — selection must
					// be identical regardless of branch order.
					wire, err := s.Encode(map[string]any{})
					if err != nil {
						t.Fatalf("order %d %v: auto-fill encode: %v", oi, order, err)
					}
					var got any
					if _, err := s.Decode(wire, &got); err != nil {
						t.Fatalf("order %d %v: decode: %v", oi, order, err)
					}
					u, ok := got.(map[string]any)["u"].(map[string]any)
					if !ok {
						t.Fatalf("order %d %v: decoded u is not a record map: %#v", oi, order, got)
					}
					if u[sh.wantField] != sh.wantVal {
						t.Fatalf("order %d %v: selected branch field %q = %#v, want %#v",
							oi, order, sh.wantField, u[sh.wantField], sh.wantVal)
					}
					if oi == 0 {
						firstDecoded = got
						firstMeta = s.Root().Fields[0].Default
					} else {
						if !reflect.DeepEqual(got, firstDecoded) {
							t.Fatalf("order %v: decoded default %#v differs from first ordering %#v", order, got, firstDecoded)
						}
						// Metadata-side sibling: branchAcceptsDefault is a pure
						// predicate (no in-place coercion), so Root().Default must
						// likewise be order-independent.
						if !reflect.DeepEqual(s.Root().Fields[0].Default, firstMeta) {
							t.Fatalf("order %v: metadata Default %#v differs from first ordering %#v",
								order, s.Root().Fields[0].Default, firstMeta)
						}
					}
				}
			}
		})
	}
}

// The minimal hand-written repro that motivates the generative matrix above.
func TestRegression_UnionDefaultLeakDoesNotRejectValidSchema(t *testing.T) {
	recA := `{"type":"record","name":"A","fields":[{"name":"x","type":"double"},{"name":"y","type":"int"}]}`
	recB := `{"type":"record","name":"B","fields":[{"name":"x","type":"string"},{"name":"y","type":"string"}]}`
	def := `{"x":"5","y":"z"}` // matches B; A coerces x:"5"->float64 then fails on y
	for _, order := range [][]string{{recA, recB}, {recB, recA}} {
		schema := fmt.Sprintf(`{"type":"record","name":"O","fields":[{"name":"u","type":[%s],"default":%s}]}`,
			strings.Join(order, ","), def)
		if _, err := avro.Parse(schema); err != nil {
			t.Fatalf("valid union default rejected (order leak): %v", err)
		}
	}
}

func permuteStrings(in []string) [][]string {
	if len(in) <= 1 {
		return [][]string{append([]string(nil), in...)}
	}
	var out [][]string
	for i := range in {
		rest := make([]string, 0, len(in)-1)
		rest = append(rest, in[:i]...)
		rest = append(rest, in[i+1:]...)
		for _, p := range permuteStrings(rest) {
			out = append(out, append([]string{in[i]}, p...))
		}
	}
	return out
}
