package avro

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// TestMatrix_SchemaForReservedKeyCaseFold pins the contract that SchemaFor's
// composition walkers (resolveNameScope, pinCustomSchemaScope,
// dedupNamedTypes, normalizeSchemaScope) read reserved attribute keys the
// way the Parse they feed does: case-insensitively, exact-case winning
// (lookupCI). A Props key differing from a reserved name only by ASCII case
// IS that reserved attribute (see Schema.Root's doc), so the walkers must
// key definitions, descend containers, and skip the namespace-pin injection
// identically for every spelling of a key.
//
// Axes: reserved key {namespace — consumed by resolveNameScope's keying and
// the pin's injection-skip; items / values / a union slice under items —
// the descent routes of pinCustomSchemaScope and dedupNamedTypes; fields —
// dedupNamedTypes' and normalizeSchemaScope's field descent} × spelling
// {exact-case control, UPPER, mIxEd} × occurrences {1, 2} × SchemaFor scope
// {default, WithNamespace}.
//
// Oracles per cell family:
//   - namespace: the declared identity x.y.F holds for every spelling
//     (canonical-visible), one definition + a dotted reference at two
//     occurrences, and all three spellings' canonicals are byte-identical.
//     Exact-case always wins in lookupCI, so the exact-case control cells —
//     and with them every natural-spelling cell of the scope matrix — are
//     unaffected by folding.
//   - items/values/union-slice/fields: spelling parity — every spelling
//     produces the same verdict, the same canonical bytes, and (success
//     cells) the same number of inline definition bodies in the full
//     String() output (strays are canonical-stripped, so body counts are
//     what distinguish a walked stray from an ignored one).
//
// The name and type keys have no divergence-reachable variant cell: the
// render always emits them exact-case (toJSONWalk builds "type" and "name"
// literally), so a case-variant can only ride along as an extra key that
// exact-first lookup ignores. Those are pinned as inert-control cells.
func TestMatrix_SchemaForReservedKeyCaseFold(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	spellings := map[string]func(string) string{
		"exact": func(k string) string { return k },
		"upper": strings.ToUpper,
		"mixed": func(k string) string {
			// First letter upper, rest as-is: "Namespace", "Items", ...
			return strings.ToUpper(k[:1]) + k[1:]
		},
	}

	// namespace × occurrences × scope: the identity axis.
	for _, occ := range []int{1, 2} {
		for _, ns := range []string{"", "b"} {
			canonicals := map[string]string{}
			for spell, f := range spellings {
				t.Run(fmt.Sprintf("namespace/%s/occ%d/ns=%q", spell, occ, ns), func(t *testing.T) {
					node := &SchemaNode{Type: "fixed", Name: "F", Size: 4,
						Props: map[string]any{f("namespace"): "x.y"}}
					s, err := schemaForScopeCell(t, scopeCellFields(occ, primary), ns, []CustomType{{GoType: primary, Schema: node}})
					if err != nil {
						t.Fatalf("cell errored: %v", err)
					}
					assertScopeFullnames(t, s, []string{topName(ns), "x.y.F"})
					if !strings.Contains(string(s.Canonical()), `"x.y.F"`) {
						t.Errorf("declared identity x.y.F missing from canonical: %s", s.Canonical())
					}
					if occ == 2 {
						if n := strings.Count(s.String(), `"size"`); n != 1 {
							t.Errorf("want one inline definition + a reference at two occurrences, found %d bodies: %s", n, s.String())
						}
					}
					canonicals[spell] = string(s.Canonical())
				})
			}
			assertOneCanonical(t, fmt.Sprintf("namespace/occ%d/ns=%q", occ, ns), canonicals)
		}
	}

	// Stray-carried descent routes: items, values, union slice, fields.
	// The carrier is an unnamed node whose Props hold a named definition
	// under a (possibly case-variant) container key; the walkers must
	// descend every spelling identically. Parse keeps the stray attribute
	// inert (structural-key posture), so the observable is spelling parity:
	// same verdict, same canonical, same inline-body count.
	routes := []struct {
		route string
		key   string // reserved key the carried value sits under
		build func(spelledKey string) *SchemaNode
	}{
		{"items", "items", func(k string) *SchemaNode {
			return &SchemaNode{Type: "string", Props: map[string]any{
				k: map[string]any{"type": "fixed", "name": "G", "size": 1}}}
		}},
		{"values", "values", func(k string) *SchemaNode {
			return &SchemaNode{Type: "string", Props: map[string]any{
				k: map[string]any{"type": "fixed", "name": "G", "size": 1}}}
		}},
		{"unionslice", "items", func(k string) *SchemaNode {
			return &SchemaNode{Type: "string", Props: map[string]any{
				k: []any{map[string]any{"type": "fixed", "name": "G", "size": 1}}}}
		}},
		{"fields", "items", func(k string) *SchemaNode {
			// A record definition carried under exact-case items, with its
			// FIELDS key case-varied: the fields-descent axis.
			return &SchemaNode{Type: "string", Props: map[string]any{
				"items": map[string]any{"type": "record", "name": "R", "namespace": "x.y",
					k: []map[string]any{{"name": "f", "type": "int"}}}}}
		}},
	}
	for _, r := range routes {
		for _, occ := range []int{1, 2} {
			for _, ns := range []string{"", "b"} {
				verdicts := map[string]string{}
				canonicals := map[string]string{}
				bodies := map[string]int{}
				for spell, f := range spellings {
					spelledKey := f(r.key)
					if r.route == "fields" {
						spelledKey = f("fields")
					}
					t.Run(fmt.Sprintf("%s/%s/occ%d/ns=%q", r.route, spell, occ, ns), func(t *testing.T) {
						node := r.build(spelledKey)
						s, err := schemaForScopeCell(t, scopeCellFields(occ, primary), ns, []CustomType{{GoType: primary, Schema: node}})
						if err != nil {
							verdicts[spell] = err.Error()
							return
						}
						verdicts[spell] = "ok"
						canonicals[spell] = string(s.Canonical())
						// Inline-body marker, spelling-neutral: the carried
						// fixed G always emits "size"; the carried record R
						// always emits its field "f" (the container KEY
						// spelling varies by cell, the body content never
						// does).
						marker := `"size"`
						if r.route == "fields" {
							marker = `"name":"f"`
						}
						bodies[spell] = strings.Count(s.String(), marker)
					})
				}
				name := fmt.Sprintf("%s/occ%d/ns=%q", r.route, occ, ns)
				assertOneValue(t, name+" verdict", verdicts)
				if verdicts["exact"] == "ok" {
					assertOneCanonical(t, name, canonicals)
					assertOneIntValue(t, name+" inline bodies", bodies)
				}
			}
		}
	}

	// Inert controls: the render always emits exact-case "name" and "type",
	// so a case-variant of either can only be an extra key that exact-first
	// lookup ignores — on the walkers AND on Parse. The composed output
	// must equal the variant-free control's canonical.
	for _, extra := range []string{"NAME", "TYPE"} {
		t.Run("inertcontrol/"+extra, func(t *testing.T) {
			control := &SchemaNode{Type: "fixed", Name: "F", Namespace: "x.y", Size: 4}
			varied := &SchemaNode{Type: "fixed", Name: "F", Namespace: "x.y", Size: 4,
				Props: map[string]any{extra: "Zed"}}
			sControl, err := schemaForScopeCell(t, scopeCellFields(2, primary), "b", []CustomType{{GoType: primary, Schema: control}})
			if err != nil {
				t.Fatalf("control: %v", err)
			}
			sVaried, err := schemaForScopeCell(t, scopeCellFields(2, primary), "b", []CustomType{{GoType: primary, Schema: varied}})
			if err != nil {
				t.Fatalf("varied: %v", err)
			}
			if string(sControl.Canonical()) != string(sVaried.Canonical()) {
				t.Errorf("case-variant %s prop is not inert:\n control: %s\n varied:  %s", extra, sControl.Canonical(), sVaried.Canonical())
			}
		})
	}
}

func topName(ns string) string {
	if ns == "" {
		return "Top"
	}
	return ns + ".Top"
}

func assertOneCanonical(t *testing.T, name string, got map[string]string) {
	t.Helper()
	assertOneValue(t, name+" canonical", got)
}

func assertOneValue(t *testing.T, name string, got map[string]string) {
	t.Helper()
	var first string
	var firstKey string
	for k, v := range got {
		if firstKey == "" {
			firstKey, first = k, v
			continue
		}
		if v != first {
			t.Errorf("%s diverges across spellings:\n %s: %s\n %s: %s", name, firstKey, first, k, v)
		}
	}
}

func assertOneIntValue(t *testing.T, name string, got map[string]int) {
	t.Helper()
	asStr := make(map[string]string, len(got))
	for k, v := range got {
		asStr[k] = fmt.Sprint(v)
	}
	assertOneValue(t, name, asStr)
}
