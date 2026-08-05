package avro_test

import (
	"encoding/json"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Schema-ACCEPTANCE parity: the bug class that keeps producing interop
// regressions is not wire bytes but which schemas parse at all (size-0
// fixed, empty enums, empty unions were all acceptance divergences). This
// axis takes every composed matrix schema, derives structurally-broken
// mutants whose rejection is spec-required and reference-verified (each
// mutator class was checked against Java's parser source and conformance
// behavior), and asserts the originals parse everywhere and the mutants
// reject in twmb and Java (the cisuite twin asserts the full set) —
// fastavro validates only a subset at parse, and the executed
// fastavroLaxMutants calibration below witnesses which mutant classes
// it accepts.
//
// Mutators deliberately avoid the documented-divergence territory (quoted
// size leniency, logical-type soft-drop vs hard-reject of bad decimal
// params, alias grammar, forward references): those are policy entries,
// not parity targets.
// ---------------------------------------------------------------------------

type schemaMutant struct {
	label  string
	schema string
}

// mutateOnce decodes the schema JSON, applies fn to the first applicable
// node (walking objects and arrays), and re-encodes. Returns "" when no
// node was applicable.
func mutateOnce(schemaJSON string, fn func(obj map[string]any) bool) string {
	var tree any
	if err := json.Unmarshal([]byte(schemaJSON), &tree); err != nil {
		return ""
	}
	applied := false
	var walk func(n any)
	walk = func(n any) {
		if applied {
			return
		}
		switch v := n.(type) {
		case map[string]any:
			if fn(v) {
				applied = true
				return
			}
			for _, k := range []string{"type", "items", "values"} {
				if c, ok := v[k]; ok {
					walk(c)
				}
			}
			if fs, ok := v["fields"].([]any); ok {
				for _, f := range fs {
					walk(f)
				}
			}
		case []any:
			for _, b := range v {
				walk(b)
			}
		}
	}
	walk(tree)
	if !applied {
		return ""
	}
	out, err := json.Marshal(tree)
	if err != nil {
		return ""
	}
	return string(out)
}

// schemaMutants derives the reference-verified reject set for one schema.
func schemaMutants(schemaJSON string) []schemaMutant {
	var out []schemaMutant
	add := func(label, s string) {
		if s != "" && s != schemaJSON {
			out = append(out, schemaMutant{label, s})
		}
	}
	isType := func(obj map[string]any, t string) bool {
		s, _ := obj["type"].(string)
		return s == t
	}

	add("fixed-missing-size", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "fixed") {
			delete(o, "size")
			return true
		}
		return false
	}))
	add("fixed-negative-size", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "fixed") {
			o["size"] = -1
			return true
		}
		return false
	}))
	add("enum-missing-symbols", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "enum") {
			delete(o, "symbols")
			return true
		}
		return false
	}))
	add("enum-duplicate-symbol", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "enum") {
			if syms, ok := o["symbols"].([]any); ok && len(syms) > 0 {
				o["symbols"] = append(syms, syms[0])
				return true
			}
		}
		return false
	}))
	add("enum-default-not-member", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "enum") {
			if syms, ok := o["symbols"].([]any); ok && len(syms) > 0 {
				o["default"] = "__not_a_symbol__"
				return true
			}
		}
		return false
	}))
	add("named-missing-name", mutateOnce(schemaJSON, func(o map[string]any) bool {
		switch {
		case isType(o, "record"), isType(o, "enum"), isType(o, "fixed"):
			if _, ok := o["name"]; ok {
				delete(o, "name")
				return true
			}
		}
		return false
	}))
	add("record-missing-fields", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "record") {
			delete(o, "fields")
			return true
		}
		return false
	}))
	add("record-duplicate-field", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "record") {
			if fs, ok := o["fields"].([]any); ok && len(fs) > 0 {
				o["fields"] = append(fs, fs[0])
				return true
			}
		}
		return false
	}))
	add("record-empty-field-name", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "record") {
			if fs, ok := o["fields"].([]any); ok && len(fs) > 0 {
				if f0, ok := fs[0].(map[string]any); ok {
					f0["name"] = ""
					return true
				}
			}
		}
		return false
	}))
	add("array-missing-items", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "array") {
			delete(o, "items")
			return true
		}
		return false
	}))
	add("map-missing-values", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "map") {
			delete(o, "values")
			return true
		}
		return false
	}))
	add("missing-type-key", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if _, ok := o["type"]; ok {
			delete(o, "type")
			return true
		}
		return false
	}))

	// Union mutants operate on the whole tree (the union is an array, not
	// an object the walker's fn sees).
	var tree any
	if json.Unmarshal([]byte(schemaJSON), &tree) == nil {
		if arr, ok := tree.([]any); ok && len(arr) > 0 {
			dup := append(append([]any{}, arr...), arr[0])
			if b, err := json.Marshal(dup); err == nil {
				out = append(out, schemaMutant{"union-duplicate-branch", string(b)})
			}
			nested := append([]any{}, arr...)
			nested[0] = []any{arr[0]}
			if b, err := json.Marshal(nested); err == nil {
				out = append(out, schemaMutant{"union-nested-union", string(b)})
			}
		}
	}
	return out
}

// acceptanceCells samples the composed schemas the acceptance axis sweeps:
// every fragment × three structural contexts.
func acceptanceCells() []string {
	var cells []string
	for _, fr := range matFrags() {
		for _, cx := range matCtxs() {
			switch cx.label {
			case "top", "field", "array":
			default:
				continue
			}
			if cx.skip != nil && cx.skip(fr.kind) {
				continue
			}
			u := &uniq{}
			cells = append(cells, cx.schema(fr.schema(u), fr.kind, u))
		}
	}
	return cells
}

// TestMatrix_AcceptanceMutantsRejectLocally: every mutant must fail twmb's
// Parse (the local half of the parity; the oracle halves assert fastavro
// and Java agree).
func TestMatrix_AcceptanceMutantsRejectLocally(t *testing.T) {
	for _, cell := range acceptanceCells() {
		if _, err := avro.Parse(cell); err != nil {
			t.Fatalf("unmutated cell must parse: %v\n%s", err, cell)
		}
		for _, m := range schemaMutants(cell) {
			if _, err := avro.Parse(m.schema); err == nil {
				t.Errorf("mutant %s unexpectedly parsed:\n%s", m.label, m.schema)
			}
		}
	}
}

// fastavroLaxMutants are mutator classes fastavro's parser does NOT
// validate per se (it defers them to read time or skips them entirely):
// missing/duplicate/empty-named record fields and negative fixed sizes
// parse there IN THEIR PLAIN FORM. The laxness is class-level, not
// uniform — a specific mutant cell can still reject when the mutation
// collaterally trips an orthogonal fastavro validation (a duplicated
// field whose type DEFINES a named type re-defines that name; a
// negative-size fixed carrying a decimal fails the precision-capacity
// check) — so the differential below requires an executed ACCEPT
// WITNESS per lax class rather than skipping or asserting uniformly: a
// fastavro upgrade that starts validating a class wholesale drops its
// witness count to zero and flips the calibration loudly. Java enforces
// every class — the cisuite twin (TestDifferentialJavaAcceptance)
// asserts the full set; the fastavro differential asserts reject only
// for what fastavro enforces.
var fastavroLaxMutants = map[string]bool{
	"record-missing-fields":   true,
	"record-duplicate-field":  true,
	"record-empty-field-name": true,
	"fixed-negative-size":     true,
}

// TestDifferentialAcceptance: fastavro must agree on every cell (accept)
// and every mutant it validates (reject); each documented-lax mutant
// class must produce at least one observed fastavro ACCEPT across the
// sweep (the executed fastavroLaxMutants calibration). Skips without the
// oracle python.
func TestDifferentialAcceptance(t *testing.T) {
	o := startOracle(t)
	laxSeen := map[string]int{}
	laxAccepted := map[string]int{}
	for _, cell := range acceptanceCells() {
		resp := o.call(oracleJob{Op: "parse", Schema: json.RawMessage(cell)})
		if !resp.OK {
			t.Fatalf("fastavro rejected a schema twmb accepts: %s\n%s", resp.Err, cell)
		}
		for _, m := range schemaMutants(cell) {
			resp := o.call(oracleJob{Op: "parse", Schema: json.RawMessage(m.schema)})
			if fastavroLaxMutants[m.label] {
				laxSeen[m.label]++
				if resp.OK {
					laxAccepted[m.label]++
				}
				continue
			}
			if resp.OK {
				t.Errorf("fastavro accepted mutant %s that twmb rejects:\n%s", m.label, m.schema)
			}
		}
	}
	for label := range fastavroLaxMutants {
		if laxSeen[label] > 0 && laxAccepted[label] == 0 {
			t.Errorf("fastavro now REJECTS every %s mutant (%d cells) — its parser started validating this class; recalibrate fastavroLaxMutants",
				label, laxSeen[label])
		}
	}
}
