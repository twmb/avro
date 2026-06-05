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
// behavior), and asserts twmb, fastavro, and Java agree: the original
// schema parses everywhere, every mutant is rejected everywhere.
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
// validate (it defers them to read time or skips them entirely), verified
// against a live fastavro: missing/duplicate/empty-named record fields and
// negative fixed sizes all parse there. Java enforces every class — the
// cisuite twin (TestDifferentialJavaAcceptance) asserts the full set; the
// fastavro differential asserts only what fastavro enforces.
var fastavroLaxMutants = map[string]bool{
	"record-missing-fields":   true,
	"record-duplicate-field":  true,
	"record-empty-field-name": true,
	"fixed-negative-size":     true,
}

// TestDifferentialAcceptance: fastavro must agree on every cell (accept)
// and every mutant it validates (reject). Skips without the oracle python.
func TestDifferentialAcceptance(t *testing.T) {
	o := startOracle(t)
	for _, cell := range acceptanceCells() {
		resp := o.call(oracleJob{Op: "parse", Schema: json.RawMessage(cell)})
		if !resp.OK {
			t.Fatalf("fastavro rejected a schema twmb accepts: %s\n%s", resp.Err, cell)
		}
		for _, m := range schemaMutants(cell) {
			if fastavroLaxMutants[m.label] {
				continue
			}
			resp := o.call(oracleJob{Op: "parse", Schema: json.RawMessage(m.schema)})
			if resp.OK {
				t.Errorf("fastavro accepted mutant %s that twmb rejects:\n%s", m.label, m.schema)
			}
		}
	}
}
