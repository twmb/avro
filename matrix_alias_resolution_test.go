package avro

import (
	"fmt"
	"testing"
)

// TestMatrix_AliasResolutionCensus crosses reader-alias spelling × writer
// namespace × named kind × match site × API over the resolution matchers
// (namesMatch for a direct writer/reader pair, kindsMatchTier for reader-
// union branch selection; Resolve routes through CheckCompatibility, both
// APIs asserted per cell).
//
// The alias-matching semantics, per cell family (the executed fastavro arm
// lives in matrix_alias_differential_test.go; Java citations are given for
// the cells where the two references disagree):
//
//   - an alias always matches the writer's exact FULLNAME. Aliases are
//     stored fully qualified — a bare alias qualifies into the reader
//     type's own namespace, a dotted alias stays verbatim, and a single
//     leading dot is the null-namespace escape (".Old" is the fullname
//     "Old"; Java's Name constructor rule, Schema.java ~1455, the same
//     rule qualifyAliases applies) — so the exact tier covers the
//     same-namespace bare cell and both dotted cells.
//
//   - an alias DECLARED without any dot additionally short-name-matches
//     the writer's unqualified name in ANY namespace. This is fastavro's
//     raw-string tier (match_schemas: `w_unqual_name in r_aliases`,
//     executed in the differential); Java has no short tier (applyAliases
//     renames through a fullname-keyed Name map, Schema.java ~2093), and
//     the permissive reference wins for a safely-decodable value.
//
//   - an explicitly-qualified alias NEVER short-matches: the spec
//     ("Aliases") gives a type named "a.b" with aliases "c" and "x.y" the
//     fully qualified alias names "a.c" and "x.y" — "n1.Old" denotes
//     exactly n1.Old. Both references reject the foreign-namespace pair
//     (Java: fullname-keyed map; fastavro: raw-string comparison finds
//     neither the writer fullname nor its short name among the aliases).
//
//   - the leading-dot spelling matches ONLY the null-namespace writer
//     (Java-aligned; fastavro keeps the alias verbatim and matches
//     nothing — the documented divergence, recorded in NOT_BUGS with the
//     executed evidence).
func TestMatrix_AliasResolutionCensus(t *testing.T) {
	writerName := map[string]string{"samens": "n1.Old", "foreignns": "n2.Old", "nullns": "Old"}
	aliasSpelling := map[string]string{
		"bare":          "Old",
		"dottedown":     "n1.Old",
		"dottedforeign": "n2.Old",
		"leadingdot":    ".Old",
	}
	// accept[spelling][writerNS]
	accept := map[string]map[string]bool{
		"bare":          {"samens": true, "foreignns": true, "nullns": true},
		"dottedown":     {"samens": true, "foreignns": false, "nullns": false},
		"dottedforeign": {"samens": false, "foreignns": true, "nullns": false},
		"leadingdot":    {"samens": false, "foreignns": false, "nullns": true},
	}

	kindSchema := func(kind, name, aliases string) string {
		aliasAttr := ""
		if aliases != "" {
			aliasAttr = fmt.Sprintf(`,"aliases":[%q]`, aliases)
		}
		switch kind {
		case "record":
			return fmt.Sprintf(`{"type":"record","name":%q%s,"fields":[{"name":"a","type":"int"}]}`, name, aliasAttr)
		case "enum":
			return fmt.Sprintf(`{"type":"enum","name":%q%s,"symbols":["A","B"]}`, name, aliasAttr)
		case "fixed":
			return fmt.Sprintf(`{"type":"fixed","name":%q%s,"size":2}`, name, aliasAttr)
		}
		panic("unknown kind")
	}
	value := map[string]any{
		"record": map[string]any{"a": int32(7)},
		"enum":   "A",
		"fixed":  []byte{1, 2},
	}

	for spelling, alias := range aliasSpelling {
		for wns, wname := range writerName {
			for _, kind := range []string{"record", "enum", "fixed"} {
				for _, site := range []string{"top", "union"} {
					want := accept[spelling][wns]
					name := fmt.Sprintf("%s/%s/%s/%s", spelling, wns, kind, site)
					t.Run(name, func(t *testing.T) {
						writer := MustParse(kindSchema(kind, wname, ""))
						readerJSON := kindSchema(kind, "n1.New", alias)
						if site == "union" {
							// A boolean decoy branch: no promotion reaches
							// boolean from any named kind, so branch
							// selection is decided by the alias rules alone.
							readerJSON = `["boolean",` + readerJSON + `]`
						}
						reader := MustParse(readerJSON)

						compatErr := CheckCompatibility(writer, reader)
						resolved, resolveErr := Resolve(writer, reader)
						if (compatErr == nil) != (resolveErr == nil) {
							t.Fatalf("CheckCompatibility (%v) and Resolve (%v) disagree", compatErr, resolveErr)
						}
						if got := compatErr == nil; got != want {
							t.Fatalf("accept=%v, want %v (CheckCompatibility: %v)", got, want, compatErr)
						}
						if !want {
							return
						}
						// Accepted cells must actually read: encode with the
						// writer, decode through the resolved schema.
						wire, err := writer.Encode(value[kind])
						if err != nil {
							t.Fatalf("encode: %v", err)
						}
						var got any
						if _, err := resolved.Decode(wire, &got); err != nil {
							t.Fatalf("resolved decode: %v", err)
						}
						if got == nil {
							t.Fatalf("resolved decode produced nil")
						}
					})
				}
			}
		}
	}

	// Scan-past discrimination for the union-branch matcher: with a single
	// named candidate, a spurious tier-match on a qualified alias is
	// dominated by the direct matcher's recheck of the selected branch, so
	// single-candidate cells cannot see kindsMatchTier's own alias rule.
	// With TWO candidates, the rule decides WHICH branch wins: the
	// qualified-alias branch must yield matchNone so selection scans past
	// it to the later branch that legitimately matches on its unqualified
	// name. The reader default on the correct branch's extra field makes
	// the selected branch visible in the decoded value.
	t.Run("scanpast/unionqualifiedalias", func(t *testing.T) {
		writer := MustParse(`{"type":"record","name":"n2.Old","fields":[{"name":"a","type":"int"}]}`)
		reader := MustParse(`["boolean",
			{"type":"record","name":"n1.New","aliases":["n1.Old"],"fields":[{"name":"a","type":"int"}]},
			{"type":"record","name":"n3.Old","fields":[{"name":"a","type":"int"},{"name":"b","type":"string","default":"x"}]}]`)
		resolved, err := Resolve(writer, reader)
		if err != nil {
			t.Fatalf("selection must scan past the qualified-alias branch to n3.Old: %v", err)
		}
		wire, err := writer.Encode(map[string]any{"a": int32(7)})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got map[string]any
		if _, err := resolved.Decode(wire, &got); err != nil {
			t.Fatalf("resolved decode: %v", err)
		}
		if got["b"] != "x" {
			t.Fatalf("selected branch lacks n3.Old's defaulted field (got %v); the qualified-alias branch was wrongly preferred", got)
		}
	})

	// The alias tiers must not disturb the spec's unqualified-NAME match:
	// reader and writer sharing a short name across namespaces match with
	// no aliases at all ("both schemas are records with the same
	// (unqualified) name" — same wording for enum/fixed). One control per
	// kind per site.
	for _, kind := range []string{"record", "enum", "fixed"} {
		for _, site := range []string{"top", "union"} {
			t.Run(fmt.Sprintf("unqualifiednamecontrol/%s/%s", kind, site), func(t *testing.T) {
				writer := MustParse(kindSchema(kind, "n2.Same", ""))
				readerJSON := kindSchema(kind, "n1.Same", "")
				if site == "union" {
					readerJSON = `["boolean",` + readerJSON + `]`
				}
				reader := MustParse(readerJSON)
				if err := CheckCompatibility(writer, reader); err != nil {
					t.Errorf("unqualified name match must hold with no aliases: %v", err)
				}
			})
		}
	}

	// Field aliases are namespace-free strings matched exactly; the type-
	// alias qualification rules must not leak into them. A dotted FIELD
	// alias matches only a writer field literally named with the dot — the
	// alias-repair scenario the spec explicitly allows ("this allows schema
	// evolution to correct illegal names in old schemata"; the old
	// illegal-name schema itself parses only under WithLaxNames).
	t.Run("fieldaliascontrol", func(t *testing.T) {
		lax := WithLaxNames(func(string) error { return nil })
		writer, err := Parse(`{"type":"record","name":"R","fields":[{"name":"weird.name","type":"int"}]}`, lax)
		if err != nil {
			t.Fatalf("lax writer: %v", err)
		}
		reader := MustParse(`{"type":"record","name":"R","fields":[{"name":"clean","type":"int","aliases":["weird.name"]}]}`)
		if err := CheckCompatibility(writer, reader); err != nil {
			t.Errorf("dotted field alias must match the literal writer field name: %v", err)
		}
		readerBare := MustParse(`{"type":"record","name":"R","fields":[{"name":"clean","type":"int","aliases":["name"]}]}`)
		if err := CheckCompatibility(writer, readerBare); err == nil {
			t.Errorf(`field alias "name" must not short-match writer field "weird.name"`)
		}
	})

}
