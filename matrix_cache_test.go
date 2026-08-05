package avro_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// TestMatrix_CacheSelfContainedNamespaces is a generative cross-product over
// SchemaCache cross-parse references: a named type (record / enum / fixed)
// whose namespace is established four different ways (null, explicit, inherited
// from an enclosing record, dotted fullname) is referenced from a schema in
// three relative namespaces (null, same, different) at four positions (record
// field, array items, map values, union branch). For every cell the
// cache-referenced schema's canonical form (and thus its Rabin fingerprint)
// must be byte-identical to the logically-identical fully-inline schema parsed
// without a cache (the independent oracle: that path is the Java-validated PCF
// emitter). The cache canonical must also re-parse, and the inner type must
// resolve to the EXPECTED fullname — an oracle-independent check that catches a
// definition silently re-namespaced to the reference site's scope.
//
// The cache stores each definition's JSON for splicing at the first reference;
// a definition that inherited its namespace (no explicit "namespace") would,
// without normalization, re-inherit the enclosing namespace wherever it is
// spliced and resolve to the wrong fullname. Neutering that normalization fails
// 36 of these 144 cells (every inherited/null definition-namespace × non-equal
// reference scope, across all kinds and positions).
func TestMatrix_CacheSelfContainedNamespaces(t *testing.T) {
	type kind struct {
		name string
		def  func(name, nsAttr string) string
	}
	kinds := []kind{
		{"record", func(n, ns string) string {
			return fmt.Sprintf(`{"type":"record","name":%q%s,"fields":[{"name":"x","type":"int"}]}`, n, ns)
		}},
		{"enum", func(n, ns string) string {
			return fmt.Sprintf(`{"type":"enum","name":%q%s,"symbols":["A","B"]}`, n, ns)
		}},
		{"fixed", func(n, ns string) string {
			return fmt.Sprintf(`{"type":"fixed","name":%q%s,"size":4}`, n, ns)
		}},
	}
	short := func(fn string) string {
		if i := strings.LastIndex(fn, "."); i >= 0 {
			return fn[i+1:]
		}
		return fn
	}
	nsAttr := func(ns string) string { // natural form: omit attr for the null namespace
		if ns == "" {
			return ""
		}
		return fmt.Sprintf(`,"namespace":%q`, ns)
	}
	posWrap := func(pos, typeJSON string) string {
		switch pos {
		case "array":
			return `{"type":"array","items":` + typeJSON + `}`
		case "map":
			return `{"type":"map","values":` + typeJSON + `}`
		case "union":
			return `["null",` + typeJSON + `]`
		}
		return typeJSON // field
	}

	defNSs := []string{"null", "explicit", "inherited", "dotted"}
	refNSs := []string{"null", "same", "diff"}
	poss := []string{"field", "array", "map", "union"}

	for _, k := range kinds {
		for _, dns := range defNSs {
			for _, rns := range refNSs {
				for _, pos := range poss {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", k.name, dns, rns, pos), func(t *testing.T) {
						// Resolve the definition into (fullname, resolved namespace,
						// registration schemas).
						var fullname, defns string
						var regs []string
						switch dns {
						case "null":
							fullname, defns = "T", ""
							regs = []string{k.def("T", "")}
						case "explicit":
							fullname, defns = "a.b.T", "a.b"
							regs = []string{k.def("T", `,"namespace":"a.b"`)}
						case "inherited":
							fullname, defns = "a.b.T", "a.b"
							regs = []string{fmt.Sprintf(`{"type":"record","name":"Wrap","namespace":"a.b","fields":[{"name":"w","type":%s}]}`, k.def("T", ""))}
						case "dotted":
							fullname, defns = "a.b.T", "a.b"
							regs = []string{k.def("a.b.T", "")}
						}
						// Self-contained oracle definition: ALWAYS an explicit
						// namespace (incl. "" to force null inside a namespaced scope).
						selfDef := k.def(short(fullname), fmt.Sprintf(`,"namespace":%q`, defns))

						refNSval := map[string]string{"null": "", "same": defns, "diff": "z.z"}[rns]
						refSchema := fmt.Sprintf(`{"type":"record","name":"Ref"%s,"fields":[{"name":"f","type":%s}]}`,
							nsAttr(refNSval), posWrap(pos, fmt.Sprintf("%q", fullname)))
						inlineSchema := fmt.Sprintf(`{"type":"record","name":"Ref"%s,"fields":[{"name":"f","type":%s}]}`,
							nsAttr(refNSval), posWrap(pos, selfDef))

						var c avro.SchemaCache
						for _, r := range regs {
							if _, err := c.Parse(r); err != nil {
								t.Fatalf("register %q: %v", r, err)
							}
						}
						viaCache, err := c.Parse(refSchema)
						if err != nil {
							t.Fatalf("cache parse %q: %v", refSchema, err)
						}
						inline, err := avro.Parse(inlineSchema)
						if err != nil {
							t.Fatalf("inline parse %q: %v", inlineSchema, err)
						}

						cc, ic := string(viaCache.Canonical()), string(inline.Canonical())
						if cc != ic {
							t.Errorf("canonical diverges:\n cache : %s\n inline: %s", cc, ic)
						}
						if string(viaCache.Fingerprint(avro.NewRabin())) != string(inline.Fingerprint(avro.NewRabin())) {
							t.Errorf("fingerprint diverges")
						}
						if _, err := avro.Parse(cc); err != nil {
							t.Errorf("cache canonical not self-contained: %v\n  %s", err, cc)
						}
						// Oracle-independent: the inner type resolves to the expected
						// fullname and did NOT absorb the reference site's namespace.
						if !strings.Contains(cc, fmt.Sprintf(`"name":%q`, fullname)) {
							t.Errorf("inner type not at expected fullname %q:\n  %s", fullname, cc)
						}
						if rns == "diff" && refNSval != "" &&
							strings.Contains(cc, fmt.Sprintf(`"name":"%s.%s"`, refNSval, short(fullname))) {
							t.Errorf("inner type re-namespaced to reference site %q:\n  %s", refNSval, cc)
						}
					})
				}
			}
		}
	}
}
