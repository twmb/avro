//go:build cisuite

package avro_test

import (
	"testing"

	"github.com/twmb/avro"
)

// TestDifferentialJavaAcceptanceAttributePlacement drives a representative
// subset of the attribute x placement census through the Java oracle: Java
// accepts every cell (stray attributes are either reserved-and-ignored via
// SCHEMA_RESERVED or kept as props — including the structural-key cells
// twmb rejects per NOT_BUGS #63 and the stray name/namespace-on-container
// cells), and for every cell twmb also accepts, Java's Parsing Canonical
// Form must equal twmb's Canonical() — proving both implementations strip
// the stray attribute identically, which is why the Rabin fingerprints
// agree cross-impl.
//
// The "error" kind is excluded: standalone error schemas are a
// protocol-context type in Java's parser, not a plain-schema kind; twmb's
// record-alias handling for it is pinned against the record twin locally.
func TestDifferentialJavaAcceptanceAttributePlacement(t *testing.T) {
	_, fpCanon := startMatrixJavaOracle(t)

	javaKinds := []string{"int", "string", "bytes", "fixed", "enum", "record", "array", "map"}
	for _, attr := range censusAttrs() {
		for _, kind := range javaKinds {
			verdict := attr.verdict(kind)
			if verdict == censusSkip {
				continue
			}
			t.Run("type/"+attr.key+"/"+kind, func(t *testing.T) {
				src := censusTypeSchema(kind, attr.key, attr.val(kind), true)
				ok, _, canon, errMsg := fpCanon(t, src)
				if !ok {
					t.Fatalf("Java rejected the placement (%s): %s", src, errMsg)
				}
				if verdict == censusReject63 {
					return // twmb rejects (documented #63 divergence); nothing to compare
				}
				s, err := avro.Parse(src)
				if err != nil {
					t.Fatalf("twmb Parse(%s): %v", src, err)
				}
				if got := string(s.Canonical()); got != canon {
					t.Errorf("PCF diverges from Java for %s:\n twmb: %s\n java: %s", src, got, canon)
				}
			})
		}
		if !attr.fieldLevel {
			continue
		}
		for _, kind := range []string{"int", "fixed", "record", "array", "union"} {
			t.Run("field/"+attr.key+"/"+kind, func(t *testing.T) {
				src := censusFieldSchema(kind, attr.key, attr.val(kind), true)
				ok, _, canon, errMsg := fpCanon(t, src)
				if !ok {
					t.Fatalf("Java rejected the field placement (%s): %s", src, errMsg)
				}
				s, err := avro.Parse(src)
				if err != nil {
					t.Fatalf("twmb Parse(%s): %v", src, err)
				}
				if got := string(s.Canonical()); got != canon {
					t.Errorf("PCF diverges from Java for %s:\n twmb: %s\n java: %s", src, got, canon)
				}
			})
		}
	}

	// The stray-namespace scoping composition: Java, like twmb and
	// fastavro, resolves a named type defined under a namespace-carrying
	// array in the ENCLOSING scope.
	t.Run("namespace-scoping", func(t *testing.T) {
		const def = `{"name":"f","type":{"type":"array","namespace":"x","items":{"type":"record","name":"Inner","fields":[{"name":"i","type":"int"}]}}}`
		src := `{"type":"record","name":"top.R","fields":[` + def + `,{"name":"g","type":"top.Inner"}]}`
		ok, _, canon, errMsg := fpCanon(t, src)
		if !ok {
			t.Fatalf("Java: enclosing-scope reference should resolve: %s", errMsg)
		}
		if got := string(avro.MustParse(src).Canonical()); got != canon {
			t.Errorf("PCF diverges from Java:\n twmb: %s\n java: %s", got, canon)
		}
		if ok, _, _, _ := fpCanon(t, `{"type":"record","name":"top.R","fields":[`+def+`,{"name":"g","type":"x.Inner"}]}`); ok {
			t.Errorf("Java resolved x.Inner — namespace-on-array scopes there; recalibrate")
		}
	})

	// The stray-name-on-container divergence direction, documented: Java
	// ignores the reserved key where twmb keeps its walker-parity reject.
	t.Run("stray-name-on-array-java-accepts", func(t *testing.T) {
		src := `{"type":"array","items":"int","name":"strayName"}`
		if ok, _, _, errMsg := fpCanon(t, src); !ok {
			t.Errorf("Java now rejects a stray name on an array (%s) — recalibrate the documented divergence", errMsg)
		}
		if _, err := avro.Parse(src); err == nil {
			t.Errorf("twmb accepted a stray name on an array; the documented keep-strict posture changed")
		}
	})
}
