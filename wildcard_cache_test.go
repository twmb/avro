package avro_test

import (
	"testing"

	"github.com/twmb/avro"
)

// TestRegression_SchemaCacheWildcardConsistentRegistration pins that a wildcard
// CustomType (empty LogicalType AND AvroType, ErrSkipCustomType — the
// documented audit/metrics-hook use case) registered CONSISTENTLY across cache
// parses resolves, like a non-wildcard does. The cache-boundary reverse guard
// disagreed with itself on wildcards: the hadCustomType stamp counted any
// wiring (len(b.custom)>0, which a wildcard populates), but
// findCustomTypeMatchInSubtree SKIPS wildcards — so a cached wildcard-bearing
// type, referenced with the SAME wildcard registered, hit
// hadCustomType && currentMatches=="" and was rejected with an error telling
// the user to register the CustomType they had already registered. A wildcard
// bakes nothing onto the shared node (its conversion lives in the per-parse
// overlay), so the stamp must exclude wildcard-only wiring.
func TestRegression_SchemaCacheWildcardConsistentRegistration(t *testing.T) {
	const innerJSON = `{"type":"record","name":"Inner","fields":[{"name":"v","type":"long","logicalType":"timestamp-millis"}]}`
	const outerRefJSON = `{"type":"record","name":"Outer","fields":[{"name":"in","type":"Inner"}]}`

	// CONTROL: a non-wildcard CustomType registered consistently on BOTH parses
	// resolves (the already-pinned behavior).
	t.Run("non-wildcard-consistent-resolves", func(t *testing.T) {
		mk := func() avro.CustomType {
			return avro.CustomType{
				AvroType:    "long",
				LogicalType: "timestamp-millis",
				Decode:      func(v any, _ *avro.SchemaNode) (any, error) { return v, avro.ErrSkipCustomType },
			}
		}
		c := &avro.SchemaCache{}
		if _, err := c.Parse(innerJSON, mk()); err != nil {
			t.Fatalf("inner parse: %v", err)
		}
		if _, err := c.Parse(outerRefJSON, mk()); err != nil {
			t.Fatalf("non-wildcard consistent registration should resolve: %v", err)
		}
	})

	// BUG: the SAME structure with a WILDCARD registered consistently on BOTH
	// parses must also resolve.
	t.Run("wildcard-consistent-resolves", func(t *testing.T) {
		mk := func() avro.CustomType {
			return avro.CustomType{
				Decode: func(v any, _ *avro.SchemaNode) (any, error) { return v, avro.ErrSkipCustomType },
			}
		}
		c := &avro.SchemaCache{}
		if _, err := c.Parse(innerJSON, mk()); err != nil {
			t.Fatalf("inner parse with wildcard: %v", err)
		}
		if _, err := c.Parse(outerRefJSON, mk()); err != nil {
			t.Fatalf("wildcard registered consistently on BOTH parses must resolve, got: %v", err)
		}
	})
}
