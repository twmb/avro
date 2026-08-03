package avro

import (
	"reflect"
	"time"
)

// Test-only bridges for the external avro_test package (compiled only into
// the test binary; not part of the library surface).

// RaceRelaxedForTest is the wall-clock ceiling authority (race_bounds_test.go),
// bridged so package avro_test ASKS it instead of keeping a second copy of the
// rule. The two packages cannot share an unexported helper, and that is exactly
// why the rule was duplicated — so the sharing is made explicit here rather
// than left to two comments agreeing with each other.
func RaceRelaxedForTest(normal time.Duration) time.Duration { return raceRelaxed(normal) }

// RaceEnabledForTest bridges the build-tagged predicate itself, so there is one
// mechanism for the whole module rather than one per test package.
const RaceEnabledForTest = raceEnabled

// SlabFreeForTest reports the internal slab-free classification: whether
// Decode bypasses the slab pool and runs this schema's deser on a nil slab.
func (s *Schema) SlabFreeForTest() bool { return s.slabFree }

// DeserNilSlabForTest drives the compiled deser directly with a nil slab,
// exactly as Decode does for slab-free schemas, regardless of
// classification. v must be a non-nil pointer. Used by the slab-free oracle
// net to prove that classification matches actual slab usage.
func (s *Schema) DeserNilSlabForTest(src []byte, v any) ([]byte, error) {
	return s.deser(src, reflect.ValueOf(v).Elem(), nil)
}
