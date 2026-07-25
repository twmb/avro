package avro

import "reflect"

// Test-only bridges for the external avro_test package (compiled only into
// the test binary; not part of the library surface).

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
