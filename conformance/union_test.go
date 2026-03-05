package conformance

import (
	"reflect"
	"testing"
)

// -----------------------------------------------------------------------
// Union & Null Edge Cases
// Spec: "Unions" (Schema Declaration) and "Binary Encoding" — union
// branch selection, null encoding, and interaction with container types.
// https://avro.apache.org/docs/1.12.0/specification/#unions
// -----------------------------------------------------------------------

func TestSpecEmptyArrayInUnion(t *testing.T) {
	schema := `["null", {"type":"array","items":"int"}]`

	arr := []int32{}
	dst := encode(t, schema, &arr)
	if len(dst) < 1 || dst[0] != 0x02 {
		t.Fatalf("empty array in union: got branch index %x, want 02 (array branch)", dst[0])
	}

	var result []int32
	decode(t, schema, dst, &result)
	if len(result) != 0 {
		t.Fatalf("expected empty slice, got %v", result)
	}
}

func TestSpecNilSliceInUnion(t *testing.T) {
	type W struct {
		Arr *[]int32 `avro:"arr"`
	}
	schema := `{"type":"record","name":"W","fields":[
		{"name":"arr","type":["null",{"type":"array","items":"int"}]}
	]}`

	dst := encode(t, schema, &W{Arr: nil})
	if len(dst) != 1 || dst[0] != 0x00 {
		t.Fatalf("nil pointer in union: got %x, want 00 (null branch)", dst)
	}

	var result W
	decode(t, schema, dst, &result)
	if result.Arr != nil {
		t.Fatalf("expected nil, got %v", result.Arr)
	}
}

func TestSpecNilMapInUnion(t *testing.T) {
	type W struct {
		M *map[string]string `avro:"m"`
	}
	schema := `{"type":"record","name":"W","fields":[
		{"name":"m","type":["null",{"type":"map","values":"string"}]}
	]}`

	dst := encode(t, schema, &W{M: nil})
	if len(dst) != 1 || dst[0] != 0x00 {
		t.Fatalf("nil pointer in union: got %x, want 00 (null branch)", dst)
	}

	var result W
	decode(t, schema, dst, &result)
	if result.M != nil {
		t.Fatalf("expected nil, got %v", result.M)
	}
}

func TestSpecUnionMultipleNamedTypes(t *testing.T) {
	schema := `[
		"null",
		{"type":"record","name":"Cat","fields":[{"name":"meow","type":"string"}]},
		{"type":"record","name":"Dog","fields":[{"name":"bark","type":"string"}]}
	]`

	t.Run("null branch via decode", func(t *testing.T) {
		var result any
		decode(t, schema, []byte{0x00}, &result)
		if result != nil {
			t.Fatalf("null branch: got %v, want nil", result)
		}
	})

	t.Run("first record branch", func(t *testing.T) {
		v := any(map[string]any{"meow": "purr"})
		dst := encode(t, schema, &v)
		if dst[0] != 0x02 {
			t.Fatalf("Cat: got branch %x, want 02", dst[0])
		}
		var result any
		decode(t, schema, dst, &result)
		m, ok := result.(map[string]any)
		if !ok {
			t.Fatalf("expected map, got %T", result)
		}
		if m["meow"] != "purr" {
			t.Fatalf("got meow=%v, want purr", m["meow"])
		}
	})

	t.Run("second record branch", func(t *testing.T) {
		v := any(map[string]any{"bark": "woof"})
		dst := encode(t, schema, &v)
		if dst[0] != 0x04 {
			t.Fatalf("Dog: got branch %x, want 04", dst[0])
		}
		var result any
		decode(t, schema, dst, &result)
		m := result.(map[string]any)
		if m["bark"] != "woof" {
			t.Fatalf("got bark=%v, want woof", m["bark"])
		}
	})
}

func TestSpecNonEmptyArrayInUnionRoundTrip(t *testing.T) {
	schema := `["null", {"type":"array","items":"int"}]`

	arr := []int32{10, 20, 30}
	dst := encode(t, schema, &arr)
	if dst[0] != 0x02 {
		t.Fatalf("non-empty array branch: got %x, want 02", dst[0])
	}

	var result []int32
	decode(t, schema, dst, &result)
	if !reflect.DeepEqual(result, arr) {
		t.Fatalf("got %v, want %v", result, arr)
	}
}
