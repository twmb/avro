package conformance

import (
	"math"
	"testing"

	"github.com/twmb/avro"
)

// -----------------------------------------------------------------------
// Type Promotion Matrix
// Spec: "Schema Resolution" — the 8 type promotions that a compliant
// implementation must support:
//   int → long, float, double
//   long → float, double
//   float → double
//   string → bytes
//   bytes → string
// https://avro.apache.org/docs/1.12.0/specification/#schema-resolution
// -----------------------------------------------------------------------

func TestPromotionIntToLong(t *testing.T) {
	writer := mustParse(t, `"int"`)
	reader := mustParse(t, `"long"`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	v := int32(42)
	encoded, err := writer.Encode(&v)
	if err != nil {
		t.Fatal(err)
	}
	var out int64
	_, err = resolved.Decode(encoded, &out)
	if err != nil {
		t.Fatal(err)
	}
	if out != 42 {
		t.Fatalf("got %d, want 42", out)
	}
}

func TestPromotionIntToFloat(t *testing.T) {
	writer := mustParse(t, `"int"`)
	reader := mustParse(t, `"float"`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	v := int32(42)
	encoded, err := writer.Encode(&v)
	if err != nil {
		t.Fatal(err)
	}
	var out float32
	_, err = resolved.Decode(encoded, &out)
	if err != nil {
		t.Fatal(err)
	}
	if out != 42.0 {
		t.Fatalf("got %v, want 42", out)
	}
}

func TestPromotionIntToDouble(t *testing.T) {
	writer := mustParse(t, `"int"`)
	reader := mustParse(t, `"double"`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	v := int32(42)
	encoded, err := writer.Encode(&v)
	if err != nil {
		t.Fatal(err)
	}
	var out float64
	_, err = resolved.Decode(encoded, &out)
	if err != nil {
		t.Fatal(err)
	}
	if out != 42.0 {
		t.Fatalf("got %v, want 42", out)
	}
}

func TestPromotionLongToFloat(t *testing.T) {
	writer := mustParse(t, `"long"`)
	reader := mustParse(t, `"float"`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	v := int64(42)
	encoded, err := writer.Encode(&v)
	if err != nil {
		t.Fatal(err)
	}
	var out float32
	_, err = resolved.Decode(encoded, &out)
	if err != nil {
		t.Fatal(err)
	}
	if out != 42.0 {
		t.Fatalf("got %v, want 42", out)
	}
}

func TestPromotionLongToDouble(t *testing.T) {
	writer := mustParse(t, `"long"`)
	reader := mustParse(t, `"double"`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	v := int64(42)
	encoded, err := writer.Encode(&v)
	if err != nil {
		t.Fatal(err)
	}
	var out float64
	_, err = resolved.Decode(encoded, &out)
	if err != nil {
		t.Fatal(err)
	}
	if out != 42.0 {
		t.Fatalf("got %v, want 42", out)
	}
}

func TestPromotionFloatToDouble(t *testing.T) {
	writer := mustParse(t, `"float"`)
	reader := mustParse(t, `"double"`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	v := float32(3.14)
	encoded, err := writer.Encode(&v)
	if err != nil {
		t.Fatal(err)
	}
	var out float64
	_, err = resolved.Decode(encoded, &out)
	if err != nil {
		t.Fatal(err)
	}
	// Float32 3.14 promoted to float64 should match the float32 value.
	if out != float64(float32(3.14)) {
		t.Fatalf("got %v, want %v", out, float64(float32(3.14)))
	}
}

func TestPromotionStringToBytes(t *testing.T) {
	writer := mustParse(t, `"string"`)
	reader := mustParse(t, `"bytes"`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	v := "hello"
	encoded, err := writer.Encode(&v)
	if err != nil {
		t.Fatal(err)
	}
	var out []byte
	_, err = resolved.Decode(encoded, &out)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "hello" {
		t.Fatalf("got %q, want hello", out)
	}
}

func TestPromotionBytesToString(t *testing.T) {
	writer := mustParse(t, `"bytes"`)
	reader := mustParse(t, `"string"`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	v := []byte("hello")
	encoded, err := writer.Encode(&v)
	if err != nil {
		t.Fatal(err)
	}
	var out string
	_, err = resolved.Decode(encoded, &out)
	if err != nil {
		t.Fatal(err)
	}
	if out != "hello" {
		t.Fatalf("got %q, want hello", out)
	}
}

func TestPromotionBoundaryValues(t *testing.T) {
	t.Run("MaxInt32 to long", func(t *testing.T) {
		writer := mustParse(t, `"int"`)
		reader := mustParse(t, `"long"`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		v := int32(math.MaxInt32)
		encoded, err := writer.Encode(&v)
		if err != nil {
			t.Fatal(err)
		}
		var out int64
		_, err = resolved.Decode(encoded, &out)
		if err != nil {
			t.Fatal(err)
		}
		if out != int64(math.MaxInt32) {
			t.Fatalf("got %d, want %d", out, math.MaxInt32)
		}
	})

	t.Run("MinInt32 to long", func(t *testing.T) {
		writer := mustParse(t, `"int"`)
		reader := mustParse(t, `"long"`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		v := int32(math.MinInt32)
		encoded, err := writer.Encode(&v)
		if err != nil {
			t.Fatal(err)
		}
		var out int64
		_, err = resolved.Decode(encoded, &out)
		if err != nil {
			t.Fatal(err)
		}
		if out != int64(math.MinInt32) {
			t.Fatalf("got %d, want %d", out, math.MinInt32)
		}
	})

	t.Run("MaxInt32 to double", func(t *testing.T) {
		writer := mustParse(t, `"int"`)
		reader := mustParse(t, `"double"`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		v := int32(math.MaxInt32)
		encoded, err := writer.Encode(&v)
		if err != nil {
			t.Fatal(err)
		}
		var out float64
		_, err = resolved.Decode(encoded, &out)
		if err != nil {
			t.Fatal(err)
		}
		if out != float64(math.MaxInt32) {
			t.Fatalf("got %v, want %v", out, float64(math.MaxInt32))
		}
	})

	t.Run("MaxInt64 to double", func(t *testing.T) {
		writer := mustParse(t, `"long"`)
		reader := mustParse(t, `"double"`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		v := int64(math.MaxInt64)
		encoded, err := writer.Encode(&v)
		if err != nil {
			t.Fatal(err)
		}
		var out float64
		_, err = resolved.Decode(encoded, &out)
		if err != nil {
			t.Fatal(err)
		}
		// MaxInt64 loses precision when converted to float64, but the
		// conversion should match Go's behavior.
		if out != float64(math.MaxInt64) {
			t.Fatalf("got %v, want %v", out, float64(math.MaxInt64))
		}
	})

	t.Run("empty string to bytes", func(t *testing.T) {
		writer := mustParse(t, `"string"`)
		reader := mustParse(t, `"bytes"`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		v := ""
		encoded, err := writer.Encode(&v)
		if err != nil {
			t.Fatal(err)
		}
		var out []byte
		_, err = resolved.Decode(encoded, &out)
		if err != nil {
			t.Fatal(err)
		}
		if len(out) != 0 {
			t.Fatalf("got %v, want empty", out)
		}
	})
}
