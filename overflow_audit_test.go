package avro

import (
	"encoding/json"
	"math"
	"testing"
)

// TestOverflowAuditAllPaths runs integer-overflow probes through every decode
// entry point that accepts a Go integer target, ensuring silent wrap/truncation
// cannot occur.
func TestOverflowAuditAllPaths(t *testing.T) {
	t.Run("binary deserInt: int->int8 overflow", func(t *testing.T) {
		s := MustParse(`"int"`)
		data, err := s.Encode(int32(200))
		if err != nil {
			t.Fatal(err)
		}
		var out int8
		if _, err := s.Decode(data, &out); err == nil {
			t.Fatalf("expected overflow error, got %d", out)
		}
	})

	t.Run("binary deserLong: long->int32 overflow", func(t *testing.T) {
		s := MustParse(`"long"`)
		data, err := s.Encode(int64(1) << 33)
		if err != nil {
			t.Fatal(err)
		}
		var out int32
		if _, err := s.Decode(data, &out); err == nil {
			t.Fatalf("expected overflow error, got %d", out)
		}
	})

	t.Run("promote int->long with int16 target", func(t *testing.T) {
		// Writer says int, reader says long. Reader Go target is int16.
		// Value 100000 fits in int32 but overflows int16.
		writer := MustParse(`"int"`)
		reader := MustParse(`"long"`)
		data, err := writer.Encode(int32(100000))
		if err != nil {
			t.Fatal(err)
		}
		resolved, err := Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		var out int16
		if _, err := resolved.Decode(data, &out); err == nil {
			t.Fatalf("expected overflow error, got %d", out)
		}
	})

	t.Run("enum ordinal overflow", func(t *testing.T) {
		// Build an enum with 200 symbols, target int8.
		symbols := `["`
		for i := 0; i < 200; i++ {
			if i > 0 {
				symbols += `","`
			}
			symbols += `s` + itoa(i)
		}
		symbols += `"]`
		s := MustParse(`{"type":"enum","name":"E","symbols":` + symbols + `}`)
		// Encode symbol at ordinal 150.
		data, err := s.Encode("s150")
		if err != nil {
			t.Fatal(err)
		}
		var out int8
		if _, err := s.Decode(data, &out); err == nil {
			t.Fatalf("expected enum overflow, got %d", out)
		}
	})

	t.Run("logical-type decoder: timestamp-millis into int32 overflow", func(t *testing.T) {
		// timestamp-millis values routinely exceed int32. Encode a modern
		// timestamp (millis since epoch > 2^31).
		s := MustParse(`{"type":"long","logicalType":"timestamp-millis"}`)
		// ~2024 in millis since epoch is ~1.7e12, well beyond int32.
		data, err := s.Encode(int64(1700000000000))
		if err != nil {
			t.Fatal(err)
		}
		var out int32
		if _, err := s.Decode(data, &out); err == nil {
			t.Fatalf("expected overflow, got %d", out)
		}
	})

	t.Run("JSON decodeInt: int->int8 overflow", func(t *testing.T) {
		s := MustParse(`"int"`)
		var out int8
		err := s.DecodeJSON([]byte(`200`), &out)
		if err == nil {
			t.Fatalf("expected overflow, got %d", out)
		}
	})

	t.Run("JSON decodeLong: long->int32 overflow", func(t *testing.T) {
		s := MustParse(`"long"`)
		var out int32
		err := s.DecodeJSON([]byte(`2147483648`), &out)
		if err == nil {
			t.Fatalf("expected overflow, got %d", out)
		}
	})

	t.Run("negative into unsigned target", func(t *testing.T) {
		s := MustParse(`"int"`)
		data, err := s.Encode(int32(-1))
		if err != nil {
			t.Fatal(err)
		}
		var out uint32
		if _, err := s.Decode(data, &out); err == nil {
			t.Fatalf("expected overflow, got %d", out)
		}
	})

	t.Run("sanity: in-range values still decode", func(t *testing.T) {
		s := MustParse(`"long"`)
		data, err := s.Encode(int64(42))
		if err != nil {
			t.Fatal(err)
		}
		var out int32
		if _, err := s.Decode(data, &out); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if out != 42 {
			t.Fatalf("got %d, want 42", out)
		}
	})
}

// TestFloatOverflowAllPaths exercises every path where narrowing float64 to
// float32 could silently produce ±Inf. Overflow must error; NaN/±Inf input
// must pass through; normal precision-loss rounding is allowed.
func TestFloatOverflowAllPaths(t *testing.T) {
	overflow := math.MaxFloat32 * 2 // finite float64 that becomes +Inf in float32

	t.Run("binary deserDouble: overflow into float32", func(t *testing.T) {
		s := MustParse(`"double"`)
		data, err := s.Encode(overflow)
		if err != nil {
			t.Fatal(err)
		}
		var out float32
		if _, err := s.Decode(data, &out); err == nil {
			t.Fatalf("expected overflow error, got %v", out)
		}
	})

	t.Run("binary deserDouble: ±Inf passes through", func(t *testing.T) {
		s := MustParse(`"double"`)
		for _, v := range []float64{math.Inf(+1), math.Inf(-1)} {
			data, err := s.Encode(v)
			if err != nil {
				t.Fatal(err)
			}
			var out float32
			if _, err := s.Decode(data, &out); err != nil {
				t.Fatalf("unexpected error on %v: %v", v, err)
			}
			if !math.IsInf(float64(out), 0) {
				t.Fatalf("%v did not round-trip: got %v", v, out)
			}
		}
	})

	t.Run("binary deserDouble: NaN passes through", func(t *testing.T) {
		s := MustParse(`"double"`)
		data, err := s.Encode(math.NaN())
		if err != nil {
			t.Fatal(err)
		}
		var out float32
		if _, err := s.Decode(data, &out); err != nil {
			t.Fatalf("unexpected error on NaN: %v", err)
		}
		if !math.IsNaN(float64(out)) {
			t.Fatalf("NaN did not round-trip: got %v", out)
		}
	})

	t.Run("binary deserDouble: in-range rounding is silent", func(t *testing.T) {
		s := MustParse(`"double"`)
		precise := 1.1234567890123456
		data, err := s.Encode(precise)
		if err != nil {
			t.Fatal(err)
		}
		var out float32
		if _, err := s.Decode(data, &out); err != nil {
			t.Fatalf("in-range value errored: %v", err)
		}
	})

	t.Run("serFloat: float64 overflow into avro float", func(t *testing.T) {
		s := MustParse(`"float"`)
		if _, err := s.Encode(overflow); err == nil {
			t.Fatalf("expected overflow error on encode")
		}
	})

	t.Run("serFloat: ±Inf passes through", func(t *testing.T) {
		s := MustParse(`"float"`)
		for _, v := range []float64{math.Inf(+1), math.Inf(-1)} {
			if _, err := s.Encode(v); err != nil {
				t.Fatalf("unexpected error encoding %v: %v", v, err)
			}
		}
	})

	t.Run("JSON decodeDouble: overflow into float32", func(t *testing.T) {
		s := MustParse(`"double"`)
		var out float32
		// Use a large exact-format number that parses but overflows float32.
		if err := s.DecodeJSON([]byte(`6.805646932770577e+38`), &out); err == nil {
			t.Fatalf("expected overflow, got %v", out)
		}
	})

	t.Run("EncodeJSON float: float64 overflow into avro float", func(t *testing.T) {
		// Match binary serFloat: reject finite float64 values that clamp
		// to ±Inf when narrowed to float32, rather than silently emitting
		// the invalid JSON literal "+Inf".
		s := MustParse(`"float"`)
		if _, err := s.EncodeJSON(overflow); err == nil {
			t.Fatal("expected overflow error on EncodeJSON")
		}
	})

	t.Run("EncodeJSON float: ±Inf and NaN pass through", func(t *testing.T) {
		s := MustParse(`"float"`)
		for _, v := range []float64{math.Inf(+1), math.Inf(-1), math.NaN()} {
			if _, err := s.EncodeJSON(v); err != nil {
				t.Fatalf("unexpected error encoding %v: %v", v, err)
			}
		}
	})

	t.Run("EncodeJSON float: json.Number overflow", func(t *testing.T) {
		s := MustParse(`"float"`)
		if _, err := s.EncodeJSON(json.Number("1e100")); err == nil {
			t.Fatal("expected overflow error for json.Number(1e100)")
		}
	})
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf []byte
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	return string(buf)
}
