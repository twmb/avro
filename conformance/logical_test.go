package conformance

import (
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// -----------------------------------------------------------------------
// Logical Type Edge Cases
// Spec: "Logical Types" — all logical types defined by the Avro spec.
//   - Timestamps: millis/micros/nanos since epoch, pre-epoch negative values
//   - Local timestamps: millis/micros/nanos (no timezone)
//   - Date: days since 1970-01-01
//   - Time-of-day: time-millis (int, ms), time-micros (long, µs)
//   - Decimal: bytes-encoded two's-complement scaled integer
//   - UUID: string or fixed(16) representation
//   - Duration: fixed(12), three little-endian uint32 fields
// https://avro.apache.org/docs/1.12.0/specification/#logical-types
// -----------------------------------------------------------------------

func TestSpecTimestampZeroValue(t *testing.T) {
	zero := time.Time{}

	t.Run("timestamp-millis", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"timestamp-millis"}`
		got := roundTrip(t, schema, zero)
		if !got.Equal(zero) {
			t.Fatalf("timestamp-millis zero: got %v, want %v", got, zero)
		}
	})

	t.Run("timestamp-micros", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"timestamp-micros"}`
		got := roundTrip(t, schema, zero)
		if !got.Equal(zero) {
			t.Fatalf("timestamp-micros zero: got %v, want %v", got, zero)
		}
	})
}

func TestSpecTimestampNanosOverflow(t *testing.T) {
	t.Run("within range", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"timestamp-nanos"}`
		ts := time.Date(2024, 1, 1, 0, 0, 0, 123456789, time.UTC)
		got := roundTrip(t, schema, ts)
		if !got.Equal(ts) {
			t.Fatalf("got %v, want %v", got, ts)
		}
	})

	t.Run("epoch exactly", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"timestamp-nanos"}`
		ts := time.Unix(0, 0).UTC()
		got := roundTrip(t, schema, ts)
		if !got.Equal(ts) {
			t.Fatalf("got %v, want %v", got, ts)
		}
	})

	t.Run("near boundary 2262", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"timestamp-nanos"}`
		ts := time.Date(2262, 4, 11, 0, 0, 0, 0, time.UTC)
		got := roundTrip(t, schema, ts)
		if !got.Equal(ts) {
			t.Fatalf("got %v, want %v", got, ts)
		}
	})
}

func TestSpecTimestampPreEpoch(t *testing.T) {
	t.Run("millis 1900", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"timestamp-millis"}`
		ts := time.Date(1900, 6, 15, 12, 0, 0, 0, time.UTC)
		got := roundTrip(t, schema, ts)
		want := ts.Truncate(time.Millisecond)
		if !got.Equal(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("micros 1000", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"timestamp-micros"}`
		ts := time.Date(1000, 3, 1, 0, 0, 0, 0, time.UTC)
		got := roundTrip(t, schema, ts)
		want := ts.Truncate(time.Microsecond)
		if !got.Equal(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
}

func TestSpecLocalTimestampRoundTrip(t *testing.T) {
	ts := time.Date(2024, 3, 1, 12, 34, 56, 789123456, time.UTC)

	t.Run("millis", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"local-timestamp-millis"}`
		got := roundTrip(t, schema, ts)
		if !got.Equal(ts.Truncate(time.Millisecond)) {
			t.Fatalf("got %v, want %v", got, ts.Truncate(time.Millisecond))
		}
	})

	t.Run("micros", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"local-timestamp-micros"}`
		got := roundTrip(t, schema, ts)
		if !got.Equal(ts.Truncate(time.Microsecond)) {
			t.Fatalf("got %v, want %v", got, ts.Truncate(time.Microsecond))
		}
	})

	t.Run("nanos", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"local-timestamp-nanos"}`
		got := roundTrip(t, schema, ts)
		if !got.Equal(ts) {
			t.Fatalf("got %v, want %v", got, ts)
		}
	})
}

func TestSpecDatePreEpoch(t *testing.T) {
	schema := `{"type":"int","logicalType":"date"}`

	t.Run("1969-12-31", func(t *testing.T) {
		ts := time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC)
		got := roundTrip(t, schema, ts)
		if !got.Equal(ts) {
			t.Fatalf("got %v, want %v", got, ts)
		}
	})

	t.Run("1900-01-01", func(t *testing.T) {
		ts := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
		got := roundTrip(t, schema, ts)
		if !got.Equal(ts) {
			t.Fatalf("got %v, want %v", got, ts)
		}
	})

	t.Run("epoch", func(t *testing.T) {
		ts := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		got := roundTrip(t, schema, ts)
		if !got.Equal(ts) {
			t.Fatalf("got %v, want %v", got, ts)
		}
	})
}

// Spec §time-millis: milliseconds after midnight as int.
func TestSpecTimeMillisRoundTrip(t *testing.T) {
	schema := `{"type":"int","logicalType":"time-millis"}`

	t.Run("zero", func(t *testing.T) {
		got := roundTrip(t, schema, time.Duration(0))
		if got != 0 {
			t.Fatalf("got %v, want 0", got)
		}
	})

	t.Run("45s 123ms", func(t *testing.T) {
		d := 45*time.Second + 123*time.Millisecond
		got := roundTrip(t, schema, d)
		if got != d {
			t.Fatalf("got %v, want %v", got, d)
		}
	})

	t.Run("max representable", func(t *testing.T) {
		// 23:59:59.999
		d := 23*time.Hour + 59*time.Minute + 59*time.Second + 999*time.Millisecond
		got := roundTrip(t, schema, d)
		if got != d {
			t.Fatalf("got %v, want %v", got, d)
		}
	})

	t.Run("one millisecond", func(t *testing.T) {
		d := time.Millisecond
		got := roundTrip(t, schema, d)
		if got != d {
			t.Fatalf("got %v, want %v", got, d)
		}
	})
}

// Spec §time-micros: microseconds after midnight as long.
func TestSpecTimeMicrosRoundTrip(t *testing.T) {
	schema := `{"type":"long","logicalType":"time-micros"}`

	t.Run("zero", func(t *testing.T) {
		got := roundTrip(t, schema, time.Duration(0))
		if got != 0 {
			t.Fatalf("got %v, want 0", got)
		}
	})

	t.Run("2m 500µs", func(t *testing.T) {
		d := 2*time.Minute + 500*time.Microsecond
		got := roundTrip(t, schema, d)
		if got != d {
			t.Fatalf("got %v, want %v", got, d)
		}
	})

	t.Run("max representable", func(t *testing.T) {
		// 23:59:59.999999
		d := 23*time.Hour + 59*time.Minute + 59*time.Second + 999999*time.Microsecond
		got := roundTrip(t, schema, d)
		if got != d {
			t.Fatalf("got %v, want %v", got, d)
		}
	})

	t.Run("one microsecond", func(t *testing.T) {
		d := time.Microsecond
		got := roundTrip(t, schema, d)
		if got != d {
			t.Fatalf("got %v, want %v", got, d)
		}
	})
}

func TestSpecDecimalBoundary(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`

	t.Run("zero", func(t *testing.T) {
		r := new(big.Rat).SetFloat64(0)
		got := roundTrip(t, schema, *r)
		if got.Cmp(r) != 0 {
			t.Fatalf("got %v, want 0", &got)
		}
	})

	t.Run("positive", func(t *testing.T) {
		r := new(big.Rat).SetFrac64(12345, 100)
		got := roundTrip(t, schema, *r)
		if got.Cmp(r) != 0 {
			t.Fatalf("got %v, want %v", &got, r)
		}
	})

	t.Run("negative", func(t *testing.T) {
		r := new(big.Rat).SetFrac64(-12345, 100)
		got := roundTrip(t, schema, *r)
		if got.Cmp(r) != 0 {
			t.Fatalf("got %v, want %v", &got, r)
		}
	})

	t.Run("max precision", func(t *testing.T) {
		r := new(big.Rat).SetFrac64(9999999999, 100)
		got := roundTrip(t, schema, *r)
		if got.Cmp(r) != 0 {
			t.Fatalf("got %v, want %v", &got, r)
		}
	})
}

func TestSpecUUIDRoundTrip(t *testing.T) {
	t.Run("string uuid", func(t *testing.T) {
		schema := `{"type":"string","logicalType":"uuid"}`
		uuid := "550e8400-e29b-41d4-a716-446655440000"
		got := roundTrip(t, schema, uuid)
		if got != uuid {
			t.Fatalf("got %q, want %q", got, uuid)
		}
	})

	t.Run("fixed16 uuid", func(t *testing.T) {
		schema := `{"type":"fixed","name":"uuid_t","size":16,"logicalType":"uuid"}`
		uuid := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
			0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
		got := roundTrip(t, schema, uuid)
		if got != uuid {
			t.Fatalf("got %v, want %v", got, uuid)
		}
	})
}

func TestSpecUUIDRejectsInvalidTextForUUIDType(t *testing.T) {
	s := mustParse(t, `{"type":"string","logicalType":"uuid"}`)
	data := []byte{0x08, 'b', 'a', 'd', '!'}
	var out [16]byte
	if _, err := s.Decode(data, &out); err == nil {
		t.Fatal("expected error for invalid UUID text")
	}
}

func TestSpecDurationRoundTrip(t *testing.T) {
	schema := `{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`

	t.Run("zero duration", func(t *testing.T) {
		d := avro.Duration{Months: 0, Days: 0, Milliseconds: 0}
		got := roundTrip(t, schema, d)
		if got != d {
			t.Fatalf("got %+v, want %+v", got, d)
		}
	})

	t.Run("typical duration", func(t *testing.T) {
		d := avro.Duration{Months: 12, Days: 30, Milliseconds: 3600000}
		got := roundTrip(t, schema, d)
		if got != d {
			t.Fatalf("got %+v, want %+v", got, d)
		}
	})

	t.Run("max uint32 values", func(t *testing.T) {
		d := avro.Duration{
			Months:       math.MaxUint32,
			Days:         math.MaxUint32,
			Milliseconds: math.MaxUint32,
		}
		got := roundTrip(t, schema, d)
		if got != d {
			t.Fatalf("got %+v, want %+v", got, d)
		}
	})
}
