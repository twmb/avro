package avro_test

import (
	"fmt"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Dual-path parity net — the STANDING guarantee that closes the recurring
// "reflect path tested, unsafe path missed" class.
//
// twmb has two encode paths: the REFLECT serializers (top-level values, []any,
// map[string]any) and the UNSAFE serializers (selected for an ADDRESSABLE
// struct field, via serRecordFast). Bug after bug landed where a fix or a net
// covered the reflect path and silently missed its compiled unsafe twin
// (usArrayRecord's zero-byte cap; usTimeMillisTime/usTimeMicrosTime were never
// executed at all). Vigilance is not a fix; this driver is.
//
// For a single-field record the wire is exactly the field's encoding (records
// have no framing), so encoding a value TOP-LEVEL (reflect) and as the single
// field of an ADDRESSABLE record struct (unsafe) MUST produce byte-identical
// wire, and both must decode back. The battery below crosses every concrete Go
// type → schema mapping, so the unsafe encoder/decoder for each is exercised
// and held to parity with the reflect one. Any new type mapping MUST add a row
// here. (Run by default through BOTH paths — do not write a value-driven net
// that only drives top-level reflect.)
// ---------------------------------------------------------------------------

func TestMatrix_ReflectUnsafePathParity(t *testing.T) {
	type inner struct {
		N int32 `avro:"n"`
	}
	rat := func(n, d int64) *big.Rat { return big.NewRat(n, d) }

	rows := []struct {
		label  string
		schema string // the FIELD/value schema
		value  any    // a concrete-typed value (NOT any/interface) so unsafe engages
	}{
		{"int32", `"int"`, int32(-7)},
		{"int64", `"long"`, int64(1 << 40)},
		{"float32", `"float"`, float32(1.5)},
		{"float64", `"double"`, float64(2.5)},
		{"bool", `"boolean"`, true},
		{"string", `"string"`, "héllo"},
		{"bytes", `"bytes"`, []byte{1, 2, 3}},
		{"fixed16", `{"type":"fixed","name":"F16","size":16}`, [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
		{"uuid-fixed", `{"type":"fixed","name":"FU","size":16,"logicalType":"uuid"}`, [16]byte{15: 1}},

		// Logical TIME types — both the time.Duration carrier and the
		// time.Time carrier. The time.Time forms against time-millis/micros
		// are usTimeMillisTime/usTimeMicrosTime: ZERO test coverage until now.
		{"date", `{"type":"int","logicalType":"date"}`, time.Date(2020, 6, 1, 0, 0, 0, 0, time.UTC)},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, time.UnixMilli(1234567).UTC()},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, time.UnixMicro(1234567).UTC()},
		{"timestamp-nanos", `{"type":"long","logicalType":"timestamp-nanos"}`, time.Unix(0, 1234567890123).UTC()},
		// local-timestamp-* and timestamp-nanos exercise usTimestampNanos /
		// usLocalTimestamp{Millis,Micros,Nanos} — unsafe twins the parity
		// battery previously omitted (twin-path catalog GAP).
		{"local-timestamp-millis", `{"type":"long","logicalType":"local-timestamp-millis"}`, time.Date(2020, 1, 2, 3, 4, 5, 6000000, time.UTC)},
		{"local-timestamp-micros", `{"type":"long","logicalType":"local-timestamp-micros"}`, time.Date(2020, 1, 2, 3, 4, 5, 6000, time.UTC)},
		{"local-timestamp-nanos", `{"type":"long","logicalType":"local-timestamp-nanos"}`, time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC)},
		{"time-millis/duration", `{"type":"int","logicalType":"time-millis"}`, 3*time.Hour + 14*time.Minute},
		{"time-micros/duration", `{"type":"long","logicalType":"time-micros"}`, 3*time.Hour + 14*time.Minute + 159*time.Microsecond},
		{"time-millis/time", `{"type":"int","logicalType":"time-millis"}`, time.Date(2020, 1, 1, 3, 14, 15, 0, time.UTC)},
		{"time-micros/time", `{"type":"long","logicalType":"time-micros"}`, time.Date(2020, 1, 1, 3, 14, 15, 926000, time.UTC)},
		{"duration-fixed", `{"type":"fixed","name":"Dur","size":12,"logicalType":"duration"}`, avro.Duration{Months: 1, Days: 2, Milliseconds: 3}},

		{"decimal", `{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}`, rat(1234, 100)},
		// uuid on a STRING carrier (usUUID/usFixedUUIDString string arm) —
		// another unsafe twin omitted from the battery.
		{"uuid-string", `{"type":"string","logicalType":"uuid"}`, "12345678-1234-1234-1234-123456789abc"},

		// Composites as struct fields (the unsafe array/map/union encoders).
		{"slice-int", `{"type":"array","items":"int"}`, []int32{1, 2, 3}},
		{"slice-record", `{"type":"array","items":{"type":"record","name":"AR","fields":[{"name":"n","type":"int"}]}}`, []inner{{1}, {2}}},
		{"slice-ptr-record", `{"type":"array","items":{"type":"record","name":"APR","fields":[{"name":"n","type":"int"}]}}`, []*inner{{1}, {2}}},
		{"map-int", `{"type":"map","values":"int"}`, map[string]int32{"a": 1}},
		{"nested-record", `{"type":"record","name":"NR","fields":[{"name":"n","type":"int"}]}`, inner{42}},
		{"ptr-int/nullable", `["null","int"]`, func() *int32 { x := int32(9); return &x }()},
		{"nil-ptr/nullable", `["null","int"]`, (*int32)(nil)},
		{"slice-null-union", `{"type":"array","items":["null","int"]}`, []*int32{nil, func() *int32 { x := int32(5); return &x }()}},
	}

	for _, r := range rows {
		t.Run(r.label, func(t *testing.T) {
			fieldS := avro.MustParse(r.schema)
			recS := avro.MustParse(fmt.Sprintf(
				`{"type":"record","name":"DP","fields":[{"name":"f","type":%s}]}`, r.schema))

			// Reflect path: encode the value top-level.
			topWire, err := fieldS.AppendEncode(nil, r.value)
			if err != nil {
				t.Fatalf("reflect (top-level) encode: %v", err)
			}

			// Unsafe path: the same value as the single field of an
			// ADDRESSABLE record struct. A single-field record's wire is
			// exactly the field's encoding, so the two MUST be byte-identical.
			st := reflect.StructOf([]reflect.StructField{
				{Name: "F", Type: reflect.TypeOf(r.value), Tag: `avro:"f"`},
			})
			pv := reflect.New(st) // pointer → addressable → unsafe field path
			pv.Elem().Field(0).Set(reflect.ValueOf(r.value))
			recWire, err := recS.AppendEncode(nil, pv.Interface())
			if err != nil {
				t.Fatalf("unsafe (struct-field) encode: %v", err)
			}

			if string(topWire) != string(recWire) {
				t.Fatalf("REFLECT↔UNSAFE WIRE DIVERGENCE for %s:\n reflect=%x\n unsafe =%x", r.label, topWire, recWire)
			}

			// Both wires must decode back through their own path.
			var topBack any
			if _, err := fieldS.Decode(topWire, &topBack); err != nil {
				t.Fatalf("reflect decode of own wire: %v", err)
			}
			recBack := reflect.New(st)
			if _, err := recS.Decode(recWire, recBack.Interface()); err != nil {
				t.Fatalf("unsafe decode of own wire: %v", err)
			}
		})
	}
}
