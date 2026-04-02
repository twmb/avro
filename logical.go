package avro

import (
	"fmt"
	"math"
	"time"
)

// Logical type conversion functions. Each pair converts between the raw
// Avro-native value and the enriched Go type. These are the single
// source of truth for conversion formulas — all encode, decode, JSON
// encode, and JSON decode paths reference these.

func timestampMillisToTime(val int64) time.Time { return time.UnixMilli(val).UTC() }
func timeToTimestampMillis(t time.Time) int64   { return t.UnixMilli() }

func timestampMicrosToTime(val int64) time.Time { return time.UnixMicro(val).UTC() }
func timeToTimestampMicros(t time.Time) int64   { return t.UnixMicro() }

func timestampNanosToTime(val int64) time.Time { return time.Unix(val/1e9, val%1e9).UTC() }
func timeToTimestampNanos(t time.Time) int64   { return t.Unix()*1e9 + int64(t.Nanosecond()) }

func dateToTime(val int32) time.Time { return time.Unix(int64(val)*86400, 0).UTC() }
func timeToDate(t time.Time) int32   { return int32(floorDiv(t.Unix(), 86400)) }

func timeMillisToDuration(val int32) time.Duration { return time.Duration(val) * time.Millisecond }
func durationToTimeMillis(d time.Duration) (int32, error) {
	ms := d.Milliseconds()
	if ms < math.MinInt32 || ms > math.MaxInt32 {
		return 0, fmt.Errorf("duration %v overflows int32", d)
	}
	return int32(ms), nil
}

func timeMicrosToDuration(val int64) time.Duration { return time.Duration(val) * time.Microsecond }
func durationToTimeMicros(d time.Duration) int64   { return d.Microseconds() }

// floorDiv returns the floor of a/b (rounds toward negative infinity),
// unlike Go's built-in integer division which truncates toward zero.
func floorDiv(a, b int64) int64 {
	d := a / b
	if (a^b) < 0 && d*b != a {
		d--
	}
	return d
}

// timeToUnixNanos converts a time.Time to nanoseconds since the Unix epoch
// without the overflow issues of time.Time.UnixNano (which is undefined
// outside ~1678-2262).
func timeToUnixNanos(t time.Time) int64 {
	return t.Unix()*1e9 + int64(t.Nanosecond())
}
