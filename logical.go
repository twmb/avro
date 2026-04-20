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

// timeToTimestampNanos converts t to nanoseconds since the Unix epoch,
// returning an error when the result would overflow int64. Representable
// range is approximately 1677-09-21 to 2262-04-11 UTC.
func timeToTimestampNanos(t time.Time) (int64, error) {
	sec := t.Unix()
	// MaxInt64 = 9_223_372_036 sec + 854_775_807 ns. Bound sec
	// symmetrically so sec*1e9 stays well within int64; since nsec ≥ 0
	// the addition can only push upward, so underflow is impossible.
	const maxSec = math.MaxInt64 / 1_000_000_000
	if sec > maxSec || sec < -maxSec {
		return 0, fmt.Errorf("time %v overflows int64 nanoseconds since epoch", t)
	}
	total := sec * 1_000_000_000
	nsec := int64(t.Nanosecond())
	// Only upward overflow is possible, and only when sec == maxSec.
	// (If sec < maxSec, total ≤ (maxSec-1)*1e9, leaving >1e9 of headroom,
	// and nsec < 1e9.)
	if sec == maxSec && nsec > math.MaxInt64-total {
		return 0, fmt.Errorf("time %v overflows int64 nanoseconds since epoch", t)
	}
	return total + nsec, nil
}

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

