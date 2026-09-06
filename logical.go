package avro

import (
	"fmt"
	"math"
	"time"
)

// Logical type conversions. Each pair converts between the raw Avro-native
// value and the enriched Go type. These are our single source of truth for
// the formulas: every encode, decode, JSON encode, and JSON decode path goes
// through them.

func timestampMillisToTime(val int64) time.Time { return time.UnixMilli(val).UTC() }

// timeToTimestampScaled converts t to (sec*scale + nsec/subScale) with int64
// overflow protection. Pass subScale=1 for nanos, which need no sub-second
// scaling. The seconds<0 && nanos>0 adjustment branch is required for inputs
// Go normalizes via time.UnixMilli(MinInt64) and friends: sec=-maxSec-1 with
// nsec in [1, 1e9) is below -maxSec under the symmetric form.
func timeToTimestampScaled(t time.Time, scale, subScale int64, unit string) (int64, error) {
	sec := t.Unix()
	nsec := int64(t.Nanosecond())
	maxSec := math.MaxInt64 / scale
	if sec < 0 && nsec > 0 {
		// Adjustment branch: scaled = (sec+1)*scale + (nsec/sub - scale).
		// Both terms are <= 0; only underflow is possible.
		if sec+1 < -maxSec {
			return 0, fmt.Errorf("time %v overflows int64 %s since epoch", t, unit)
		}
		scaled := (sec + 1) * scale
		adjustment := nsec/subScale - scale
		if adjustment < math.MinInt64-scaled {
			return 0, fmt.Errorf("time %v overflows int64 %s since epoch", t, unit)
		}
		return scaled + adjustment, nil
	}
	if sec > maxSec || sec < -maxSec {
		return 0, fmt.Errorf("time %v overflows int64 %s since epoch", t, unit)
	}
	total := sec * scale
	sub := nsec / subScale
	if sec == maxSec && sub > math.MaxInt64-total {
		return 0, fmt.Errorf("time %v overflows int64 %s since epoch", t, unit)
	}
	return total + sub, nil
}

func timeToTimestampMillis(t time.Time) (int64, error) {
	return timeToTimestampScaled(t, 1_000, 1_000_000, "milliseconds")
}

func timestampMicrosToTime(val int64) time.Time { return time.UnixMicro(val).UTC() }

func timeToTimestampMicros(t time.Time) (int64, error) {
	return timeToTimestampScaled(t, 1_000_000, 1_000, "microseconds")
}

func timestampNanosToTime(val int64) time.Time { return time.Unix(val/1e9, val%1e9).UTC() }

// The local-timestamp encoders read t's wall-clock fields as if UTC, whatever
// t's location, matching Java's TimeConversions.LocalTimestamp*Conversion and
// fastavro's tzinfo replace. Per the spec, local-timestamp-* "represents a
// timestamp in a local timezone, regardless of what specific time zone is
// considered local": the wire long encodes wall-clock components, not an
// instant. Decode produces a UTC time.Time, which holds the same components, so
// a Java-encoded value round-trips.

func timeToLocalUTC(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
}

func timeToLocalTimestampMillis(t time.Time) (int64, error) {
	return timeToTimestampMillis(timeToLocalUTC(t))
}

func timeToLocalTimestampMicros(t time.Time) (int64, error) {
	return timeToTimestampMicros(timeToLocalUTC(t))
}

func timeToLocalTimestampNanos(t time.Time) (int64, error) {
	return timeToTimestampNanos(timeToLocalUTC(t))
}

// timeToTimestampNanos passes subScale=1: the unit *is* nanoseconds, so
// nsec/1 == nsec.
//
// We diverge from Java's TimestampNanosConversion.toLong, whose adjustment
// branch subtracts nanos - 1_000_000 where the millis and micros branches
// subtract the scale: an off-by-1000 that corrupts every negative-second
// instant by about 999ms. We use the spec's sec*1e9 + nsec, the formula
// Java's other branches use. fastavro has no timestamp-nanos and avro-rs
// stores the raw int64, so neither corroborates either way.
func timeToTimestampNanos(t time.Time) (int64, error) {
	return timeToTimestampScaled(t, 1_000_000_000, 1, "nanoseconds")
}

// timeLogicalToInt64 returns nil if logical is not a long-typed time logical.
// JSON encode's "long" arm and the binary ser-side timestamp wrappers both go
// through here, so a new logical is wired in once.
func timeLogicalToInt64(logical string) func(time.Time) (int64, error) {
	switch logical {
	case "timestamp-millis":
		return timeToTimestampMillis
	case "timestamp-micros":
		return timeToTimestampMicros
	case "timestamp-nanos":
		return timeToTimestampNanos
	case "local-timestamp-millis":
		return timeToLocalTimestampMillis
	case "local-timestamp-micros":
		return timeToLocalTimestampMicros
	case "local-timestamp-nanos":
		return timeToLocalTimestampNanos
	}
	return nil
}

func dateToTime(val int32) time.Time { return time.Unix(int64(val)*86400, 0).UTC() }

// timeToDate converts t to epoch-days since 1970-01-01, taking its wall-clock
// year/month/day in its own location and ignoring the zone offset. Java's
// LocalDate.toEpochDay and fastavro's prepare_date are calendar-only the same
// way.
//
// We error when the day count leaves int32 range, reachable for a year
// outside roughly +/-5.8 million. Without the check, int32() truncation
// silently corrupts the wire value.
func timeToDate(t time.Time) (int32, error) {
	utc := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	days := utc.Unix() / 86400
	if days < math.MinInt32 || days > math.MaxInt32 {
		return 0, fmt.Errorf("date %v overflows int32 (days since epoch = %d)", t, days)
	}
	return int32(days), nil
}

func timeMillisToDuration(val int32) time.Duration { return time.Duration(val) * time.Millisecond }
func durationToTimeMillis(d time.Duration) (int32, error) {
	ms := d.Milliseconds()
	if ms < math.MinInt32 || ms > math.MaxInt32 {
		return 0, fmt.Errorf("duration %v overflows int32", d)
	}
	return int32(ms), nil
}

// timeMicrosToDuration errors when val * time.Microsecond would wrap (|val| >
// MaxInt64/Microsecond, about 9.2e15). The guard lives here so every caller
// (binary, unsafe, JSON-any, JSON-typed) shares one check instead of each
// remembering it.
func timeMicrosToDuration(val int64) (time.Duration, error) {
	if val > math.MaxInt64/int64(time.Microsecond) || val < math.MinInt64/int64(time.Microsecond) {
		return 0, fmt.Errorf("time-micros value %d overflows time.Duration", val)
	}
	return time.Duration(val) * time.Microsecond, nil
}

// timeOfDayToTime materializes a time-of-day duration as a time.Time at Unix
// epoch midnight (UTC). Avro's time-millis and time-micros annotate an
// int/long holding a time-of-day count. Go has no time-of-day type, so we
// pair them with time.Duration canonically, and with time.Time when you want
// the same target shape as date and timestamp-* (say a SchemaFor over a
// `time.Time` field tagged time-millis).
//
// The base date is arbitrary, since the encoder strips date components from
// any time.Time input, but it is stable: encoding then decoding a time.Time
// preserves the time-of-day fields and zeroes the date.
func timeOfDayToTime(d time.Duration) time.Time {
	return time.Unix(0, int64(d)).UTC()
}

// timeOfDay returns the time-of-day portion of t as a Duration since midnight
// in t's wall-clock fields, ignoring date and zone offset, which the Avro
// time-millis and time-micros wire forms cannot represent.
func timeOfDay(t time.Time) time.Duration {
	return time.Duration(t.Hour())*time.Hour +
		time.Duration(t.Minute())*time.Minute +
		time.Duration(t.Second())*time.Second +
		time.Duration(t.Nanosecond())
}
