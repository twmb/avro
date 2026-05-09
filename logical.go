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

// timeToTimestampMillis converts t to milliseconds since the Unix epoch,
// returning an error when the result would overflow int64. Mirrors
// Java's Instant.toEpochMilli (used by TimestampMillisConversion.toLong),
// including the seconds<0 && nanos>0 adjustment branch that accepts
// time.UnixMilli(MinInt64) — the symmetric form sec*1000+nsec/1e6 would
// reject those inputs because Go normalizes them to sec=-maxSec-1
// with nsec in [1, 1e9), which is below -maxSec.
func timeToTimestampMillis(t time.Time) (int64, error) {
	sec := t.Unix()
	nsec := int64(t.Nanosecond())
	const scale = int64(1_000)
	const subScale = int64(1_000_000) // ns per ms
	const maxSec = math.MaxInt64 / scale
	if sec < 0 && nsec > 0 {
		// Adjustment branch: millis = (sec+1)*scale + (nsec/sub - scale).
		// Both terms are ≤ 0; only underflow is possible.
		if sec+1 < -maxSec {
			return 0, fmt.Errorf("time %v overflows int64 milliseconds since epoch", t)
		}
		millis := (sec + 1) * scale
		adjustment := nsec/subScale - scale
		if adjustment < math.MinInt64-millis {
			return 0, fmt.Errorf("time %v overflows int64 milliseconds since epoch", t)
		}
		return millis + adjustment, nil
	}
	if sec > maxSec || sec < -maxSec {
		return 0, fmt.Errorf("time %v overflows int64 milliseconds since epoch", t)
	}
	total := sec * scale
	ms := nsec / subScale
	if sec == maxSec && ms > math.MaxInt64-total {
		return 0, fmt.Errorf("time %v overflows int64 milliseconds since epoch", t)
	}
	return total + ms, nil
}

func timestampMicrosToTime(val int64) time.Time { return time.UnixMicro(val).UTC() }

// timeToTimestampMicros mirrors timeToTimestampMillis, with the
// micros bound and Java parity (TimestampMicrosConversion.toLong has
// the explicit `seconds<0 && nanos>0` adjustment branch).
func timeToTimestampMicros(t time.Time) (int64, error) {
	sec := t.Unix()
	nsec := int64(t.Nanosecond())
	const scale = int64(1_000_000)
	const subScale = int64(1_000) // ns per µs
	const maxSec = math.MaxInt64 / scale
	if sec < 0 && nsec > 0 {
		if sec+1 < -maxSec {
			return 0, fmt.Errorf("time %v overflows int64 microseconds since epoch", t)
		}
		micros := (sec + 1) * scale
		adjustment := nsec/subScale - scale
		if adjustment < math.MinInt64-micros {
			return 0, fmt.Errorf("time %v overflows int64 microseconds since epoch", t)
		}
		return micros + adjustment, nil
	}
	if sec > maxSec || sec < -maxSec {
		return 0, fmt.Errorf("time %v overflows int64 microseconds since epoch", t)
	}
	total := sec * scale
	us := nsec / subScale
	if sec == maxSec && us > math.MaxInt64-total {
		return 0, fmt.Errorf("time %v overflows int64 microseconds since epoch", t)
	}
	return total + us, nil
}

func timestampNanosToTime(val int64) time.Time { return time.Unix(val/1e9, val%1e9).UTC() }

// Local-timestamp encoders interpret t's wall-clock fields as if they
// were UTC, matching Java's reference behavior:
//   Instant instant = timestamp.toInstant(ZoneOffset.UTC);
// (See org.apache.avro.data.TimeConversions.LocalTimestampMillisConversion;
// fastavro does the same via data.replace(tzinfo=datetime.timezone.utc).)
//
// Avro 1.12 spec: local-timestamp-* "represents a timestamp in a local
// timezone, regardless of what specific time zone is considered local"
// — i.e., the wire long encodes wall-clock components, not an instant.
//
// twmb/avro decodes local-timestamps to UTC time.Time, which already
// preserves the wall-clock components, so a Java-encoded value
// round-trips correctly. Encode treats the time.Time's wall-clock
// fields as if UTC (see timeToLocalTimestamp* below) regardless of
// the input's location.

func timeToLocalTimestampMillis(t time.Time) (int64, error) {
	return timeToTimestampMillis(time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC))
}

func timeToLocalTimestampMicros(t time.Time) (int64, error) {
	return timeToTimestampMicros(time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC))
}

func timeToLocalTimestampNanos(t time.Time) (int64, error) {
	return timeToTimestampNanos(time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC))
}

// timeToTimestampNanos converts t to nanoseconds since the Unix epoch,
// returning an error when the result would overflow int64.
//
// We deliberately diverge from Java's TimestampNanosConversion.toLong
// here: Java's adjustment branch at TimeConversions.java:238 has an
// off-by-1000 typo (uses `nanos - 1_000_000` where the analogous
// millis/micros branches subtract `scale` — `nanos - 1_000_000_000`
// for nanos), which would corrupt every negative-second instant by
// ~999ms. Java's millis/micros conversions are correct so we mirror
// them; nanos aligns with avro-rs and fastavro, both of which produce
// the mathematically correct sec*1e9 + nsec via the same adjustment
// formula we use here.
func timeToTimestampNanos(t time.Time) (int64, error) {
	sec := t.Unix()
	nsec := int64(t.Nanosecond())
	const scale = int64(1_000_000_000)
	const maxSec = math.MaxInt64 / scale
	if sec < 0 && nsec > 0 {
		// Adjustment branch: nanos = (sec+1)*scale + (nsec - scale).
		// Algebraically equivalent to sec*scale + nsec but avoids the
		// (sec)*scale multiplication overflow when sec is very negative
		// (e.g. time.Unix(0, MinInt64) normalizes to sec=-MaxInt64/scale-1
		// with nsec ≈ 145_224_192).
		if sec+1 < -maxSec {
			return 0, fmt.Errorf("time %v overflows int64 nanoseconds since epoch", t)
		}
		nanos := (sec + 1) * scale
		adjustment := nsec - scale
		if adjustment < math.MinInt64-nanos {
			return 0, fmt.Errorf("time %v overflows int64 nanoseconds since epoch", t)
		}
		return nanos + adjustment, nil
	}
	// MaxInt64 = 9_223_372_036 sec + 854_775_807 ns. Bound sec
	// symmetrically so sec*1e9 stays well within int64; since nsec ≥ 0
	// the addition can only push upward, so underflow is impossible.
	if sec > maxSec || sec < -maxSec {
		return 0, fmt.Errorf("time %v overflows int64 nanoseconds since epoch", t)
	}
	total := sec * scale
	if sec == maxSec && nsec > math.MaxInt64-total {
		return 0, fmt.Errorf("time %v overflows int64 nanoseconds since epoch", t)
	}
	return total + nsec, nil
}

// timeLogicalToInt64 returns the time.Time→int64 conversion for any
// long-typed time logical (timestamp / local-timestamp at millis,
// micros, or nanos resolution), or nil if logical is not one of those.
// Single source of truth for the mapping — used by JSON encode's
// "long" arm and by the binary ser-side timestamp wrappers — so a new
// logical addition only needs to be wired in once.
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

// timeToDate converts a time.Time to its Avro date logical value
// (epoch-days since 1970-01-01). Calendar-date interpretation: takes
// t's wall-clock year/month/day in t's own location, ignoring the
// zone offset. Mirrors Java's LocalDate.toEpochDay and fastavro's
// prepare_date (both calendar-only). Re-anchoring wall-clock fields
// at UTC for the day count is the same shape used by
// timeToLocalTimestamp* for the long-typed wall-clock logicals.
//
// Pre-fix this used floorDiv(t.Unix(), 86400) which is the UTC
// instant's day. For a time.Time whose wall-clock date is D in a
// non-UTC zone, the encoded day was D-1 or D+1 — wire value differed
// from Java for the same calendar date, and a TZ-offset string like
// "2020-01-01T00:00:00+05:00" encoded to a different day than the
// bare "2020-01-01" form.
//
// Returns an error when the day count exceeds int32 range —
// possible for time.Time values whose year falls outside roughly
// ±5.8 million. Without the bounds check, int32(...) silent
// truncation would corrupt the wire value.
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

// timeMicrosToDuration converts a wire-decoded time-micros value to a
// time.Duration. Returns an error when val * time.Microsecond would
// wrap (|val| > MaxInt64/Microsecond ≈ 9.2e15). The guard lives here
// so every caller (binary, unsafe, JSON-any, JSON-typed) uses the
// same check rather than each remembering it independently.
func timeMicrosToDuration(val int64) (time.Duration, error) {
	if val > math.MaxInt64/int64(time.Microsecond) || val < math.MinInt64/int64(time.Microsecond) {
		return 0, fmt.Errorf("time-micros value %d overflows time.Duration", val)
	}
	return time.Duration(val) * time.Microsecond, nil
}

// timeOfDayToTime materializes a time-of-day duration as a time.Time at
// the Unix epoch midnight (UTC). Avro's time-millis / time-micros
// logical types annotate an int/long storing a time-of-day count; Go
// has no time-of-day-only type, so we pair them with time.Duration
// canonically and with time.Time when callers want the same target
// shape used for date / timestamp-* logical types (e.g. SchemaFor
// generated from a `time.Time` field with a time-millis tag).
//
// The base date is arbitrary — the encoder strips date components from
// any time.Time input — but stable, so encode → decode of a time.Time
// preserves the time-of-day fields and zeroes the date.
func timeOfDayToTime(d time.Duration) time.Time {
	return time.Unix(0, int64(d)).UTC()
}


// floorDiv returns the floor of a/b (rounds toward negative infinity),
// unlike Go's built-in integer division which truncates toward zero.
func floorDiv(a, b int64) int64 {
	d := a / b
	if (a^b) < 0 && d*b != a {
		d--
	}
	return d
}

