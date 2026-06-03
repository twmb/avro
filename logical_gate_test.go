package avro

import "testing"

// jsonDecodeAppliesLogical derives its answer by probing decodeLogical*, so it
// can't drift from what decode actually does. This test independently pins the
// probe's output against the HUMAN-KNOWN transform set for every logical — if
// the probe's type-assertion logic is ever wrong (or a decodeLogical* change
// flips a logical's transform behavior), one of these explicit expectations
// fails, forcing a conscious review. Expected values are spelled out (not
// re-probed) so this is a genuine check, not a tautology.
func TestRegression_JSONDecodeAppliesLogicalMatchesDecode(t *testing.T) {
	cases := []struct {
		kind, logical string
		size          int
		want          bool
	}{
		// Transforming logicals (decode → enriched Go type).
		{"int", "date", 0, true},                    // → time.Time
		{"int", "time-millis", 0, true},             // → time.Duration
		{"long", "time-micros", 0, true},            // → time.Duration
		{"long", "timestamp-millis", 0, true},       // → time.Time
		{"long", "timestamp-micros", 0, true},       // → time.Time
		{"long", "timestamp-nanos", 0, true},        // → time.Time
		{"long", "local-timestamp-millis", 0, true}, // → time.Time
		{"long", "local-timestamp-micros", 0, true}, // → time.Time
		{"long", "local-timestamp-nanos", 0, true},  // → time.Time
		{"bytes", "decimal", 0, true},               // → *big.Rat
		{"fixed", "decimal", 8, true},               // → *big.Rat
		{"bytes", "big-decimal", 0, true},           // → *big.Rat
		{"fixed", "uuid", 16, true},                 // → [16]byte
		{"fixed", "duration", 12, true},             // → avro.Duration

		// uuid-on-string transforms for a TYPED target — decodeString parses the
		// hex-dash string into a [16]byte / UUID-typed target (into *any/string
		// it is identity, but the gate must report the transform so a no-Decode
		// CustomType installs the suppression wrapper and the raw decode matches
		// binary's deserString, which has no [16]byte arm).
		{"string", "uuid", 0, true},

		// Non-transforming: no logical; and an unknown future logical
		// (decodeLogical* returns raw).
		{"int", "", 0, false},
		{"long", "", 0, false},
		{"bytes", "", 0, false},
		{"string", "", 0, false},
		{"fixed", "", 16, false},
		{"long", "some-future-logical", 0, false},
		{"bytes", "some-future-logical", 0, false},
	}
	for _, c := range cases {
		node := &schemaNode{kind: c.kind, logical: c.logical, size: c.size}
		if got := jsonDecodeAppliesLogical(node); got != c.want {
			t.Errorf("jsonDecodeAppliesLogical(kind=%s logical=%q)=%v, want %v — probe disagrees with the known transform set (decodeLogical* changed?)", c.kind, c.logical, got, c.want)
		}
	}
}
