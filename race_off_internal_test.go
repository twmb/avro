//go:build !race

package avro

// raceEnabled reports whether the race detector is compiled into the
// test binary — the package-avro mirror of the avro_test pair in
// race_on_test.go / race_off_test.go, for white-box tests that relax
// wall-clock thresholds under the detector's ~5-10x overhead.
const raceEnabled = false
