//go:build !race

package avro

// raceEnabled reports whether the race detector is compiled into the
// test binary — the module's one build-tagged predicate, which package
// avro_test reads through the export_test.go bridge (RaceEnabledForTest),
// for tests that relax wall-clock thresholds under the detector's
// ~5-10x overhead.
const raceEnabled = false
