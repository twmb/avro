//go:build !race

package avro_test

// raceEnabled reports whether the race detector is compiled into the
// test binary. Build-tagged so tests can relax wall-clock thresholds
// when the detector adds ~5-10x overhead. The non-race build returns
// false; the race-build variant in race_on_test.go returns true.
const raceEnabled = false
