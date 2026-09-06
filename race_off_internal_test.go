//go:build !race

package avro

// raceEnabled reports whether the race detector is compiled into the test
// binary: the module's one build-tagged predicate. Its only consumers are
// raceRelaxed and raceInflated, which widen liveness deadlines to cover the
// detector's ~5-10x overhead. Nothing asserts a wall-clock budget, so
// nothing else needs to know.
const raceEnabled = false
