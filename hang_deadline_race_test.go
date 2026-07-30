//go:build race

package avro

import "time"

// hangDeadline is the wall-clock backstop the schema-node budget batteries use
// to turn a HANG into a failure. It is a liveness detector, never a
// performance assertion: the property under test is that an over-budget walk
// REJECTS, and the goroutine plus deadline exist only so a regression that
// stopped bounding the walk surfaces as a failure instead of wedging the suite.
//
// Those batteries are the one place in the suite whose work is at the budget by
// construction — a cell must EXCEED maxSchemaJSONNodes or nothing is over
// budget — so they are also the slowest thing here, and the race detector's
// instrumentation multiplies that. Measured in isolation under -race, two of
// them run 21s and 33s against a 30s deadline; under the full suite's
// parallelism they exceed it, while the detector itself reports no race. A
// deadline inside the band of legitimate work is mis-sized for its purpose, so
// it scales with the instrumentation rather than pretending the cost is the
// same.
const hangDeadline = 4 * time.Minute
