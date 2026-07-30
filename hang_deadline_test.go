//go:build !race

package avro

import "time"

// hangDeadline is the wall-clock backstop the schema-node budget batteries use
// to turn a HANG into a failure; see the -race build of this file for why it
// scales with the instrumentation. Uninstrumented, those batteries run in a
// couple of seconds, so this is already a large multiple of the real work.
const hangDeadline = 30 * time.Second
