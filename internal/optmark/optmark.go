// Package optmark marks avro decode options by a property the code hosting a
// decode has to act on. An [avro.Opt] is opaque outside package avro, so a
// host cannot ask what one does; a marker interface lets it ask the one
// question it must answer for itself.
package optmark

// AliasesInput is implemented by options that make decoded values reference
// the decode input rather than copying out of it.
//
// If you decode from a buffer *you* own and reuse, drop such options before
// forwarding them: the values you hand back would point into memory your next
// read overwrites. ocf's block buffer is exactly that. Implementing this on a
// new aliasing option keeps every such host correct without editing any of
// them.
type AliasesInput interface{ AvroOptAliasesInput() }
