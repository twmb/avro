// Package optmark marks avro decode options by a property the code hosting a
// decode has to act on. An [avro.Opt] is opaque outside package avro, so a host
// cannot ask what one does; a marker interface lets it ask the one question it
// must answer for itself.
package optmark

// AliasesInput is implemented by options that make decoded values reference the
// caller's decode input rather than copying out of it.
//
// A host that decodes from a buffer IT owns and reuses must drop such options
// before forwarding them: the values it hands back would point into memory the
// next read overwrites. ocf's block buffer is exactly that. Implementing this
// on a new aliasing option is what keeps every such host correct without
// editing any of them.
type AliasesInput interface{ AvroOptAliasesInput() }
