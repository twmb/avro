// Package avro encodes and decodes [Avro specification] data.
//
// Parse an Avro JSON schema with [Parse] (or [MustParse] for package-level
// vars), then call [Schema.Encode] / [Schema.Decode] for binary encoding,
// or [Schema.EncodeJSON] / [Schema.DecodeJSON] for JSON encoding.
// Use [SchemaFor] to infer a schema from a Go struct type, or
// [Schema.Root] to inspect a parsed schema's structure.
//
// # Basic usage
//
//	schema := avro.MustParse(`{
//	    "type": "record",
//	    "name": "User",
//	    "fields": [
//	        {"name": "name", "type": "string"},
//	        {"name": "age",  "type": "int"}
//	    ]
//	}`)
//
//	type User struct {
//	    Name string `avro:"name"`
//	    Age  int    `avro:"age"`
//	}
//
//	// Encode
//	data, err := schema.Encode(&User{Name: "Alice", Age: 30})
//
//	// Decode
//	var u User
//	_, err = schema.Decode(data, &u)
//
// # JSON encoding
//
// [Schema.EncodeJSON] is schema-aware and handles bytes, unions, and
// NaN/Infinity floats correctly — use it instead of [encoding/json.Marshal]
// when serializing decoded Avro data to JSON. Options control the output
// format: [TaggedUnions] for Avro JSON union wrappers ({"type": value}),
// [TagLogicalTypes] for qualified branch names, and [LinkedinFloats] for
// the goavro NaN/Infinity convention.
//
// # Encoding from JSON input
//
// Data from [encoding/json.Unmarshal] (map[string]any with float64 numbers
// and string timestamps) can be encoded directly. Missing map keys are
// filled from schema defaults, [encoding/json.Number] is accepted for all
// numeric types, and timestamp fields accept RFC 3339 strings. String
// fields accept [encoding.TextAppender] and [encoding.TextMarshaler]
// implementations (with [encoding.TextUnmarshaler] on decode).
//
// # Schema evolution
//
// Avro data is always written with a specific schema — the "writer schema."
// When you read that data later, your application may expect a different
// schema — the "reader schema." For example, you may have added a field,
// removed one, or widened a type from int to long. The data on disk doesn't
// change, but your code expects the new layout.
//
// [Resolve] bridges this gap. Given the writer and reader schemas, it returns
// a new schema that knows how to decode the old wire format and produce
// values in the reader's layout:
//
//   - Fields in the reader but not the writer are filled from defaults.
//   - Fields in the writer but not the reader are skipped.
//   - Fields that exist in both are matched by name (or alias) and decoded,
//     with type promotion applied where needed (e.g. int → long).
//
// You typically get the writer schema from the data itself: an OCF file
// header embeds it, and schema registries store it by ID or fingerprint.
//
// As a concrete example, suppose v1 of your application wrote User records
// with just a name:
//
//	var writerSchema = avro.MustParse(`{
//	    "type": "record", "name": "User",
//	    "fields": [
//	        {"name": "name", "type": "string"}
//	    ]
//	}`)
//
// In v2, you added an email field with a default:
//
//	var readerSchema = avro.MustParse(`{
//	    "type": "record", "name": "User",
//	    "fields": [
//	        {"name": "name",  "type": "string"},
//	        {"name": "email", "type": "string", "default": ""}
//	    ]
//	}`)
//
//	type User struct {
//	    Name  string `avro:"name"`
//	    Email string `avro:"email"`
//	}
//
// To read old v1 data with your v2 struct, resolve the two schemas:
//
//	resolved, err := avro.Resolve(writerSchema, readerSchema)
//
//	// Decode v1 data: "email" is absent in the old data, so it gets
//	// the reader default ("").
//	var u User
//	_, err = resolved.Decode(v1Data, &u)
//	// u == User{Name: "Alice", Email: ""}
//
// If you just want to check whether two schemas are compatible without
// building a resolved schema, use [CheckCompatibility].
//
// A null union branch decodes to the target's Go zero value, always replacing
// any prior value — matching [encoding/json/v2.Unmarshal]. Use *T to
// distinguish null from zero. Numeric values that don't fit the Go target's
// range return an error; values within range but without exact representation
// are rounded silently, matching json/v2's "rounded or clamped" rule.
//
// # Struct tags
//
// Use the "avro" struct tag to control field mapping and schema inference.
// The format is avro:"[name][,option]..." where the name maps the Go field
// to the Avro field name (empty = use Go field name, "-" = exclude).
//
// Encoding/decoding options:
//
//	avro:"name"           // map to Avro field "name"
//	avro:"-"              // exclude field
//	avro:",inline"        // flatten nested struct fields into parent record
//	avro:",omitzero"      // encode zero values as the schema default
//
// Schema inference options (used by [SchemaFor]):
//
//	avro:",default=value"         // set field default (must be last option; scalars only)
//	avro:",alias=old_name"        // field alias for evolution (repeatable)
//	avro:",timestamp-micros"      // override logical type (also: timestamp-nanos, date, time-millis, time-micros)
//	avro:",decimal(10,2)"         // decimal logical type with precision and scale
//	avro:",uuid"                  // UUID logical type
//
// When encoding a map[string]any as a record, missing keys are filled
// from the schema's default values. For structs, omitzero does the same
// for zero-valued fields (or fields whose IsZero() method returns true).
//
// Embedded (anonymous) struct fields are automatically inlined. To prevent
// inlining, give the field an explicit name tag. When multiple fields at
// different depths resolve to the same name, the shallowest wins; among
// fields at the same depth, a tagged field wins over an untagged one.
//
// # Custom types
//
// [CustomType] registers custom Go type conversions for logical types,
// domain types, or to replace built-in behavior. A matching custom type
// replaces the built-in logical type deserializer — Decode callbacks
// receive raw Avro-native values, not enriched types like [time.Time].
// A [CustomType] with nil Decode suppresses the built-in handler with
// zero overhead, producing raw values directly. Use [NewCustomType] for
// type-safe primitive conversions, or the [CustomType] struct directly
// for complex cases (records, fixed types, property-based dispatch).
// Custom types are registered per-schema via [SchemaOpt].
//
// # Parsing options
//
// [Parse] and [SchemaCache.Parse] accept [WithLaxNames] to allow
// non-standard characters in type and field names.
//
// # Errors
//
// Encode and decode errors can be inspected with [errors.As]:
//
//   - [*SemanticError]: type mismatch (includes a dotted field path for nested records)
//   - [*ShortBufferError]: input truncated mid-value
//   - [*CompatibilityError]: schema evolution incompatibility
//
// # Other features
//
//   - Schema Cache: [SchemaCache] accumulates named types across Parse calls for schema registry workflows
//   - Schema Introspection: [Schema.Root] returns a [SchemaNode]; [Schema.String] returns the original JSON
//   - Single Object Encoding: [Schema.AppendSingleObject], [Schema.DecodeSingleObject]
//   - Fingerprinting: [Schema.Canonical], [Schema.Fingerprint], [NewRabin]
//   - Object Container Files: the [github.com/twmb/avro/ocf] sub-package
//
// [encoding/json/v2.Unmarshal]: https://pkg.go.dev/encoding/json/v2#Unmarshal
// [Avro specification]: https://avro.apache.org/docs/current/specification/
package avro
