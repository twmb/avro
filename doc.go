// Package avro encodes and decodes Avro data.
//
// Parse an Avro JSON schema with [Parse] (or [MustParse] for package-level
// vars), then call [Schema.Encode] / [Schema.Decode] for binary encoding,
// or [Schema.EncodeJSON] / [Schema.DecodeJSON] for JSON encoding.
// Use [SchemaFor] to infer a schema from a Go struct type, or
// [Schema.Root] to inspect a parsed schema's structure. See
// [Schema.Decode] for the full Go-to-Avro type mapping.
//
// [Schema.EncodeJSON] produces standard JSON by default (bare union values,
// bytes as \uXXXX strings). Pass [TaggedUnions] to wrap union values as
// {"type": value} (Avro JSON), or [LinkedinFloats] to encode NaN/Infinity
// using the linkedin/goavro convention (null/±1e999 instead of
// "NaN"/"Infinity" strings). [Schema.DecodeJSON] accepts all formats.
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
// # Map encoding and defaults
//
// When encoding a map[string]any as a record, any field whose key is absent
// from the map is filled from the schema's default value. If a field has no
// default and the key is missing, encoding returns an error. This does not
// apply to struct encoding, where fields are always present (though they may
// be zero-valued; see the omitzero tag option).
//
//	schema := avro.MustParse(`{
//	    "type": "record", "name": "Event",
//	    "fields": [
//	        {"name": "id",   "type": "string"},
//	        {"name": "tags", "type": {"type": "array", "items": "string"}, "default": []}
//	    ]
//	}`)
//
//	// "tags" is absent from the map, so the schema default ([]) is used.
//	data, err := schema.Encode(map[string]any{"id": "abc"})
//
// Note: the Avro spec says defaults are for schema evolution at read time, but
// in Go the common pattern is json.Unmarshal into map[string]any, where keys
// for defaulted fields are simply absent. Filling them from schema defaults at
// encode time is consistent with hamba/avro and linkedin/goavro.
//
// Timestamp and date logical types also accept RFC 3339 strings when
// encoding:
//
//	schema := avro.MustParse(`{
//	    "type": "record", "name": "Event",
//	    "fields": [
//	        {"name": "id",         "type": "string"},
//	        {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
//	    ]
//	}`)
//
//	var record any
//	json.Unmarshal([]byte(`{"id":"abc","created_at":"2026-03-19T10:00:00Z"}`), &record)
//	data, err := schema.Encode(record) // string parsed as RFC 3339
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
// # Struct tags
//
// Struct fields are matched to Avro record fields by name. Use the "avro"
// struct tag to control the mapping:
//
//	type Example struct {
//	    Name    string  `avro:"name"`          // maps to Avro field "name"
//	    Ignored int     `avro:"-"`             // excluded from encoding/decoding
//	    Inner   Nested  `avro:",inline"`       // inline Nested's fields into this record
//	    Value   int     `avro:"val,omitzero"`  // encode zero value as Avro default
//	}
//
// The tag format is:
//
//	avro:"[name][,option][,option]..."
//
// The name portion maps the struct field to the Avro field with that name. If
// empty, the Go field name is used as-is. A tag of "-" excludes the field
// entirely.
//
// Supported options:
//
//   - inline: flatten a nested struct's fields into the parent record,
//     as if they were declared directly on the parent. The field must be a
//     struct or pointer to struct. This works like anonymous (embedded) struct
//     fields, but for named fields. When using inline, the name portion of
//     the tag must be empty.
//
//   - omitzero: when encoding, if the field is the zero value for its type
//     (or implements an IsZero() bool method that returns true), the Avro
//     default value from the schema is used instead. This is useful for
//     optional fields in ["null", T] unions or fields with explicit defaults.
//
// Embedded (anonymous) struct fields are automatically inlined — their
// fields are promoted into the parent as if declared directly. To prevent
// inlining an embedded struct, give it an explicit name tag:
//
//	type Parent struct {
//	    Nested                    // inlined: Nested's fields are promoted
//	    Other  Aux `avro:"other"` // not inlined: treated as a single field
//	}
//
// When multiple fields at different depths resolve to the same Avro field
// name, the shallowest field wins. Among fields at the same depth, a tagged
// field wins over an untagged one.
//
// # Single Object Encoding
//
// For sending self-describing values over the wire (as opposed to files,
// where OCF is preferred), use [Schema.AppendSingleObject] and
// [Schema.DecodeSingleObject]. To decode without knowing the schema in
// advance, extract the fingerprint with [SingleObjectFingerprint] and look
// it up in your own registry.
//
// # Fingerprinting
//
// [Schema.Canonical] returns the Parsing Canonical Form for deterministic
// comparison. [Schema.Fingerprint] hashes it with any [hash.Hash]; use
// [NewRabin] for the Avro-standard CRC-64-AVRO.
//
// [Avro specification]: https://avro.apache.org/docs/current/specification/
package avro
