// Package avro encodes and decodes Avro data.
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
// when serializing decoded Avro data to JSON. Pass [TaggedUnions] for
// Avro JSON union wrappers ({"type": value}).
//
// # Encoding from JSON input
//
// Data from [encoding/json.Unmarshal] (map[string]any with float64 numbers
// and string timestamps) can be encoded directly. Missing map keys are
// filled from schema defaults, [encoding/json.Number] is accepted for all
// numeric types, and timestamp fields accept RFC 3339 strings.
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
// Use the "avro" struct tag to control field mapping:
//
//	avro:"name"           // map to Avro field "name"
//	avro:"-"              // exclude field
//	avro:",inline"        // flatten nested struct fields
//	avro:",omitzero"      // use schema default for zero values
//
// Embedded structs are inlined by default. See the README for the full
// tag reference.
//
// # Other features
//
//   - Single Object Encoding: [Schema.AppendSingleObject], [Schema.DecodeSingleObject]
//   - Fingerprinting: [Schema.Canonical], [Schema.Fingerprint], [NewRabin]
//   - Object Container Files: the [github.com/twmb/avro/ocf] sub-package
//
// [Avro specification]: https://avro.apache.org/docs/current/specification/
package avro
