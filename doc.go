// Package avro encodes and decodes Avro binary data.
//
// Parse an Avro JSON schema with [Parse] (or [MustParse] for package-level
// vars), then call [Schema.Encode] / [Schema.Decode] to convert between Go
// values and Avro binary. See [Schema.Decode] for the full Go-to-Avro type
// mapping.
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
// You typically get the writer schema from the data itself: an [ocf] file
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
// # Single Object Encoding
//
// For sending self-describing values over the wire (as opposed to files,
// where [ocf] is preferred), use [Schema.AppendSingleObject] and
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
