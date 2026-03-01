# avro

[![Go Reference](https://pkg.go.dev/badge/github.com/twmb/avro.svg)](https://pkg.go.dev/github.com/twmb/avro)

Encode and decode [Avro](https://avro.apache.org/docs/current/specification/) binary data.

Parse an Avro JSON schema, then encode and decode Go values directly — no
code generation required. Supports all primitive and complex types, logical
types, schema evolution, Object Container Files, Single Object Encoding, and
fingerprinting.

## Index

- [Quick Start](#quick-start)
- [Type Mapping](#type-mapping)
- [Struct Tags](#struct-tags)
- [Logical Types](#logical-types)
- [Schema Evolution](#schema-evolution)
- [Object Container Files](#object-container-files)
- [Single Object Encoding](#single-object-encoding)
- [Fingerprinting](#fingerprinting)
- [Performance](#performance)

## Quick Start

```go
package main

import (
	"fmt"
	"log"

	"github.com/twmb/avro"
)

var schema = avro.MustParse(`{
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age",  "type": "int"}
    ]
}`)

type User struct {
	Name string `avro:"name"`
	Age  int    `avro:"age"`
}

func main() {
	// Encode
	data, err := schema.Encode(&User{Name: "Alice", Age: 30})
	if err != nil {
		log.Fatal(err)
	}

	// Decode
	var u User
	_, err = schema.Decode(data, &u)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(u) // {Alice 30}
}
```

## Type Mapping

The table below shows which Go types can be used with each Avro type when
encoding and decoding.

| Avro Type | Go Types |
|-----------|----------|
| null      | `any` (always nil) |
| boolean   | `bool`, `any` |
| int, long | `int`, `int8`, `int16`, `int32`, `int64`, `uint`, `uint8`, `uint16`, `uint32`, `uint64`, `any` |
| float     | `float32`, `float64`, `any` |
| double    | `float64`, `float32`, `any` |
| string    | `string`, `[]byte`, `any`; also `encoding.TextUnmarshaler` |
| bytes     | `[]byte`, `string`, `any` |
| enum      | `string`, any integer type (ordinal), `any` |
| fixed     | `[N]byte`, `[]byte`, `any` |
| array     | slice, `any` |
| map       | `map[string]T`, `any` |
| union     | `any`, `*T` (for `["null", T]` unions), or the matched branch type |
| record    | struct (matched by field name or `avro` tag), `map[string]any`, `any` |

When decoding into `any`, values use their natural Go types: `nil`, `bool`,
`int64`, `float32`, `float64`, `string`, `[]byte`, `[]any`, `map[string]any`.

Encoding also accepts `fmt.Stringer` and `encoding.TextMarshaler` for string
schema types.

## Struct Tags

Struct fields are mapped to Avro record fields using the `avro` tag:

```go
type Example struct {
    Name    string `avro:"name"`           // maps to Avro field "name"
    Ignored int    `avro:"-"`              // skipped entirely
    Inner   Nested `avro:",inline"`        // inlines nested struct fields into parent
    Email   *string `avro:",omitzero"`     // encodes as null when zero (for ["null", T] unions)
}
```

Without a tag, the exported Go field name is used. Embedded (anonymous) structs
are inlined automatically unless they have an explicit `avro:"name"` tag.

## Logical Types

Logical types are decoded into natural Go types when available, and fall back
to the underlying Avro type otherwise.

| Logical Type | Avro Type | Go Type |
|---|---|---|
| `date` | int | `time.Time` or int types |
| `time-millis` | int | `time.Duration` or int types |
| `time-micros` | long | `time.Duration` or int types |
| `timestamp-millis` | long | `time.Time` or int types |
| `timestamp-micros` | long | `time.Time` or int types |
| `timestamp-nanos` | long | `time.Time` or int types |
| `local-timestamp-millis` | long | `time.Time` or int types |
| `local-timestamp-micros` | long | `time.Time` or int types |
| `local-timestamp-nanos` | long | `time.Time` or int types |
| `uuid` | string or fixed(16) | `[16]byte` (RFC 4122 hex-dash ↔ binary) or `string` |
| `decimal` | bytes or fixed | underlying bytes/fixed |
| `duration` | fixed(12) | underlying fixed |

Unknown logical types are silently ignored per the Avro spec, and the
underlying type is used as-is.

## Schema Evolution

Avro data is always written with a specific schema — the **writer schema**.
When you read that data later, your application may expect a different schema —
the **reader schema**. You may have added a field, removed one, or widened a
type from int to long.

`Resolve` bridges this gap. Given the writer and reader schemas, it returns a
new schema that decodes data in the old wire format and produces values in the
reader's layout:

- Fields in the reader but not the writer are filled from **defaults**.
- Fields in the writer but not the reader are **skipped**.
- Fields that exist in both are matched by **name** (or **alias**) and decoded,
  with type promotion applied where needed (e.g. int → long).

### Example

Suppose v1 of your application wrote User records with just a name:

```go
var writerSchema = avro.MustParse(`{
    "type": "record", "name": "User",
    "fields": [
        {"name": "name", "type": "string"}
    ]
}`)
```

In v2 you added an email field with a default:

```go
var readerSchema = avro.MustParse(`{
    "type": "record", "name": "User",
    "fields": [
        {"name": "name",  "type": "string"},
        {"name": "email", "type": "string", "default": ""}
    ]
}`)

type User struct {
    Name  string `avro:"name"`
    Email string `avro:"email"`
}
```

To read old v1 data with your v2 struct, resolve the two schemas:

```go
resolved, err := avro.Resolve(writerSchema, readerSchema)

var u User
_, err = resolved.Decode(v1Data, &u)
// u == User{Name: "Alice", Email: ""}
```

The following type promotions are supported:

| Writer → Reader |
|---|
| int → long, float, double |
| long → float, double |
| float → double |
| string ↔ bytes |

`CheckCompatibility` checks whether two schemas are compatible without
building a resolved schema. The direction you check depends on the guarantee
you need:

```go
// Backward: new schema can read old data.
avro.CheckCompatibility(oldSchema, newSchema)

// Forward: old schema can read new data.
avro.CheckCompatibility(newSchema, oldSchema)

// Full: check both directions.
avro.CheckCompatibility(oldSchema, newSchema)
avro.CheckCompatibility(newSchema, oldSchema)
```

## Object Container Files

The `ocf` sub-package reads and writes [Avro Object Container Files](https://avro.apache.org/docs/current/specification/#object-container-files) —
self-describing binary files that embed the schema in the header and store
data in compressed blocks.

### Writing

```go
var schema = avro.MustParse(`{
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age",  "type": "int"}
    ]
}`)

f, _ := os.Create("users.avro")
w, err := ocf.NewWriter(f, schema, ocf.WithCodec(ocf.SnappyCodec()))
if err != nil {
    log.Fatal(err)
}
w.Encode(&User{Name: "Alice", Age: 30})
w.Encode(&User{Name: "Bob", Age: 25})
w.Close()
f.Close()
```

### Reading

```go
f, _ := os.Open("users.avro")
r, err := ocf.NewReader(f)
if err != nil {
    log.Fatal(err)
}
defer r.Close()
for {
    var u User
    err := r.Decode(&u)
    if err == io.EOF {
        break
    }
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(u)
}
```

The reader's `Schema()` method returns the schema parsed from the file header,
which you can pass as the writer schema to `Resolve`.

### Codecs

Built-in codecs: **null** (default, no compression), **deflate**
(`DeflateCodec`), **snappy** (`SnappyCodec`), and **zstandard** (`ZstdCodec`).
Custom codecs can be provided via the `Codec` interface.

### Appending

`NewAppendWriter` opens an existing OCF for appending — it reads the header to
recover the schema, codec, and sync marker, then seeks to the end.

## Single Object Encoding

For sending self-describing values over the wire (as opposed to files, where
OCF is preferred), use Single Object Encoding. Each message is a 2-byte magic
header, an 8-byte CRC-64-AVRO fingerprint, and the Avro binary payload.

```go
// Encode with fingerprint header
data, err := schema.AppendSingleObject(nil, &user)

// Decode (schema known)
_, err = schema.DecodeSingleObject(data, &user)

// Decode (schema unknown): extract fingerprint, look up schema
fp, payload, err := avro.SingleObjectFingerprint(data)
schema := registry.Lookup(fp) // your schema registry
_, err = schema.Decode(payload, &user)
```

## Fingerprinting

`Canonical` returns the [Parsing Canonical Form](https://avro.apache.org/docs/current/specification/#parsing-canonical-form-for-schemas)
of a schema — a deterministic JSON representation stripped of doc, aliases,
defaults, and other non-essential attributes. Use it for schema comparison and
fingerprinting.

```go
canonical := schema.Canonical() // []byte

// CRC-64-AVRO (Rabin) — the Avro-standard fingerprint
fp := schema.Fingerprint(avro.NewRabin())

// SHA-256 — common for cross-language registries
fp256 := schema.Fingerprint(sha256.New())
```

## Performance

Struct field access uses `unsafe` pointer arithmetic (similar to
`encoding/json` v2) to avoid `reflect.Value` overhead on every encode/decode.
All schemas, type mappings, and codec state are cached after first use so
repeated operations pay no extra allocation cost.
