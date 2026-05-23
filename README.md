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
- [Schema Inference](#schema-inference)
- [Schema Introspection](#schema-introspection)
- [Logical Types](#logical-types)
- [Schema Evolution](#schema-evolution)
- [Schema Cache](#schema-cache)
- [Custom Types](#custom-types)
- [Object Container Files](#object-container-files)
- [JSON Encoding](#json-encoding)
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

`Parse` accepts options: pass `WithLaxNames()` to allow non-standard characters
in type and field names (useful for interop with schemas from other languages).

## Type Mapping

The table below shows which Go types can be used with each Avro type.

| Avro Type | Encode | Decode |
|-----------|--------|--------|
| null      | `any` (nil) | `any` |
| boolean   | `bool` | `bool`, `any` |
| int, long | `int`, `int8`–`int64`, `uint`–`uint64`, `float64`, `json.Number` | `int`, `int8`–`int64`, `uint`–`uint64`, `any` |
| float     | `float32`, `float64`, `json.Number` | `float32`, `float64`, `any` |
| double    | `float64`, `float32`, `json.Number` | `float64`, `float32`, `any` |
| string    | `string`, `[]byte`, `encoding.TextAppender`, `encoding.TextMarshaler` | `string`, `[]byte`, `encoding.TextUnmarshaler`, `any` |
| bytes     | `[]byte`, `string` | `[]byte`, `string`, `any` |
| enum      | `string`, any integer type (ordinal) | `string`, any integer type (ordinal), `any` |
| fixed     | `[N]byte`, `[]byte` | `[N]byte`, `[]byte`, `any` |
| array     | slice | slice, `any` |
| map       | `map[string]T` | `map[string]T`, `any` |
| union     | `any`, `*T`, or the matched branch type | `any`, `*T`, or the matched branch type |
| record    | struct, `map[string]any` | struct, `map[string]any`, `any` |

When decoding into `any`, values use their natural Go types: `nil`, `bool`,
`int32`, `int64`, `float32`, `float64`, `string`, `[]byte`, `[]any`,
`map[string]any`. Logical types use `time.Time` (UTC) for timestamps and
dates, `time.Duration` for time-of-day types, `*big.Rat` for decimals,
and `avro.Duration` for the duration logical type.

Encoding also accepts `json.Number` for any numeric type (supporting
`json.Decoder.UseNumber()` pipelines) and `[]byte` for string fields (and
vice versa).

A null union branch decodes to the target's Go zero value, always replacing
any prior value — matching [`encoding/json/v2.Unmarshal`][jsonv2-null]. Use
`*T` to distinguish null from zero.

Numeric values that don't fit the Go target's range (e.g. Avro `long` `2^33`
into Go `int32`, or Avro `double` overflowing `float32` to `±Inf`) return an
error rather than silently wrapping or clamping. Values within range but
without exact representation are rounded silently, matching
[`encoding/json/v2`][jsonv2-null]'s "rounded or clamped" rule.

[jsonv2-null]: https://pkg.go.dev/encoding/json/v2#Unmarshal

## Struct Tags

Struct fields are matched to Avro record fields by name. Use the `avro` struct
tag to control the mapping:

```go
type Example struct {
    Name    string  `avro:"name"`          // maps to Avro field "name"
    Ignored int     `avro:"-"`             // excluded from encoding/decoding
    Inner   Nested  `avro:",inline"`       // inline Nested's fields into this record
    Value   int     `avro:"val,omitzero"`  // encode zero value as Avro default
}
```

The tag format is:

```
avro:"[name][,option][,option]..."
```

The name portion maps the struct field to the Avro field with that name. If
empty, the Go field name is used as-is. A tag of `"-"` excludes the field
entirely.

Supported options:

- **inline**: flatten a nested struct's fields into the parent record, as if
  they were declared directly on the parent. The field must be a struct or
  pointer to struct. This works like anonymous (embedded) struct fields, but
  for named fields. When using inline, the name portion of the tag must be
  empty.

- **omitzero**: when encoding, if the field is the zero value for its type (or
  implements an `IsZero() bool` method that returns true), the Avro default
  value from the schema is used instead. This is useful for optional fields in
  `["null", T]` unions or fields with explicit defaults.

Embedded (anonymous) struct fields are automatically inlined — their fields are
promoted into the parent as if declared directly. To prevent inlining an
embedded struct, give it an explicit name tag:

```go
type Parent struct {
    Nested                    // inlined: Nested's fields are promoted
    Other  Aux `avro:"other"` // not inlined: treated as a single field
}
```

When multiple fields at different depths resolve to the same Avro field name,
the shallowest field wins. Among fields at the same depth, a tagged field wins
over an untagged one.

## Schema Inference

`SchemaFor` infers an Avro schema from a Go struct type, using the same struct
tags as encoding/decoding:

```go
type User struct {
    Name      string     `avro:"name"`
    Age       int32      `avro:"age,default=18"`
    Email     *string    `avro:"email"`
    CreatedAt time.Time  `avro:"created_at"`
}

schema := avro.MustSchemaFor[User](avro.WithNamespace("com.example"))
```

This produces the equivalent of:

```json
{
  "type": "record",
  "name": "User",
  "namespace": "com.example",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "age", "type": "int", "default": 18},
    {"name": "email", "type": ["null", "string"]},
    {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

Go types map to Avro types automatically: `*T` becomes a `["null", T]` union
with `"default": null` (backward-compatible by default), `[N]byte` becomes a
fixed type named after the Go type (or `fixed_N` for unnamed arrays),
`time.Time` becomes `timestamp-millis`, and so on (see [Type Mapping](#type-mapping)).

Additional tag options for schema inference:

| Tag | Example | Description |
|-----|---------|-------------|
| `default=` | `avro:",default=0"` | Default value (must be last; scalars only) |
| `alias=` | `avro:",alias=old"` | Field alias for schema evolution (repeatable, or `alias=[a,b]`) |
| `type-alias=` | `avro:",type-alias=old"` | Alias for the field's named type — record, enum, or fixed (repeatable, or `type-alias=[a,b]`) |
| `timestamp-micros` | `avro:",timestamp-micros"` | Override logical type |
| `decimal(p,s)` | `avro:",decimal(10,2)"` | Decimal logical type (required for `*big.Rat`) |
| `uuid` | `avro:",uuid"` | UUID logical type |
| `date` | `avro:",date"` | Date logical type |

`alias` and `type-alias` serve different purposes in schema evolution. `alias`
adds an alias to the **field** — it lets a writer field with a different name
match this reader field. `type-alias` adds an alias to the **named type**
(record, enum, or fixed) that the field references — it lets a writer type with
a different name match the reader type. The alias is applied to the innermost
named type, walking through pointers, slices, and maps:

```go
type FieldSummary struct {
    ContainsNull bool    `avro:"contains_null"`
    ContainsNaN  *bool   `avro:"contains_nan"`
}

type ManifestFile struct {
    // alias=old_partitions: match writer field named "old_partitions"
    // type-alias=r508:      match writer record type named "r508" for FieldSummary
    Partitions *[]FieldSummary `avro:"partitions,type-alias=r508"`
}
```

Options:

- `WithNamespace(ns)` sets the Avro namespace for the record.
- `WithName(name)` overrides the record name (defaults to the Go struct name).

## Schema Introspection

`Schema.Root()` returns a `SchemaNode` representing the parsed schema. This
provides read access to all schema metadata including field types, logical
types, doc strings, and custom properties:

```go
schema, _ := avro.Parse(schemaJSON)
root := schema.Root()

for _, f := range root.Fields {
    fmt.Printf("field %s: type=%s\n", f.Name, f.Type.Type)
    if cn, ok := f.Props["connect.name"].(string); ok {
        fmt.Printf("  kafka connect type: %s\n", cn)
    }
}
```

`SchemaNode` can also be used to build schemas programmatically:

```go
node := &avro.SchemaNode{
    Type: "record",
    Name: "User",
    Fields: []avro.SchemaField{
        {Name: "name", Type: avro.SchemaNode{Type: "string"}},
        {Name: "age", Type: avro.SchemaNode{Type: "int"}, Default: 18},
    },
}
schema, err := node.Schema()
```

## Logical Types

Logical types decode to their natural Go equivalents:

| Logical Type | Avro Type | Encode | Decode |
|---|---|---|---|
| date | int | time.Time, RFC 3339 or YYYY-MM-DD string, or int | time.Time (UTC) |
| time-millis | int | time.Duration, time.Time (lossy †), or int | time.Duration or time.Time (lossy †) |
| time-micros | long | time.Duration, time.Time (lossy †), or int | time.Duration or time.Time (lossy †) |
| timestamp-millis | long | time.Time, RFC 3339 string, or int | time.Time (UTC) |
| timestamp-micros | long | time.Time, RFC 3339 string, or int | time.Time (UTC) |
| timestamp-nanos | long | time.Time, RFC 3339 string, or int | time.Time (UTC) |
| local-timestamp-millis | long | time.Time, RFC 3339 string, or int | time.Time (UTC) |
| local-timestamp-micros | long | time.Time, RFC 3339 string, or int | time.Time (UTC) |
| local-timestamp-nanos | long | time.Time, RFC 3339 string, or int | time.Time (UTC) |
| uuid (string) | string | [16]byte or string | string into any; [16]byte or string into typed target |
| uuid (fixed(16)) | fixed(16) | [16]byte, []byte, or hex-dash string | [16]byte into any or [16]byte target; string into string target |
| decimal | bytes or fixed | *big.Rat, float64, numeric string, json.Number, or underlying type | *big.Rat, float64/float32, numeric string, json.Number, or underlying type |
| big-decimal | bytes | *big.Rat, float64, numeric string, json.Number | *big.Rat, float64/float32, numeric string, json.Number |
| duration | fixed(12) | avro.Duration or underlying type | avro.Duration or underlying type |

When encoding, timestamp and date fields accept RFC 3339 strings, and decimal
fields accept float64 and numeric strings (e.g. "3.14"). Values that don't
match the expected format fall through to the underlying type's encoder, which
will return an error.

† **time-millis / time-micros with `time.Time`** is a convenience escape hatch
for users whose Go data already lives in `time.Time`. Avro's `time-millis` and
`time-micros` logical types are time-of-day only — the wire bytes physically
cannot represent a date or zone. On encode, only the wall-clock fields
(hour/minute/second/nanosecond) are written; year/month/day/location are
silently discarded. On decode into a `time.Time` target, the wire value is
materialized at the Unix epoch (`1970-01-01 UTC`) plus the time-of-day. A
round-trip through `time.Time` therefore preserves the time-of-day but
resets the date and zone: `2024-01-15 12:34:56 PST` → wire → `1970-01-01
12:34:56 UTC`. If round-trip date fidelity matters, use `timestamp-millis` /
`timestamp-micros` (which preserve the full instant) or convert to and from
`time.Duration` explicitly. `time.Duration` is always lossless for these
types.

big-decimal carries no schema-level precision or scale; scale is derived
per value, and rationals with no finite decimal expansion (e.g.
`big.NewRat(1, 3)`) return an error.

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

## Schema Cache

When working with a schema registry, schemas often reference types defined in
other schemas. `SchemaCache` accumulates named types across multiple Parse
calls so they can be resolved:

```go
var cache avro.SchemaCache

// Parse referenced schema first — order matters.
_, err := cache.Parse(`{
    "type": "record",
    "name": "Address",
    "fields": [{"name": "city", "type": "string"}]
}`)

// Now parse a schema that references Address.
schema, err := cache.Parse(`{
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name",    "type": "string"},
        {"name": "address", "type": "Address"}
    ]
}`)
```

Parsing the same schema string multiple times returns the cached result,
handling diamond dependencies without caller-side deduplication. The returned
`*Schema` is independent of the cache and safe to use concurrently.

## Custom Types

Register custom Go type conversions with `NewCustomType` for type-safe
primitive conversions, or `CustomType` for advanced cases:

```go
type Money struct {
    Cents    int64
    Currency string
}

moneyType := avro.NewCustomType[Money, int64]("money",
    func(m Money, _ *avro.SchemaNode) (int64, error) { return m.Cents, nil },
    func(c int64, _ *avro.SchemaNode) (Money, error) {
        return Money{Cents: c, Currency: "USD"}, nil
    },
)

schema := avro.MustParse(moneySchema, moneyType)

// Encode and decode — Money fields are automatically converted.
data, _ := schema.Encode(&order)
var out Order
schema.Decode(data, &out) // out.Price is Money{Cents: 500, ...}

// Works with SchemaFor too.
schema = avro.MustSchemaFor[Order](moneyType)
```

A matching custom type replaces the built-in logical type deserializer.
Decode callbacks receive raw Avro-native values (int64 for long, int32
for int, etc.). A nil Decode suppresses the built-in handler with zero
overhead, producing raw values directly:

```go
// Decode timestamps as raw int64 instead of time.Time.
schema := avro.MustParse(raw, avro.CustomType{
    LogicalType: "timestamp-millis",
    AvroType:    "long",
})
```

For property-based dispatch (e.g., Kafka Connect / Debezium types), use
an empty matching criteria with `ErrSkipCustomType`:

```go
avro.CustomType{
    Decode: func(v any, node *avro.SchemaNode) (any, error) {
        name, _ := node.Props["connect.name"].(string)
        switch name {
        case "io.debezium.time.Timestamp":
            return time.UnixMilli(v.(int64)).UTC(), nil
        default:
            return nil, avro.ErrSkipCustomType
        }
    },
}
```

## Type name constants

The `atype` sub-package exports constants for Avro primitive type names,
complex type names, logical type names, and field sort orders — the string
values used in `SchemaNode`, `SchemaField`, and `CustomType`:

```go
import (
    "github.com/twmb/avro"
    "github.com/twmb/avro/atype"
)

node := avro.SchemaNode{
    Type:        atype.Long,
    LogicalType: atype.TimestampMicros,
}

ct := avro.CustomType{LogicalType: atype.Decimal}
```

They are untyped string constants and can be used anywhere a string is
expected. Helps catch typos (`atype.TimestampMicros` vs
`"timestam-micros"`) at compile time.

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

**Memory bounds.** `WithMaxBlockBytes` (default 64 MiB) caps the *compressed*
block size, not the *decompressed* size. A maliciously-crafted block can
declare a large decompressed length that the codec (snappy/deflate/zstd)
pre-allocates before validating the payload. This matches Java and fastavro
behavior. For OCF read from untrusted sources, bound memory at the transport
or process layer (request size cap, cgroup limit).

### Appending

`NewAppendWriter` opens an existing OCF for appending — it reads the header to
recover the schema, codec, and sync marker, then seeks to the end.

## JSON Encoding

`EncodeJSON` is a schema-aware JSON serializer. By default it produces standard
JSON with bare union values and `\uXXXX`-encoded bytes:

```go
// Standard JSON (default): bare unions
jsonBytes, err := schema.EncodeJSON(&user)
// {"name":"Alice","email":"a@b.com"}

// Avro JSON: unions wrapped as {"type_name": value}
jsonBytes, err = schema.EncodeJSON(&user, avro.TaggedUnions())
// {"name":"Alice","email":{"string":"a@b.com"}}
```

`DecodeJSON` accepts both formats (tagged and bare unions) and all NaN/Infinity
conventions:

```go
var user User
err = schema.DecodeJSON(jsonBytes, &user)
```

`Decode` and `DecodeJSON` also accept `TaggedUnions()` to wrap union values
when decoding into `*any`:

```go
var native any
schema.Decode(binary, &native, avro.TaggedUnions())
// native["email"] is map[string]any{"string": "a@b.com"}
```

`Encode` and `DecodeJSON` accept both tagged and bare union input, so
tagged union output from `Decode` can round-trip through `Encode` directly.

Pass `TagLogicalTypes()` with `TaggedUnions()` to qualify union branch names
with their logical type (e.g. `"long.timestamp-millis"` instead of `"long"`),
matching the linkedin/goavro naming convention.

NaN and Infinity float values are encoded as `"NaN"`, `"Infinity"`, `"-Infinity"`
strings by default (Java Avro convention). Pass `LinkedinFloats()` for
the linkedin/goavro convention (`null` for NaN, `±1e999` for Infinity).

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

## Errors

Encode and decode errors can be inspected with `errors.As`:

- **`*SemanticError`**: type mismatch between Go and Avro (includes a dotted
  field path for nested records, e.g. `"address.zip"`).
- **`*ShortBufferError`**: input truncated mid-value.
- **`*CompatibilityError`**: schema evolution incompatibility (from `Resolve`
  or `CheckCompatibility`).

## Performance

Struct field access uses `unsafe` pointer arithmetic (similar to
`encoding/json` v2) to avoid `reflect.Value` overhead on every encode/decode.
All schemas, type mappings, and codec state are cached after first use so
repeated operations pay no extra allocation cost.

## Encode/decode behavior contract

The encoder and decoder are mostly symmetric: any Go shape the encoder accepts
as input is a Go shape the decoder accepts as a target, and an encode→decode
round-trip through the same Go type yields the same value. The cases below are
deliberate exceptions.

### Lossy by design

- **`time.Time` → `time-millis` / `time-micros`**: the encoder extracts the
  wall-clock time-of-day fields (hours, minutes, seconds, sub-second nanos)
  and discards the date + zone — the wire format can't carry them. Round-trip
  preserves time-of-day only; the decoded `time.Time` sits at the Unix epoch
  with the original time-of-day.
- **`time.Time` → `date`**: the encoder takes the UTC date (year, month, day)
  and discards the time-of-day + zone. Round-trip preserves the date only.

### Spec / interop choices

- **Writer-union schema resolution fails eagerly.** Every branch must resolve
  at `Resolve` / `CheckCompatibility` time. Java's `Resolver.WriterUnion`
  defers per-branch errors to decode; we choose internal consistency with the
  rest of the package (`resolveEnum`, `resolveReaderUnion`, `resolveNode`,
  `validateDefault` are all eager).
- **`NaN` / `±Infinity` emit as JSON-quoted strings** (`"NaN"`, `"Infinity"`)
  by default. Java's `JsonEncoder` emits bare RFC-invalid tokens; we emit
  valid JSON. Bare tokens are accepted on decode for fastavro interop. Use
  `LinkedinFloats` for the goavro `null` / `1e999` / `-1e999` convention.
- **`DecodeJSON` fills schema-declared defaults for absent record fields.**
  Java's `JsonDecoder` errors on missing fields; we follow the binary-side
  defaulting behavior so a record can omit fields with defaults from JSON
  input.
- **`local-timestamp-*` encode wall-clock fields as if UTC** (matching Java's
  `TimeConversions.LocalTimestampMillisConversion` and fastavro). Decoded
  values are UTC `time.Time`.
- **OCF Snappy CRC is verified on read.** fastavro silently discards; we
  fail-fast on integrity errors.
- **Big-decimal canonical scale.** The encoder normalizes to the canonical
  `(unscaled, scale)` form. Java preserves trailing-zero scale information on
  the `BigDecimal` carrier; our `big.Rat` carrier can't represent the
  distinction, so the trailing-zero scale is not round-tripped.
- **Decimal JSON decode accepts both the spec form** (codepoint-mapped string)
  **and the bare-number form**. Java is strict (spec form only); we accept the
  lenient form for goavro / LinkedIn interop. Encode emits only the spec form.
- **JSON null-union fast paths accept non-canonical multi-byte varint
  encodings** of indices 0/1 (e.g. `0x80 0x00` = 0). Java's
  `BinaryDecoder.readIndex` accepts both canonical and non-canonical forms.
- **Union dispatch is type-based, not value-based.** When encoding a Go
  value into a union, the encoder routes by the value's static Go type:
  `int`, `int64`, `uint`, `uint32`, `uint64` all map to the `"long"`
  branch, never `"int"`, even when the runtime value would fit `int32`.
  A union `["int","long"]` with value `int(42)` emits `"long"`. To force
  the `"int"` branch for a small value, pass `int32(value)` explicitly
  or use a tagged-map wrapper `map[string]any{"int": value}`. The
  type-based rule keeps wire size deterministic per Go type — the
  alternative (value-aware) would mean the same Go field encodes
  differently across calls depending on the runtime magnitude.
- **Precision: the reader schema decides.**
  - Reader schema is lossy (`float`/`double`): encode and decode both
    silently IEEE-round. `s.AppendEncode(int64(9007199254740993), "double")`
    succeeds and emits the float64 rounding of the input. Matches
    Java/fastavro.
  - Reader schema is exact (`int`/`long`/`bytes`/`string`): decoding
    into a lossy Go target is allowed only if the wire value fits
    exactly. `s.Decode(longWire, &f float64)` against a `"long"`
    schema errors when the wire value exceeds float64's mantissa.

  Users wanting exact large-integer round-trip should keep `"long"`.
  Users evolving to `"double"` should expect silent IEEE rounding.
- **`json.Number` in fractional/exponent form is accepted against
  integer schemas when the value is exact.**
  `s.AppendEncode(json.Number("9.5e17"), "long")` succeeds (the
  literal represents an exact int64); twmb parses with arbitrary
  precision and the JSON encoder emits the integer-decimal form.

  Interop caveat: a schema with an exponent-form integer default like
  `{"type":"long","default":9.5e17}` parses in twmb but is rejected
  by Java's `Schema.parseField` (Jackson treats `.`/`e` literals as
  non-integral). Twmb preserves the schema text verbatim, so a
  twmb-published schema with this shape will not load in a Java
  consumer. Prefer integer-literal defaults (`"default":950000000000000000`)
  if Java reads your schemas.

### Decoder leniencies without a symmetric encoder shape

- **`TaggedUnions` on `DecodeJSON` wraps non-null union values** as
  `map[string]any{branchName: value}` when the decode target is `*any`.
  `EncodeJSON` accepts both the wrapped and the bare form on input, so an
  encode→decode round-trip into `*any` produces the wrapped form even when
  the user-provided input was bare. Documented on the `TaggedUnions` option.
- **Schema parser accepts `{"type":"Node"}` as an alternate spelling** of the
  bare `"Node"` name reference (matches Java's `TestUnionSelfReference`). The
  encoder emits only the bare form; the decoder accepts either.
- **Top-level union into a typed-map target** (e.g. `*map[string]any` against
  `["null","float"]`) is rejected by both binary and JSON paths — the target
  type doesn't fit a union schema. Use `*any` instead.
