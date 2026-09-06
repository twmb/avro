// Package atype names the Avro schema types, logical types, and field sort
// orders. These are the string values you see in [avro.SchemaNode],
// [avro.SchemaField], and [avro.CustomType].
package atype

// Avro primitive types.
const (
	Null    = "null"
	Boolean = "boolean"
	Int     = "int"
	Long    = "long"
	Float   = "float"
	Double  = "double"
	String  = "string"
	Bytes   = "bytes"
)

// Avro complex types.
const (
	Record = "record"
	Error  = "error"
	Enum   = "enum"
	Array  = "array"
	Map    = "map"
	Union  = "union"
	Fixed  = "fixed"
)

// Avro logical types.
const (
	Date                 = "date"
	TimeMillis           = "time-millis"
	TimeMicros           = "time-micros"
	TimestampMillis      = "timestamp-millis"
	TimestampMicros      = "timestamp-micros"
	TimestampNanos       = "timestamp-nanos"
	LocalTimestampMillis = "local-timestamp-millis"
	LocalTimestampMicros = "local-timestamp-micros"
	LocalTimestampNanos  = "local-timestamp-nanos"
	Decimal              = "decimal"
	BigDecimal           = "big-decimal"
	UUID                 = "uuid"
	Duration             = "duration"
)

// Avro field sort orders.
const (
	Ascending  = "ascending"
	Descending = "descending"
	Ignore     = "ignore"
)
