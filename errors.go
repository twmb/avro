package avro

import (
	"fmt"
	"reflect"
)

// SemanticError describes a problem with the Avro-to-Go mapping.
type SemanticError struct {
	// GoType is the Go type involved, if applicable.
	GoType reflect.Type
	// AvroType is the Avro schema type (e.g. "int", "record", "boolean").
	AvroType string
	// Field is the record field name, if within a record.
	Field string
	// Err is the underlying error.
	Err error
}

func (e *SemanticError) Error() string {
	var s string
	switch {
	case e.Field != "" && e.GoType != nil:
		s = fmt.Sprintf("avro: field %s: cannot use Go type %s with Avro type %s", e.Field, e.GoType, e.AvroType)
	case e.GoType != nil && e.AvroType != "":
		s = fmt.Sprintf("avro: cannot use Go type %s with Avro type %s", e.GoType, e.AvroType)
	case e.GoType != nil:
		s = fmt.Sprintf("avro: unsupported Go type %s", e.GoType)
	case e.AvroType != "":
		s = fmt.Sprintf("avro: unsupported Avro type %s", e.AvroType)
	default:
		s = "avro: semantic error"
	}
	if e.Err != nil {
		s += ": " + e.Err.Error()
	}
	return s
}

func (e *SemanticError) Unwrap() error { return e.Err }

// ShortBufferError indicates that the input buffer is too short for the
// value being decoded.
type ShortBufferError struct {
	// Type is what was being read (e.g. "boolean", "string", "uint32").
	Type string
	// Need is the number of bytes required (0 if unknown).
	Need int
	// Have is the number of bytes available.
	Have int
}

func (e *ShortBufferError) Error() string {
	if e.Need > 0 {
		return fmt.Sprintf("avro: short buffer for %s: need %d, have %d", e.Type, e.Need, e.Have)
	}
	return fmt.Sprintf("avro: short buffer for %s", e.Type)
}

// CompatibilityError describes an incompatibility between a reader and writer
// schema at a specific path in the schema tree.
type CompatibilityError struct {
	// Path is the dotted path to the incompatible element (e.g. "User.address.zip").
	Path string
	// ReaderType is the Avro type in the reader schema.
	ReaderType string
	// WriterType is the Avro type in the writer schema.
	WriterType string
	// Detail describes the specific incompatibility.
	Detail string
}

func (e *CompatibilityError) Error() string {
	return fmt.Sprintf("avro: incompatible at %s: reader %s vs writer %s: %s", e.Path, e.ReaderType, e.WriterType, e.Detail)
}
