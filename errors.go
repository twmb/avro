package avro

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// SemanticError indicates a Go type is incompatible with an Avro schema type
// during encoding or decoding.
type SemanticError struct {
	// GoType is the Go type involved, if applicable.
	GoType reflect.Type
	// AvroType is the Avro schema type (e.g. "int", "record", "boolean").
	AvroType string
	// Field is the dotted path to the record field (e.g. "address.zip"),
	// if the error occurred within a record.
	Field string
	// Err is the underlying error.
	Err error
}

func (e *SemanticError) Error() string {
	gt := formatGoType(e.GoType)
	var s string
	switch {
	case e.Field != "" && gt != "":
		s = fmt.Sprintf("avro: field %s: cannot use %s with Avro type %s", e.Field, gt, e.AvroType)
	case gt != "" && e.AvroType != "":
		s = fmt.Sprintf("avro: cannot use %s with Avro type %s", gt, e.AvroType)
	case gt != "":
		s = fmt.Sprintf("avro: unsupported type %s", gt)
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

// formatGoType returns a human-friendly type name, replacing verbose
// reflect output like "interface {}" with "any".
func formatGoType(t reflect.Type) string {
	if t == nil {
		return ""
	}
	return strings.ReplaceAll(t.String(), "interface {}", "any")
}

func (e *SemanticError) Unwrap() error { return e.Err }

// recordFieldError wraps an error from a record field serializer/deserializer,
// building a dotted path for nested records. If the inner error is a
// SemanticError, the field name is prepended to the path and the inner
// error's type information is preserved (avoiding misleading intermediate
// "record" types in the error chain).
func recordFieldError(goType reflect.Type, fieldName string, err error) error {
	var inner *SemanticError
	if errors.As(err, &inner) {
		field := fieldName
		if inner.Field != "" {
			field = fieldName + "." + inner.Field
		}
		return &SemanticError{
			GoType:   inner.GoType,
			AvroType: inner.AvroType,
			Field:    field,
			Err:      inner.Err,
		}
	}
	return &SemanticError{GoType: goType, AvroType: "record", Field: fieldName, Err: err}
}

// ShortBufferError indicates the input buffer is too short for the value
// being decoded.
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
// schema, as returned by [CheckCompatibility] and [Resolve].
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
