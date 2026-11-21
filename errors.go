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
	case e.Field != "" && gt != "" && e.AvroType != "":
		// Field is built from schema field names, which have no length cap
		// (WithLaxNames admits any non-empty string), so we truncate it here
		// rather than let a hostile name amplify into an equally large error
		// on every mismatched datum. The public e.Field keeps the full value.
		s = fmt.Sprintf("avro: field %s: cannot use %s with Avro type %s", truncForError(e.Field), gt, e.AvroType)
	case e.Field != "":
		// The field is what tells you where the cause is. A field that
		// failed for a cause other than a type mismatch has no Avro type,
		// and a record that decodes into an any or a map has no Go type to
		// name. With no cause, the Avro type is all there is to say.
		s = fmt.Sprintf("avro: field %s", truncForError(e.Field))
		if e.Err == nil && e.AvroType != "" {
			s += ": unsupported Avro type " + e.AvroType
		}
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
		s += ": " + causeText(e.Err)
	}
	return s
}

// causeText returns the text of err for the end of one of our messages. We
// drop a leading "avro: " or "avro json: ", which starts each of our own
// errors, so that a chain of our errors names the package once.
func causeText(err error) string {
	s := err.Error()
	if t, ok := strings.CutPrefix(s, "avro: "); ok {
		return t
	}
	return strings.TrimPrefix(s, "avro json: ")
}

// causeError is an error that reads "msg: cause", where the cause's text is
// as causeText returns it, and that unwraps to the cause.
type causeError struct {
	msg   string
	cause error
}

func (e *causeError) Error() string { return e.msg + ": " + causeText(e.cause) }
func (e *causeError) Unwrap() error { return e.cause }

func formatGoType(t reflect.Type) string {
	if t == nil {
		return ""
	}
	return strings.ReplaceAll(t.String(), "interface {}", "any")
}

func (e *SemanticError) Unwrap() error { return e.Err }

func semErr(v reflect.Value, avroType string) error {
	return &SemanticError{GoType: v.Type(), AvroType: avroType}
}

func semErrW(v reflect.Value, avroType string, err error) error {
	return &SemanticError{GoType: v.Type(), AvroType: avroType, Err: err}
}

// recordFieldError builds the dotted path for nested records. For an inner
// SemanticError we prepend the field name to the path and keep the inner
// error's type information, so the chain does not fill with misleading
// intermediate "record" types. Any other cause is not a type mismatch, so the
// error names the record's Go type and no Avro type.
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
	return &SemanticError{GoType: goType, Field: fieldName, Err: err}
}

// defaultFieldError reports a field whose default could not go into the
// target. Unlike recordFieldError, it keeps the cause behind "default", so the
// text says the value came from the default and not from the document.
func defaultFieldError(goType reflect.Type, fieldName string, err error) error {
	return &SemanticError{GoType: goType, Field: fieldName, Err: &causeError{"default", err}}
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
	// Path is the dotted path to the incompatible element, e.g.
	// "User.address.zip".
	Path string
	// ReaderType is the Avro type in the reader schema.
	ReaderType string
	// WriterType is the Avro type in the writer schema.
	WriterType string
	// Detail describes the specific incompatibility.
	Detail string
}

func (e *CompatibilityError) Error() string {
	// Path, ReaderType, and WriterType carry user-controlled names with no
	// length cap at parse, so we truncate them here; the public fields keep
	// their full values. Detail truncates its own embedded names when it is
	// built.
	return fmt.Sprintf("avro: incompatible at %s: reader %s vs writer %s: %s",
		truncForError(e.Path), truncForError(e.ReaderType), truncForError(e.WriterType), e.Detail)
}
