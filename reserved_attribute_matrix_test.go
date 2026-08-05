package avro_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// The reserved-attribute enumeration.
//
// The question this answers is a CLASS, not a list of remembered cases: for a
// reserved attribute A with body shape B at level L on kind K, what is the
// outcome on every surface? The corpus below is that cross product, and every
// expected value in it was DERIVED FROM THE REFERENCE IMPLEMENTATIONS rather
// than read off this package's behavior — an expectation copied from the code
// under test asserts only that nothing changed, which is what let each of
// these families drift in the first place.
//
// How each expectation was obtained, and how to redo it:
//
//   - Apache Avro (Java) is a SOURCE-DERIVED model of Schema.java on
//     apache/avro:main, because the check runs without a JVM. Every rule in it
//     carries the line it came from, and the model is validated against the
//     already-adjudicated cells before any of its new answers are used. The
//     rules that matter most are the EMISSION conditions, which differ per
//     attribute: doc emits when non-null (:1039/:1154/:1367/:1062), aliases
//     when non-empty (:886/:1070), order is decided on the JSON node
//     (:1895-1897), and anything absent from SCHEMA_RESERVED (:175-176)
//     survives as an ordinary property through parseProperties (:1983).
//   - fastavro 1.12.2 was EXECUTED per cell.
//   - Where the two disagree, the expectation is one of THEIR answers and the
//     provenance table records which. This package never invents a third.
//   - Where neither can adjudicate — a stray structural key has no analogue in
//     either, since neither exposes a metadata tree — the standing rulings
//     govern and the provenance says so.
//   - Where NEITHER reference has the placement at all, PLACEMENT AUTHORITY
//     decides: the authority for a placement is whichever reference actually
//     HAS it, and it governs the empty and the non-empty body alike. Where
//     neither has it — a structural key on a kind that binds nothing, which
//     Apache Avro skips wholesale and fastavro keeps wholesale — this
//     package's own adjudicated posture governs, and the provenance says so.
//
// Every cell the enumeration produces is now settled. The UNRULED code stays
// in the vocabulary because a later round widening an axis will produce cells
// nothing rules yet, and recording one is the honest move; what may not
// happen is a cell quietly acquiring today's behavior as its expectation.
//
// The surfaces are read through the REBUILD (Root().Schema()), because that is
// the one that loses things. String() is the schema's own source text and
// carries every written key whatever the parse did with it, so asking it would
// report every accepted cell as preserved and prove nothing.
// ---------------------------------------------------------------------------

// Outcome codes, one character per body class in reservedBodyClasses order.
const (
	outReject  = 'R' // the parse rejects
	outKeep    = 'K' // the attribute survives to the rebuild
	outDropped = 'D' // accepted, and the attribute reaches no surface
	outAbsent  = 'A' // nothing was written
	outUnruled = '?' // recorded, not asserted: see the header
	outNA      = '-' // the body class does not apply to this attribute
)

var reservedBodyClasses = []string{"absent", "valid", "zero", "null", "wrong", "quoted"}

var reservedAttrs = []string{"type", "name", "namespace", "doc", "aliases", "fields",
	"items", "values", "symbols", "size", "default", "order", "logicalType",
	"precision", "scale"}

var reservedKinds = []string{"null", "boolean", "int", "long", "float", "double",
	"bytes", "string", "record", "error", "enum", "array", "map", "fixed"}

// reservedCellRow is one (attribute, level, kind) row; Outcomes holds one code
// per body class, in reservedBodyClasses order.
type reservedCellRow struct {
	Attr     string
	Level    string
	Kind     string
	Outcomes string
}

var reservedCellTable = []reservedCellRow{
	{"type", "type", "null", "RKRRR-"},
	{"type", "type", "boolean", "RKRRR-"},
	{"type", "type", "int", "RKRRR-"},
	{"type", "type", "long", "RKRRR-"},
	{"type", "type", "float", "RKRRR-"},
	{"type", "type", "double", "RKRRR-"},
	{"type", "type", "bytes", "RKRRR-"},
	{"type", "type", "string", "RKRRR-"},
	{"type", "type", "record", "RKRRR-"},
	{"type", "type", "error", "RKRRR-"},
	{"type", "type", "enum", "RKRRR-"},
	{"type", "type", "array", "RKRRR-"},
	{"type", "type", "map", "RKRRR-"},
	{"type", "type", "fixed", "RKRRR-"},
	{"type", "field", "null", "RKRRR-"},
	{"type", "field", "boolean", "RKRRR-"},
	{"type", "field", "int", "RKRRR-"},
	{"type", "field", "long", "RKRRR-"},
	{"type", "field", "float", "RKRRR-"},
	{"type", "field", "double", "RKRRR-"},
	{"type", "field", "bytes", "RKRRR-"},
	{"type", "field", "string", "RKRRR-"},
	{"type", "field", "record", "RRRRR-"},
	{"type", "field", "error", "RRRRR-"},
	{"type", "field", "enum", "RRRRR-"},
	{"type", "field", "array", "RRRRR-"},
	{"type", "field", "map", "RRRRR-"},
	{"type", "field", "fixed", "RRRRR-"},
	{"name", "type", "null", "AKKKK-"},
	{"name", "type", "boolean", "AKKKK-"},
	{"name", "type", "int", "AKKKK-"},
	{"name", "type", "long", "AKKKK-"},
	{"name", "type", "float", "AKKKK-"},
	{"name", "type", "double", "AKKKK-"},
	{"name", "type", "bytes", "AKKKK-"},
	{"name", "type", "string", "AKKKK-"},
	{"name", "type", "record", "RKRRR-"},
	{"name", "type", "error", "RKRRR-"},
	{"name", "type", "enum", "RKRRR-"},
	{"name", "type", "array", "ARKKK-"},
	{"name", "type", "map", "ARKKK-"},
	{"name", "type", "fixed", "RKRRR-"},
	{"name", "field", "null", "RKRRR-"},
	{"name", "field", "boolean", "RKRRR-"},
	{"name", "field", "int", "RKRRR-"},
	{"name", "field", "long", "RKRRR-"},
	{"name", "field", "float", "RKRRR-"},
	{"name", "field", "double", "RKRRR-"},
	{"name", "field", "bytes", "RKRRR-"},
	{"name", "field", "string", "RKRRR-"},
	{"name", "field", "record", "RKRRR-"},
	{"name", "field", "error", "RKRRR-"},
	{"name", "field", "enum", "RKRRR-"},
	{"name", "field", "array", "RKRRR-"},
	{"name", "field", "map", "RKRRR-"},
	{"name", "field", "fixed", "RKRRR-"},
	{"namespace", "type", "null", "AKKKK-"},
	{"namespace", "type", "boolean", "AKKKK-"},
	{"namespace", "type", "int", "AKKKK-"},
	{"namespace", "type", "long", "AKKKK-"},
	{"namespace", "type", "float", "AKKKK-"},
	{"namespace", "type", "double", "AKKKK-"},
	{"namespace", "type", "bytes", "AKKKK-"},
	{"namespace", "type", "string", "AKKKK-"},
	{"namespace", "type", "record", "AKDRR-"},
	{"namespace", "type", "error", "AKDRR-"},
	{"namespace", "type", "enum", "AKDRR-"},
	{"namespace", "type", "array", "AKKKK-"},
	{"namespace", "type", "map", "AKKKK-"},
	{"namespace", "type", "fixed", "AKDRR-"},
	{"namespace", "field", "null", "AKKKK-"},
	{"namespace", "field", "boolean", "AKKKK-"},
	{"namespace", "field", "int", "AKKKK-"},
	{"namespace", "field", "long", "AKKKK-"},
	{"namespace", "field", "float", "AKKKK-"},
	{"namespace", "field", "double", "AKKKK-"},
	{"namespace", "field", "bytes", "AKKKK-"},
	{"namespace", "field", "string", "AKKKK-"},
	{"namespace", "field", "record", "AKKKK-"},
	{"namespace", "field", "error", "AKKKK-"},
	{"namespace", "field", "enum", "AKKKK-"},
	{"namespace", "field", "array", "AKKKK-"},
	{"namespace", "field", "map", "AKKKK-"},
	{"namespace", "field", "fixed", "AKKKK-"},
	{"doc", "type", "null", "AKKDD-"},
	{"doc", "type", "boolean", "AKKDD-"},
	{"doc", "type", "int", "AKKDD-"},
	{"doc", "type", "long", "AKKDD-"},
	{"doc", "type", "float", "AKKDD-"},
	{"doc", "type", "double", "AKKDD-"},
	{"doc", "type", "bytes", "AKKDD-"},
	{"doc", "type", "string", "AKKDD-"},
	{"doc", "type", "record", "AKKDD-"},
	{"doc", "type", "error", "AKKDD-"},
	{"doc", "type", "enum", "AKKDD-"},
	{"doc", "type", "array", "AKKDD-"},
	{"doc", "type", "map", "AKKDD-"},
	{"doc", "type", "fixed", "AKKDD-"},
	{"doc", "field", "null", "AKKDD-"},
	{"doc", "field", "boolean", "AKKDD-"},
	{"doc", "field", "int", "AKKDD-"},
	{"doc", "field", "long", "AKKDD-"},
	{"doc", "field", "float", "AKKDD-"},
	{"doc", "field", "double", "AKKDD-"},
	{"doc", "field", "bytes", "AKKDD-"},
	{"doc", "field", "string", "AKKDD-"},
	{"doc", "field", "record", "AKKDD-"},
	{"doc", "field", "error", "AKKDD-"},
	{"doc", "field", "enum", "AKKDD-"},
	{"doc", "field", "array", "AKKDD-"},
	{"doc", "field", "map", "AKKDD-"},
	{"doc", "field", "fixed", "AKKDD-"},
	{"aliases", "type", "null", "AKKKK-"},
	{"aliases", "type", "boolean", "AKKKK-"},
	{"aliases", "type", "int", "AKKKK-"},
	{"aliases", "type", "long", "AKKKK-"},
	{"aliases", "type", "float", "AKKKK-"},
	{"aliases", "type", "double", "AKKKK-"},
	{"aliases", "type", "bytes", "AKKKK-"},
	{"aliases", "type", "string", "AKKKK-"},
	{"aliases", "type", "record", "AKDRR-"},
	{"aliases", "type", "error", "AKDRR-"},
	{"aliases", "type", "enum", "AKDRR-"},
	{"aliases", "type", "array", "AKKKK-"},
	{"aliases", "type", "map", "AKKKK-"},
	{"aliases", "type", "fixed", "AKDRR-"},
	{"aliases", "field", "null", "AKDRR-"},
	{"aliases", "field", "boolean", "AKDRR-"},
	{"aliases", "field", "int", "AKDRR-"},
	{"aliases", "field", "long", "AKDRR-"},
	{"aliases", "field", "float", "AKDRR-"},
	{"aliases", "field", "double", "AKDRR-"},
	{"aliases", "field", "bytes", "AKDRR-"},
	{"aliases", "field", "string", "AKDRR-"},
	{"aliases", "field", "record", "AKDRR-"},
	{"aliases", "field", "error", "AKDRR-"},
	{"aliases", "field", "enum", "AKDRR-"},
	{"aliases", "field", "array", "AKDRR-"},
	{"aliases", "field", "map", "AKDRR-"},
	{"aliases", "field", "fixed", "AKDRR-"},
	{"fields", "type", "null", "AKKKK-"},
	{"fields", "type", "boolean", "AKKKK-"},
	{"fields", "type", "int", "AKKKK-"},
	{"fields", "type", "long", "AKKKK-"},
	{"fields", "type", "float", "AKKKK-"},
	{"fields", "type", "double", "AKKKK-"},
	{"fields", "type", "bytes", "AKKKK-"},
	{"fields", "type", "string", "AKKKK-"},
	{"fields", "type", "record", "RKKRR-"},
	{"fields", "type", "error", "RKKRR-"},
	{"fields", "type", "enum", "ARKKK-"},
	{"fields", "type", "array", "ARKKK-"},
	{"fields", "type", "map", "ARKKK-"},
	{"fields", "type", "fixed", "ARKKK-"},
	{"fields", "field", "null", "AKKKK-"},
	{"fields", "field", "boolean", "AKKKK-"},
	{"fields", "field", "int", "AKKKK-"},
	{"fields", "field", "long", "AKKKK-"},
	{"fields", "field", "float", "AKKKK-"},
	{"fields", "field", "double", "AKKKK-"},
	{"fields", "field", "bytes", "AKKKK-"},
	{"fields", "field", "string", "AKKKK-"},
	{"fields", "field", "record", "AKKKK-"},
	{"fields", "field", "error", "AKKKK-"},
	{"fields", "field", "enum", "AKKKK-"},
	{"fields", "field", "array", "AKKKK-"},
	{"fields", "field", "map", "AKKKK-"},
	{"fields", "field", "fixed", "AKKKK-"},
	{"items", "type", "null", "AKKKK-"},
	{"items", "type", "boolean", "AKKKK-"},
	{"items", "type", "int", "AKKKK-"},
	{"items", "type", "long", "AKKKK-"},
	{"items", "type", "float", "AKKKK-"},
	{"items", "type", "double", "AKKKK-"},
	{"items", "type", "bytes", "AKKKK-"},
	{"items", "type", "string", "AKKKK-"},
	{"items", "type", "record", "ARRKK-"},
	{"items", "type", "error", "ARRKK-"},
	{"items", "type", "enum", "ARRKK-"},
	{"items", "type", "array", "RKRRR-"},
	{"items", "type", "map", "ARRKK-"},
	{"items", "type", "fixed", "ARRKK-"},
	{"items", "field", "null", "AKKKK-"},
	{"items", "field", "boolean", "AKKKK-"},
	{"items", "field", "int", "AKKKK-"},
	{"items", "field", "long", "AKKKK-"},
	{"items", "field", "float", "AKKKK-"},
	{"items", "field", "double", "AKKKK-"},
	{"items", "field", "bytes", "AKKKK-"},
	{"items", "field", "string", "AKKKK-"},
	{"items", "field", "record", "AKKKK-"},
	{"items", "field", "error", "AKKKK-"},
	{"items", "field", "enum", "AKKKK-"},
	{"items", "field", "array", "AKKKK-"},
	{"items", "field", "map", "AKKKK-"},
	{"items", "field", "fixed", "AKKKK-"},
	{"values", "type", "null", "AKKKK-"},
	{"values", "type", "boolean", "AKKKK-"},
	{"values", "type", "int", "AKKKK-"},
	{"values", "type", "long", "AKKKK-"},
	{"values", "type", "float", "AKKKK-"},
	{"values", "type", "double", "AKKKK-"},
	{"values", "type", "bytes", "AKKKK-"},
	{"values", "type", "string", "AKKKK-"},
	{"values", "type", "record", "ARRKK-"},
	{"values", "type", "error", "ARRKK-"},
	{"values", "type", "enum", "ARRKK-"},
	{"values", "type", "array", "ARRKK-"},
	{"values", "type", "map", "RKRRR-"},
	{"values", "type", "fixed", "ARRKK-"},
	{"values", "field", "null", "AKKKK-"},
	{"values", "field", "boolean", "AKKKK-"},
	{"values", "field", "int", "AKKKK-"},
	{"values", "field", "long", "AKKKK-"},
	{"values", "field", "float", "AKKKK-"},
	{"values", "field", "double", "AKKKK-"},
	{"values", "field", "bytes", "AKKKK-"},
	{"values", "field", "string", "AKKKK-"},
	{"values", "field", "record", "AKKKK-"},
	{"values", "field", "error", "AKKKK-"},
	{"values", "field", "enum", "AKKKK-"},
	{"values", "field", "array", "AKKKK-"},
	{"values", "field", "map", "AKKKK-"},
	{"values", "field", "fixed", "AKKKK-"},
	{"symbols", "type", "null", "AKKKK-"},
	{"symbols", "type", "boolean", "AKKKK-"},
	{"symbols", "type", "int", "AKKKK-"},
	{"symbols", "type", "long", "AKKKK-"},
	{"symbols", "type", "float", "AKKKK-"},
	{"symbols", "type", "double", "AKKKK-"},
	{"symbols", "type", "bytes", "AKKKK-"},
	{"symbols", "type", "string", "AKKKK-"},
	{"symbols", "type", "record", "ARKKK-"},
	{"symbols", "type", "error", "ARKKK-"},
	{"symbols", "type", "enum", "RKKRR-"},
	{"symbols", "type", "array", "ARKKK-"},
	{"symbols", "type", "map", "ARKKK-"},
	{"symbols", "type", "fixed", "ARKKK-"},
	{"symbols", "field", "null", "AKKKK-"},
	{"symbols", "field", "boolean", "AKKKK-"},
	{"symbols", "field", "int", "AKKKK-"},
	{"symbols", "field", "long", "AKKKK-"},
	{"symbols", "field", "float", "AKKKK-"},
	{"symbols", "field", "double", "AKKKK-"},
	{"symbols", "field", "bytes", "AKKKK-"},
	{"symbols", "field", "string", "AKKKK-"},
	{"symbols", "field", "record", "AKKKK-"},
	{"symbols", "field", "error", "AKKKK-"},
	{"symbols", "field", "enum", "AKKKK-"},
	{"symbols", "field", "array", "AKKKK-"},
	{"symbols", "field", "map", "AKKKK-"},
	{"symbols", "field", "fixed", "AKKKK-"},
	{"size", "type", "null", "AKKKKK"},
	{"size", "type", "boolean", "AKKKKK"},
	{"size", "type", "int", "AKKKKK"},
	{"size", "type", "long", "AKKKKK"},
	{"size", "type", "float", "AKKKKK"},
	{"size", "type", "double", "AKKKKK"},
	{"size", "type", "bytes", "AKKKKK"},
	{"size", "type", "string", "AKKKKK"},
	{"size", "type", "record", "ARRKKR"},
	{"size", "type", "error", "ARRKKR"},
	{"size", "type", "enum", "ARRKKR"},
	{"size", "type", "array", "ARRKKR"},
	{"size", "type", "map", "ARRKKR"},
	{"size", "type", "fixed", "RKKRRK"},
	{"size", "field", "null", "AKKKKK"},
	{"size", "field", "boolean", "AKKKKK"},
	{"size", "field", "int", "AKKKKK"},
	{"size", "field", "long", "AKKKKK"},
	{"size", "field", "float", "AKKKKK"},
	{"size", "field", "double", "AKKKKK"},
	{"size", "field", "bytes", "AKKKKK"},
	{"size", "field", "string", "AKKKKK"},
	{"size", "field", "record", "AKKKKK"},
	{"size", "field", "error", "AKKKKK"},
	{"size", "field", "enum", "AKKKKK"},
	{"size", "field", "array", "AKKKKK"},
	{"size", "field", "map", "AKKKKK"},
	{"size", "field", "fixed", "AKKKKK"},
	{"default", "type", "null", "AKKKK-"},
	{"default", "type", "boolean", "AKKKK-"},
	{"default", "type", "int", "AKKKK-"},
	{"default", "type", "long", "AKKKK-"},
	{"default", "type", "float", "AKKKK-"},
	{"default", "type", "double", "AKKKK-"},
	{"default", "type", "bytes", "AKKKK-"},
	{"default", "type", "string", "AKKKK-"},
	{"default", "type", "record", "AKKKK-"},
	{"default", "type", "error", "AKKKK-"},
	{"default", "type", "enum", "AKRRR-"},
	{"default", "type", "array", "AKKKK-"},
	{"default", "type", "map", "AKKKK-"},
	{"default", "type", "fixed", "AKKKK-"},
	{"default", "field", "null", "AKKKR-"},
	{"default", "field", "boolean", "AKKRR-"},
	{"default", "field", "int", "AKKRR-"},
	{"default", "field", "long", "AKKRR-"},
	{"default", "field", "float", "AKKRR-"},
	{"default", "field", "double", "AKKRR-"},
	{"default", "field", "bytes", "AKKRR-"},
	{"default", "field", "string", "AKKRR-"},
	{"default", "field", "record", "AKRRR-"},
	{"default", "field", "error", "AKRRR-"},
	{"default", "field", "enum", "AKRRR-"},
	{"default", "field", "array", "AKKRR-"},
	{"default", "field", "map", "AKKRR-"},
	{"default", "field", "fixed", "AKRRR-"},
	{"order", "type", "null", "AKKKK-"},
	{"order", "type", "boolean", "AKKKK-"},
	{"order", "type", "int", "AKKKK-"},
	{"order", "type", "long", "AKKKK-"},
	{"order", "type", "float", "AKKKK-"},
	{"order", "type", "double", "AKKKK-"},
	{"order", "type", "bytes", "AKKKK-"},
	{"order", "type", "string", "AKKKK-"},
	{"order", "type", "record", "AKKKK-"},
	{"order", "type", "error", "AKKKK-"},
	{"order", "type", "enum", "AKKKK-"},
	{"order", "type", "array", "AKKKK-"},
	{"order", "type", "map", "AKKKK-"},
	{"order", "type", "fixed", "AKKKK-"},
	{"order", "field", "null", "AKRRR-"},
	{"order", "field", "boolean", "AKRRR-"},
	{"order", "field", "int", "AKRRR-"},
	{"order", "field", "long", "AKRRR-"},
	{"order", "field", "float", "AKRRR-"},
	{"order", "field", "double", "AKRRR-"},
	{"order", "field", "bytes", "AKRRR-"},
	{"order", "field", "string", "AKRRR-"},
	{"order", "field", "record", "AKRRR-"},
	{"order", "field", "error", "AKRRR-"},
	{"order", "field", "enum", "AKRRR-"},
	{"order", "field", "array", "AKRRR-"},
	{"order", "field", "map", "AKRRR-"},
	{"order", "field", "fixed", "AKRRR-"},
	{"logicalType", "type", "null", "AKKKK-"},
	{"logicalType", "type", "boolean", "AKKKK-"},
	{"logicalType", "type", "int", "AKKKK-"},
	{"logicalType", "type", "long", "AKKKK-"},
	{"logicalType", "type", "float", "AKKKK-"},
	{"logicalType", "type", "double", "AKKKK-"},
	{"logicalType", "type", "bytes", "ARKKK-"},
	{"logicalType", "type", "string", "AKKKK-"},
	{"logicalType", "type", "record", "AKKKK-"},
	{"logicalType", "type", "error", "AKKKK-"},
	{"logicalType", "type", "enum", "AKKKK-"},
	{"logicalType", "type", "array", "AKKKK-"},
	{"logicalType", "type", "map", "AKKKK-"},
	{"logicalType", "type", "fixed", "AKKKK-"},
	{"logicalType", "field", "null", "AKKKK-"},
	{"logicalType", "field", "boolean", "AKKKK-"},
	{"logicalType", "field", "int", "AKKKK-"},
	{"logicalType", "field", "long", "AKKKK-"},
	{"logicalType", "field", "float", "AKKKK-"},
	{"logicalType", "field", "double", "AKKKK-"},
	{"logicalType", "field", "bytes", "ARKKK-"},
	{"logicalType", "field", "string", "AKKKK-"},
	{"logicalType", "field", "record", "AKKKK-"},
	{"logicalType", "field", "error", "AKKKK-"},
	{"logicalType", "field", "enum", "AKKKK-"},
	{"logicalType", "field", "array", "AKKKK-"},
	{"logicalType", "field", "map", "AKKKK-"},
	{"logicalType", "field", "fixed", "AKKKK-"},
	{"precision", "type", "null", "AKKKKK"},
	{"precision", "type", "boolean", "AKKKKK"},
	{"precision", "type", "int", "AKKKKK"},
	{"precision", "type", "long", "AKKKKK"},
	{"precision", "type", "float", "AKKKKK"},
	{"precision", "type", "double", "AKKKKK"},
	{"precision", "type", "bytes", "AKKKKK"},
	{"precision", "type", "string", "AKKKKK"},
	{"precision", "type", "record", "AKKKKK"},
	{"precision", "type", "error", "AKKKKK"},
	{"precision", "type", "enum", "AKKKKK"},
	{"precision", "type", "array", "AKKKKK"},
	{"precision", "type", "map", "AKKKKK"},
	{"precision", "type", "fixed", "AKKKKK"},
	{"precision", "field", "null", "AKKKKK"},
	{"precision", "field", "boolean", "AKKKKK"},
	{"precision", "field", "int", "AKKKKK"},
	{"precision", "field", "long", "AKKKKK"},
	{"precision", "field", "float", "AKKKKK"},
	{"precision", "field", "double", "AKKKKK"},
	{"precision", "field", "bytes", "AKKKKK"},
	{"precision", "field", "string", "AKKKKK"},
	{"precision", "field", "record", "AKKKKK"},
	{"precision", "field", "error", "AKKKKK"},
	{"precision", "field", "enum", "AKKKKK"},
	{"precision", "field", "array", "AKKKKK"},
	{"precision", "field", "map", "AKKKKK"},
	{"precision", "field", "fixed", "AKKKKK"},
	{"scale", "type", "null", "AKKKKK"},
	{"scale", "type", "boolean", "AKKKKK"},
	{"scale", "type", "int", "AKKKKK"},
	{"scale", "type", "long", "AKKKKK"},
	{"scale", "type", "float", "AKKKKK"},
	{"scale", "type", "double", "AKKKKK"},
	{"scale", "type", "bytes", "AKKKKK"},
	{"scale", "type", "string", "AKKKKK"},
	{"scale", "type", "record", "AKKKKK"},
	{"scale", "type", "error", "AKKKKK"},
	{"scale", "type", "enum", "AKKKKK"},
	{"scale", "type", "array", "AKKKKK"},
	{"scale", "type", "map", "AKKKKK"},
	{"scale", "type", "fixed", "AKKKKK"},
	{"scale", "field", "null", "AKKKKK"},
	{"scale", "field", "boolean", "AKKKKK"},
	{"scale", "field", "int", "AKKKKK"},
	{"scale", "field", "long", "AKKKKK"},
	{"scale", "field", "float", "AKKKKK"},
	{"scale", "field", "double", "AKKKKK"},
	{"scale", "field", "bytes", "AKKKKK"},
	{"scale", "field", "string", "AKKKKK"},
	{"scale", "field", "record", "AKKKKK"},
	{"scale", "field", "error", "AKKKKK"},
	{"scale", "field", "enum", "AKKKKK"},
	{"scale", "field", "array", "AKKKKK"},
	{"scale", "field", "map", "AKKKKK"},
	{"scale", "field", "fixed", "AKKKKK"},
}

// reservedProvenance records, per (attribute, body class, level), what settled
// the expectation. It is data, not decoration: a cell whose provenance says
// "unruled" must not be treated as pinned behavior, and a cell that says
// "follows-fastavro" is one where this package chose the permissive side of a
// reference disagreement and can be revisited on new evidence.
type reservedProvRow struct {
	Attr, Body, Level, By string
}

var reservedProvenance = []reservedProvRow{
	{"aliases", "absent", "field", "java-model"},
	{"aliases", "absent", "type", "java-model"},
	{"aliases", "null", "field", "both-references"},
	{"aliases", "null", "type", "follows-java|standing-ruling"},
	{"aliases", "valid", "field", "both-references"},
	{"aliases", "valid", "type", "both-references|standing-ruling"},
	{"aliases", "wrong", "field", "both-references"},
	{"aliases", "wrong", "type", "follows-java|standing-ruling"},
	{"aliases", "zero", "field", "follows-java"},
	{"aliases", "zero", "type", "placement-authority(stray-routing)"},
	{"default", "absent", "field", "java-model"},
	{"default", "absent", "type", "java-model"},
	{"default", "null", "field", "both-references|follows-fastavro"},
	{"default", "null", "type", "follows-fastavro|standing-ruling"},
	{"default", "valid", "field", "both-references"},
	{"default", "valid", "type", "both-references|standing-ruling"},
	{"default", "wrong", "field", "follows-fastavro"},
	{"default", "wrong", "type", "follows-fastavro|standing-ruling"},
	{"default", "zero", "field", "both-references|documented-divergence|follows-java"},
	{"default", "zero", "type", "both-references|standing-ruling"},
	{"doc", "absent", "field", "java-model"},
	{"doc", "absent", "type", "java-model"},
	{"doc", "null", "field", "follows-java"},
	{"doc", "null", "type", "follows-java|placement-authority(fastavro-has-the-placement)"},
	{"doc", "valid", "field", "both-references"},
	{"doc", "valid", "type", "both-references|placement-authority(fastavro-has-the-placement)"},
	{"doc", "wrong", "field", "follows-java"},
	{"doc", "wrong", "type", "follows-java|placement-authority(fastavro-has-the-placement)"},
	{"doc", "zero", "field", "both-references"},
	{"doc", "zero", "type", "both-references|placement-authority(fastavro-has-the-placement)"},
	{"fields", "absent", "field", "java-model"},
	{"fields", "absent", "type", "java-model"},
	{"fields", "null", "field", "standing-ruling"},
	{"fields", "null", "type", "both-references|standing-ruling"},
	{"fields", "valid", "field", "standing-ruling"},
	{"fields", "valid", "type", "both-references|standing-ruling"},
	{"fields", "wrong", "field", "standing-ruling"},
	{"fields", "wrong", "type", "both-references|standing-ruling"},
	{"fields", "zero", "field", "standing-ruling"},
	{"fields", "zero", "type", "placement-authority(stray-routing)"},
	{"items", "absent", "field", "java-model"},
	{"items", "absent", "type", "java-model"},
	{"items", "null", "field", "standing-ruling"},
	{"items", "null", "type", "both-references|standing-ruling"},
	{"items", "valid", "field", "standing-ruling"},
	{"items", "valid", "type", "both-references|standing-ruling"},
	{"items", "wrong", "field", "standing-ruling"},
	{"items", "wrong", "type", "both-references|standing-ruling"},
	{"items", "zero", "field", "standing-ruling"},
	{"items", "zero", "type", "both-references|standing-ruling"},
	{"logicalType", "absent", "field", "java-model"},
	{"logicalType", "absent", "type", "java-model"},
	{"logicalType", "null", "field", "standing-ruling"},
	{"logicalType", "null", "type", "both-references"},
	{"logicalType", "valid", "field", "standing-ruling"},
	{"logicalType", "valid", "type", "both-references|documented-divergence"},
	{"logicalType", "wrong", "field", "standing-ruling"},
	{"logicalType", "wrong", "type", "both-references"},
	{"logicalType", "zero", "field", "standing-ruling"},
	{"logicalType", "zero", "type", "both-references"},
	{"name", "absent", "field", "java-model"},
	{"name", "absent", "type", "java-model"},
	{"name", "null", "field", "follows-java"},
	{"name", "null", "type", "both-references|standing-ruling"},
	{"name", "valid", "field", "both-references"},
	{"name", "valid", "type", "both-references|standing-ruling"},
	{"name", "wrong", "field", "follows-java"},
	{"name", "wrong", "type", "both-references|standing-ruling"},
	{"name", "zero", "field", "follows-java"},
	{"name", "zero", "type", "placement-authority(stray-routing)"},
	{"namespace", "absent", "field", "java-model"},
	{"namespace", "absent", "type", "java-model"},
	{"namespace", "null", "field", "standing-ruling"},
	{"namespace", "null", "type", "ruled(uniform-name-strictness)"},
	{"namespace", "valid", "field", "standing-ruling"},
	{"namespace", "valid", "type", "both-references|standing-ruling"},
	{"namespace", "wrong", "field", "standing-ruling"},
	{"namespace", "wrong", "type", "ruled(uniform-name-strictness)"},
	{"namespace", "zero", "field", "standing-ruling"},
	{"namespace", "zero", "type", "placement-authority(stray-routing)"},
	{"order", "absent", "field", "java-model"},
	{"order", "absent", "type", "java-model"},
	{"order", "null", "field", "follows-java"},
	{"order", "null", "type", "standing-ruling"},
	{"order", "valid", "field", "both-references"},
	{"order", "valid", "type", "standing-ruling"},
	{"order", "wrong", "field", "follows-java"},
	{"order", "wrong", "type", "standing-ruling"},
	{"order", "zero", "field", "follows-java"},
	{"order", "zero", "type", "standing-ruling"},
	{"precision", "absent", "field", "java-model"},
	{"precision", "absent", "type", "java-model"},
	{"precision", "null", "field", "standing-ruling"},
	{"precision", "null", "type", "both-references"},
	{"precision", "quoted", "field", "standing-ruling"},
	{"precision", "quoted", "type", "both-references"},
	{"precision", "valid", "field", "standing-ruling"},
	{"precision", "valid", "type", "both-references"},
	{"precision", "wrong", "field", "standing-ruling"},
	{"precision", "wrong", "type", "both-references"},
	{"precision", "zero", "field", "standing-ruling"},
	{"precision", "zero", "type", "both-references"},
	{"scale", "absent", "field", "java-model"},
	{"scale", "absent", "type", "java-model"},
	{"scale", "null", "field", "standing-ruling"},
	{"scale", "null", "type", "both-references"},
	{"scale", "quoted", "field", "standing-ruling"},
	{"scale", "quoted", "type", "both-references"},
	{"scale", "valid", "field", "standing-ruling"},
	{"scale", "valid", "type", "both-references"},
	{"scale", "wrong", "field", "standing-ruling"},
	{"scale", "wrong", "type", "both-references"},
	{"scale", "zero", "field", "standing-ruling"},
	{"scale", "zero", "type", "both-references"},
	{"size", "absent", "field", "java-model"},
	{"size", "absent", "type", "java-model"},
	{"size", "null", "field", "standing-ruling"},
	{"size", "null", "type", "follows-java|standing-ruling"},
	{"size", "quoted", "field", "standing-ruling"},
	{"size", "quoted", "type", "follows-fastavro|standing-ruling"},
	{"size", "valid", "field", "standing-ruling"},
	{"size", "valid", "type", "both-references|standing-ruling"},
	{"size", "wrong", "field", "standing-ruling"},
	{"size", "wrong", "type", "follows-java|standing-ruling"},
	{"size", "zero", "field", "standing-ruling"},
	{"size", "zero", "type", "placement-authority(stray-routing)"},
	{"symbols", "absent", "field", "java-model"},
	{"symbols", "absent", "type", "java-model"},
	{"symbols", "null", "field", "standing-ruling"},
	{"symbols", "null", "type", "both-references|standing-ruling"},
	{"symbols", "valid", "field", "standing-ruling"},
	{"symbols", "valid", "type", "both-references|standing-ruling"},
	{"symbols", "wrong", "field", "standing-ruling"},
	{"symbols", "wrong", "type", "both-references|standing-ruling"},
	{"symbols", "zero", "field", "standing-ruling"},
	{"symbols", "zero", "type", "placement-authority(stray-routing)"},
	{"type", "absent", "field", "java-model"},
	{"type", "absent", "type", "java-model"},
	{"type", "null", "field", "both-references"},
	{"type", "null", "type", "both-references"},
	{"type", "valid", "field", "both-references"},
	{"type", "valid", "type", "both-references"},
	{"type", "wrong", "field", "both-references"},
	{"type", "wrong", "type", "both-references"},
	{"type", "zero", "field", "both-references"},
	{"type", "zero", "type", "both-references"},
	{"values", "absent", "field", "java-model"},
	{"values", "absent", "type", "java-model"},
	{"values", "null", "field", "standing-ruling"},
	{"values", "null", "type", "both-references|standing-ruling"},
	{"values", "valid", "field", "standing-ruling"},
	{"values", "valid", "type", "both-references|standing-ruling"},
	{"values", "wrong", "field", "standing-ruling"},
	{"values", "wrong", "type", "both-references|standing-ruling"},
	{"values", "zero", "field", "standing-ruling"},
	{"values", "zero", "type", "both-references|standing-ruling"},
}

// reservedBody renders the attribute's body for one class, and reports
// whether the class applies to this attribute at all. The ZERO body is the
// JSON zero of the attribute's DESTINATION — "" for a string, [] for an
// array, {} for an object, 0 for an int — which is the whole point of the
// axis: an attribute written as its destination's zero is written, and a
// reader that tests the value alone cannot see it.
func reservedBody(attr, kind, class string) (string, bool) {
	valid, zero, wrong, quoted := "", "", "", ""
	switch attr {
	case "type":
		valid, zero, wrong = `"`+kind+`"`, `""`, "5"
	case "name":
		valid, zero, wrong = `"Nm"`, `""`, "5"
	case "namespace":
		valid, zero, wrong = `"ns"`, `""`, "5"
	case "doc":
		valid, zero, wrong = `"d"`, `""`, "5"
	case "aliases", "symbols":
		valid, zero, wrong = `["A"]`, "[]", "5"
	case "fields":
		valid, zero, wrong = `[{"name":"z","type":"int"}]`, "[]", "5"
	case "items", "values":
		valid, zero, wrong = `"int"`, "{}", "5"
	case "size", "scale":
		valid, zero, wrong, quoted = "2", "0", "[]", `"2"`
	case "precision":
		valid, zero, wrong, quoted = "4", "0", "[]", `"4"`
	case "order":
		valid, zero, wrong = `"ignore"`, `""`, "5"
	case "logicalType":
		valid, zero, wrong = reservedLogicalFor(kind), `""`, "5"
	case "default":
		valid, zero, wrong = reservedDefaultFor(kind)
	}
	switch class {
	case "absent":
		return "", true
	case "valid":
		return valid, true
	case "zero":
		return zero, true
	case "null":
		return "null", true
	case "wrong":
		return wrong, true
	case "quoted":
		return quoted, quoted != ""
	}
	return "", false
}

func reservedLogicalFor(kind string) string {
	switch kind {
	case "long":
		return `"timestamp-millis"`
	case "bytes":
		return `"decimal"`
	case "fixed":
		return `"duration"`
	case "string":
		return `"uuid"`
	}
	return `"date"`
}

// reservedDefaultFor gives a value the kind can take, the kind's zero value,
// and a token of a JSON class it can never take.
func reservedDefaultFor(kind string) (valid, zero, wrong string) {
	wrong = "[]"
	switch kind {
	case "null":
		return "null", "null", wrong
	case "boolean":
		return "true", "false", wrong
	case "int", "long":
		return "3", "0", wrong
	case "float", "double":
		return "1.5", "0", wrong
	case "bytes", "string":
		return `"s"`, `""`, wrong
	case "record", "error":
		return `{"z":0}`, "{}", wrong
	case "enum":
		return `"A"`, `""`, wrong
	case "array":
		return "[]", "[]", `"s"`
	case "map":
		return "{}", "{}", wrong
	case "fixed":
		return `"AAAA"`, `""`, wrong
	}
	return "null", "null", wrong
}

// reservedBaseKeys is the kind's own object, as an ordered key list so the
// generated text is deterministic. The ABSENT body DELETES the attribute from
// it: several of these keys are required by their kind, so leaving the base's
// own copy in place would silently test the valid body twice.
func reservedBaseKeys(kind string) [][2]string {
	switch kind {
	case "record", "error":
		return [][2]string{{"type", `"` + kind + `"`}, {"name", `"K"`},
			{"fields", `[{"name":"z","type":"int"}]`}}
	case "enum":
		return [][2]string{{"type", `"enum"`}, {"name", `"K"`}, {"symbols", `["A","B"]`}}
	case "fixed":
		return [][2]string{{"type", `"fixed"`}, {"name", `"K"`}, {"size", "4"}}
	case "array":
		return [][2]string{{"type", `"array"`}, {"items", `"int"`}}
	case "map":
		return [][2]string{{"type", `"map"`}, {"values", `"int"`}}
	}
	return [][2]string{{"type", `"` + kind + `"`}}
}

// reservedCellSchema builds the whole schema for one cell. Every cell is a
// host record with one field whose type is the kind under test, so the
// type-level and field-level spellings differ only in WHICH object carries
// the attribute.
func reservedCellSchema(attr, kind, level, class string) (string, bool) {
	body, applies := reservedBody(attr, kind, class)
	if !applies {
		return "", false
	}
	typeKeys := reservedBaseKeys(kind)
	fieldKeys := [][2]string{{"name", `"a"`}}
	set := func(keys [][2]string) [][2]string {
		out := keys[:0:0]
		for _, kv := range keys {
			if kv[0] != attr {
				out = append(out, kv)
			}
		}
		if class != "absent" {
			out = append(out, [2]string{attr, body})
		}
		return out
	}
	if level == "type" {
		typeKeys = set(typeKeys)
	} else {
		fieldKeys = set(fieldKeys)
	}
	obj := func(keys [][2]string) string {
		var b strings.Builder
		b.WriteByte('{')
		for i, kv := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "%q:%s", kv[0], kv[1])
		}
		b.WriteByte('}')
		return b.String()
	}
	// The field's "type" key is the field's schema, so a field-level cell
	// writing "type" REPLACES it rather than adding a second attribute —
	// which is the real question at that level.
	inner := obj(typeKeys)
	hasOwnType := false
	for _, kv := range fieldKeys {
		if kv[0] == "type" {
			hasOwnType = true
		}
	}
	if !hasOwnType && !(level == "field" && attr == "type" && class == "absent") {
		fieldKeys = append(fieldKeys, [2]string{"type", inner})
	}
	return `{"type":"record","name":"Host","fields":[` + obj(fieldKeys) + `]}`, true
}

// reservedCellOutcome reads the cell's outcome off the surface that can lose
// an attribute.
func reservedCellOutcome(t *testing.T, attr, level, src, class string) byte {
	t.Helper()
	s, err := avro.Parse(src)
	if err != nil {
		return outReject
	}
	if class == "absent" {
		return outAbsent
	}
	root := s.Root()
	rb, err := root.Schema()
	if err != nil {
		return outDropped
	}
	if reservedCarrierHasKey(rb.String(), attr, level) {
		return outKeep
	}
	return outDropped
}

// reservedCarrierHasKey walks a rendered schema down to the object the cell
// wrote its attribute on and reports whether that object still carries it.
func reservedCarrierHasKey(text, attr, level string) bool {
	var host map[string]any
	if json.Unmarshal([]byte(text), &host) != nil {
		return false
	}
	fs, _ := host["fields"].([]any)
	if len(fs) == 0 {
		return false
	}
	f, _ := fs[0].(map[string]any)
	if f == nil {
		return false
	}
	obj := f
	if level == "type" {
		if _, bare := f["type"].(string); bare {
			// The type object collapsed to its bare name: that IS the type
			// attribute surviving, and nothing else did.
			return attr == "type"
		}
		obj, _ = f["type"].(map[string]any)
		if obj == nil {
			return false
		}
	}
	_, ok := obj[attr]
	return ok
}

// TestMatrix_ReservedAttributeEnumeration drives every cell of the cross
// product against the reference-derived expectation.
func TestMatrix_ReservedAttributeEnumeration(t *testing.T) {
	var checked, unruled int
	for _, row := range reservedCellTable {
		if len(row.Outcomes) != len(reservedBodyClasses) {
			t.Fatalf("row %s/%s/%s has %d outcomes, want one per body class",
				row.Attr, row.Level, row.Kind, len(row.Outcomes))
		}
		for i, class := range reservedBodyClasses {
			want := row.Outcomes[i]
			src, applies := reservedCellSchema(row.Attr, row.Kind, row.Level, class)
			if want == outNA {
				if applies {
					t.Errorf("%s/%s/%s/%s is marked not-applicable but the corpus produces a schema for it",
						row.Attr, row.Level, row.Kind, class)
				}
				continue
			}
			if !applies {
				t.Errorf("%s/%s/%s/%s expects %q but the corpus produces no schema for it",
					row.Attr, row.Level, row.Kind, class, string(want))
				continue
			}
			got := reservedCellOutcome(t, row.Attr, row.Level, src, class)
			if want == outUnruled {
				unruled++
				// Recorded, not asserted: the cell is real and its answer is
				// open. Pinning today's behavior would convert a question
				// nothing settles into a decision nobody made.
				continue
			}
			checked++
			if got != want {
				t.Errorf("%s/%s/%s/%s = %q, want %q\n  schema: %s",
					row.Attr, row.Level, row.Kind, class, string(got), string(want), src)
			}
		}
	}
	t.Logf("reserved-attribute enumeration: %d cells asserted, %d recorded unruled", checked, unruled)
	if checked < 2000 {
		t.Fatalf("only %d cells were asserted; the corpus is not spanning the cross product", checked)
	}
	if unruled != 0 {
		t.Errorf("%d cells are unruled; every cell this corpus produces is settled, so a new one is a ruling to make rather than a gap to leave", unruled)
	}
}

// TestMatrix_ReservedAttributeEnumerationIsNotVacuous fails when the corpus
// stops spanning the axes, or when its outcomes collapse toward one answer —
// either would let an implementation pass by never being asked.
func TestMatrix_ReservedAttributeEnumerationIsNotVacuous(t *testing.T) {
	attrs, kinds, levels := map[string]int{}, map[string]int{}, map[string]int{}
	codes := map[byte]int{}
	for _, row := range reservedCellTable {
		attrs[row.Attr]++
		kinds[row.Kind]++
		levels[row.Level]++
		for i := range reservedBodyClasses {
			codes[row.Outcomes[i]]++
		}
	}
	if len(attrs) != len(reservedAttrs) {
		t.Errorf("corpus covers %d attributes, the axis names %d", len(attrs), len(reservedAttrs))
	}
	if len(kinds) != len(reservedKinds) {
		t.Errorf("corpus covers %d kinds, the axis names %d", len(kinds), len(reservedKinds))
	}
	if len(levels) != 2 {
		t.Errorf("corpus covers %d levels, want both", len(levels))
	}
	// Every outcome must be REACHED. A table that only ever expects "keep"
	// would pass against an implementation that accepted and preserved
	// everything, which is exactly the failure this enumeration exists to
	// catch on the other side.
	for _, c := range []byte{outReject, outKeep, outDropped, outAbsent} {
		if codes[c] < 50 {
			t.Errorf("outcome %q appears %d times; the table has collapsed toward one answer", string(c), codes[c])
		}
	}
	// Every cell is settled. A table that grew an unruled one has produced a
	// question, which is fine — but it must be answered rather than left, so
	// the driver above reports it.
	if codes[outUnruled] != 0 {
		t.Errorf("%d cells carry the unruled code", codes[outUnruled])
	}
	// Provenance must exist for every (attribute, body, level) family the
	// corpus produces, or a cell's authority is unrecorded.
	have := map[string]bool{}
	for _, p := range reservedProvenance {
		have[p.Attr+"/"+p.Body+"/"+p.Level] = true
	}
	for _, row := range reservedCellTable {
		for i, class := range reservedBodyClasses {
			if row.Outcomes[i] == outNA {
				continue
			}
			if !have[row.Attr+"/"+class+"/"+row.Level] {
				t.Errorf("no provenance recorded for %s/%s/%s", row.Attr, class, row.Level)
			}
		}
	}
}
