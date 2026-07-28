package avro_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// An explicit JSON null is a PRESENT attribute whose body names no value, and
// it is the one body shape a typed decode accepts in silence.
//
// encoding/json documents it: "To unmarshal JSON into a pointer, Unmarshal
// first handles the case of the JSON being the JSON literal null. In that
// case, Unmarshal sets the pointer to nil." — and unmarshaling null into any
// non-pointer destination "has no effect on the value and produces no error".
// So a reader that decides presence by asking "did the decode fail" reads a
// present-but-undecodable attribute as an ABSENT one and hands back the
// destination's zero value.
//
// That zero value is not neutral for Avro schema attributes. A fixed's size 0
// is a legal, distinct schema (a zero-width fixed, which encodes and decodes),
// and a decimal's scale 0 is a legal, distinct scale that changes the wire
// meaning of every value written against it. Coercing a null body to either
// one silently substitutes a different schema for the one that was written.
//
// The references never produce those schemas:
//
//   - Java REJECTS a null size outright: parseFixed reads
//     `JsonNode sizeNode = schema.get("size"); if (sizeNode == null ||
//     !sizeNode.isInt()) throw new SchemaParseException("Invalid or no size")`
//     (Schema.java:1957-1960), and NullNode.isInt() is false.
//   - Java never builds decimal(p,0) from a null scale either: the Decimal
//     logical type reads each parameter through getInt, which throws unless
//     the prop is an Integer (LogicalTypes.java:414-421), and a JSON null prop
//     comes back as Java null. Schema parse calls fromSchemaIgnoreInvalid
//     (Schema.java:1979), which swallows the throw and drops the logical
//     entirely, leaving plain bytes. Silently dropping a decimal annotation is
//     the hazard this package already declined to copy, so it rejects loudly
//     where Java soft-drops — but neither behavior produces the decimal(p,0)
//     that a coerced zero produces.
//   - fastavro 1.12.2 accepts these at parse and then FAILS every write
//     against the result (executed: a null-size fixed reports "data of length
//     N does not match schema size: None" for every input length, and a
//     null-scale decimal raises TypeError inside its scale arithmetic). Its
//     accept is not a usable accept, so the permissive lean has nothing to
//     lean toward. TestDifferentialFastavroNullBodyIsNotUsable executes that
//     calibration so it fails here if fastavro ever changes it.
//
// The rule these tests pin is one sentence: a null body is a MALFORMED body,
// never an absent one — so it reaches exactly the verdict its wrong-typed
// siblings reach, at every key and placement. The boundary that must NOT
// move: a written 0 is a value, not a null, and every zero-valued attribute
// keeps parsing.
// ---------------------------------------------------------------------------

// TestRegression_NullSizeRejectedOnFixed holds the fixed-size boundary from
// both sides: an explicit null size is rejected because it names no width,
// while the written zero width that Java, fastavro and avro-rs all accept
// keeps parsing and keeps encoding.
func TestRegression_NullSizeRejectedOnFixed(t *testing.T) {
	if _, err := avro.Parse(`{"type":"fixed","name":"F","size":null}`); err == nil {
		t.Error("a null size parsed; it names no width, so there is no fixed to build")
	} else if !strings.Contains(err.Error(), "size") {
		t.Errorf("the reject does not name the offending key: %v", err)
	}

	// A written zero is a VALUE, and a legal one: a zero-width fixed is
	// reference-legal and usable. The reject above must not reach it.
	zero, err := avro.Parse(`{"type":"fixed","name":"F","size":0}`)
	if err != nil {
		t.Fatalf(`"size":0 must stay legal, it is a width: %v`, err)
	}
	if got := zero.Root().Size; got != 0 {
		t.Errorf("zero-width fixed Size = %d, want 0", got)
	}
	b, err := zero.Encode([]byte{})
	if err != nil || len(b) != 0 {
		t.Errorf("zero-width fixed encode = %x, %v; want no bytes and no error", b, err)
	}

	// The quoted integer form stays accepted: the Parsing Canonical Form
	// rule blesses a quoted size, and quoting is a spelling of a value, not
	// an absence.
	quoted, err := avro.Parse(`{"type":"fixed","name":"F","size":"2"}`)
	if err != nil {
		t.Fatalf(`quoted "size" must stay accepted: %v`, err)
	}
	if got := quoted.Root().Size; got != 2 {
		t.Errorf("quoted size read back as %d, want 2", got)
	}
	if _, err := avro.Parse(`{"type":"fixed","name":"F","size":"0"}`); err != nil {
		t.Errorf(`quoted "0" must stay accepted, it is the same width as 0: %v`, err)
	}

	// A quoted null is a STRING whose content is not an integer, so it fails
	// for the ordinary reason and not through the null path.
	if _, err := avro.Parse(`{"type":"fixed","name":"F","size":"null"}`); err == nil {
		t.Error(`"size":"null" parsed; the string "null" is not an integer`)
	}
}

// TestRegression_NullDecimalParamsRejectedWhereConsumed pins the decimal
// parameters at the placements that consume them. A null scale must not become
// scale 0: the two schemas encode different bytes for the same value, so
// accepting one as the other silently rewrites the wire contract.
func TestRegression_NullDecimalParamsRejectedWhereConsumed(t *testing.T) {
	carriers := []struct {
		name string
		src  func(precision, scale string) string
	}{{
		name: "bytes-type-object",
		src: func(p, s string) string {
			return `{"type":"bytes","logicalType":"decimal","precision":` + p + `,"scale":` + s + `}`
		},
	}, {
		name: "fixed-type-object",
		src: func(p, s string) string {
			return `{"type":"fixed","name":"D","size":8,"logicalType":"decimal","precision":` + p + `,"scale":` + s + `}`
		},
	}, {
		name: "field-lift-onto-bytes",
		src: func(p, s string) string {
			return `{"type":"record","name":"R","fields":[{"name":"f","type":"bytes",` +
				`"logicalType":"decimal","precision":` + p + `,"scale":` + s + `}]}`
		},
	}, {
		name: "field-lift-onto-union-branch",
		src: func(p, s string) string {
			return `{"type":"record","name":"R","fields":[{"name":"f","type":["null","bytes"],` +
				`"logicalType":"decimal","precision":` + p + `,"scale":` + s + `}]}`
		},
	}}

	for _, c := range carriers {
		t.Run(c.name, func(t *testing.T) {
			_, err := avro.Parse(c.src("4", "null"))
			if err == nil {
				t.Error("a null scale parsed; scale 0 is a different schema than the one written")
			} else if !strings.Contains(err.Error(), "scale") {
				t.Errorf("the reject does not name the offending key: %v", err)
			}

			// precision is the sibling, and its reject must come from the
			// null itself. A zero precision is separately illegal (it can
			// hold no digits), so a guard that only checked positivity would
			// mask a missing null check here: the error must name the key as
			// a JSON-shape problem rather than report a precision of 0.
			_, err = avro.Parse(c.src("null", "2"))
			if err == nil {
				t.Fatal("a null precision parsed")
			}
			if !strings.Contains(err.Error(), "precision") {
				t.Errorf("the reject does not name the offending key: %v", err)
			}
			if strings.Contains(err.Error(), "positive") {
				t.Errorf("the null precision was rejected by the positivity check on a coerced 0, "+
					"not by a check on the body that was written: %v", err)
			}

			// The valid twin must still parse, or the rejects above prove
			// nothing about the null in particular.
			if _, err := avro.Parse(c.src("4", "2")); err != nil {
				t.Fatalf("the valid twin stopped parsing: %v", err)
			}
			// Zero is a value on both parameters where it is legal: scale 0
			// is the spec's own default, and it must keep parsing.
			if _, err := avro.Parse(c.src("4", "0")); err != nil {
				t.Errorf(`"scale":0 must stay legal, it is the spec default: %v`, err)
			}
		})
	}
}

// TestRegression_NullScaleWouldChangeTheWire is the consequence that makes the
// reject above a correctness matter rather than a strictness preference: the
// same value encodes to different bytes at scale 2 and scale 0, and at scale 0
// a two-decimal value cannot be represented at all.
func TestRegression_NullScaleWouldChangeTheWire(t *testing.T) {
	type rec struct {
		F float64 `avro:"f"`
	}
	scaled := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":` +
		`{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}}]}`)
	want, err := scaled.Encode(rec{F: 1.23})
	if err != nil {
		t.Fatalf("scale 2 encode: %v", err)
	}
	if len(want) == 0 {
		t.Fatal("scale 2 encoded to nothing, so the comparison would prove nothing")
	}

	zeroScale := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":` +
		`{"type":"bytes","logicalType":"decimal","precision":4,"scale":0}}]}`)
	if _, err := zeroScale.Encode(rec{F: 1.23}); err == nil {
		t.Fatal("scale 0 accepted a two-decimal value; the two scales must differ observably " +
			"or the null-scale reject would be arbitrary")
	}

	// With the null rejected at parse there is no schema to encode against,
	// which is the point: the caller learns at parse time instead of holding
	// a schema whose scale nobody wrote.
	if _, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":` +
		`{"type":"bytes","logicalType":"decimal","precision":4,"scale":null}}]}`); err == nil {
		t.Error("a null scale parsed inside a nested field type")
	}
}

// TestRegression_NullStrayBodyRidesToProps is the other half of the rule. At a
// placement the kind does not bind, a malformed body is inert metadata whose
// ONLY surface is Props, as-written — so a null must land there like every
// other malformed body instead of vanishing from both surfaces.
//
// The container and named hosts also exercise the exclusivity rule, which
// rejects a kind carrying ANOTHER kind's defining key only when that key
// parsed as a real definition; a body that names no value defines nothing, so
// it must route like the other malformed bodies rather than trip exclusivity.
func TestRegression_NullStrayBodyRidesToProps(t *testing.T) {
	hosts := []struct{ name, src string }{
		{"int", `{"type":"int","size":null}`},
		{"string", `{"type":"string","size":null}`},
		{"array", `{"type":"array","items":"int","size":null}`},
		{"map", `{"type":"map","values":"int","size":null}`},
		{"record", `{"type":"record","name":"R","fields":[],"size":null}`},
		{"enum", `{"type":"enum","name":"E","symbols":["A"],"size":null}`},
	}
	for _, h := range hosts {
		t.Run(h.name, func(t *testing.T) {
			s, err := avro.Parse(h.src)
			if err != nil {
				t.Fatalf("a stray null body must be inert metadata, not a reject: %v", err)
			}
			n := s.Root()
			v, ok := n.Props["size"]
			if !ok {
				t.Fatalf("the stray null reached NEITHER surface; Props is its only surface: %#v", n.Props)
			}
			if v != nil {
				t.Errorf("Props[%q] = %#v, want the as-written null", "size", v)
			}
			if n.Size != 0 {
				t.Errorf("a declined body still set the structural Size = %d", n.Size)
			}
			// The rebuild reads only the metadata tree, so it is where an
			// attribute the tree never captured disappears.
			rb, err := n.Schema()
			if err != nil {
				t.Fatalf("Root().Schema(): %v", err)
			}
			if !strings.Contains(rb.String(), `"size":null`) {
				t.Errorf("the rebuild lost the as-written null: %s", rb)
			}
			// The malformed sibling must reach the same place, or "null" is
			// being treated as a body class of its own.
			sib, err := avro.Parse(strings.Replace(h.src, `"size":null`, `"size":[]`, 1))
			if err != nil {
				t.Fatalf("the malformed sibling stopped parsing: %v", err)
			}
			if _, ok := sib.Root().Props["size"]; !ok {
				t.Errorf("the malformed sibling did not ride to Props: %#v", sib.Root().Props)
			}

			// The boundary on the other side: a READABLE size is a real
			// width, and a container or named kind carrying another kind's
			// defining key as a real definition still hard-rejects. Routing
			// the unreadable bodies to Props must not soften that.
			readable := strings.Replace(h.src, `"size":null`, `"size":2`, 1)
			switch h.name {
			case "int", "string":
				// A primitive object surfaces a readable stray as-written.
				ps, err := avro.Parse(readable)
				if err != nil {
					t.Fatalf("a readable stray size on a primitive must surface as-written: %v", err)
				}
				if ps.Root().Size != 2 {
					t.Errorf("a readable stray size did not reach the structural field: %d", ps.Root().Size)
				}
			default:
				if _, err := avro.Parse(readable); err == nil {
					t.Errorf("a readable size on a %s stopped rejecting; the exclusivity rule is "+
						"about keys that parsed as a real definition", h.name)
				}
			}
		})
	}
}

// TestRegression_NullUnconsumedDecimalParamsRideToProps is the same rule for
// the decimal parameters: where no decimal consumes them they are ordinary
// metadata, so a null body rides through verbatim rather than rejecting.
func TestRegression_NullUnconsumedDecimalParamsRideToProps(t *testing.T) {
	for _, src := range []string{
		`{"type":"int","scale":null,"precision":null}`,
		`{"type":"bytes","scale":null,"precision":null}`,
		`{"type":"bytes","logicalType":"date","scale":null,"precision":null}`,
		`{"type":"record","name":"R","fields":[{"name":"f","type":"int","scale":null,"precision":null}]}`,
	} {
		s, err := avro.Parse(src)
		if err != nil {
			t.Fatalf("an unconsumed null param must stay inert metadata: %v (%s)", err, src)
		}
		props := s.Root().Props
		if len(s.Root().Fields) > 0 {
			props = s.Root().Fields[0].Props
		}
		for _, k := range []string{"scale", "precision"} {
			v, ok := props[k]
			if !ok {
				t.Errorf("%q did not reach Props: %#v (%s)", k, props, src)
			} else if v != nil {
				t.Errorf("Props[%q] = %#v, want the as-written null (%s)", k, v, src)
			}
		}
	}
}

// TestRegression_NullDefaultIsAValue holds the boundary the matrix below
// excludes: a default's body is kept as raw JSON rather than decoded into a
// typed destination, so a null default is the null VALUE and must stay
// accepted and distinguishable from an absent default.
func TestRegression_NullDefaultIsAValue(t *testing.T) {
	s, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":["null","int"],"default":null}]}`)
	if err != nil {
		t.Fatalf("a null default is a legal value for a null-first union: %v", err)
	}
	f := s.Root().Fields[0]
	if !f.HasDefault {
		t.Error("HasDefault = false for a written null default; presence is not the value")
	}
	if f.Default != nil {
		t.Errorf("Default = %#v, want the null value", f.Default)
	}
	// The enum-LEVEL default is the one place a default's body IS read into
	// a typed destination, and it decides by token type before membership, so
	// a null rejects there. That check is the shape every other typed read
	// has to match.
	if _, err := avro.Parse(`{"type":"enum","name":"E","symbols":["A"],"default":null}`); err == nil {
		t.Error("a null enum-level default parsed; it names no symbol")
	}
}

// TestRegression_NullBodyRejectIsBounded keeps the reject off the cost path a
// hostile schema could exploit: a null body is four bytes, so the decision
// must not depend on the size of the rest of the schema, and the error must
// not echo it.
func TestRegression_NullBodyRejectIsBounded(t *testing.T) {
	pad := strings.Repeat("a", 1<<20)
	src := `{"type":"fixed","name":"F","size":null,"pad":"` + pad + `"}`
	_, err := avro.Parse(src)
	if err == nil {
		t.Fatal("a null size parsed")
	}
	if len(err.Error()) > 200 {
		t.Errorf("the reject echoed %d bytes of the input", len(err.Error()))
	}
}

// ---------------------------------------------------------------------------
// The matrix.
//
// Every reserved key whose body is decoded into a TYPED destination, crossed
// with the body classes a caller can write, at both levels, read off every
// surface. The rule it drives: for a given key and placement, a NULL body
// reaches the same verdict as a body of the wrong JSON type. Equality with the
// wrong-typed twin is what stops null from being its own body class; the valid
// twin's differing verdict is what keeps the equality from holding vacuously.
//
// "default" is deliberately absent: its body is kept as raw JSON rather than
// decoded into a typed destination, so a null default is a VALUE, not an
// undecodable body (TestRegression_NullDefaultIsAValue).
// ---------------------------------------------------------------------------

// malformedRoute is where a body the placement cannot read ends up. Each cell
// states its route from the rulings, not from the code.
type malformedRoute int

const (
	// routeReject: the kind binds the key and its read is strict, so an
	// unreadable body fails the parse.
	routeReject malformedRoute = iota
	// routeProps: the kind does not bind the key (or binds it only for a
	// body of the right JSON type), so an unreadable body is inert metadata
	// riding to Props verbatim as its only surface.
	routeProps
	// routeDrop: the key is bound on every kind but its capture is a
	// silently-declining read, so an unreadable body reaches NEITHER
	// surface. Exactly one key behaves this way ("doc"), matching Apache
	// Avro, and it is spelled as an outcome here so it stays counted and
	// cannot widen unnoticed.
	routeDrop
)

func (r malformedRoute) String() string {
	switch r {
	case routeReject:
		return "reject"
	case routeProps:
		return "props"
	}
	return "drop"
}

// presenceCell is one reserved key at one placement.
type presenceCell struct {
	key   string
	level string // "type" or "field": which object carries the attribute
	// host spells the whole schema with the attribute clause substituted in.
	host func(attr string) string
	// route is where a body this placement cannot read must end up.
	route malformedRoute
	// validStructural states whether a READABLE body lands on a structural
	// field. It is false for the two keys a non-binding kind surfaces only
	// through Props (precision/scale, which have no stray structural route).
	validStructural bool
	// validRejects marks the exclusivity cells: a container kind carrying
	// another kind's DEFINING key rejects when that key parsed as a real
	// definition, so there the readable body is the one that fails and the
	// unreadable one rides to Props. Stating it keeps the rule visible
	// instead of steering the corpus around it.
	validRejects bool
	valid        string
	wrong        string
	quoted       string // "" when the key's valid body is already a string
	// structural reads the destination a readable body lands on, so
	// "consumed" is measured off the metadata API rather than assumed.
	structural func(avro.SchemaNode) bool
	// props reads the Props map of the object that carried the attribute.
	props func(avro.SchemaNode) map[string]any
}

func (c presenceCell) name() string {
	return fmt.Sprintf("%s/%s/%s", c.level, c.key, c.route)
}

// The three Props surfaces a cell can carry its attribute on. Every host below
// wraps the subject in one record field, so the three are always present.
func pcFieldTypeProps(n avro.SchemaNode) map[string]any { return n.Fields[0].Type.Props }
func pcFieldProps(n avro.SchemaNode) map[string]any     { return n.Fields[0].Props }

// pcTypeHost puts the attribute clause on the field's TYPE object.
func pcTypeHost(typeSrc string) func(string) string {
	return func(attr string) string {
		return fmt.Sprintf(`{"type":"record","name":"PHost","fields":[{"name":"a","type":%s}]}`,
			fmt.Sprintf(typeSrc, attr))
	}
}

// pcFieldHost puts the attribute clause on the record FIELD object.
func pcFieldHost(typeSrc string) func(string) string {
	return func(attr string) string {
		return fmt.Sprintf(`{"type":"record","name":"PHost","fields":[{"name":"a","type":%s%s}]}`,
			typeSrc, attr)
	}
}

func pcType(n avro.SchemaNode) avro.SchemaNode { return n.Fields[0].Type }

func presenceCells() []presenceCell {
	return []presenceCell{
		// ---- the laxInt destination ----
		{
			key: "size", level: "type", host: pcTypeHost(`{"type":"fixed","name":"PF"%s}`),
			route: routeReject, validStructural: true,
			valid: "2", wrong: "[]", quoted: `"2"`,
			structural: func(n avro.SchemaNode) bool { return pcType(n).Size != 0 },
			props:      pcFieldTypeProps,
		},
		{
			key: "size", level: "type", host: pcTypeHost(`{"type":"int"%s}`),
			route: routeProps, validStructural: true,
			valid: "2", wrong: "[]", quoted: `"2"`,
			structural: func(n avro.SchemaNode) bool { return pcType(n).Size != 0 },
			props:      pcFieldTypeProps,
		},
		{
			key: "size", level: "type", host: pcTypeHost(`{"type":"map","values":"int"%s}`),
			route: routeProps, validStructural: true, validRejects: true,
			valid: "2", wrong: "[]", quoted: `"2"`,
			structural: func(n avro.SchemaNode) bool { return pcType(n).Size != 0 },
			props:      pcFieldTypeProps,
		},
		// The flat ("linkedin/goavro") field form writes the defining key
		// beside the field's own keys, so the lift carries the body into the
		// type object it builds — the field-level reach of the same decode.
		{
			key: "size", level: "field", host: pcFieldHost(`"fixed","name":"PFF"`),
			route: routeReject, validStructural: true,
			valid: "2", wrong: "[]", quoted: `"2"`,
			structural: func(n avro.SchemaNode) bool { return pcType(n).Size != 0 },
			props:      pcFieldProps,
		},
		// ---- the *int destinations ----
		{
			key: "scale", level: "type", host: pcTypeHost(`{"type":"bytes","logicalType":"decimal","precision":4%s}`),
			route: routeReject, validStructural: true,
			valid: "2", wrong: "[]", quoted: `"2"`,
			structural: func(n avro.SchemaNode) bool { return pcType(n).Scale != 0 },
			props:      pcFieldTypeProps,
		},
		{
			key: "precision", level: "type", host: pcTypeHost(`{"type":"bytes","logicalType":"decimal","scale":2%s}`),
			route: routeReject, validStructural: true,
			valid: "4", wrong: "[]", quoted: `"4"`,
			structural: func(n avro.SchemaNode) bool { return pcType(n).Precision != 0 },
			props:      pcFieldTypeProps,
		},
		{
			key: "scale", level: "type", host: pcTypeHost(`{"type":"bytes"%s}`),
			route: routeProps, validStructural: false,
			valid: "2", wrong: "[]", quoted: `"2"`,
			structural: func(n avro.SchemaNode) bool { return pcType(n).Scale != 0 },
			props:      pcFieldTypeProps,
		},
		{
			key: "precision", level: "type", host: pcTypeHost(`{"type":"int"%s}`),
			route: routeProps, validStructural: false,
			valid: "4", wrong: "[]", quoted: `"4"`,
			structural: func(n avro.SchemaNode) bool { return pcType(n).Precision != 0 },
			props:      pcFieldTypeProps,
		},
		{
			key: "scale", level: "field", host: pcFieldHost(`"int"`),
			route: routeProps, validStructural: false,
			valid: "2", wrong: "[]", quoted: `"2"`,
			structural: func(n avro.SchemaNode) bool { return pcType(n).Scale != 0 },
			props:      pcFieldProps,
		},
		// ---- the string destinations ----
		{
			key: "name", level: "type", host: pcTypeHost(`{"type":"fixed","size":1%s}`),
			route: routeReject, validStructural: true,
			valid: `"PN"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return pcType(n).Name != "" },
			props:      pcFieldTypeProps,
		},
		{
			key: "namespace", level: "type", host: pcTypeHost(`{"type":"fixed","name":"PNS","size":1%s}`),
			route: routeReject, validStructural: true,
			valid: `"ns"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return pcType(n).Namespace != "" },
			props:      pcFieldTypeProps,
		},
		{
			key: "namespace", level: "type", host: pcTypeHost(`{"type":"int"%s}`),
			route: routeProps, validStructural: true,
			valid: `"ns"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return pcType(n).Namespace != "" },
			props:      pcFieldTypeProps,
		},
		{
			// logicalType binds only for a STRING body: a non-string can
			// name no logical type, so it is an ordinary property.
			key: "logicalType", level: "type", host: pcTypeHost(`{"type":"int"%s}`),
			route: routeProps, validStructural: true,
			valid: `"date"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return pcType(n).LogicalType != "" },
			props:      pcFieldTypeProps,
		},
		{
			key: "order", level: "field", host: pcFieldHost(`"int"`),
			route: routeReject, validStructural: true,
			valid: `"ignore"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return n.Fields[0].Order != "" },
			props:      pcFieldProps,
		},
		{
			key: "name", level: "field", host: pcFieldHostNoName(`"int"`),
			route: routeReject, validStructural: true,
			valid: `"a"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return n.Fields[0].Name != "" },
			props:      pcFieldProps,
		},
		// ---- the []string destinations ----
		{
			key: "aliases", level: "type", host: pcTypeHost(`{"type":"fixed","name":"PA","size":1%s}`),
			route: routeReject, validStructural: true,
			valid: `["x"]`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return len(pcType(n).Aliases) > 0 },
			props:      pcFieldTypeProps,
		},
		{
			key: "aliases", level: "type", host: pcTypeHost(`{"type":"int"%s}`),
			route: routeProps, validStructural: true,
			valid: `["x"]`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return len(pcType(n).Aliases) > 0 },
			props:      pcFieldTypeProps,
		},
		{
			key: "aliases", level: "field", host: pcFieldHost(`"int"`),
			route: routeReject, validStructural: true,
			valid: `["x"]`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return len(n.Fields[0].Aliases) > 0 },
			props:      pcFieldProps,
		},
		{
			key: "symbols", level: "type", host: pcTypeHost(`{"type":"enum","name":"PE"%s}`),
			route: routeReject, validStructural: true,
			valid: `["A"]`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return len(pcType(n).Symbols) > 0 },
			props:      pcFieldTypeProps,
		},
		{
			key: "symbols", level: "type", host: pcTypeHost(`{"type":"int"%s}`),
			route: routeProps, validStructural: true,
			valid: `["A"]`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return len(pcType(n).Symbols) > 0 },
			props:      pcFieldTypeProps,
		},
		// ---- the schema-shaped destinations ----
		{
			key: "items", level: "type", host: pcTypeHost(`{"type":"array"%s}`),
			route: routeReject, validStructural: true,
			valid: `"int"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return pcType(n).Items != nil },
			props:      pcFieldTypeProps,
		},
		{
			key: "values", level: "type", host: pcTypeHost(`{"type":"map"%s}`),
			route: routeReject, validStructural: true,
			valid: `"int"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return pcType(n).Values != nil },
			props:      pcFieldTypeProps,
		},
		{
			key: "items", level: "type", host: pcTypeHost(`{"type":"int"%s}`),
			route: routeProps, validStructural: true,
			valid: `"int"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return pcType(n).Items != nil },
			props:      pcFieldTypeProps,
		},
		{
			key: "fields", level: "type", host: pcTypeHost(`{"type":"record","name":"PR"%s}`),
			route: routeReject, validStructural: true,
			valid: `[{"name":"z","type":"int"}]`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return len(pcType(n).Fields) > 0 },
			props:      pcFieldTypeProps,
		},
		// ---- doc: bound on every kind, with the declining read that keeps
		// an unreadable body off BOTH surfaces ----
		{
			key: "doc", level: "type", host: pcTypeHost(`{"type":"int"%s}`),
			route: routeDrop, validStructural: true,
			valid: `"d"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return pcType(n).Doc != "" },
			props:      pcFieldTypeProps,
		},
		{
			key: "doc", level: "field", host: pcFieldHost(`"int"`),
			route: routeDrop, validStructural: true,
			valid: `"d"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return n.Fields[0].Doc != "" },
			props:      pcFieldProps,
		},
	}
}

// pcFieldHostNoName spells the field WITHOUT its own name key, so a cell can
// write the "name" attribute itself.
func pcFieldHostNoName(typeSrc string) func(string) string {
	return func(attr string) string {
		return fmt.Sprintf(`{"type":"record","name":"PHost","fields":[{"type":%s%s}]}`, typeSrc, attr)
	}
}

// bodyClass is how a matrix cell writes the attribute.
type bodyClass int

const (
	bodyAbsent bodyClass = iota
	bodyValid
	bodyNull
	bodyWrong  // a body of the wrong JSON type, never null
	bodyQuoted // the valid body written as a JSON string
)

func (b bodyClass) String() string {
	switch b {
	case bodyAbsent:
		return "absent"
	case bodyValid:
		return "valid"
	case bodyNull:
		return "null"
	case bodyWrong:
		return "wrong"
	}
	return "quoted"
}

// body renders the attribute clause for one body class, and reports whether
// the class applies to this cell at all.
func (c presenceCell) body(b bodyClass) (string, bool) {
	switch b {
	case bodyAbsent:
		return "", true
	case bodyValid:
		return fmt.Sprintf(`,%q:%s`, c.key, c.valid), true
	case bodyNull:
		return fmt.Sprintf(`,%q:null`, c.key), true
	case bodyWrong:
		return fmt.Sprintf(`,%q:%s`, c.key, c.wrong), true
	case bodyQuoted:
		if c.quoted == "" {
			return "", false
		}
		return fmt.Sprintf(`,%q:%s`, c.key, c.quoted), true
	}
	return "", false
}

// presenceOutcome is what one cell produced, read off every surface. Every
// field is a comparable scalar so two outcomes can be compared directly; the
// Props value is carried as its JSON rendering so an array or object body
// does not make the struct uncomparable.
type presenceOutcome struct {
	rejected   bool
	structural bool
	inProps    bool
	propJSON   string
	canonical  string
	wire       string
}

func (o presenceOutcome) String() string {
	if o.rejected {
		return "REJECT"
	}
	return fmt.Sprintf("structural=%v props=%v(%s) canonical=%s wire=%s",
		o.structural, o.inProps, o.propJSON, o.canonical, o.wire)
}

// presenceRouting is the outcome WITHOUT the attribute's value: which surface
// the body reached, and what the schema compiled to. Two bodies of different
// JSON types must route identically while still riding through as-written, so
// the value is what distinguishes them and the routing is what must match.
type presenceRouting struct {
	rejected   bool
	structural bool
	inProps    bool
	canonical  string
	wire       string
}

func (o presenceOutcome) routing() presenceRouting {
	return presenceRouting{
		rejected:   o.rejected,
		structural: o.structural,
		inProps:    o.inProps,
		canonical:  o.canonical,
		wire:       o.wire,
	}
}

// run parses one cell and reads every surface. The wire column encodes a value
// against the schema when it can, so a routing change that altered the
// compiled codec shows up as a different (or newly failing) encoding rather
// than only as different metadata.
func (c presenceCell) run(t *testing.T, b bodyClass) (presenceOutcome, string) {
	t.Helper()
	attr, ok := c.body(b)
	if !ok {
		return presenceOutcome{}, ""
	}
	src := c.host(attr)
	s, err := avro.Parse(src)
	if err != nil {
		return presenceOutcome{rejected: true}, src
	}
	n := s.Root()
	props := c.props(n)
	v, in := props[c.key]
	raw, merr := json.Marshal(v)
	if merr != nil {
		raw = []byte(fmt.Sprintf("<unmarshalable %T>", v))
	}
	out := presenceOutcome{
		structural: c.structural(n),
		inProps:    in,
		propJSON:   string(raw),
		canonical:  string(s.Canonical()),
	}
	if enc, err := s.AppendEncode(nil, map[string]any{n.Fields[0].Name: presenceWireValue(n.Fields[0].Type)}); err != nil {
		out.wire = "err"
	} else {
		out.wire = fmt.Sprintf("%x", enc)
	}
	return out, src
}

// presenceWireValue produces a value the compiled field type accepts, chosen
// off the metadata so it follows whatever the cell's routing produced. A
// decimal is deliberately not fed a value: its error text is the observable.
func presenceWireValue(n avro.SchemaNode) any {
	switch n.Type {
	case "fixed":
		return make([]byte, n.Size)
	case "enum":
		if len(n.Symbols) > 0 {
			return n.Symbols[0]
		}
		return ""
	case "array":
		return []any{}
	case "map":
		return map[string]any{}
	case "record", "error":
		m := make(map[string]any, len(n.Fields))
		for _, f := range n.Fields {
			m[f.Name] = presenceWireValue(f.Type)
		}
		return m
	case "bytes", "string":
		if n.LogicalType != "" {
			return nil
		}
		return []byte{}
	}
	return 0
}

// TestMatrix_ReservedKeyBodyPresence drives the rule over the whole
// cross-product: a null body reaches the same verdict as a wrong-typed body,
// on every surface, for every reserved key with a typed destination.
func TestMatrix_ReservedKeyBodyPresence(t *testing.T) {
	for _, c := range presenceCells() {
		t.Run(c.name(), func(t *testing.T) {
			nullOut, nullSrc := c.run(t, bodyNull)
			wrongOut, wrongSrc := c.run(t, bodyWrong)
			validOut, validSrc := c.run(t, bodyValid)
			absentOut, absentSrc := c.run(t, bodyAbsent)

			// The rule: same ROUTING. The value each body rides through with
			// is necessarily different — that is the as-written contract —
			// so it is asserted separately below and excluded here.
			if nullOut.routing() != wrongOut.routing() {
				t.Errorf("a null body reached a different verdict than a wrong-typed body:\n"+
					"  null  %s\n        %s\n  wrong %s\n        %s",
					nullOut, nullSrc, wrongOut, wrongSrc)
			}
			if !nullOut.rejected && nullOut.inProps && nullOut.propJSON == wrongOut.propJSON {
				t.Errorf("both malformed bodies surfaced the same value %s; each must ride "+
					"through as-written (%s vs %s)", nullOut.propJSON, nullSrc, wrongSrc)
			}

			// Non-vacuity: the readable body must reach a different verdict,
			// or the equality above would hold for an implementation that
			// treated every body alike.
			if validOut.rejected != c.validRejects {
				t.Fatalf("the readable body was %s, want the opposite; this cell tests nothing as written: %s",
					map[bool]string{true: "rejected", false: "accepted"}[validOut.rejected], validSrc)
			}
			if nullOut == validOut {
				t.Errorf("a null body reached the SAME verdict as the valid body %s; "+
					"a body that names no value cannot mean the value: %s", c.valid, nullSrc)
			}
			if !c.validRejects && validOut.structural != c.validStructural {
				t.Errorf("a readable body landed structurally = %v, want %v (%s)",
					validOut.structural, c.validStructural, validSrc)
			}

			// The declared route.
			switch c.route {
			case routeReject:
				if !nullOut.rejected {
					t.Errorf("a bound placement accepted a body it cannot read: %s (%s)", nullOut, nullSrc)
				}
			case routeProps:
				if nullOut.rejected {
					t.Errorf("an unbound placement rejected a body it never reads; Props is its "+
						"only surface (%s)", nullSrc)
				}
				if !nullOut.inProps {
					t.Errorf("an unreadable body reached neither surface: %s (%s)", nullOut, nullSrc)
				}
				if nullOut.propJSON != "null" {
					t.Errorf("Props[%q] = %s, want the as-written null (%s)", c.key, nullOut.propJSON, nullSrc)
				}
				if nullOut.structural {
					t.Errorf("an unreadable body still set a structural field: %s (%s)", nullOut, nullSrc)
				}
			case routeDrop:
				if nullOut.rejected || nullOut.inProps || nullOut.structural {
					t.Errorf("the documented declining read must reach NEITHER surface: %s (%s)",
						nullOut, nullSrc)
				}
			}

			// Absence is its own verdict. Where the placement reads the key,
			// a written null must not be indistinguishable from having
			// written nothing — that confusion is the whole point of the
			// rule. The one exception is the declining read, whose ruling IS
			// that the two coincide, so it is stated rather than silent.
			if c.route != routeDrop && !absentOut.rejected && absentOut == nullOut {
				t.Errorf("a written null is indistinguishable from an absent attribute: %s (%s vs %s)",
					nullOut, nullSrc, absentSrc)
			}
			if c.route == routeDrop && !absentOut.rejected && absentOut != nullOut {
				t.Errorf("the declining read is documented to leave no trace, so it must match the "+
					"absent verdict:\n  null   %s\n  absent %s", nullOut, absentOut)
			}

			// Quoting is a spelling of a value, so wherever it applies the
			// verdict must be the key's own policy and never the null one.
			if quotedOut, quotedSrc := c.run(t, bodyQuoted); quotedSrc != "" {
				if !quotedOut.rejected && quotedOut == nullOut {
					t.Errorf("a quoted body collapsed into the null verdict: %s (%s)", quotedOut, quotedSrc)
				}
			}
		})
	}
}

// TestMatrix_ReservedKeyBodyPresenceIsNotVacuous fails when the corpus stops
// spanning the axes the rule distinguishes, so an implementation that ignored
// one of them could not pass by never being asked.
func TestMatrix_ReservedKeyBodyPresenceIsNotVacuous(t *testing.T) {
	routes := map[malformedRoute]int{}
	levels := map[string]int{}
	dests := map[string]int{}
	keys := map[string]int{}
	quoted := 0
	for _, c := range presenceCells() {
		routes[c.route]++
		levels[c.level]++
		keys[c.key]++
		if c.quoted != "" {
			quoted++
		}
		switch c.key {
		case "size", "precision", "scale":
			dests["int"]++
		case "name", "namespace", "doc", "logicalType", "order":
			dests["string"]++
		case "aliases", "symbols":
			dests["[]string"]++
		case "items", "values", "fields":
			dests["schema"]++
		}
	}
	if routes[routeReject] < 8 || routes[routeProps] < 6 {
		t.Fatalf("the routing split is not exercised: reject=%d props=%d", routes[routeReject], routes[routeProps])
	}
	// The declining read is documented as exactly one key. More cells than
	// that would mean the exception had quietly become a second rule.
	dropKeys := map[string]bool{}
	for _, c := range presenceCells() {
		if c.route == routeDrop {
			dropKeys[c.key] = true
		}
	}
	if len(dropKeys) != 1 || !dropKeys["doc"] {
		t.Fatalf("the declining read is meant to be exactly one key (%q); got %v", "doc", dropKeys)
	}
	if routes[routeDrop] < 2 {
		t.Fatalf("the declining read must be exercised at both levels, got %d cells", routes[routeDrop])
	}
	if levels["type"] < 15 || levels["field"] < 5 {
		t.Fatalf("the level axis is not exercised: type=%d field=%d", levels["type"], levels["field"])
	}
	if quoted < 6 {
		t.Fatalf("the quoted-body class is barely exercised: %d cells", quoted)
	}
	// Every typed destination the parser decodes into must appear: the
	// hazard belongs to the DESTINATION's decode, not to the key.
	for _, d := range []string{"int", "string", "[]string", "schema"} {
		if dests[d] == 0 {
			t.Fatalf("no cell decodes into a %s destination; the hazard is per-destination", d)
		}
	}
	if len(keys) < 10 {
		t.Fatalf("the corpus covers only %d reserved keys", len(keys))
	}
}

// TestDifferentialFastavroNullBodyIsNotUsable records the reference
// calibration this posture rests on: fastavro ACCEPTS a null-bodied size or
// decimal scale at parse and then fails every write against the result, so its
// acceptance is not an acceptance the permissive lean can follow.
func TestDifferentialFastavroNullBodyIsNotUsable(t *testing.T) {
	o := startOracle(t)
	cases := []struct {
		name string
		// nullSchema carries the null body; twinSchema is the same schema
		// with a written value in its place. The twin runs the IDENTICAL
		// value plumbing, so a write that fails for both would be a broken
		// probe rather than evidence about the null.
		nullSchema string
		twinSchema string
		value      json.RawMessage
		kind       string
	}{
		{
			name:       "fixed-null-size",
			nullSchema: `{"type":"fixed","name":"FNull","size":null}`,
			twinSchema: `{"type":"fixed","name":"FNull","size":0}`,
			value:      json.RawMessage(`""`), // base64 of zero bytes
			kind:       "fixed",
		},
		{
			name:       "decimal-null-scale",
			nullSchema: `{"type":"bytes","logicalType":"decimal","precision":4,"scale":null}`,
			twinSchema: `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
			value:      json.RawMessage(`"1.23"`),
			kind:       "decimal",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			twin := o.call(oracleJob{Op: "encode", Schema: json.RawMessage(c.twinSchema),
				Value: c.value, Kind: c.kind})
			if !twin.OK {
				t.Fatalf("fastavro could not write the written-value twin either (%s); the probe "+
					"proves nothing about the null body", twin.Err)
			}

			parsed := o.call(oracleJob{Op: "parsedump", Schema: json.RawMessage(c.nullSchema)})
			if !parsed.OK {
				t.Logf("fastavro now rejects %s at parse (%s), which agrees with this package "+
					"more closely than the recorded calibration", c.name, parsed.Err)
			} else if enc := o.call(oracleJob{Op: "encode", Schema: json.RawMessage(c.nullSchema),
				Value: c.value, Kind: c.kind}); enc.OK {
				t.Errorf("fastavro now WRITES against a null-bodied attribute (hex %q); its accept "+
					"has become a usable accept, which is new evidence for the permissive lean "+
					"this package declined", enc.Hex)
			}

			// twmb rejects at parse, which is the difference that matters:
			// the caller learns before it holds a schema, not at the first
			// write.
			if _, err := avro.Parse(c.nullSchema); err == nil {
				t.Errorf("twmb accepted %s at parse", c.name)
			}
			if _, err := avro.Parse(c.twinSchema); err != nil {
				t.Errorf("twmb rejected the written-value twin: %v", err)
			}
		})
	}
}
