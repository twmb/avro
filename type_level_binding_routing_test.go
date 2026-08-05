package avro_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// "default" and "order" at the TYPE level.
//
// Both keys are FIELD attributes: a record field binds each of them, and the
// Avro spec's only type-level binding of either is the enum evolution default.
// A type object of any other kind binds neither, so neither has a structural
// field to land on — and the routing rule the reserved-key rulings share is a
// biconditional: the structural field is set IFF the key was consumed, and
// Props holds exactly the raw keys that were not. Never both, never neither.
//
// Both references agree on preserving them where the kind does not bind:
//
//   - Java's SCHEMA_RESERVED (Schema.java:175-176) is
//     {doc, fields, items, name, namespace, size, symbols, values, type,
//     aliases} — it omits BOTH keys, so parsePropertiesAndLogicalType keeps
//     each as a schema PROPERTY on primitives (:1857), records (:1880),
//     arrays (:1940), maps (:1950) and fixed (:1963). ENUM_RESERVED
//     (:178-180) is SCHEMA_RESERVED plus "default" alone, applied at :1928 —
//     so an enum consumes "default" and keeps "order" as a property.
//   - fastavro 1.12.2 keeps both keys on the parsed schema of every kind
//     (executed; TestDifferentialFastavroTypeLevelBindingRouting drives the
//     matrix through it per accepted cell).
//
// The FIELD level is the boundary this must not cross: Java's FIELD_RESERVED
// (Schema.java:503-504) is {default, doc, name, order, type, aliases}, which
// binds both, and so does twmb — a field's "default" and "order" are consumed
// into SchemaField.Default / SchemaField.Order and must never appear in
// SchemaField.Props. The matrix below crosses the binding axis precisely so
// that boundary is asserted rather than assumed.
//
// Neither key reaches the wire, the Parsing Canonical Form, or the
// fingerprint on either side of the change: PCF keeps only
// type/name/fields/symbols/items/values/size, and no codec reads an
// unconsumed attribute. Every cell asserts that identity against a twin
// spelled without the key.
// ---------------------------------------------------------------------------

// bindingLevel is where a cell writes the attribute.
type bindingLevel int

const (
	levelType  bindingLevel = iota // on the type object
	levelField                     // on the enclosing record field object
)

func (l bindingLevel) String() string {
	if l == levelType {
		return "type"
	}
	return "field"
}

// bindingCell is one {key} x {kind} x {level} cell of the matrix.
type bindingCell struct {
	key   string
	kind  string
	level bindingLevel
}

// binds states, from the RULINGS rather than from the code, whether the
// placement consumes the attribute into a structural field. A cell that binds
// must keep the key out of Props; a cell that does not must put it there.
func (c bindingCell) binds() bool {
	if c.level == levelField {
		return true // FIELD_RESERVED binds both, on every field
	}
	return c.key == "default" && c.kind == "enum" // the enum evolution default
}

// body is the attribute's JSON value: a legal sort order for "order", and for
// "default" a value the kind can actually take, so a BINDING cell reaches the
// consumer's validation instead of failing for an unrelated reason.
func (c bindingCell) body() string {
	if c.key == "order" {
		return `"ignore"`
	}
	switch c.kind {
	case "null":
		return `null`
	case "boolean":
		return `true`
	case "int", "long":
		return `3`
	case "float", "double":
		return `1.5`
	case "string":
		return `"s"`
	case "bytes":
		return `"AB"`
	case "fixed":
		return `"AAAA"` // len == the size spelled by bindingTypeSchema
	case "enum":
		return `"A"`
	case "record", "error":
		return `{"x":0}`
	case "map":
		return `{}`
	case "array":
		return `[]`
	case "union":
		return `null`
	}
	return `null`
}

// bindingTypeSchema spells the kind as a type object, optionally carrying the
// attribute. Fixed is size 4 so the "fixed" default body above is a legal
// 4-codepoint string.
func bindingTypeSchema(kind, attr string) string {
	switch kind {
	case "fixed":
		return fmt.Sprintf(`{"type":"fixed","name":"BF","size":4%s}`, attr)
	case "enum":
		return fmt.Sprintf(`{"type":"enum","name":"BE","symbols":["A","B"]%s}`, attr)
	case "record":
		return fmt.Sprintf(`{"type":"record","name":"BR","fields":[{"name":"x","type":"int"}]%s}`, attr)
	case "error":
		return fmt.Sprintf(`{"type":"error","name":"BErr","fields":[{"name":"x","type":"int"}]%s}`, attr)
	case "array":
		return fmt.Sprintf(`{"type":"array","items":"int"%s}`, attr)
	case "map":
		return fmt.Sprintf(`{"type":"map","values":"int"%s}`, attr)
	default:
		return fmt.Sprintf(`{"type":%q%s}`, kind, attr)
	}
}

// bindingValue is a wire-encodable value for the kind, matched to
// bindingTypeSchema's spelling (fixed is 4 bytes here, not the census's 8).
func bindingValue(kind string) any {
	if kind == "fixed" {
		return []byte{1, 2, 3, 4}
	}
	return censusValue(kind)
}

// schema returns the cell's schema source, with the attribute present or
// absent. Every cell is wrapped in a host record so the type-level and
// field-level spellings differ only in WHICH object carries the key.
func (c bindingCell) schema(withAttr bool) string {
	typeAttr, fieldAttr := "", ""
	if withAttr {
		if c.level == levelType {
			typeAttr = fmt.Sprintf(`,%q:%s`, c.key, c.body())
		} else {
			fieldAttr = fmt.Sprintf(`,%q:%s`, c.key, c.body())
		}
	}
	return fmt.Sprintf(`{"type":"record","name":"BindHost","fields":[{"name":"a","type":%s%s}]}`,
		bindingTypeSchema(c.kind, typeAttr), fieldAttr)
}

func (c bindingCell) name() string {
	return c.level.String() + "/" + c.key + "/" + c.kind
}

func bindingCells() []bindingCell {
	var cells []bindingCell
	for _, key := range []string{"default", "order"} {
		for _, kind := range censusKinds {
			for _, level := range []bindingLevel{levelType, levelField} {
				cells = append(cells, bindingCell{key: key, kind: kind, level: level})
			}
		}
	}
	return cells
}

// bindingStructuralSurface reads the structural field the attribute lands on
// when it IS consumed, so "consumed" is measured off the metadata API rather
// than inferred from the routing predicate the test is checking.
func bindingStructuralSurface(f avro.SchemaField, c bindingCell) bool {
	switch {
	case c.level == levelField && c.key == "default":
		return f.HasDefault
	case c.level == levelField && c.key == "order":
		return f.Order != ""
	case c.key == "default":
		return f.Type.HasEnumDefault
	default:
		// No type-level kind binds "order": there is no structural field
		// for it to reach, which is exactly why it must ride to Props.
		return false
	}
}

// bindingProps reads the Props map of the object that carried the attribute.
func bindingProps(f avro.SchemaField, c bindingCell) map[string]any {
	if c.level == levelField {
		return f.Props
	}
	return f.Type.Props
}

// TestMatrix_TypeLevelBindingRouting crosses {default, order} x every kind x
// {type object, record field} and asserts the biconditional on both surfaces,
// plus the inertness of the attribute on every representation that is not the
// metadata tree.
func TestMatrix_TypeLevelBindingRouting(t *testing.T) {
	for _, c := range bindingCells() {
		t.Run(c.name(), func(t *testing.T) {
			src := c.schema(true)
			s, err := avro.Parse(src)
			if err != nil {
				t.Fatalf("Parse(%s): %v", src, err)
			}
			f := s.Root().Fields[0]

			structural := bindingStructuralSurface(f, c)
			props := bindingProps(f, c)
			_, inProps := props[c.key]

			// The biconditional: consumed IFF structural, and Props is
			// exactly the raw keys that were not consumed.
			if structural != c.binds() {
				t.Errorf("structural field set = %v, want %v (schema %s)", structural, c.binds(), src)
			}
			if inProps == c.binds() {
				t.Errorf("key in Props = %v, want %v (schema %s, props %#v)", inProps, !c.binds(), src, props)
			}
			if structural && inProps {
				t.Errorf("%q surfaced BOTH structurally and in Props; the routing picks exactly one", c.key)
			}
			if !structural && !inProps {
				t.Errorf("%q surfaced on NEITHER surface — the as-written attribute was dropped (schema %s)", c.key, src)
			}
			// The unconsumed value rides verbatim, not coerced.
			if !c.binds() {
				var want any
				if err := json.Unmarshal([]byte(c.body()), &want); err != nil {
					t.Fatalf("cell body: %v", err)
				}
				if got := props[c.key]; !bindingValueEqual(got, want) {
					t.Errorf("Props[%q] = %#v, want the as-written %#v", c.key, got, want)
				}
			}
			// The other object must stay clean: a type-level key must not
			// leak onto the field, and a field-level key must not leak onto
			// the type.
			other := f.Props
			if c.level == levelField {
				other = f.Type.Props
			}
			if _, leaked := other[c.key]; leaked {
				t.Errorf("%q leaked onto the other object's Props: %#v", c.key, other)
			}

			assertBindingInert(t, s, c)
		})
	}
}

// bindingValueEqual compares a Props value against the JSON literal it was
// written as. Integer literals come back as int64 from the precision-
// preserving decode where a plain json.Unmarshal produces float64, at every
// depth, so both sides are numerically normalized before comparison rather
// than pinning one Go type per nesting level.
func bindingValueEqual(got, want any) bool {
	return reflect.DeepEqual(bindingNumNorm(got), bindingNumNorm(want))
}

func bindingNumNorm(v any) any {
	switch t := v.(type) {
	case int64:
		return float64(t)
	case int:
		return float64(t)
	case json.Number:
		f, err := t.Float64()
		if err != nil {
			return t.String()
		}
		return f
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = bindingNumNorm(e)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = bindingNumNorm(e)
		}
		return out
	}
	return v
}

// assertBindingInert holds the attribute to the metadata tree: the wire bytes,
// the Avro-JSON encoding, the canonical form and the fingerprint must equal
// the twin spelled without it, and String() / Root().Schema() must stay
// canonical-stable. The comparisons are made non-vacuous by requiring both
// sides to be non-empty for the surfaces that can be.
func assertBindingInert(t *testing.T, s *avro.Schema, c bindingCell) {
	t.Helper()
	twinSrc := c.schema(false)
	twin, err := avro.Parse(twinSrc)
	if err != nil {
		t.Fatalf("twin Parse(%s): %v", twinSrc, err)
	}
	val := map[string]any{"a": bindingValue(c.kind)}

	enc, err := s.AppendEncode(nil, val)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	twinEnc, err := twin.AppendEncode(nil, val)
	if err != nil {
		t.Fatalf("twin AppendEncode: %v", err)
	}
	if !bytes.Equal(enc, twinEnc) {
		t.Errorf("wire diverges from the attribute-free twin:\n got %x\nwant %x", enc, twinEnc)
	}

	jenc, err := s.AppendEncodeJSON(nil, val)
	if err != nil {
		t.Fatalf("AppendEncodeJSON: %v", err)
	}
	twinJSON, err := twin.AppendEncodeJSON(nil, val)
	if err != nil {
		t.Fatalf("twin AppendEncodeJSON: %v", err)
	}
	if !bytes.Equal(jenc, twinJSON) {
		t.Errorf("Avro-JSON diverges from the attribute-free twin:\n got %s\nwant %s", jenc, twinJSON)
	}
	if len(jenc) == 0 {
		t.Error("Avro-JSON came back empty, so the comparison proved nothing")
	}

	canon, twinCanon := s.Canonical(), twin.Canonical()
	if len(canon) == 0 || len(twinCanon) == 0 {
		t.Fatalf("canonical form came back empty (%q / %q), so the comparison proved nothing", canon, twinCanon)
	}
	if !bytes.Equal(canon, twinCanon) {
		t.Errorf("Canonical diverges from the attribute-free twin:\n got %s\nwant %s", canon, twinCanon)
	}
	fp, twinFP := s.Fingerprint(avro.NewRabin()), twin.Fingerprint(avro.NewRabin())
	if len(fp) == 0 || len(twinFP) == 0 {
		t.Fatalf("fingerprint came back empty, so the comparison proved nothing")
	}
	if !bytes.Equal(fp, twinFP) {
		t.Errorf("Rabin diverges from the attribute-free twin: got %x want %x", fp, twinFP)
	}

	// String() is the source text; its reparse must describe the same schema.
	if !strings.Contains(s.String(), c.key) {
		t.Errorf("String() lost the as-written attribute %q: %s", c.key, s.String())
	}
	s2, err := avro.Parse(s.String())
	if err != nil {
		t.Fatalf("String() reparse: %v", err)
	}
	if !bytes.Equal(s2.Canonical(), canon) {
		t.Errorf("String() reparse changed Canonical: %s -> %s", canon, s2.Canonical())
	}

	// Root().Schema() rebuilds from the metadata tree, so it is the surface
	// that loses an attribute the tree never captured. It must round-trip
	// the attribute to the same place the original parse put it.
	root := s.Root()
	rb, err := root.Schema()
	if err != nil {
		t.Fatalf("Root().Schema(): %v", err)
	}
	if !bytes.Equal(rb.Canonical(), canon) {
		t.Errorf("rebuild changed Canonical: %s -> %s", canon, rb.Canonical())
	}
	rf := rb.Root().Fields[0]
	if got := bindingStructuralSurface(rf, c); got != c.binds() {
		t.Errorf("rebuild changed the structural verdict for %q: %v -> %v (%s)", c.key, c.binds(), got, rb)
	}
	if _, got := bindingProps(rf, c)[c.key]; got == c.binds() {
		t.Errorf("rebuild changed the Props verdict for %q: %s", c.key, rb)
	}
}

// TestMatrix_TypeLevelBindingRoutingIsNotVacuous fails when the corpus stops
// spanning the axes the rule distinguishes, so a routing that ignored one of
// them could not pass by never being asked.
func TestMatrix_TypeLevelBindingRoutingIsNotVacuous(t *testing.T) {
	var binding, nonBinding, typeLevel, fieldLevel int
	keys := map[string]int{}
	kinds := map[string]int{}
	for _, c := range bindingCells() {
		if c.binds() {
			binding++
		} else {
			nonBinding++
		}
		if c.level == levelType {
			typeLevel++
		} else {
			fieldLevel++
		}
		keys[c.key]++
		kinds[c.kind]++
	}
	if binding < 15 || nonBinding < 15 {
		t.Errorf("corpus misses a binding verdict: binding=%d nonBinding=%d", binding, nonBinding)
	}
	if typeLevel < 14 || fieldLevel < 14 {
		t.Errorf("corpus misses a level: type=%d field=%d", typeLevel, fieldLevel)
	}
	if len(keys) != 2 || len(kinds) != len(censusKinds) {
		t.Errorf("corpus misses a key or a kind: keys=%v kinds=%d", keys, len(kinds))
	}
	// The one type-level BINDING cell is the enum default; without it the
	// matrix would never exercise a type-level consumer at all.
	var enumDefault bool
	for _, c := range bindingCells() {
		if c.level == levelType && c.binds() {
			enumDefault = c.key == "default" && c.kind == "enum"
		}
	}
	if !enumDefault {
		t.Error("corpus has no type-level BINDING cell; the enum evolution default is the only one and must be present")
	}
}

// TestRegression_TypeLevelDefaultOrderSurviveTheRebuild pins the specific
// as-written loss the routing rule closes: a type-level attribute the kind
// does not bind has Props as its ONLY metadata surface, so a tree that drops
// it makes Root().Schema() describe a different schema than the input.
//
// Recursive and diamond type graphs are included because a named type's
// SECOND occurrence is a reference rather than a definition, and a reference
// carrying the attribute reaches the metadata splice rather than the plain
// object emitter.
func TestRegression_TypeLevelDefaultOrderSurviveTheRebuild(t *testing.T) {
	cases := []struct {
		name string
		src  string
		// node picks the node whose Props must carry the attribute.
		node func(avro.SchemaNode) avro.SchemaNode
		key  string
		val  any
	}{
		{
			name: "order-on-primitive",
			src:  `{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"int","order":"ignore"}}]}`,
			node: func(r avro.SchemaNode) avro.SchemaNode { return r.Fields[0].Type },
			key:  "order", val: "ignore",
		},
		{
			name: "default-on-primitive",
			src:  `{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"int","default":3}}]}`,
			node: func(r avro.SchemaNode) avro.SchemaNode { return r.Fields[0].Type },
			key:  "default", val: int64(3),
		},
		{
			name: "order-on-enum-which-binds-only-default",
			src:  `{"type":"enum","name":"E","symbols":["A","B"],"default":"A","order":"ignore"}`,
			node: func(r avro.SchemaNode) avro.SchemaNode { return r },
			key:  "order", val: "ignore",
		},
		{
			name: "order-on-array",
			src:  `{"type":"array","items":"int","order":"ignore"}`,
			node: func(r avro.SchemaNode) avro.SchemaNode { return r },
			key:  "order", val: "ignore",
		},
		{
			// A recursive definition: the attribute sits on the DEFINITION,
			// which the rebuild emits once and references thereafter.
			name: "order-on-recursive-definition",
			src: `{"type":"record","name":"Node","order":"ignore","fields":[
				{"name":"next","type":["null","Node"]}]}`,
			node: func(r avro.SchemaNode) avro.SchemaNode { return r },
			key:  "order", val: "ignore",
		},
		{
			// A diamond: two fields reach the same named type, the second
			// through a wrapped reference carrying the attribute. The
			// wrapper's props merge onto the spliced definition, so the
			// attribute must survive on the rebuilt tree.
			name: "order-on-diamond-reference",
			src: `{"type":"record","name":"Top","fields":[
				{"name":"l","type":{"type":"record","name":"D","fields":[{"name":"v","type":"int"}]}},
				{"name":"r","type":{"type":"D","order":"ignore"}}]}`,
			node: func(r avro.SchemaNode) avro.SchemaNode { return r.Fields[1].Type },
			key:  "order", val: "ignore",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := avro.Parse(c.src)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			n := c.node(*s.Root())
			got, ok := n.Props[c.key]
			if !ok {
				t.Fatalf("%q is absent from the node's only metadata surface: Props=%#v", c.key, n.Props)
			}
			if !reflect.DeepEqual(got, c.val) {
				t.Errorf("Props[%q] = %#v (%T), want %#v", c.key, got, got, c.val)
			}
			root := s.Root()
			rb, err := root.Schema()
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if !strings.Contains(rb.String(), `"`+c.key+`"`) {
				t.Errorf("the rebuild dropped the as-written %q: %s", c.key, rb)
			}
			if !bytes.Equal(rb.Canonical(), s.Canonical()) {
				t.Errorf("rebuild changed Canonical: %s -> %s", s.Canonical(), rb.Canonical())
			}
		})
	}
}

// TestRegression_FieldLevelDefaultOrderStayConsumed is the boundary control:
// a record field binds both attributes, so they are consumed into
// SchemaField.Default / SchemaField.Order and must never reach
// SchemaField.Props — matching Java's FIELD_RESERVED (Schema.java:503-504,
// {default, doc, name, order, type, aliases}). Without this the type-level
// routing could be "fixed" by routing everywhere.
func TestRegression_FieldLevelDefaultOrderStayConsumed(t *testing.T) {
	s, err := avro.Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int","default":3,"order":"descending"}]}`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	f := s.Root().Fields[0]
	if !f.HasDefault || f.Default != int32(3) {
		t.Errorf("field default not consumed: HasDefault=%v Default=%#v", f.HasDefault, f.Default)
	}
	if f.Order != "descending" {
		t.Errorf("field order not consumed: %q", f.Order)
	}
	if len(f.Props) != 0 {
		t.Errorf("a field-bound reserved key reached SchemaField.Props: %#v", f.Props)
	}
	if len(f.Type.Props) != 0 {
		t.Errorf("a field attribute leaked onto the field's type node: %#v", f.Type.Props)
	}
}

// ---------------------------------------------------------------------------
// "doc" is the one reserved key that BINDS on every kind while its capture
// silently declines a non-string body — so a non-string doc reaches neither
// structural field nor Props, and is the single documented exception to the
// "never neither" half of the reserved-key routing rule.
//
// This is exact Apache Avro (Java) behavior, and the two tests below pin both
// directions of it so neither can drift alone:
//
//   - Java reads doc ONLY through parseDoc (Schema.java:1996-1998, called at
//     :1864 for records, :1912 for enums, :1956 for fixed) and, for a field,
//     at :1888. Both are getOptionalText (:2039-2042), which is
//     jsonNode.textValue() — null for ANY non-text node, including an
//     explicit JSON null.
//   - doc is then a member of SCHEMA_RESERVED (:176) and of FIELD_RESERVED
//     (:504), so parseProperties (:1982-1988) skips it at every call site.
//
// Those two facts together mean Java keeps a non-string doc nowhere, and the
// same membership fact is what makes a non-string logicalType behave the
// OPPOSITE way: logicalType is absent from SCHEMA_RESERVED, so Java's
// parseProperties keeps it as an ordinary schema property. One reserved-set
// membership test predicts both routings, which is why the two must not be
// "made consistent" with each other.
//
// fastavro 1.12.2 preserves a non-string doc verbatim at both levels; the
// references disagree and this package follows Java. Nothing observable on
// the wire depends on it: the canonical form and the fingerprint never carry
// doc, which each case asserts against a doc-free twin.
// ---------------------------------------------------------------------------

// docBodiesNonString spans the JSON token classes a doc can be written as
// while not being a string. An explicit null is included because it is the
// one shape where a lenient reader could plausibly treat the key as absent
// rather than as present-and-unusable, and Java's textValue() maps both to
// the same null.
var docBodiesNonString = []string{`5`, `[]`, `null`, `{"a":1}`, `true`}

func TestRegression_NonStringDocDroppedAtBothLevels(t *testing.T) {
	for _, body := range docBodiesNonString {
		t.Run("type-level/"+body, func(t *testing.T) {
			s, err := avro.Parse(`{"type":"int","doc":` + body + `}`)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			n := s.Root()
			if n.Doc != "" {
				t.Errorf("Doc = %q, want empty: a non-text body cannot become documentation", n.Doc)
			}
			if _, ok := n.Props["doc"]; ok {
				t.Errorf(`"doc" reached Props: %#v — the key is bound on every kind, so Props is not its surface`, n.Props)
			}
			rb, err := n.Schema()
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if strings.Contains(rb.String(), `"doc"`) {
				t.Errorf("the rebuild emitted a doc that never landed: %s", rb)
			}
			twin := avro.MustParse(`{"type":"int"}`)
			if !bytes.Equal(s.Canonical(), twin.Canonical()) {
				t.Errorf("canonical form differs from the doc-free twin: %s vs %s", s.Canonical(), twin.Canonical())
			}
			if !bytes.Equal(s.Fingerprint(avro.NewRabin()), twin.Fingerprint(avro.NewRabin())) {
				t.Errorf("fingerprint differs from the doc-free twin: %x vs %x",
					s.Fingerprint(avro.NewRabin()), twin.Fingerprint(avro.NewRabin()))
			}
		})

		t.Run("field-level/"+body, func(t *testing.T) {
			s, err := avro.Parse(`{"type":"record","name":"R","fields":[
				{"name":"f","type":"int","doc":` + body + `}]}`)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			f := s.Root().Fields[0]
			if f.Doc != "" {
				t.Errorf("SchemaField.Doc = %q, want empty", f.Doc)
			}
			if _, ok := f.Props["doc"]; ok {
				t.Errorf(`"doc" reached SchemaField.Props: %#v — FIELD_RESERVED binds it, so Props is not its surface`, f.Props)
			}
			hostRoot := s.Root()
			rb, err := hostRoot.Schema()
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if strings.Contains(rb.String(), `"doc"`) {
				t.Errorf("the rebuild emitted a field doc that never landed: %s", rb)
			}
			twin := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":"int"}]}`)
			if !bytes.Equal(s.Canonical(), twin.Canonical()) {
				t.Errorf("canonical form differs from the doc-free twin: %s vs %s", s.Canonical(), twin.Canonical())
			}
		})
	}
}

// TestRegression_StringDocConsumedAndSurfaced is the other direction, and the
// control that keeps the drop above from being "fixed" by never reading doc at
// all: with a string body the key is consumed into the structural field on
// every level and kind that has one, stays out of Props, and survives the
// metadata rebuild.
//
// The body is deliberately non-empty. SchemaNode.Doc has no present/absent
// companion, so an EMPTY doc string is indistinguishable from an absent one
// on the structural field — a separate question about the zero value of a
// string field, not about the token type this pair of tests fixes.
func TestRegression_StringDocConsumedAndSurfaced(t *testing.T) {
	cases := []struct {
		name string
		src  string
		// doc reads the documentation off the surface that should hold it.
		doc func(avro.SchemaNode) string
	}{
		{
			name: "primitive-type-object",
			src:  `{"type":"int","doc":"d"}`,
			doc:  func(n avro.SchemaNode) string { return n.Doc },
		},
		{
			name: "record",
			src:  `{"type":"record","name":"R","doc":"d","fields":[]}`,
			doc:  func(n avro.SchemaNode) string { return n.Doc },
		},
		{
			name: "enum",
			src:  `{"type":"enum","name":"E","doc":"d","symbols":["A"]}`,
			doc:  func(n avro.SchemaNode) string { return n.Doc },
		},
		{
			name: "fixed",
			src:  `{"type":"fixed","name":"F","doc":"d","size":2}`,
			doc:  func(n avro.SchemaNode) string { return n.Doc },
		},
		{
			name: "array",
			src:  `{"type":"array","items":"int","doc":"d"}`,
			doc:  func(n avro.SchemaNode) string { return n.Doc },
		},
		{
			name: "map",
			src:  `{"type":"map","values":"int","doc":"d"}`,
			doc:  func(n avro.SchemaNode) string { return n.Doc },
		},
		{
			name: "record-field",
			src:  `{"type":"record","name":"R","fields":[{"name":"f","type":"int","doc":"d"}]}`,
			doc:  func(n avro.SchemaNode) string { return n.Fields[0].Doc },
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := avro.Parse(c.src)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			root := s.Root()
			if got := c.doc(*root); got != "d" {
				t.Errorf("doc = %q, want %q — a string body must be consumed into the structural field", got, "d")
			}
			if _, ok := root.Props["doc"]; ok {
				t.Errorf(`a consumed "doc" also reached Props: %#v`, root.Props)
			}
			if !strings.Contains(s.String(), `"doc"`) {
				t.Errorf("String() dropped the doc: %s", s)
			}
			rb, err := root.Schema()
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if !strings.Contains(rb.String(), `"doc":"d"`) {
				t.Errorf("the rebuild dropped the consumed doc: %s", rb)
			}
			// Re-parsing the rebuild must reach the same surface, or the
			// emitter and the reader disagree about where doc lives.
			back, err := avro.Parse(rb.String())
			if err != nil {
				t.Fatalf("re-parse of the rebuild: %v", err)
			}
			if got := c.doc(*back.Root()); got != "d" {
				t.Errorf("doc did not round-trip through the rebuild: %q", got)
			}
		})
	}
}

// TestDifferentialFastavroTypeLevelBindingRouting executes every accepted cell
// against fastavro and asserts the PRESERVATION claim rather than mere
// acceptance: fastavro 1.12.2 keeps both keys on the parsed schema of every
// kind, which is what makes routing them to Props the cross-implementation
// answer rather than a twmb invention. A release that starts dropping them
// flips these cells loudly.
func TestDifferentialFastavroTypeLevelBindingRouting(t *testing.T) {
	o := startOracle(t)
	for _, c := range bindingCells() {
		if c.level != levelType {
			continue // fastavro's parsed FIELD objects are not top-level dicts
		}
		t.Run(c.name(), func(t *testing.T) {
			src := bindingTypeSchema(c.kind, fmt.Sprintf(`,%q:%s`, c.key, c.body()))
			resp := o.call(oracleJob{Op: "parsedump", Schema: json.RawMessage(src)})
			if !resp.OK {
				t.Fatalf("fastavro rejected a schema twmb accepts: %s\n%s", resp.Err, src)
			}
			var parsed map[string]any
			if err := json.Unmarshal([]byte(resp.Parsed), &parsed); err != nil {
				t.Fatalf("oracle parsedump did not return an object: %v (%s)", err, resp.Parsed)
			}
			if len(parsed) == 0 {
				t.Fatalf("fastavro returned an empty parsed schema, so the comparison proved nothing: %s", resp.Parsed)
			}
			if _, ok := parsed[c.key]; !ok {
				t.Errorf("fastavro no longer preserves %q on %s — the calibration this routing rests on changed: %s",
					c.key, c.kind, resp.Parsed)
			}
		})
	}
}
