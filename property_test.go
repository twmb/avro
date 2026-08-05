package avro_test

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// equalAvro compares two decoded `any` trees with Avro-semantic equality: a
// nil slice/map equals an empty one (Avro doesn't distinguish them, and the
// binary vs JSON decoders differ only cosmetically there — e.g. empty bytes
// decode to []byte(nil) on the binary path and []byte{} on JSON). Floats are
// compared with == (the generator avoids NaN). This keeps the round-trip
// property focused on substantive value divergences.
func equalAvro(a, b any) bool {
	switch av := a.(type) {
	case nil:
		return b == nil
	case []byte:
		bv, ok := b.([]byte)
		return ok && bytes.Equal(av, bv)
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !equalAvro(av[i], bv[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			bvv, ok := bv[k]
			if !ok || !equalAvro(v, bvv) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(a, b)
	}
}

// Tier-2 property testing. A deterministic generator produces a valid schema
// tree; values are then drawn from that tree on demand. Properties assert
// invariants that hold for ALL generated (schema, value) pairs — instead of
// the author hand-picking example inputs (which miss the intersections where
// bugs live). The generator is seeded, so a failing case is reproducible:
// re-run the reported seed. See CORRECTNESS_PLAN.md §T2.

// genNode is a generated schema node: enough structure to (a) emit the schema
// JSON and (b) draw arbitrarily many matching Go values.
type genNode struct {
	kind     string
	schema   string // compact schema JSON for this node
	symbols  []string
	items    *genNode
	values   *genNode
	fields   []genField
	branches []*genNode // union (primitive branches only, in v1)
}

type genField struct {
	name string
	node *genNode
}

type schemaGen struct {
	r       *rand.Rand
	nameSeq int
}

func (g *schemaGen) name(prefix string) string {
	g.nameSeq++
	return fmt.Sprintf("%s%d", prefix, g.nameSeq)
}

func (g *schemaGen) avroName() string {
	const first = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_"
	const rest = first + "0123456789"
	n := 1 + g.r.Intn(6)
	var b strings.Builder
	b.WriteByte(first[g.r.Intn(len(first))])
	for i := 1; i < n; i++ {
		b.WriteByte(rest[g.r.Intn(len(rest))])
	}
	return b.String()
}

// primitiveKinds are union-eligible (distinct kinds keep twmb's Go-type union
// dispatch unambiguous, matching fastavro/Java).
var primitiveKinds = []string{"null", "boolean", "int", "long", "float", "double", "string", "bytes"}

func quoteJoin(ss []string) string {
	q := make([]string, len(ss))
	for i, s := range ss {
		q[i] = fmt.Sprintf("%q", s)
	}
	return strings.Join(q, ",")
}

// build returns a generated schema node with its .schema populated. depth
// bounds nesting; collections stay small to keep cases fast.
func (g *schemaGen) build(depth int) *genNode {
	if depth >= 4 || g.r.Intn(3) == 0 {
		k := primitiveKinds[g.r.Intn(len(primitiveKinds))]
		return &genNode{kind: k, schema: fmt.Sprintf("%q", k)}
	}
	switch g.r.Intn(5) {
	case 0: // enum
		nsym := 1 + g.r.Intn(4)
		syms := make([]string, 0, nsym)
		seen := map[string]bool{}
		for len(syms) < nsym {
			if s := g.avroName(); !seen[s] {
				seen[s] = true
				syms = append(syms, s)
			}
		}
		n := &genNode{kind: "enum", symbols: syms}
		n.schema = fmt.Sprintf(`{"type":"enum","name":%q,"symbols":[%s]}`, g.name("E"), quoteJoin(syms))
		return n
	case 1: // array
		items := g.build(depth + 1)
		return &genNode{kind: "array", items: items, schema: fmt.Sprintf(`{"type":"array","items":%s}`, items.schema)}
	case 2: // map
		values := g.build(depth + 1)
		return &genNode{kind: "map", values: values, schema: fmt.Sprintf(`{"type":"map","values":%s}`, values.schema)}
	case 3: // record
		nf := g.r.Intn(4)
		fields := make([]genField, 0, nf)
		fragments := make([]string, 0, nf)
		seen := map[string]bool{}
		for range nf {
			fn := g.avroName()
			for seen[fn] {
				fn = g.avroName()
			}
			seen[fn] = true
			fnode := g.build(depth + 1)
			fields = append(fields, genField{name: fn, node: fnode})
			fragments = append(fragments, fmt.Sprintf(`{"name":%q,"type":%s}`, fn, fnode.schema))
		}
		n := &genNode{kind: "record", fields: fields}
		n.schema = fmt.Sprintf(`{"type":"record","name":%q,"fields":[%s]}`, g.name("R"), strings.Join(fragments, ","))
		return n
	default: // union of distinct primitive kinds
		nb := 1 + g.r.Intn(3)
		perm := g.r.Perm(len(primitiveKinds))[:nb]
		branches := make([]*genNode, nb)
		frags := make([]string, nb)
		for i, ki := range perm {
			k := primitiveKinds[ki]
			branches[i] = &genNode{kind: k, schema: fmt.Sprintf("%q", k)}
			frags[i] = fmt.Sprintf("%q", k)
		}
		return &genNode{kind: "union", branches: branches, schema: "[" + strings.Join(frags, ",") + "]"}
	}
}

func (g *schemaGen) genString() string {
	runes := []rune("abcdefghijklmnopqrstuvwxyz0123456789 _-.ABCXYZéñ日本🎉")
	n := g.r.Intn(8)
	out := make([]rune, n)
	for i := range out {
		out[i] = runes[g.r.Intn(len(runes))]
	}
	return string(out)
}

func (g *schemaGen) finiteFloat() float64 {
	for {
		f := math.Float64frombits(g.r.Uint64())
		if !math.IsNaN(f) && !math.IsInf(f, 0) {
			return f
		}
	}
}

// value draws a fresh Go value matching n. The value types are exactly what
// Schema.Encode accepts in the dynamic (map[string]any / []any / primitive)
// representation, and exactly what Decode/DecodeJSON produce into *any.
func (g *schemaGen) value(n *genNode) any {
	switch n.kind {
	case "null":
		return nil
	case "boolean":
		return g.r.Intn(2) == 0
	case "int":
		return int32(g.r.Uint32())
	case "long":
		return int64(g.r.Uint64())
	case "float":
		return float32(g.finiteFloat())
	case "double":
		return g.finiteFloat()
	case "string":
		return g.genString()
	case "bytes":
		b := make([]byte, g.r.Intn(8))
		g.r.Read(b)
		return b
	case "enum":
		return n.symbols[g.r.Intn(len(n.symbols))]
	case "array":
		k := g.r.Intn(4)
		out := make([]any, k)
		for i := range out {
			out[i] = g.value(n.items)
		}
		return out
	case "map":
		k := g.r.Intn(4)
		out := make(map[string]any, k)
		for range k {
			out[g.avroName()] = g.value(n.values)
		}
		return out
	case "record":
		out := make(map[string]any, len(n.fields))
		for _, f := range n.fields {
			out[f.name] = g.value(f.node)
		}
		return out
	case "union":
		return g.value(n.branches[g.r.Intn(len(n.branches))])
	}
	panic("unknown kind " + n.kind)
}

// TestProperty_LogicalCustomTypeParseNeverPanics crosses every logical type
// with every underlying primitive AND a registered CustomType for that
// logical, asserting Parse never panics. A logical on a wrong underlying is
// soft-dropped, then resurrected by the matching CustomType; a resurrected
// logical that enters a built-in code path assuming the right underlying
// type can dereference a nil pointer (the F1 class). This generic matrix
// covers that cell without a bug-specific test — reverting the F1 gate makes
// it panic here.
func TestProperty_LogicalCustomTypeParseNeverPanics(t *testing.T) {
	logicals := []string{
		"decimal", "big-decimal", "uuid", "date", "time-millis", "time-micros",
		"timestamp-millis", "timestamp-micros", "timestamp-nanos", "duration", "unknownlogic",
	}
	underlyings := []string{"null", "boolean", "int", "long", "float", "double", "string", "bytes"}
	dec := func(v any, _ *avro.SchemaNode) (any, error) { return v, nil }
	for _, lt := range logicals {
		for _, ut := range underlyings {
			schema := fmt.Sprintf(`{"type":%q,"logicalType":%q}`, ut, lt)
			cts := []avro.CustomType{
				{LogicalType: lt, Decode: dec},
				{LogicalType: lt, AvroType: ut, Decode: dec},
			}
			for _, ct := range cts {
				func() {
					defer func() {
						if r := recover(); r != nil {
							t.Errorf("Parse PANICKED: schema=%s + CustomType{logical=%q,avro=%q}: %v", schema, lt, ct.AvroType, r)
						}
					}()
					_, _ = avro.Parse(schema, avro.WithCustomType(ct)) // result irrelevant; must not panic
				}()
			}
		}
	}
}

// TestProperty_BinaryJSONRoundTripAgree asserts that for every generated
// (schema, value), decoding the binary encoding and decoding the JSON
// encoding yield the SAME Go value. This is a within-twmb oracle: the binary
// and JSON paths must agree on what a value round-trips to. A divergence (the
// kind audits keep finding one example at a time) fails here for a whole
// class of inputs at once.
//
// Unions are round-tripped in TAGGED form on both paths. A bare (untagged)
// JSON union value carries no branch tag, so a multi-numeric union like
// ["int","float","long"] legitimately resolves to the first token-matching
// branch on bare-JSON decode while the binary wire carries the exact branch
// — a documented leniency, not a divergence. TaggedUnions carries the branch
// on both sides, so the comparison tests all union shapes without conflating
// that documented behavior with a real bug.
func TestProperty_BinaryJSONRoundTripAgree(t *testing.T) {
	const iters = 3000
	tagged := avro.TaggedUnions()
	for seed := int64(1); seed <= iters; seed++ {
		g := &schemaGen{r: rand.New(rand.NewSource(seed))}
		root := g.build(0)
		value := g.value(root)

		s, err := avro.Parse(root.schema)
		if err != nil {
			t.Fatalf("seed %d: Parse(%s): %v", seed, root.schema, err)
		}

		bin, err := s.Encode(value)
		if err != nil {
			t.Fatalf("seed %d: Encode(%#v) [%s]: %v", seed, value, root.schema, err)
		}
		var binOut any
		if _, err := s.Decode(bin, &binOut, tagged); err != nil {
			t.Fatalf("seed %d: Decode [%s]: %v", seed, root.schema, err)
		}

		jsn, err := s.EncodeJSON(value, tagged)
		if err != nil {
			t.Fatalf("seed %d: EncodeJSON(%#v) [%s]: %v", seed, value, root.schema, err)
		}
		var jsonOut any
		if err := s.DecodeJSON(jsn, &jsonOut, tagged); err != nil {
			t.Fatalf("seed %d: DecodeJSON(%s) [%s]: %v", seed, jsn, root.schema, err)
		}

		if !equalAvro(binOut, jsonOut) {
			t.Fatalf("seed %d: binary vs JSON round-trip disagree\n schema: %s\n value:  %#v\n binary: %#v\n json:   %#v\n binwire %x\n jsonout %s",
				seed, root.schema, value, binOut, jsonOut, bin, jsn)
		}
	}
}
