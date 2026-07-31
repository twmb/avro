package avro

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// The producer half of "integer arithmetic over a schema-declared magnitude",
// and the source-derived registry that keeps the class closed.
//
// The behavioral cells live next door (magnitude_arithmetic_matrix_test.go);
// they can only see arithmetic that reaches an observable outcome. Two things
// they cannot see are here: the invariant the PRODUCER of a derived bound
// owes its callers, and the enumeration of every site in the package where a
// magnitude meets an arithmetic operator at all.
// ---------------------------------------------------------------------------

// TestInvariant_SchemaMinBytesSaturates is what makes a ceiling at the
// producer sufficient. Two properties, both derived from what a per-element
// minimum MEANS rather than from what the code returns:
//
//   - It is a byte count, so it is never negative, and `1 + it` (which three
//     callers compute) is never zero — a zero divides, and a negative one
//     turns a buffer-relative bound into its own opposite.
//   - A value that provably occupies at least one wire byte must report at
//     least one. Reporting zero or less for such an element is not a loose
//     bound but a MISCLASSIFICATION: the zero-byte element cap and the
//     buffer-relative bound are different rules with different limits, and a
//     non-positive minimum silently routes an element through the wrong one.
//     That half is invisible to a decode test, because both rules end in an
//     error for a truncated wire.
func TestInvariant_SchemaMinBytesSaturates(t *testing.T) {
	const huge = `{"type":"fixed","name":"HF","size":9223372036854775807}`
	// Sums that wrap: a small lead, then magnitudes large enough to carry the
	// running total past the top of the range and back around.
	const sumToZero = `{"type":"record","name":"WZ","fields":[
		{"name":"lead","type":"long"},
		{"name":"a","type":{"type":"fixed","name":"WZA","size":9223372036854775807}},
		{"name":"b","type":{"type":"fixed","name":"WZB","size":9223372036854775807}}]}`
	const sumToNeg = `{"type":"record","name":"WN","fields":[
		{"name":"u","type":[{"type":"fixed","name":"WNU","size":9223372036854775807}]},
		{"name":"a","type":{"type":"fixed","name":"WNA","size":9223372036854775807}}]}`

	type probe struct {
		name    string
		schema  string
		nonZero bool // every value of this schema occupies >= 1 wire byte
	}
	probes := []probe{
		{"huge-fixed", huge, true},
		{"sum-wraps-to-zero", sumToZero, true},
		{"sum-wraps-negative", sumToNeg, true},
		// A union reports one byte for its branch index plus its SMALLEST
		// branch, so a union carrying a null branch reports 1 no matter what
		// else is in it. Only a union whose every branch is huge drives the
		// union arm's own arithmetic — the ["null", huge] shape below is the
		// one that looks like it covers this and does not.
		{"union-of-huge-only", `[` + huge + `]`, true},
		{"union-of-two-huge", `[` + huge + `,{"type":"fixed","name":"HF2","size":9223372036854775806}]`, true},
		{"huge-fixed-in-union", `["null",` + huge + `]`, true},
		{"huge-fixed-in-array", `{"type":"array","items":` + huge + `}`, true},
		{"huge-fixed-in-map", `{"type":"map","values":` + huge + `}`, true},
		{"wrap-nested-in-record", `{"type":"record","name":"NW","fields":[{"name":"in","type":` + sumToZero + `}]}`, true},
		{"wrap-behind-array", `{"type":"array","items":` + sumToZero + `}`, true},
		{"wrap-behind-map", `{"type":"map","values":` + sumToZero + `}`, true},
		{"many-huge-fields", `{"type":"record","name":"MH","fields":[
			{"name":"a","type":"long"},
			{"name":"b","type":{"type":"fixed","name":"MHB","size":9223372036854775807}},
			{"name":"c","type":{"type":"fixed","name":"MHC","size":9223372036854775807}},
			{"name":"d","type":{"type":"fixed","name":"MHD","size":9223372036854775807}},
			{"name":"e","type":{"type":"fixed","name":"MHE","size":9223372036854775807}}]}`, true},
		// Controls: genuinely zero-byte shapes must keep reporting zero, or
		// the zero-byte element cap stops applying to the elements it exists
		// for.
		{"plain-int", `"int"`, true},
		{"plain-null", `"null"`, false},
		{"empty-record", `{"type":"record","name":"ER","fields":[]}`, false},
		{"record-of-null", `{"type":"record","name":"RN","fields":[{"name":"n","type":"null"}]}`, false},
	}
	for _, p := range probes {
		t.Run(p.name, func(t *testing.T) {
			s, err := Parse(p.schema)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			got := schemaMinBytes(s.node)
			if got < 0 {
				t.Errorf("minimum wire bytes is %d; a byte count cannot be negative, and a negative one inverts every bound derived from it", got)
			}
			if 1+got <= 0 {
				t.Errorf("minimum wire bytes is %d, so a caller's `1 + minimum` is %d; three callers compute that and one divides by it", got, 1+got)
			}
			if got > maxSchemaMagnitude {
				t.Errorf("minimum wire bytes is %d, above the stated ceiling %d; callers add to this value and must not have to re-derive the headroom", got, maxSchemaMagnitude)
			}
			if p.nonZero && got < 1 {
				t.Errorf("every value of this schema occupies at least one wire byte, but the minimum reports %d — "+
					"a non-positive minimum routes the block bound through the zero-byte element cap instead of the buffer-relative one", got)
			}
			if !p.nonZero && got != 0 {
				t.Errorf("this schema's values occupy no wire bytes, but the minimum reports %d — "+
					"the zero-byte element cap exists for exactly these and stops applying if they report more", got)
			}
		})
	}
}

// TestInvariant_UnionMinimumCoversItsBranches catches the one wrong answer a
// range check cannot: a union arm that scans for the smallest branch starting
// from a SENTINEL reports the no-branches answer whenever a branch's own
// minimum happens to equal that sentinel — and the no-branches answer is 1,
// which is small, plausible, and inside every bound anyone would assert.
//
// The property is calibration-free: a union writes a branch index and then a
// branch, so it cannot cost less than its cheapest branch costs alone.
func TestInvariant_UnionMinimumCoversItsBranches(t *testing.T) {
	const huge = `{"type":"fixed","name":"UHF","size":9223372036854775807}`
	for _, schema := range []string{
		`[` + huge + `]`,
		`[` + huge + `,{"type":"fixed","name":"UHF2","size":9223372036854775806}]`,
		`["null",` + huge + `]`,
		`["int","string"]`,
		`["null","int"]`,
		`[{"type":"fixed","name":"UF3","size":4},{"type":"fixed","name":"UF4","size":9}]`,
	} {
		s, err := Parse(schema)
		if err != nil {
			t.Fatalf("parse %s: %v", schema, err)
		}
		got := schemaMinBytes(s.node)
		cheapest := -1
		for _, b := range s.node.branches {
			m := schemaMinBytesSeen(b, map[*schemaNode]struct{}{})
			if cheapest < 0 || m < cheapest {
				cheapest = m
			}
		}
		if got < cheapest {
			t.Errorf("union %s reports a minimum of %d, below its cheapest branch's own %d — "+
				"a union writes a branch index AND a branch, so it cannot cost less than the branch does alone",
				schema, got, cheapest)
		}
	}
}

// TestInvariant_SaturateSchemaMagnitudeIsTotal pins the accessor's own
// contract, which every consumer leans on: the result is in range for ANY
// input, including the ones no parse can produce today. A consumer that has
// to ask whether its input was already validated is a consumer that will
// eventually guess wrong.
func TestInvariant_SaturateSchemaMagnitudeIsTotal(t *testing.T) {
	const maxInt = int(^uint(0) >> 1)
	const minInt = -maxInt - 1
	for _, n := range []int{
		minInt, minInt + 1, -maxSchemaMagnitude, -1, 0, 1,
		maxSchemaMagnitude - 1, maxSchemaMagnitude, maxSchemaMagnitude + 1,
		1 << 40, maxInt - 1, maxInt,
	} {
		got := saturateSchemaMagnitude(n)
		if got < 0 || got > maxSchemaMagnitude {
			t.Errorf("saturateSchemaMagnitude(%d) = %d, outside [0, %d]", n, got, maxSchemaMagnitude)
		}
		if n >= 0 && n <= maxSchemaMagnitude && got != n {
			t.Errorf("saturateSchemaMagnitude(%d) = %d; a value already in range must pass through unchanged", n, got)
		}
	}
}

// TestInvariant_MagnitudeCeilingSurvivesItsLargestMultiplier is the reason the
// ceiling has the value it does. The package's consumers do not just add
// magnitudes, they scale them — the widest is bits-per-byte in the decimal
// capacity calculation — and the ceiling has to leave that product inside a
// 32-bit int so the arithmetic is safe on every build, not just the ones
// where int happens to be 64 bits.
func TestInvariant_MagnitudeCeilingSurvivesItsLargestMultiplier(t *testing.T) {
	const int32Max = 1<<31 - 1
	if got := int64(maxSchemaMagnitude) * magnitudeWidestMultiplier; got > int32Max {
		t.Errorf("ceiling %d times the widest multiplier %d is %d, past a 32-bit int's %d — "+
			"the ceiling no longer covers its own consumers", maxSchemaMagnitude, magnitudeWidestMultiplier, got, int32Max)
	}
	// And it must still be generous enough that no real schema is clipped:
	// the largest fixed anyone writes is orders of magnitude below it.
	if maxSchemaMagnitude < 1<<24 {
		t.Errorf("ceiling %d is below 16 MiB; a legitimate large fixed would be clipped, loosening every bound derived from it", maxSchemaMagnitude)
	}
}

// ---------------------------------------------------------------------------
// The enumeration: every site in the package where a schema-declared magnitude
// meets an arithmetic operator.
//
// WHAT DISTINGUISHES A HAZARDOUS SITE FROM A SAFE ONE. Three conditions, all
// required:
//
//  1. An operand carries a magnitude the schema TEXT chose. The only primitive
//     one is a `fixed` size (the parser leaves its upper bound open); the
//     others — precision, scale — are parse-capped, and counts are bounded by
//     the input length. But magnitude PROPAGATES: through arithmetic, through
//     a function that returns it, and through a function it is passed into.
//     A per-record sum over field minimums holds no `.size` anywhere in its
//     expression and is the wrap this file exists for, which is why a set
//     built by grepping `.size` is the wrong set.
//  2. The operator can leave the integer range: + - * / % <<, or a make()
//     length. Comparisons cannot — `a < b` has the same answer at every
//     magnitude — and neither can formatting or assignment. This is why
//     grepping `.size` OVER-reports: most of its reads are comparisons.
//  3. The value is an INTEGER. A magnitude handed on as []byte, string or
//     *big.Int has left the integer domain, and nothing downstream of it can
//     wrap on the magnitude's account.
//
// So the rule is reachability, not a pattern, and it is derived below rather
// than listed: seeds are the magnitude-bearing fields, and taint flows to
// integer-typed returns and integer-typed parameters until it stops growing.
// The derivation deliberately OVER-approximates — it has no types, so a
// reflect.Type's Size() reads as a magnitude — and every over-report is a row
// saying so. An enumeration with a reason per entry is auditable; the count
// alone is what keeps costing rounds.
// ---------------------------------------------------------------------------

type magVerdict string

const (
	// The magnitude reaching this expression is saturated, so the operator
	// cannot leave the range.
	magSaturated magVerdict = "saturated"
	// Bounded at this site for a reason of its own, stated in the row.
	magBoundedHere magVerdict = "bounded-here"
	// An over-report: the operand is not a schema-declared magnitude.
	magNotAMagnitude magVerdict = "not-a-magnitude"
	// Wrapping IS the operation's definition.
	magWrapIsTheContract magVerdict = "wrap-is-the-contract"
)

type magnitudeSite struct {
	where   string // "file.go::funcName"
	count   int    // hazardous expressions the derivation finds there
	verdict magVerdict
	reason  string
}

// magnitudeSites classifies every site the derivation reports. A site that
// appears with no row FAILS: that is a new consumer of a magnitude, and the
// point of the table is that someone has to say what happens to it at the top
// of the range. A row naming no site fails too, so the table cannot go stale
// while reading as coverage.
var magnitudeSites = []magnitudeSite{
	{
		where: "deser.go::schemaMinBytesSeen", count: 2, verdict: magSaturated,
		reason: "the producer. `1 + m` over the smallest branch and the running sum over a record's fields; " +
			"both are clamped by saturateSchemaMagnitude, and the sum is clamped per FIELD so the next addition " +
			"starts in range. This is the wrap that reached a divisor",
	},
	{
		where: "deser.go::checkMapBlockBounds", count: 1, verdict: magSaturated,
		reason: "divides by minEntryBytes, which is `1 + a saturated minimum` at all four call sites, so it is >= 1. " +
			"This is the division the wrap turned into a panic",
	},
	{
		where: "deser.go::checkArrayBlockBounds", count: 1, verdict: magSaturated,
		reason: "divides by minItemBytes only inside `if minItemBytes > 0`; the saturated producer also makes the " +
			"non-positive branch mean what it says, since only a genuinely zero-byte element can reach it now",
	},
	{
		where: "resolve.go::resolveMap", count: 1, verdict: magSaturated,
		reason: "`1 + schemaMinBytes(w.values)` — the resolver's own derivation of the per-entry bound",
	},
	{
		where: "skip.go::skipMap", count: 1, verdict: magSaturated,
		reason: "`1 + schemaMinBytes(w.values)` — the derivation used when a writer field is dropped and skipped",
	},
	{
		where: "schema.go::builder.buildComplex", count: 2, verdict: magSaturated,
		reason: "`1 + schemaMinBytes(mf.node)`, the parse-time derivation; and `make(_, len(nd.fields))`, whose " +
			"length is a field COUNT — bounded by the input, since every field costs bytes to write",
	},
	{
		where: "schema.go::maxDecimalDigits", count: 3, verdict: magSaturated,
		reason: "`8*size - 1` and the float scale that follows. Asks the shared accessor rather than clamping to a " +
			"ceiling of its own; magnitudeWidestMultiplier is this site's factor and is what the ceiling is chosen against",
	},
	{
		where: "json_decode.go::jsonDecodeAppliesLogical", count: 1, verdict: magBoundedHere,
		reason: "`make(_, probeLen)` is an ALLOCATION, not arithmetic: it needs a far tighter bound than the " +
			"arithmetic ceiling, and caps at the largest length any fixed logical inspects. See the accessor's note " +
			"on why allocation is a different question",
	},
	{
		where: "unsafe.go::udArrayDirect", count: 1, verdict: magNotAMagnitude,
		reason: "elemSize is reflect.Type.Size() — a Go type's in-memory width, fixed by the compiler. The " +
			"derivation has no type information and reads the selector name as a schema size",
	},
	{
		where: "unsafe.go::usArrayDirect", count: 1, verdict: magNotAMagnitude,
		reason: "elemSize is reflect.Type.Size(); see udArrayDirect",
	},
	{
		where: "unsafe.go::udArrayPtrRecord", count: 1, verdict: magNotAMagnitude,
		reason: "innerSize is reflect.Type.Size(); see udArrayDirect",
	},
	{
		where: "unsafe.go::usArrayRecord", count: 1, verdict: magNotAMagnitude,
		reason: "elemSize is reflect.Type.Size(); see udArrayDirect",
	},
	{
		where: "varint.go::appendVarlong", count: 1, verdict: magWrapIsTheContract,
		reason: "`uint64(i) << 1` is the zigzag transform. It operates on a wire value, not a schema magnitude, and " +
			"the shift discarding the top bit is what zigzag IS",
	},
}

// magSeedFields are the schema-object fields that carry a caller-chosen
// magnitude. `size` is the one with no parse-time ceiling; precision and scale
// are capped during validation and are here so a site that starts doing
// arithmetic on them has to say so rather than inherit the cap silently.
var magSeedFields = map[string]bool{
	"size": true, "Size": true,
	"precision": true, "Precision": true,
	"scale": true, "Scale": true,
}

var magArithOps = map[token.Token]bool{
	token.ADD: true, token.SUB: true, token.MUL: true,
	token.QUO: true, token.REM: true, token.SHL: true,
}

var magArithAssign = map[token.Token]bool{
	token.ADD_ASSIGN: true, token.SUB_ASSIGN: true, token.MUL_ASSIGN: true,
	token.QUO_ASSIGN: true, token.REM_ASSIGN: true, token.SHL_ASSIGN: true,
}

var magIntTypes = map[string]bool{
	"int": true, "int8": true, "int16": true, "int32": true, "int64": true,
	"uint": true, "uint8": true, "uint16": true, "uint32": true, "uint64": true,
	"uintptr": true, "byte": true, "rune": true, "laxInt": true,
}

type magSrcFile struct {
	name string
	f    *ast.File
}

type magTaint struct {
	fns    map[string]bool            // integer-returning fns that return a magnitude
	params map[string]map[string]bool // "Recv.Fn" -> integer param names carrying one
}

// magReturnsInteger reports whether fd has an integer-typed result. Integer
// overflow is the class, so a function whose results are all slices, strings,
// errors or structs cannot carry a magnitude onward as an integer.
func magReturnsInteger(fd *ast.FuncDecl) bool {
	if fd.Type.Results == nil {
		return false
	}
	for _, r := range fd.Type.Results.List {
		if id, ok := r.Type.(*ast.Ident); ok && magIntTypes[id.Name] {
			return true
		}
	}
	return false
}

func magFuncName(fd *ast.FuncDecl) string {
	if fd.Recv != nil && len(fd.Recv.List) > 0 {
		t := fd.Recv.List[0].Type
		if star, ok := t.(*ast.StarExpr); ok {
			t = star.X
		}
		if id, ok := t.(*ast.Ident); ok {
			return id.Name + "." + fd.Name.Name
		}
	}
	return fd.Name.Name
}

func magHasSeed(n ast.Node) bool {
	found := false
	ast.Inspect(n, func(x ast.Node) bool {
		if sel, ok := x.(*ast.SelectorExpr); ok && magSeedFields[sel.Sel.Name] {
			found = true
			return false
		}
		return true
	})
	return found
}

func magCalleeName(call *ast.CallExpr) string {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return fn.Name
	case *ast.SelectorExpr:
		return fn.Sel.Name
	}
	return ""
}

func magCallsMagnitude(n ast.Node, st *magTaint) bool {
	found := false
	ast.Inspect(n, func(x ast.Node) bool {
		if call, ok := x.(*ast.CallExpr); ok && st.fns[magCalleeName(call)] {
			found = true
		}
		return true
	})
	return found
}

func magUsesIdent(n ast.Node, names map[string]bool) bool {
	found := false
	ast.Inspect(n, func(x ast.Node) bool {
		if id, ok := x.(*ast.Ident); ok && names[id.Name] {
			found = true
		}
		return true
	})
	return found
}

// magTainted returns every identifier in fd carrying a magnitude: its tainted
// PARAMETERS plus locals assigned from a seed, a magnitude-returning call, or
// another tainted identifier. The local loop runs to a fixpoint so order of
// assignment inside the function does not matter.
func magTainted(fd *ast.FuncDecl, st *magTaint) map[string]bool {
	out := map[string]bool{}
	for p := range st.params[magFuncName(fd)] {
		out[p] = true
	}
	for {
		grew := false
		ast.Inspect(fd, func(n ast.Node) bool {
			var lhs, rhs []ast.Expr
			switch s := n.(type) {
			case *ast.AssignStmt:
				lhs, rhs = s.Lhs, s.Rhs
			case *ast.ValueSpec:
				for _, id := range s.Names {
					lhs = append(lhs, id)
				}
				rhs = s.Values
			default:
				return true
			}
			hot := false
			for _, r := range rhs {
				if magHasSeed(r) || magCallsMagnitude(r, st) || magUsesIdent(r, out) {
					hot = true
				}
			}
			if !hot {
				return true
			}
			for _, l := range lhs {
				if id, ok := l.(*ast.Ident); ok && !out[id.Name] && id.Name != "_" {
					out[id.Name] = true
					grew = true
				}
			}
			return true
		})
		if !grew {
			return out
		}
	}
}

// magParamNames returns fd's parameter names positionally, blanking any whose
// declared type is not an integer: a magnitude handed over as []byte, string
// or *big.Int has left the integer domain and cannot wrap downstream.
func magParamNames(fd *ast.FuncDecl) []string {
	if fd.Type.Params == nil {
		return nil
	}
	var out []string
	for _, f := range fd.Type.Params.List {
		id, isIdent := f.Type.(*ast.Ident)
		isInt := isIdent && magIntTypes[id.Name]
		if len(f.Names) == 0 {
			out = append(out, "")
			continue
		}
		for _, n := range f.Names {
			if !isInt {
				out = append(out, "")
				continue
			}
			out = append(out, n.Name)
		}
	}
	return out
}

// magScan derives the taint fixpoint and returns hazardous-expression counts
// keyed "file.go::funcName", plus the magnitude-returning function set.
func magScan(t *testing.T) (map[string]int, map[string]bool) {
	t.Helper()
	var files []magSrcFile
	for _, dir := range []string{".", "ocf"} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
				continue
			}
			p := filepath.Join(dir, e.Name())
			f, err := parser.ParseFile(token.NewFileSet(), p, nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", p, err)
			}
			name := e.Name()
			if dir != "." {
				name = dir + "/" + name
			}
			files = append(files, magSrcFile{name, f})
		}
	}

	type decl struct {
		file string
		fd   *ast.FuncDecl
	}
	var decls []decl
	byName := map[string]*ast.FuncDecl{}
	for _, fl := range files {
		for _, d := range fl.f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil {
				decls = append(decls, decl{fl.name, fd})
				byName[fd.Name.Name] = fd
			}
		}
	}

	st := &magTaint{fns: map[string]bool{}, params: map[string]map[string]bool{}}
	for {
		grew := false
		for _, d := range decls {
			local := magTainted(d.fd, st)
			if magReturnsInteger(d.fd) && !st.fns[d.fd.Name.Name] {
				ast.Inspect(d.fd.Body, func(n ast.Node) bool {
					rs, ok := n.(*ast.ReturnStmt)
					if !ok {
						return true
					}
					for _, r := range rs.Results {
						if magHasSeed(r) || magCallsMagnitude(r, st) || magUsesIdent(r, local) {
							st.fns[d.fd.Name.Name] = true
							grew = true
						}
					}
					return true
				})
			}
			ast.Inspect(d.fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				target, known := byName[magCalleeName(call)]
				if !known {
					return true
				}
				names := magParamNames(target)
				for i, a := range call.Args {
					if i >= len(names) || names[i] == "" || names[i] == "_" {
						continue
					}
					if !magHasSeed(a) && !magCallsMagnitude(a, st) && !magUsesIdent(a, local) {
						continue
					}
					key := magFuncName(target)
					if st.params[key] == nil {
						st.params[key] = map[string]bool{}
					}
					if !st.params[key][names[i]] {
						st.params[key][names[i]] = true
						grew = true
					}
				}
				return true
			})
		}
		if !grew {
			break
		}
	}

	counts := map[string]int{}
	for _, d := range decls {
		local := magTainted(d.fd, st)
		ast.Inspect(d.fd.Body, func(n ast.Node) bool {
			hit := false
			switch s := n.(type) {
			case *ast.BinaryExpr:
				if !magArithOps[s.Op] {
					return true
				}
				hit = magHasSeed(s.X) || magHasSeed(s.Y) ||
					magCallsMagnitude(s.X, st) || magCallsMagnitude(s.Y, st) ||
					magUsesIdent(s.X, local) || magUsesIdent(s.Y, local)
			case *ast.AssignStmt:
				if !magArithAssign[s.Tok] {
					return true
				}
				for _, r := range s.Rhs {
					hit = hit || magHasSeed(r) || magCallsMagnitude(r, st) || magUsesIdent(r, local)
				}
				for _, l := range s.Lhs {
					hit = hit || magUsesIdent(l, local)
				}
			case *ast.CallExpr:
				id, ok := s.Fun.(*ast.Ident)
				if !ok || id.Name != "make" || len(s.Args) < 2 {
					return true
				}
				for _, a := range s.Args[1:] {
					hit = hit || magHasSeed(a) || magCallsMagnitude(a, st) || magUsesIdent(a, local)
				}
			}
			if hit {
				counts[d.file+"::"+magFuncName(d.fd)]++
			}
			return true
		})
	}
	return counts, st.fns
}

// TestInvariant_EveryMagnitudeArithmeticSiteIsClassified is the completeness
// half. A new expression that puts a schema-declared magnitude under an
// arithmetic operator lands with no row and fails here until someone says what
// it does at the top of the range; a row that names no site fails too, so the
// table cannot quietly describe code that no longer exists.
func TestInvariant_EveryMagnitudeArithmeticSiteIsClassified(t *testing.T) {
	counts, magFns := magScan(t)

	// Anti-rot: the derivation is name-based, so a rename of the seed fields
	// or of the producer would leave it scanning for nothing and reporting a
	// clean table. These two functions ARE the class; if the fixpoint stops
	// finding them, the guard is watching an empty set.
	for _, want := range []string{"schemaMinBytesSeen", "maxDecimalDigits"} {
		if !magFns[want] {
			t.Fatalf("the taint fixpoint no longer reaches %s — the seed fields or the producer were renamed, "+
				"and this guard is now watching nothing", want)
		}
	}
	if len(counts) == 0 {
		t.Fatal("the derivation found no arithmetic on any magnitude at all — it has rotted")
	}

	rows := map[string]magnitudeSite{}
	for _, r := range magnitudeSites {
		if _, dup := rows[r.where]; dup {
			t.Errorf("duplicate row for %s", r.where)
		}
		rows[r.where] = r
	}
	for where, n := range counts {
		row, ok := rows[where]
		if !ok {
			t.Errorf("%s puts a schema-declared magnitude under an arithmetic operator (%d expression(s)) and has no row.\n"+
				"  Say what happens at the top of the range: saturated (asks saturateSchemaMagnitude), bounded-here\n"+
				"  (with the reason its bound differs), not-a-magnitude (with what the operand really is), or\n"+
				"  wrap-is-the-contract. A count alone is what keeps this class open.", where, n)
			continue
		}
		if row.count != n {
			t.Errorf("%s now has %d magnitude-arithmetic expression(s), the table says %d.\n"+
				"  A changed count means an expression was added or removed here; re-read the row's reason (%q)\n"+
				"  and confirm it still covers every expression at this site.", where, n, row.count, row.reason)
		}
	}
	for where := range rows {
		if _, ok := counts[where]; !ok {
			t.Errorf("row %s names no magnitude arithmetic in the sources — the site moved or was deleted, "+
				"and this row now reads as coverage it does not have", where)
		}
	}
	t.Logf("classified %d sites: %d saturated, %d bounded-here, %d not-a-magnitude, %d wrap-is-the-contract",
		len(magnitudeSites),
		magCountVerdict(magSaturated), magCountVerdict(magBoundedHere),
		magCountVerdict(magNotAMagnitude), magCountVerdict(magWrapIsTheContract))
}

func magCountVerdict(v magVerdict) int {
	n := 0
	for _, r := range magnitudeSites {
		if r.verdict == v {
			n++
		}
	}
	return n
}

// TestInvariant_ClippedMagnitudeStillRejects is the control for the one thing
// saturation gives up: a magnitude above the ceiling yields a LOOSER
// buffer-relative bound than the true magnitude would. The bound still has to
// refuse a block that cannot fit, which for any buffer below the ceiling it
// does on the bound itself — so the concession costs an error message on
// absurd schemas and nothing else.
func TestInvariant_ClippedMagnitudeStillRejects(t *testing.T) {
	// Each element needs 2^40 bytes; the ceiling clips that to 2^27.
	s, err := Parse(`{"type":"array","items":{"type":"fixed","name":"CBF","size":1099511627776}}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	for _, count := range []int{1, 4, 1000} {
		wire := append([]byte{byte(count << 1)}, make([]byte, 1<<16)...)
		var v any
		if _, err := s.Decode(wire, &v); err == nil {
			t.Errorf("a block claiming %d elements of 2^40 bytes was accepted from a 64 KiB buffer", count)
		}
	}
}
