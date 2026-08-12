package avro

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------
// The oracle: encoding/json, executed rather than restated.
//
// Every twin below is the code the corresponding production site ran before
// one decoder replaced five hand-spelled ones. They are kept here, and only
// here, so the matrix compares against a reference OUTSIDE this package
// instead of against whatever the decoder currently happens to do.
// ---------------------------------------------------------------------

func oracleDecodeLenient(schema string) (any, error) {
	v, _, err := oracleDecodeLenientOffset(schema)
	return v, err
}

// oracleDecodeLenientOffset also reports where the stdlib decoder stopped, so
// the consumed count the shared decoder returns has an independent answer to
// be checked against rather than only being self-consistent.
func oracleDecodeLenientOffset(schema string) (any, int, error) {
	dec := json.NewDecoder(strings.NewReader(schema))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, 0, err
	}
	return v, int(dec.InputOffset()), nil
}

func oracleDecodeStrict(schema string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(schema))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	var tail json.RawMessage
	if err := dec.Decode(&tail); !errors.Is(err, io.EOF) {
		return nil, errors.New("invalid schema: unexpected trailing content")
	}
	return v, nil
}

// oracleParseSchemaTree is parseSchemaTree with the stdlib decode.
func oracleParseSchemaTree(schema string) (*aschema, error) {
	v, err := oracleDecodeStrict(schema)
	if err != nil {
		return nil, boundJSONErrorEcho(err)
	}
	var s aschema
	if err := aschemaFromAny(v, &s, nil); err != nil {
		return nil, err
	}
	return &s, nil
}

// oracleUnmarshalAny is unmarshalAnyPreservePrecision with the stdlib decode.
func oracleUnmarshalAny(raw string) (any, error) {
	v, err := oracleDecodeStrict(raw)
	if err != nil {
		return nil, err
	}
	return normalizeJSONValue(v), nil
}

// oracleCacheNormalize is SchemaCache.Parse's input normalization with the
// stdlib decode: re-marshal only when the input is exactly one JSON value.
func oracleCacheNormalize(schema string) string {
	v, err := oracleDecodeStrict(schema)
	if err != nil {
		return schema
	}
	normalized, err := json.Marshal(v)
	if err != nil {
		return schema
	}
	return string(normalized)
}

// oracleTagDefault is the struct-tag default read, built on the stdlib decode.
//
// Unlike the other twins here it is NOT a copy of what the site used to do,
// because this is the one site whose behavior changed: it used to ask
// json.Decoder.More whether anything followed, and More answers `c != ']' &&
// c != '}'`, so it called `42]` a complete value and discarded the bracket.
// A twin spelling that would asserts the behavior the change removed.
//
// So the rule is stated independently on this side instead: decode one value,
// then require the rest of the text to be whitespace, using the decoder's OWN
// offset accounting (InputOffset) rather than the consumed count the
// implementation computes. That keeps the oracle answerable from stdlib alone
// while also crossing the arithmetic the implementation uses to find the same
// boundary.
func oracleTagDefault(raw string) any {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return raw
	}
	for _, c := range raw[dec.InputOffset():] {
		switch c {
		case ' ', '\t', '\n', '\r':
		default:
			return raw
		}
	}
	return v
}

// ---------------------------------------------------------------------
// The corpus: the input axis.
// ---------------------------------------------------------------------

// decodeCell is one schema-text input plus the class it belongs to. The class
// is what the liveness floor counts, so an axis that stops being generated
// reds instead of quietly emptying.
type decodeCell struct {
	name  string
	class string
	in    string
}

// decodeCorpus spans the input axis: every JSON shape a schema can be written
// in, crossed with the boundaries where a decoder can silently differ from the
// stdlib one it replaced. The number rows are the ones that matter most — see
// [decodeSchemaAny] for the two silent failures a resolving decoder causes.
var decodeCorpus = func() []decodeCell {
	cells := []decodeCell{
		// Numbers: syntax forms whose LITERAL must survive the decode.
		{"int", "number", `{"type":"int","p":42}`},
		{"negative-zero-integer", "number", `{"type":"int","p":-0}`},
		{"negative-zero-float", "number", `{"type":"int","p":-0.0}`},
		{"exact-integer-exponent", "number", `{"type":"int","p":1.0e3}`},
		{"fractional", "number", `{"type":"int","p":1.5}`},
		{"exponent-plus", "number", `{"type":"int","p":1E+2}`},
		{"exponent-minus", "number", `{"type":"int","p":1e-2}`},
		{"beyond-int64", "number", `{"type":"int","p":123456789012345678901234567890}`},
		{"at-int64-edge", "number", `{"type":"int","p":9007199254740993}`},
		{"overflow-to-inf", "number", `{"type":"int","p":1e1000}`},
		{"zero", "number", `{"type":"int","p":0}`},
		{"negative", "number", `{"type":"int","p":-17}`},

		// Strings: escapes and the byte sequences a decode must repair.
		{"plain", "string", `{"type":"int","p":"plain"}`},
		{"escapes", "string", `{"type":"int","p":"a\tb\nc\"d\\e\/f"}`},
		{"unicode-escape", "string", `{"type":"int","p":"\u00e9"}`},
		{"surrogate-pair", "string", `{"type":"int","p":"\ud83d\ude00"}`},
		{"unpaired-surrogate", "string", `{"type":"int","p":"\ud800"}`},
		{"unpaired-surrogate-then-text", "string", `{"type":"int","p":"\ud800x"}`},
		{"literal-multibyte", "string", "{\"type\":\"int\",\"p\":\"\u00e9\u4e2d\"}"},
		{"empty", "string", `{"type":"int","p":""}`},
		{"key-with-escape", "string", `{"type":"int","abc":1}`},

		// Containers.
		{"empty-object", "container", `{"type":"int","p":{}}`},
		{"empty-array", "container", `{"type":"int","p":[]}`},
		{"nested", "container", `{"type":"int","p":{"a":[1,{"b":[2]}]}}`},
		{"duplicate-keys", "container", `{"type":"int","p":1,"p":2}`},
		{"union", "container", `["null","int"]`},
		{"record", "container", `{"type":"record","name":"R","doc":"d","fields":[{"name":"a","type":"int"}]}`},

		// Bare literals.
		{"true", "literal", `{"type":"int","p":true}`},
		{"false", "literal", `{"type":"int","p":false}`},
		{"null", "literal", `{"type":"int","p":null}`},
		{"bare-primitive", "literal", `"string"`},

		// Whitespace placement.
		{"leading-space", "space", "  \t\n\r" + `{"type":"int"}`},
		{"trailing-space", "space", `{"type":"int"}` + "  \t\n\r"},
		{"interior-space", "space", `{ "type" : "int" , "p" : [ 1 , 2 ] }`},

		// Trailing content: the rule the strict callers own.
		{"second-value", "trailing", `{"type":"int"} {"type":"long"}`},
		{"trailing-garbage", "trailing", `{"type":"int"} oops`},
		{"digit-run", "trailing", `00`},
		{"trailing-comma-object", "trailing", `{"type":"int",}`},

		// Malformed: the accept/reject boundary itself.
		{"unterminated-string", "malformed", `{"type":"int","p":"abc}`},
		{"unterminated-object", "malformed", `{"type":"int"`},
		{"leading-zero", "malformed", `{"type":"int","p":01}`},
		{"bare-nan", "malformed", `{"type":"int","p":NaN}`},
		{"bare-infinity", "malformed", `{"type":"int","p":Infinity}`},
		{"lone-minus", "malformed", `{"type":"int","p":-}`},
		{"dot-no-digit", "malformed", `{"type":"int","p":1.}`},
		{"exponent-no-digit", "malformed", `{"type":"int","p":1e}`},
		{"single-quotes", "malformed", `{'type':'int'}`},
		{"empty-input", "malformed", ``},
		{"only-space", "malformed", `   `},
		{"bad-escape", "malformed", `{"type":"int","p":"\q"}`},
		{"raw-control-char", "malformed", "{\"type\":\"int\",\"p\":\"a\x01b\"}"},
	}
	// Raw invalid UTF-8, which a decode repairs one replacement rune per
	// BYTE rather than per run — a distinction no escape can express, so it
	// is built here rather than written as a literal.
	cells = append(cells,
		decodeCell{"invalid-utf8-single", "string", "{\"type\":\"int\",\"p\":\"\xa8\"}"},
		decodeCell{"invalid-utf8-run", "string", "{\"type\":\"int\",\"p\":\"\xa8\xa8\xa8\"}"},
		decodeCell{"invalid-utf8-in-key", "string", "{\"type\":\"int\",\"\xa8\":1}"},
		decodeCell{"truncated-utf8", "string", "{\"type\":\"int\",\"p\":\"\xe4\xb8\"}"},
	)
	// The nesting boundary, which no mutation-driven corpus reaches: the
	// decoder accepts exactly the depth the stdlib decoder accepted.
	//
	// The innermost value is an axis of its own, not a detail. An EMPTY
	// innermost container agrees with the stdlib decode whatever the bound is
	// charged per — value or per container — because there is no leaf to spend
	// the last unit on. Only a NON-EMPTY innermost separates the two, so a
	// ladder of empty containers can be run at every depth and still measure
	// nothing about which rule the decoder implements.
	for _, n := range []int{2, 9999, 10000, 10001, 10002} {
		for _, inner := range []struct{ name, body string }{
			{"empty", ""},
			{"number", "1"},
			{"string", `"x"`},
			{"object", `{"a":1}`},
		} {
			cells = append(cells,
				decodeCell{
					name:  fmt.Sprintf("nesting-array-%d-%s", n, inner.name),
					class: "depth",
					in:    strings.Repeat("[", n) + inner.body + strings.Repeat("]", n),
				},
				decodeCell{
					name:  fmt.Sprintf("nesting-object-%d-%s", n, inner.name),
					class: "depth",
					in:    strings.Repeat(`{"a":`, n) + cmp.Or(inner.body, "{}") + strings.Repeat("}", n),
				})
		}
	}
	return cells
}()

// ---------------------------------------------------------------------
// The matrix: input class x call site.
// ---------------------------------------------------------------------

// decodeSite is one production caller of the shared decoder, paired with the
// stdlib twin of what that caller used to do. The site axis is what makes the
// matrix a class net rather than a decoder net: the decoder's contract is
// re-entered once per caller, and a caller with no cell is a route on which
// nothing is proven. [TestCensus_SchemaJSONDecodeCallSites] is what keeps this
// list equal to the set the source actually contains.
type decodeSite struct {
	name string
	// run returns a comparable rendering of the site's output, or an error.
	// Rendering rather than the value itself, because two sites hand back
	// unexported trees that only compare usefully as a whole.
	run    func(in string) (any, error)
	oracle func(in string) (any, error)
}

var decodeSites = []decodeSite{
	{
		name:   "decodeSchemaAnyStrict",
		run:    func(in string) (any, error) { return decodeSchemaAnyStrict(in) },
		oracle: func(in string) (any, error) { return oracleDecodeStrict(in) },
	},
	{
		name:   "parseSchemaTree",
		run:    func(in string) (any, error) { return parseSchemaTree(in) },
		oracle: func(in string) (any, error) { return oracleParseSchemaTree(in) },
	},
	{
		name:   "unmarshalAnyPreservePrecision",
		run:    func(in string) (any, error) { return unmarshalAnyPreservePrecision(in) },
		oracle: func(in string) (any, error) { return oracleUnmarshalAny(in) },
	},
	{
		name: "unmarshalDefault",
		// Never fails by contract: it re-reads bytes an earlier decode
		// already accepted. Feeding it the whole corpus asks more of it
		// than production does, which is the point — a decoder that
		// diverges on malformed input diverges on valid input too.
		run:    func(in string) (any, error) { return unmarshalDefault(json.RawMessage(in)), nil },
		oracle: func(in string) (any, error) { v, _ := oracleDecodeLenient(in); return v, nil },
	},
	{
		name:   "SchemaCache.Parse normalization",
		run:    func(in string) (any, error) { return cacheNormalizeSchema(in), nil },
		oracle: func(in string) (any, error) { return oracleCacheNormalize(in), nil },
	},
	{
		name:   "SchemaFor struct-tag default",
		run:    func(in string) (any, error) { return tagDefaultValue(in), nil },
		oracle: func(in string) (any, error) { return oracleTagDefault(in), nil },
	},
}

// TestMatrix_SchemaJSONDecodeCallSiteParity crosses every schema-text input
// class with every caller of the shared decoder, and requires each caller to
// land exactly where the stdlib decode it replaced landed — same value, same
// accept/reject verdict.
//
// The oracle is encoding/json, executed here rather than described, so no cell
// can pass by agreeing with the decoder about something they are both wrong
// about. The site axis is the one the mechanism turns on: a single decoder
// serving six callers is proven on the callers, not on itself.
func TestMatrix_SchemaJSONDecodeCallSiteParity(t *testing.T) {
	classSeen := map[string]int{}
	siteSeen := map[string]int{}
	for _, cell := range decodeCorpus {
		for _, site := range decodeSites {
			t.Run(cell.class+"/"+cell.name+"/"+site.name, func(t *testing.T) {
				classSeen[cell.class]++
				siteSeen[site.name]++
				want, wantErr := site.oracle(cell.in)
				got, gotErr := site.run(cell.in)
				if (wantErr == nil) != (gotErr == nil) {
					t.Fatalf("accept/reject differs: stdlib err=%v, shared err=%v\n  input %q", wantErr, gotErr, cell.in)
				}
				if wantErr != nil {
					return
				}
				if !reflect.DeepEqual(want, got) {
					t.Fatalf("value differs for %q\n  stdlib %#v\n  shared %#v", cell.in, want, got)
				}
			})
		}
	}
	// A generated axis proves nothing until it is realized. Both axes carry
	// a floor so an arm that stops being produced reds here rather than
	// leaving the matrix looking crossed.
	for _, class := range []string{"number", "string", "container", "literal", "space", "trailing", "malformed", "depth"} {
		if classSeen[class] < len(decodeSites) {
			t.Errorf("input class %q ran %d cells, expected at least one per site (%d)", class, classSeen[class], len(decodeSites))
		}
	}
	for _, site := range decodeSites {
		if siteSeen[site.name] != len(decodeCorpus) {
			t.Errorf("site %q ran %d cells, expected %d", site.name, siteSeen[site.name], len(decodeCorpus))
		}
	}
}

// TestInvariant_SchemaDecodeNumbersStayLiteral pins the property the parse path
// depends on, answered FROM THE INPUT rather than from a sibling decoder: every
// number the shared decoder emits is the author's literal, byte for byte.
//
// The parity matrix would also red if a number were resolved here, but only for
// as long as its twin stays stdlib. This states the rule without a second
// implementation to agree with, because the two failures it prevents are
// silent: a re-marshal of a resolved "-0" loses the sign a float default
// encodes, and a re-marshal of a resolved long literal is short enough to walk
// past the length cap that refuses the literal. Both produce wrong bytes and no
// error, so the guard has to be one nothing can quietly co-edit.
func TestInvariant_SchemaDecodeNumbersStayLiteral(t *testing.T) {
	literals := []string{
		"0", "-0", "-0.0", "1", "-17", "1.5", "1e3", "1.0e3", "1E+2", "1e-2",
		"9007199254740993", "123456789012345678901234567890", "1e1000",
		"1." + strings.Repeat("0", 1021) + "e3",
		"1." + strings.Repeat("0", 1020) + "e3",
	}
	for _, lit := range literals {
		t.Run(lit[:min(len(lit), 24)], func(t *testing.T) {
			v, err := decodeSchemaAnyStrict(`{"p":` + lit + `,"a":[` + lit + `]}`)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			m := v.(map[string]any)
			for path, got := range map[string]any{
				"object member": m["p"],
				"array element": m["a"].([]any)[0],
			} {
				n, ok := got.(json.Number)
				if !ok {
					t.Fatalf("%s: decoder resolved the number to %T(%v); it must stay the literal, or a re-marshal writes a different number than the author did", path, got, got)
				}
				if string(n) != lit {
					t.Fatalf("%s: literal changed: wrote %q, decoder returned %q", path, lit, string(n))
				}
			}
		})
	}
}

// ---------------------------------------------------------------------
// The call-site guard.
// ---------------------------------------------------------------------

// sharedDecoderEntryPoints are the decoder's exported-to-the-package entry
// points. A call to either is a site whose behavior the matrix owes a cell.
var sharedDecoderEntryPoints = map[string]bool{
	"decodeSchemaAny":       true,
	"decodeSchemaAnyStrict": true,
}

// schemaDecodeCallers is the call-site set the matrix claims to cover, keyed by
// the enclosing function. Derived from source by
// [TestCensus_SchemaJSONDecodeCallSites] rather than trusted from this list —
// the list is what the derivation is compared AGAINST, so a site appearing or
// disappearing is a decision made here rather than a silent change.
var schemaDecodeCallers = map[string]string{
	"parseSchemaTree":               "schema_parse.go",
	"unmarshalAnyPreservePrecision": "schema.go",
	"unmarshalDefault":              "schema.go",
	"cacheNormalizeSchema":          "cache.go",
	"tagDefaultValue":               "schema_for.go",
	"decodeSchemaAnyStrict":         "schema_decode.go", // the strict wrapper calls the lenient form
}

// TestCensus_SchemaJSONDecodeCallSites derives the shared decoder's callers
// from the package source and requires them to be exactly the set the parity
// matrix exercises. It reds in BOTH directions: a new caller that no cell
// covers fails here, and a caller that disappears fails here too, so the guard
// cannot go stale by watching code that is gone.
//
// It also requires that no source file reconstruct the decode this replaced. A
// json.Decoder put into UseNumber mode IS the old hand-spelled site — five of
// them had already drifted into three different trailing-content rules — so a
// sixth reappearing has to fail rather than quietly coexist.
func TestCensus_SchemaJSONDecodeCallSites(t *testing.T) {
	files := censusSourceFiles(t)
	fset := token.NewFileSet()

	found := map[string]string{}
	useNumber := map[string][]int{}
	for _, path := range files {
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok {
				return true
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch sel := call.Fun.(type) {
				case *ast.Ident:
					if sharedDecoderEntryPoints[sel.Name] {
						found[fn.Name.Name] = path
					}
				case *ast.SelectorExpr:
					if sel.Sel.Name == "UseNumber" {
						useNumber[path] = append(useNumber[path], fset.Position(call.Pos()).Line)
					}
				}
				return true
			})
			return true
		})
	}

	for name, wantFile := range schemaDecodeCallers {
		gotFile, ok := found[name]
		if !ok {
			t.Errorf("registered caller %s (%s) no longer calls the shared decoder — either it was removed, in which case drop its row and its matrix cell, or it went back to decoding by hand", name, wantFile)
			continue
		}
		if gotFile != wantFile {
			t.Errorf("caller %s moved from %s to %s; update the row so the guard keeps describing the code", name, wantFile, gotFile)
		}
	}
	for name, file := range found {
		if _, ok := schemaDecodeCallers[name]; !ok {
			t.Errorf("%s (%s) calls the shared decoder but has no row here and no cell in the parity matrix — a caller re-enters the decoder's contract, so it is unproven until the matrix crosses it", name, file)
		}
	}
	for file, lines := range useNumber {
		t.Errorf("%s puts a json.Decoder into UseNumber mode at line(s) %v — that is the hand-spelled schema decode the shared decoder replaced. Route it through decodeSchemaAny / decodeSchemaAnyStrict", file, lines)
	}

	// Anti-rot: with an empty derivation this guard passes forever while
	// proving nothing.
	if len(found) == 0 {
		t.Fatal("the AST scan found no callers at all; the scan is not seeing the package")
	}
	names := make([]string, 0, len(found))
	for name := range found {
		names = append(names, name)
	}
	sort.Strings(names)
	t.Logf("shared decoder callers derived from source: %s", strings.Join(names, ", "))
}

// TestMatrix_ParseDecodeErrorSentinels pins what a caller can ask about a
// schema that failed to decode. [Parse] echoes the decode error rather than
// wrapping it in one of its own, so the sentinels reach errors.Is: [io.EOF]
// means the text held no value at all, [io.ErrUnexpectedEOF] means it ran out
// part way through one, and a well-formed-but-wrong schema is neither.
//
// Two answers rather than one is the whole point — "you passed me nothing" and
// "you passed me a truncated schema" are different mistakes — so a decoder that
// collapsed them, or that reported its own error type for both, would take a
// distinction away from callers without anything failing.
func TestMatrix_ParseDecodeErrorSentinels(t *testing.T) {
	for _, c := range []struct {
		name      string
		schema    string
		sentinel  error
		neither   bool
		alsoValid bool
	}{
		{name: "empty", schema: ``, sentinel: io.EOF},
		{name: "only-whitespace", schema: "  \t\n ", sentinel: io.EOF},
		{name: "truncated-string", schema: `"abc`, sentinel: io.ErrUnexpectedEOF},
		{name: "truncated-object", schema: `{"type":"int"`, sentinel: io.ErrUnexpectedEOF},
		{name: "truncated-array", schema: `["null"`, sentinel: io.ErrUnexpectedEOF},
		{name: "truncated-literal", schema: `tru`, sentinel: io.ErrUnexpectedEOF},
		{name: "truncated-escape", schema: `"\`, sentinel: io.ErrUnexpectedEOF},
		{name: "truncated-unicode-escape", schema: `"\u00`, sentinel: io.ErrUnexpectedEOF},
		{name: "syntax-error", schema: `{"type":}`, neither: true},
		{name: "bad-hex-escape", schema: `"\uX"`, neither: true},
		{name: "trailing-content", schema: `"int" "long"`, neither: true},
		{name: "valid-but-unknown-type", schema: `"nope"`, neither: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse(c.schema)
			if err == nil {
				t.Fatalf("Parse(%q) succeeded", c.schema)
			}
			if c.neither {
				for _, s := range []error{io.EOF, io.ErrUnexpectedEOF} {
					if errors.Is(err, s) {
						t.Fatalf("Parse(%q) reported %v; this input is malformed, not truncated: %v", c.schema, s, err)
					}
				}
				return
			}
			if !errors.Is(err, c.sentinel) {
				t.Fatalf("Parse(%q) = %v; want errors.Is(err, %v)", c.schema, err, c.sentinel)
			}
			other := io.ErrUnexpectedEOF
			if c.sentinel == io.ErrUnexpectedEOF {
				other = io.EOF
			}
			if errors.Is(err, other) {
				t.Fatalf("Parse(%q) reported BOTH sentinels, which collapses the distinction: %v", c.schema, err)
			}
		})
	}
}

// TestInvariant_RootCannotFailToDecode pins why [Schema.Root] may panic on a
// decode error: the text it decodes is the exact text a parse already accepted,
// through the SAME decoder, so the panic is unreachable rather than merely
// unlikely.
//
// It used to be unreachable for a weaker reason — Root's decode was the LENIENT
// one, accepting a superset of what the parse accepted, so the two agreeing was
// a coincidence of two spellings. Now they are one function, and the property is
// that a decoder is deterministic and carries no state between calls.
func TestInvariant_RootCannotFailToDecode(t *testing.T) {
	// Every schema in the corpus that parses at all, plus the ownership
	// shapes, re-decoded exactly as Root re-decodes them.
	var texts []string
	for _, cell := range decodeCorpus {
		texts = append(texts, cell.in)
	}
	for _, sch := range ownershipShapes {
		texts = append(texts, sch)
	}
	checked := 0
	for _, text := range texts {
		s, err := Parse(text)
		if err != nil {
			continue
		}
		checked++
		if s.full != text {
			t.Errorf("Schema.full (%q) is not the text that parsed (%q); Root would decode something no parse validated", s.full, text)
		}
		if _, err := unmarshalAnyPreservePrecision(s.full); err != nil {
			t.Errorf("re-decoding an accepted schema failed: %v\n  schema %q", err, text)
		}
	}
	if checked == 0 {
		t.Fatal("no corpus entry parsed, so this asserts nothing")
	}
	t.Logf("re-decoded %d accepted schemas without error", checked)
}

// ---------------------------------------------------------------------
// What a `default=` tag body is allowed to be.
// ---------------------------------------------------------------------

// tagDefaultCase is one `default=` tag body and the value it must produce.
// wantRaw means the body is not JSON and the whole text stands in as a string.
type tagDefaultCase struct {
	body    string
	want    any
	wantRaw bool
	why     string
}

// TestMatrix_SchemaForTagDefaultAcceptSet fixes which `default=` tag bodies are
// read as JSON and which fall back to the text verbatim.
//
// The accept set is CHOSEN, not inherited. This site used to ask
// json.Decoder.More whether anything followed the value, and More answers
// `c != ']' && c != '}'` — so a body of `42]` reported nothing-follows, the
// bracket was silently discarded, and the field got the number 42. The two
// sites that decode a whole schema asked a different question (a second decode
// must reach EOF) and rejected the same text. That was a divergence between
// two spellings of one rule, not a decision, and one of them threw away input
// without saying so.
//
// Discarding is the worse answer, so all of them now reject: `42]` is not the
// number 42, it is a body that is not JSON, and it takes the same fallback as
// `hello`. On a typed field that fallback then fails validation, which is the
// point — the author gets an error instead of a silently different default.
func TestMatrix_SchemaForTagDefaultAcceptSet(t *testing.T) {
	cases := []tagDefaultCase{
		{body: `42`, want: json.Number("42"), why: "a bare JSON value is the value"},
		{body: ` 42 `, want: json.Number("42"), why: "surrounding whitespace is not content"},
		{body: "\t42\n", want: json.Number("42"), why: "every JSON space is whitespace here"},
		{body: `"hi"`, want: "hi", why: "a quoted JSON string is that string"},
		{body: `true`, want: true, why: "a bare literal is the literal"},
		{body: `[1,2]`, want: []any{json.Number("1"), json.Number("2")}, why: "a container is the container"},
		{body: `9223372036854775807`, want: json.Number("9223372036854775807"), why: "the literal survives, so a long default is not rounded"},

		{body: `42]`, wantRaw: true, why: "a trailing bracket is content; it used to be discarded in silence"},
		{body: `42}`, wantRaw: true, why: "a trailing brace is content; it used to be discarded in silence"},
		{body: `42 43`, wantRaw: true, why: "a second value is content"},
		{body: `42,`, wantRaw: true, why: "a trailing comma is content"},
		{body: `hello`, wantRaw: true, why: "unquoted text is not JSON, which is what the fallback is for"},
		{body: `note (a`, wantRaw: true, why: "the tag splitter hands the rest of the tag over verbatim, brackets unbalanced"},
		{body: ``, wantRaw: true, why: "an empty body names no value"},
		{body: `[1,2`, wantRaw: true, why: "a truncated container is not a container"},
	}
	for _, c := range cases {
		t.Run(strings.NewReplacer(" ", "_", "\t", "_tab_", "\n", "_nl_").Replace(c.body), func(t *testing.T) {
			got := tagDefaultValue(c.body)
			want := c.want
			if c.wantRaw {
				want = c.body
			}
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("%s: want %#v, got %#v (%s)", c.body, want, got, c.why)
			}
		})
	}
}

// tagTrailingBracket is the shape the rule above changes: a `long` field whose
// default body carries a trailing bracket.
type tagTrailingBracket struct {
	N int64 `avro:"n,default=42]"`
}

// tagPlainDefault is its unchanged twin, so the cell measures the bracket
// rather than the tag machinery.
type tagPlainDefault struct {
	N int64 `avro:"n,default=42"`
}

// TestMatrix_SchemaForTagDefaultTrailingBracket carries the rule to the surface
// an author actually types, because the fallback is only half the story: on a
// typed field the verbatim string then has to survive Avro validation, and it
// does not. A body of `42]` used to build a schema with the long default 42.
func TestMatrix_SchemaForTagDefaultTrailingBracket(t *testing.T) {
	if _, err := SchemaFor[tagTrailingBracket](); err == nil {
		t.Fatal("a `default=42]` tag built a schema; the trailing bracket must not be discarded, and the string it falls back to is not a long")
	}
	s, err := SchemaFor[tagPlainDefault]()
	if err != nil {
		t.Fatalf("`default=42` must still build: %v", err)
	}
	if !strings.Contains(s.String(), `"default":42`) {
		t.Fatalf("`default=42` lost its default: %s", s)
	}
}

// ---------------------------------------------------------------------
// What a returned tree may share.
// ---------------------------------------------------------------------

// mutableContainers records the identity of every map and slice reachable from
// a SchemaNode tree's caller-writable surfaces, keyed by address so two trees
// can be compared for overlap. Strings are excluded on purpose: nothing can be
// written through one, so sharing them is not sharing state.
func mutableContainers(n *SchemaNode, path string, out map[uintptr]string, seen map[*SchemaNode]bool) {
	if n == nil || seen[n] {
		return
	}
	seen[n] = true
	var walk func(p string, v any)
	walk = func(p string, v any) {
		switch t := v.(type) {
		case map[string]any:
			out[reflect.ValueOf(t).Pointer()] = p
			for k, e := range t {
				walk(p+"."+k, e)
			}
		case []any:
			if len(t) > 0 {
				out[reflect.ValueOf(t).Pointer()] = p
			}
			for i, e := range t {
				walk(fmt.Sprintf("%s[%d]", p, i), e)
			}
		case []byte:
			if len(t) > 0 {
				out[reflect.ValueOf(t).Pointer()] = p
			}
		}
	}
	if n.Props != nil {
		out[reflect.ValueOf(n.Props).Pointer()] = path + ".Props"
		walk(path+".Props", any(n.Props))
	}
	if len(n.Symbols) > 0 {
		out[reflect.ValueOf(n.Symbols).Pointer()] = path + ".Symbols"
	}
	if len(n.Aliases) > 0 {
		out[reflect.ValueOf(n.Aliases).Pointer()] = path + ".Aliases"
	}
	for i := range n.Fields {
		f := &n.Fields[i]
		fp := fmt.Sprintf("%s.Fields[%d]", path, i)
		if f.Props != nil {
			out[reflect.ValueOf(f.Props).Pointer()] = fp + ".Props"
			walk(fp+".Props", any(f.Props))
		}
		if len(f.Aliases) > 0 {
			out[reflect.ValueOf(f.Aliases).Pointer()] = fp + ".Aliases"
		}
		if f.HasDefault {
			walk(fp+".Default", f.Default)
		}
		mutableContainers(&f.Type, fp+".Type", out, seen)
	}
	mutableContainers(n.Items, path+".Items", out, seen)
	mutableContainers(n.Values, path+".Values", out, seen)
	for i := range n.Branches {
		mutableContainers(&n.Branches[i], fmt.Sprintf("%s.Branches[%d]", path, i), out, seen)
	}
}

// ownershipShapes puts a decoded container on each surface a caller can write
// through, including the second-occurrence reference paths — a self-reference
// and a diamond where one definition is reached twice — because a tree sharing
// a container with ITSELF would share it across calls too.
var ownershipShapes = map[string]string{
	"node props":     `{"type":"record","name":"R","meta":{"a":[1,2],"b":{"c":3}},"fields":[{"name":"x","type":"int"}]}`,
	"field props":    `{"type":"record","name":"R","fields":[{"name":"x","type":"int","tags":{"t":[1]}}]}`,
	"nested props":   `{"type":"record","name":"R","fields":[{"name":"x","type":{"type":"int","p":{"q":{"r":[1,{"s":2}]}}}}]}`,
	"union props":    `["null",{"type":"record","name":"R","up":{"z":[1,{"k":2}]},"fields":[{"name":"a","type":"int"}]}]`,
	"stray routed":   `{"type":"record","name":"R","fields":[{"name":"x","type":{"type":"int","size":{"q":1},"symbols":[1,2]}}]}`,
	"map default":    `{"type":"record","name":"R","fields":[{"name":"m","type":{"type":"map","values":"long"},"default":{"a":1,"b":2}}]}`,
	"array default":  `{"type":"record","name":"R","fields":[{"name":"l","type":{"type":"array","items":"int"},"default":[1,2,3]}]}`,
	"record default": `{"type":"record","name":"R","fields":[{"name":"r","type":{"type":"record","name":"S","fields":[{"name":"v","type":"string"}]},"default":{"v":"z"}}]}`,
	"bytes default":  `{"type":"record","name":"R","fields":[{"name":"b","type":"bytes","default":"\u00ff\u00fe"}]}`,
	"symbols":        `{"type":"enum","name":"E","symbols":["A","B"],"aliases":["X"]}`,
	"recursive":      `{"type":"record","name":"R","rp":{"n":[1]},"fields":[{"name":"self","type":["null","R"],"default":null}]}`,
	"diamond":        `{"type":"record","name":"R","fields":[{"name":"x","type":{"type":"record","name":"S","sp":{"n":[1]},"fields":[{"name":"v","type":"int"}]}},{"name":"y","type":"S"}]}`,
}

// TestInvariant_RootTreesShareNoMutableState pins [Schema.Root]'s ownership
// contract: the tree handed back is the caller's alone, so two calls share no
// map and no slice either could write through, and neither shares one with the
// schema's own internals — the props a [CustomType] callback reads while other
// goroutines encode.
//
// The contract is not new, but what makes it true moved: the schema decoder
// hands back SUBSTRINGS of the schema text where a reflect-driven decode
// allocated fresh strings, so what a returned tree shares with the text it came
// from became a live question. A string cannot be written through, which is why
// only the containers are counted.
func TestInvariant_RootTreesShareNoMutableState(t *testing.T) {
	names := make([]string, 0, len(ownershipShapes))
	for name := range ownershipShapes {
		names = append(names, name)
	}
	sort.Strings(names)
	totalContainers := 0
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			s, err := Parse(ownershipShapes[name])
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			first := map[uintptr]string{}
			second := map[uintptr]string{}
			mutableContainers(s.Root(), "$", first, map[*SchemaNode]bool{})
			mutableContainers(s.Root(), "$", second, map[*SchemaNode]bool{})
			if len(first) == 0 {
				t.Fatalf("no writable container reached the tree, so this cell asserts nothing")
			}
			totalContainers += len(first)
			for addr, path := range first {
				if other, ok := second[addr]; ok {
					t.Errorf("two Root trees share a container: %s and %s are the same object; a caller writing through one would change the other", path, other)
				}
			}
			// The parse-side props are the CustomType callback's surface,
			// documented read-only and read concurrently. A Root tree
			// reaching them would put a documented-writable surface on top
			// of a documented-read-only one.
			internal := map[uintptr]string{}
			collectNodeProps(s.node, "$", internal, map[*schemaNode]bool{})
			for addr, path := range first {
				if other, ok := internal[addr]; ok {
					t.Errorf("a Root tree shares %s with the schema's internal props at %s", path, other)
				}
			}
		})
	}
	if totalContainers < len(ownershipShapes) {
		t.Errorf("only %d writable containers across %d shapes; the corpus stopped producing them", totalContainers, len(ownershipShapes))
	}
}

// collectNodeProps records the containers reachable from the compiled tree's
// props — what a CustomType callback is handed.
func collectNodeProps(n *schemaNode, path string, out map[uintptr]string, seen map[*schemaNode]bool) {
	if n == nil || seen[n] {
		return
	}
	seen[n] = true
	var walk func(p string, v any)
	walk = func(p string, v any) {
		switch t := v.(type) {
		case map[string]any:
			out[reflect.ValueOf(t).Pointer()] = p
			for k, e := range t {
				walk(p+"."+k, e)
			}
		case []any:
			if len(t) > 0 {
				out[reflect.ValueOf(t).Pointer()] = p
			}
			for i, e := range t {
				walk(fmt.Sprintf("%s[%d]", p, i), e)
			}
		}
	}
	if n.props != nil {
		walk(path+".props", any(n.props))
	}
	for i := range n.fields {
		collectNodeProps(n.fields[i].node, fmt.Sprintf("%s.fields[%d]", path, i), out, seen)
	}
	collectNodeProps(n.items, path+".items", out, seen)
	collectNodeProps(n.values, path+".values", out, seen)
	for i := range n.branches {
		collectNodeProps(n.branches[i], fmt.Sprintf("%s.branches[%d]", path, i), out, seen)
	}
}

// ---------------------------------------------------------------------
// Fuzz: the same parities, over inputs nobody wrote down.
// ---------------------------------------------------------------------

func addDecodeSeeds(f *testing.F) {
	for _, cell := range decodeCorpus {
		if cell.class == "depth" {
			continue // seeds are stored verbatim; 10k brackets bloat the corpus
		}
		f.Add(cell.in)
	}
}

// FuzzSchemaDecodeParity: the decoder against the stdlib decode, in both the
// lenient and strict forms, plus the normalized tree the metadata surfaces read.
func FuzzSchemaDecodeParity(f *testing.F) {
	addDecodeSeeds(f)
	f.Fuzz(func(t *testing.T, in string) {
		wantLenient, wantOff, wantErr := oracleDecodeLenientOffset(in)
		gotLenient, gotOff, gotErr := decodeSchemaAny(in)
		if (wantErr == nil) != (gotErr == nil) {
			t.Fatalf("lenient accept/reject differs: stdlib=%v shared=%v", wantErr, gotErr)
		}
		// The consumed count is an ANSWER, not bookkeeping: it is the whole
		// of what every strict caller decides on, so a decoder landing on
		// the right value at the wrong offset would split the two callers
		// that share this decode.
		if wantErr == nil && wantOff != gotOff {
			t.Fatalf("consumed count differs: stdlib=%d shared=%d for %q", wantOff, gotOff, in)
		}
		// Parse echoes a decode error rather than replacing it, so these two
		// sentinels are part of what a caller can ask about the failure —
		// "you gave me nothing" against "you gave me a truncated schema".
		for _, sentinel := range []error{io.EOF, io.ErrUnexpectedEOF} {
			if errors.Is(wantErr, sentinel) != errors.Is(gotErr, sentinel) {
				t.Fatalf("errors.Is(%v) differs: stdlib err=%v shared err=%v", sentinel, wantErr, gotErr)
			}
		}
		if wantErr == nil && !reflect.DeepEqual(wantLenient, gotLenient) {
			t.Fatalf("lenient value differs\n stdlib %#v\n shared %#v", wantLenient, gotLenient)
		}

		wantStrict, wantErr := oracleDecodeStrict(in)
		gotStrict, gotErr := decodeSchemaAnyStrict(in)
		if (wantErr == nil) != (gotErr == nil) {
			t.Fatalf("strict accept/reject differs: stdlib=%v shared=%v", wantErr, gotErr)
		}
		if wantErr != nil {
			return
		}
		if !reflect.DeepEqual(wantStrict, gotStrict) {
			t.Fatalf("strict value differs\n stdlib %#v\n shared %#v", wantStrict, gotStrict)
		}
		if !reflect.DeepEqual(normalizeJSONValue(wantStrict), normalizeJSONValue(gotStrict)) {
			t.Fatalf("normalized value differs for %q", in)
		}
	})
}

// FuzzSchemaParseTreeParity: the whole parse tree, including the raw default
// bytes and the props map, must match the tree the stdlib decode produced.
func FuzzSchemaParseTreeParity(f *testing.F) {
	addDecodeSeeds(f)
	f.Fuzz(func(t *testing.T, in string) {
		want, wantErr := oracleParseSchemaTree(in)
		got, gotErr := parseSchemaTree(in)
		if (wantErr == nil) != (gotErr == nil) {
			t.Fatalf("accept/reject differs: stdlib=%v shared=%v", wantErr, gotErr)
		}
		if wantErr != nil {
			return
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("parse tree differs\n stdlib %#v\n shared %#v", want, got)
		}
	})
}

// FuzzSchemaParseEndToEnd: a schema that parses must still round-trip its own
// text and produce a metadata tree and canonical form.
func FuzzSchemaParseEndToEnd(f *testing.F) {
	addDecodeSeeds(f)
	f.Fuzz(func(t *testing.T, in string) {
		_, wantErr := oracleParseSchemaTree(in)
		_, gotErr := parseSchemaTree(in)
		if (wantErr == nil) != (gotErr == nil) {
			t.Fatalf("accept/reject differs: stdlib=%v shared=%v", wantErr, gotErr)
		}
		s, err := Parse(in)
		if err != nil {
			return
		}
		if got := s.String(); got != in {
			t.Fatalf("String() returned %q for input %q", got, in)
		}
		if s.Root() == nil {
			t.Fatal("Root returned nil for a schema that parsed")
		}
		_ = s.Canonical()
	})
}

// FuzzSchemaTagDefaultParity covers the struct-tag default on its own, because
// it is the one caller whose contract is not "decode this": a value that is not
// exactly one JSON value stays a verbatim string, so its accept path and its
// FALLBACK path are different answers the other sites never produce.
func FuzzSchemaTagDefaultParity(f *testing.F) {
	addDecodeSeeds(f)
	for _, s := range []string{
		"note (a", "hello", "42 oops", "", "  ", "true", "[1,2]", "-0",
		// A complete value followed by a closing bracket or brace. This is
		// the shape the old rule called complete and this one calls
		// trailing, and no other seed here ends that way — a corpus without
		// it cannot tell the two rules apart.
		"0}", "0]", "42]", `"s"}`, "[1,2]]", `{"a":1}}`, "true]", "null}",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, in string) {
		want := oracleTagDefault(in)
		got := tagDefaultValue(in)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("tag default differs for %q\n stdlib %#v\n shared %#v", in, want, got)
		}
	})
}
