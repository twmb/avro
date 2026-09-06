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
	"time"
)

// ---------------------------------------------------------------------
// The oracle: encoding/json, executed rather than restated.
//
// Every twin below is the code the corresponding production site ran before
// one decoder replaced five hand-spelled ones. We keep them here, and only
// here, so the matrix compares against a reference outside this package. The
// alternative is comparing against whatever the decoder currently does.
// ---------------------------------------------------------------------

func oracleDecodeLenient(schema string) (any, error) {
	v, _, err := oracleDecodeLenientOffset(schema)
	return v, err
}

// oracleDecodeLenientOffset also reports where the stdlib decoder stopped. That
// gives the consumed count the shared decoder returns an independent answer to
// check against, rather than leaving it merely self-consistent.
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
// Unlike the other twins here it is NOT a copy of what the site used to do.
// This is the one site whose behavior changed. It used to ask
// json.Decoder.More whether anything followed, and More answers `c != ']' &&
// c != '}'`, so it called `42]` a complete value and discarded the bracket.
// A twin copying that would assert the behavior the change removed.
//
// So we state the rule independently on this side: decode one value, then
// require the rest of the text to be whitespace, using the decoder's own
// offset accounting (InputOffset) rather than the consumed count the
// implementation computes. That keeps the oracle answerable from stdlib alone,
// and it still crosses the arithmetic the implementation uses to find the same
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
// is what the liveness floor counts. An axis that stops being generated reds
// instead of quietly emptying.
type decodeCell struct {
	name  string
	class string
	in    string
}

// decodeCorpus spans the input axis: every JSON shape a schema can be written
// in, crossed with the boundaries where a decoder can silently differ from the
// stdlib one it replaced. The number rows matter most; see [decodeSchemaAny]
// for the two silent failures a resolving decoder causes.
var decodeCorpus = func() []decodeCell {
	cells := []decodeCell{
		// Numbers: syntax forms whose literal must survive the decode.
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
	// Raw invalid UTF-8. A decode repairs it one replacement rune per byte
	// rather than per run, a distinction no escape can express, so we build
	// it here rather than writing it as a literal.
	cells = append(cells,
		decodeCell{"invalid-utf8-single", "string", "{\"type\":\"int\",\"p\":\"\xa8\"}"},
		decodeCell{"invalid-utf8-run", "string", "{\"type\":\"int\",\"p\":\"\xa8\xa8\xa8\"}"},
		decodeCell{"invalid-utf8-in-key", "string", "{\"type\":\"int\",\"\xa8\":1}"},
		decodeCell{"truncated-utf8", "string", "{\"type\":\"int\",\"p\":\"\xe4\xb8\"}"},
	)
	// The nesting boundary, which no mutation-driven corpus reaches: the
	// decoder accepts exactly the depth the stdlib decoder accepted.
	//
	// The innermost value is an axis of its own, not a detail. An empty
	// innermost container agrees with the stdlib decode whatever the bound is
	// charged per, value or container, because there is no leaf to spend the
	// last unit on. Only a *non-empty* innermost separates the two, so a
	// ladder of empty containers can run at every depth and still measure
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
// matrix a class net rather than a decoder net. Every caller re-enters the
// decoder's contract, and a caller with no cell is a route on which we prove
// nothing. [TestCensus_SchemaJSONDecodeCallSites] keeps this list equal to the
// set the source actually contains.
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
		// already accepted. Feeding it the whole corpus asks more of
		// it than production does, which is the point. A decoder that
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
// class with every caller of the shared decoder. Each caller must land exactly
// where the stdlib decode it replaced landed: same value, same accept/reject
// verdict.
//
// The oracle is encoding/json, executed here rather than described, so no cell
// passes by agreeing with the decoder about something they are both wrong
// about. The site axis is the one the mechanism turns on. We prove a single
// decoder serving six callers on the callers, not on itself.
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
// depends on: every number the shared decoder emits is the author's literal,
// byte for byte. Answered from the input rather than from a sibling decoder.
//
// The parity matrix would also red if a number were resolved here, but only for
// as long as its twin stays stdlib. So we state the rule with no second
// implementation to agree with, because the two failures it prevents are
// silent. A re-marshal of a resolved "-0" loses the sign a float default
// encodes. A re-marshal of a resolved long literal is short enough to walk past
// the length cap that refuses the literal. Both produce wrong bytes and no
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
// the enclosing function. [TestCensus_SchemaJSONDecodeCallSites] derives the
// set from source rather than trusting this list, and we compare the derivation
// against the list. A site appearing or disappearing is then a decision made
// here rather than a silent change.
var schemaDecodeCallers = map[string]string{
	"parseSchemaTree":               "schema_parse.go",
	"unmarshalAnyPreservePrecision": "schema.go",
	"unmarshalDefault":              "schema.go",
	"cacheNormalizeSchema":          "cache.go",
	"tagDefaultValue":               "schema_for.go",
	"decodeSchemaAnyStrict":         "schema_decode.go", // the strict wrapper calls the lenient form
}

// TestCensus_SchemaJSONDecodeCallSites derives the shared decoder's callers
// from the package source. They must be exactly the set the parity matrix
// exercises. It reds in both directions: a new caller that no cell covers fails
// here, and a caller that disappears fails here too, so the guard cannot go
// stale by watching code that is gone.
//
// We also require that no source file reconstruct the decode this replaced. A
// json.Decoder put into UseNumber mode *is* the old hand-spelled site. Five of
// them had already drifted into three different trailing-content rules, so a
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
// Two answers rather than one is the whole point. "Nothing was passed" and "a
// truncated schema was passed" are different mistakes. A decoder that collapsed
// them, or that reported its own error type for both, would take a distinction
// away from callers without anything failing.
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
// decode error. The text it decodes is the exact text a parse already accepted,
// through the same decoder, so the panic is unreachable rather than merely
// unlikely.
//
// It used to be unreachable for a weaker reason. Root's decode was the lenient
// one, accepting a superset of what the parse accepted, so the two agreeing was
// a coincidence of two spellings. Now they are one function, and the property
// is that a decoder is deterministic and carries no state between calls.
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
// The accept set is chosen, not inherited. This site used to ask
// json.Decoder.More whether anything followed the value, and More answers
// `c != ']' && c != '}'`, so a body of `42]` reported nothing-follows, the
// bracket was silently discarded, and the field got the number 42. The two
// sites that decode a whole schema asked a different question (a second decode
// must reach EOF) and rejected the same text. That was a divergence between two
// spellings of one rule, not a decision, and one of them threw away input
// without saying so.
//
// Discarding is the worse answer, so all of them now reject. `42]` is not the
// number 42, it is a body that is not JSON, and it takes the same fallback as
// `hello`. On a typed field that fallback then fails validation, which is the
// point: we get an error instead of a silently different default.
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
// an author actually types. The fallback is only half the story: on a typed
// field the verbatim string then has to survive Avro validation, and it does
// not. A body of `42]` used to build a schema with the long default 42.
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
// through, including the second-occurrence reference paths (a self-reference,
// and a diamond where one definition is reached twice), because a tree sharing
// a container with itself would share it across calls too.
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
// contract: the tree handed back is the caller's alone. Two calls share no map
// and no slice either could write through. Neither shares one with the schema's
// own internals, the props a [CustomType] callback reads while other goroutines
// encode.
//
// The contract is not new, but what makes it true moved. The schema decoder
// hands back substrings of the schema text where a reflect-driven decode
// allocated fresh strings, so what a returned tree shares with the text it came
// from became a live question. A string cannot be written through, which is why
// we count only the containers.
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
// props, which is what a CustomType callback is handed.
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
		// The consumed count is an answer, not bookkeeping: it is the whole
		// of what every strict caller decides on, so a decoder landing on
		// the right value at the wrong offset would split the two callers
		// that share this decode.
		if wantErr == nil && wantOff != gotOff {
			t.Fatalf("consumed count differs: stdlib=%d shared=%d for %q", wantOff, gotOff, in)
		}
		// Parse echoes a decode error rather than replacing it, so these two
		// sentinels are part of what a caller can ask about the failure:
		// "nothing was given" against "a truncated schema was given".
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
// it is the one caller whose contract is not "decode this". A value that is not
// exactly one JSON value stays a verbatim string, so its accept path and its
// fallback path are different answers the other sites never produce.
func FuzzSchemaTagDefaultParity(f *testing.F) {
	addDecodeSeeds(f)
	for _, s := range []string{
		"note (a", "hello", "42 oops", "", "  ", "true", "[1,2]", "-0",
		// A complete value followed by a closing bracket or brace. This is
		// the shape the old rule called complete and this one calls
		// trailing, and no other seed here ends that way; a corpus without
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

////////////////////////
// EXPAND REFERENCES  //
////////////////////////

// expandCountNodes counts every SchemaNode in a tree, descending the same
// structure ExpandReferences copies.
func expandCountNodes(n *SchemaNode, depth int) int {
	if n == nil || depth > maxSchemaJSONDepth {
		return 0
	}
	c := 1
	c += expandCountNodes(n.Items, depth+1)
	c += expandCountNodes(n.Values, depth+1)
	for i := range n.Fields {
		c += expandCountNodes(&n.Fields[i].Type, depth+1)
	}
	for i := range n.Branches {
		c += expandCountNodes(&n.Branches[i], depth+1)
	}
	return c
}

// expandJSON renders a node for comparison. json.Marshal walks the exported
// fields only, which is exactly the surface a caller can see.
func expandJSON(t *testing.T, n *SchemaNode) string {
	t.Helper()
	b, err := json.Marshal(n)
	if err != nil {
		t.Fatalf("marshaling node: %v", err)
	}
	return string(b)
}

// TestExpandReferencesRepeatedType: every occurrence of a repeated named type
// carries the full body, wherever the occurrence sits.
func TestExpandReferencesRepeatedType(t *testing.T) {
	s := mustParse(t, `{"type":"record","name":"Top","namespace":"ns","fields":[
		{"name":"a","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}},
		{"name":"b","type":"ns.Inner"},
		{"name":"c","type":{"type":"array","items":"Inner"}},
		{"name":"d","type":{"type":"map","values":"ns.Inner"}},
		{"name":"e","type":["null","Inner"]}]}`)
	e := s.Root().ExpandReferences()

	full := func(n *SchemaNode) bool {
		return n != nil && n.Type == "record" && n.Name == "Inner" &&
			n.Namespace == "ns" && len(n.Fields) == 1 && n.Fields[0].Name == "x"
	}
	for _, c := range []struct {
		name string
		node *SchemaNode
	}{
		{"a (the definition)", &e.Fields[0].Type},
		{"b (a plain reference)", &e.Fields[1].Type},
		{"c (an array item)", e.Fields[2].Type.Items},
		{"d (a map value)", e.Fields[3].Type.Values},
		{"e (a union branch)", &e.Fields[4].Type.Branches[1]},
	} {
		if !full(c.node) {
			t.Errorf("%s: not expanded: %s", c.name, expandJSON(t, c.node))
		}
	}
}

// TestExpandReferencesCyclesStayReferences: expanding a recursive definition
// does not terminate, so the edge that closes the cycle keeps its reference.
func TestExpandReferencesCyclesStayReferences(t *testing.T) {
	t.Run("self", func(t *testing.T) {
		s := mustParse(t, `{"type":"record","name":"N","fields":[
			{"name":"next","type":["null","N"]},
			{"name":"v","type":"int"}]}`)
		e := s.Root().ExpandReferences()
		back := e.Fields[0].Type.Branches[1]
		if back.Type != "N" || len(back.Fields) != 0 {
			t.Errorf("self back-reference expanded: %s", expandJSON(t, &back))
		}
		if len(e.Fields) != 2 || e.Fields[1].Type.Type != "int" {
			t.Errorf("the rest of the record was lost: %s", expandJSON(t, e))
		}
	})
	t.Run("mutual", func(t *testing.T) {
		// A and B name each other, so both are on the cycle and neither
		// expands anywhere. Expanding whichever copy is not yet on the path
		// would give one B the recursive body and another the expanded one,
		// and Schema reads two same-named bodies that differ as a conflict.
		s := mustParse(t, `{"type":"record","name":"Top","fields":[
			{"name":"x","type":{"type":"record","name":"A","fields":[
				{"name":"b","type":{"type":"record","name":"B","fields":[
					{"name":"a","type":["null","A"]}]}}]}},
			{"name":"y","type":"B"}]}`)
		e := s.Root().ExpandReferences()
		inA := e.Fields[0].Type.Fields[0].Type // B, inside A
		backA := inA.Fields[0].Type.Branches[1]
		if backA.Type != "A" || len(backA.Fields) != 0 {
			t.Errorf("the cycle-closing reference to A expanded: %s", expandJSON(t, &backA))
		}
		// B does not close the cycle, so it expands, and every copy of it has
		// to come out identical, which is the whole reason the verdict is per
		// name rather than per position.
		atY := e.Fields[1].Type
		if atY.Type != "record" || atY.Name != "B" {
			t.Fatalf("B did not expand: %s", expandJSON(t, &atY))
		}
		if a, b := expandJSON(t, &inA), expandJSON(t, &atY); a != b {
			t.Errorf("two copies of B differ:\n %s\n %s", a, b)
		}
		if _, err := e.Schema(); err != nil {
			t.Errorf("mutually recursive expansion does not rebuild: %v", err)
		}
	})
}

// TestExpandReferencesDoesNotMutateReceiver: the receiver is unchanged, and
// nothing in the result shares a container with it.
func TestExpandReferencesDoesNotMutateReceiver(t *testing.T) {
	s := mustParse(t, `{"type":"record","name":"Top","namespace":"ns","doc":"d","aliases":["Alt"],"extra":1,"fields":[
		{"name":"a","type":{"type":"enum","name":"E","symbols":["X","Y"],"extra":2},"aliases":["aa"],"fx":3},
		{"name":"b","type":"ns.E"},
		{"name":"c","type":{"type":"array","items":"E"}}]}`)
	r := s.Root()
	before := expandJSON(t, r)
	e := r.ExpandReferences()
	if after := expandJSON(t, r); after != before {
		t.Errorf("receiver changed:\n before %s\n after  %s", before, after)
	}

	// We write through every container the result hands back; none of it may
	// reach the receiver either.
	if n := expandMutateAll(e, 0); n < 8 {
		t.Fatalf("only %d containers to write through; the cell is not reaching the result's structure", n)
	}
	if after := expandJSON(t, r); after != before {
		t.Errorf("writing to the result reached the receiver:\n before %s\n after  %s", before, after)
	}
}

// expandMutateAll writes through every container in n's tree, returning how
// many it wrote to. Written as a walk rather than a list of paths, so it keeps
// reaching the containers whatever the expansion produced.
func expandMutateAll(n *SchemaNode, depth int) int {
	if n == nil || depth > maxSchemaJSONDepth {
		return 0
	}
	c := 0
	for i := range n.Aliases {
		n.Aliases[i], c = "MUTATED", c+1
	}
	for i := range n.Symbols {
		n.Symbols[i], c = "MUTATED", c+1
	}
	for k := range n.Props {
		n.Props[k], c = "MUTATED", c+1
	}
	c += expandMutateAll(n.Items, depth+1)
	c += expandMutateAll(n.Values, depth+1)
	for i := range n.Fields {
		for j := range n.Fields[i].Aliases {
			n.Fields[i].Aliases[j], c = "MUTATED", c+1
		}
		for k := range n.Fields[i].Props {
			n.Fields[i].Props[k], c = "MUTATED", c+1
		}
		n.Fields[i].Name, c = "MUTATED", c+1
		c += expandMutateAll(&n.Fields[i].Type, depth+1)
	}
	for i := range n.Branches {
		c += expandMutateAll(&n.Branches[i], depth+1)
	}
	return c
}

// expandRoundTripSchemas are the shapes ExpandReferences must leave a schema's
// meaning untouched for.
var expandRoundTripSchemas = []struct {
	name   string
	schema string
	// fullDiffers marks the cases where the rebuilt text differs from the
	// unexpanded rebuild in reference spelling alone: Schema re-spells a
	// collapsed repeat by fullname, so a source reference written as an
	// in-scope short name comes back qualified. The canonical comparison,
	// which normalizes both to the fullname, still runs.
	fullDiffers bool
}{
	{"repeat", `{"type":"record","name":"Top","fields":[{"name":"a","type":{"type":"record","name":"I","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":"I"}]}`, false},
	{"namespaced", `{"type":"record","name":"Top","namespace":"a.b","fields":[{"name":"a","type":{"type":"record","name":"I","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":"a.b.I"},{"name":"c","type":"I"}]}`, true},
	{"null-namespace-escape", `{"type":"record","name":"Top","namespace":"a.b","fields":[{"name":"a","type":{"type":"record","name":"I","namespace":"","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":".I"}]}`, true},
	{"cross-namespace", `{"type":"record","name":"Top","namespace":"a","fields":[{"name":"a","type":{"type":"record","name":"I","namespace":"c.d","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":"c.d.I"}]}`, false},
	{"recursive", `{"type":"record","name":"N","fields":[{"name":"n","type":["null","N"]},{"name":"v","type":"int"}]}`, false},
	{"mutual", `{"type":"record","name":"A","fields":[{"name":"b","type":{"type":"record","name":"B","fields":[{"name":"a","type":["null","A"]}]}},{"name":"b2","type":"B"}]}`, false},
	{"enum-and-fixed", `{"type":"record","name":"Top","fields":[{"name":"a","type":{"type":"enum","name":"E","symbols":["X"]}},{"name":"b","type":"E"},{"name":"c","type":{"type":"fixed","name":"F","size":4}},{"name":"d","type":"F"}]}`, false},
	{"wrapped-reference", `{"type":"record","name":"Top","fields":[{"name":"a","type":{"type":"record","name":"I","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":{"type":"I","tag":"keep"}}]}`, false},
	{"defaults", `{"type":"record","name":"Top","fields":[{"name":"a","type":{"type":"record","name":"I","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":"I","default":{"x":1}}]}`, false},
	{"props-and-docs", `{"type":"record","name":"Top","doc":"","aliases":[],"fields":[{"name":"a","type":{"type":"record","name":"I","doc":"hi","fields":[{"name":"x","type":"int","doc":""}]}},{"name":"b","type":"I"}]}`, false},
	{"array-of-map-of-ref", `{"type":"record","name":"Top","fields":[{"name":"a","type":{"type":"record","name":"I","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":{"type":"array","items":{"type":"map","values":"I"}}}]}`, false},
	{"no-references", `{"type":"record","name":"Top","fields":[{"name":"x","type":"int"},{"name":"y","type":{"type":"array","items":"string"}}]}`, false},
	{"primitive", `"int"`, false},
	{"union-top", `["null",{"type":"record","name":"I","fields":[{"name":"x","type":"int"}]},{"type":"record","name":"J","fields":[{"name":"i","type":"I"}]}]`, false},
	{"deep-chain", `{"type":"record","name":"R2","fields":[{"name":"a","type":{"type":"record","name":"R1","fields":[{"name":"a","type":{"type":"record","name":"R0","fields":[{"name":"v","type":"int"}]}},{"name":"b","type":"R0"}]}},{"name":"b","type":"R1"}]}`, false},
}

// TestExpandReferencesRoundTrips: Schema collapses repeats back to references
// on emit, so expanding first must land on exactly the same schema.
func TestExpandReferencesRoundTrips(t *testing.T) {
	for _, c := range expandRoundTripSchemas {
		t.Run(c.name, func(t *testing.T) {
			s := mustParse(t, c.schema)
			e := s.Root().ExpandReferences()
			got, err := e.Schema()
			if err != nil {
				t.Fatalf("rebuilding the expanded tree: %v", err)
			}
			if string(got.Canonical()) != string(s.Canonical()) {
				t.Errorf("canonical form changed:\n got  %s\n want %s", got.Canonical(), s.Canonical())
			}
			// The full form too: canonical drops docs, props and defaults,
			// so on its own it would not notice an expansion that lost
			// them. We compare against the tree rebuilt *without*
			// expanding, so the difference measured is the expansion
			// and not Schema's own re-emission of the source text.
			plain, err := s.Root().Schema()
			if err != nil {
				t.Fatalf("rebuilding the unexpanded tree: %v", err)
			}
			if same := got.String() == plain.String(); same == c.fullDiffers {
				t.Errorf("full form: same=%v, want same=%v\n got  %s\n want %s",
					same, !c.fullDiffers, got.String(), plain.String())
			}
		})
	}
}

// expandDoublingSchema names one record per level, each holding its
// predecessor once as a definition and once as a reference. The text grows
// linearly and the fully expanded tree grows as 2^levels.
func expandDoublingSchema(levels int) string {
	s := `{"type":"record","name":"R0","fields":[{"name":"v","type":"int"}]}`
	for i := 1; i <= levels; i++ {
		s = fmt.Sprintf(`{"type":"record","name":"R%d","fields":[{"name":"a","type":%s},{"name":"b","type":"R%d"}]}`,
			i, s, i-1)
	}
	return s
}

// TestExpandReferencesIsBounded: a schema whose full expansion is over the
// ceiling comes back copied but NOT expanded. Stopping partway is not an
// option. A half-expanded copy of a name conflicts with the whole one and
// Schema refuses the tree, so the verdict is all or nothing.
func TestExpandReferencesIsBounded(t *testing.T) {
	// 2^20 expanded nodes against a 2^18 ceiling, from ~40 lines of text.
	s := mustParse(t, expandDoublingSchema(20))
	r := s.Root()
	in := expandCountNodes(r, 0)
	if in > 500 {
		t.Fatalf("the input itself is %d nodes; this cell measures expansion, not input size", in)
	}
	if got := expandCountNodes(s.Root().ExpandReferences(), 0); got != in {
		t.Errorf("expanded to %d nodes; over the ceiling the copy must stay at the input's %d", got, in)
	}
	// Still a valid tree: what was not expanded is what it already was.
	if _, err := s.Root().ExpandReferences().Schema(); err != nil {
		t.Errorf("the unexpanded copy no longer rebuilds: %v", err)
	}

	// The same shape under the ceiling expands in full, so the cell measures
	// the ceiling and not a blanket refusal to expand.
	small := mustParse(t, expandDoublingSchema(10))
	sr := small.Root()
	se := sr.ExpandReferences()
	sin, sout := expandCountNodes(sr, 0), expandCountNodes(se, 0)
	if sout <= sin {
		t.Errorf("a %d-node tree whose full expansion fits the ceiling came back at %d nodes", sin, sout)
	}
	if sout > maxExpandedNodes {
		t.Errorf("expanded to %d nodes, over the %d ceiling", sout, maxExpandedNodes)
	}
	if _, err := se.Schema(); err != nil {
		t.Errorf("the fully expanded tree does not rebuild: %v", err)
	}
}

// TestExpandReferencesSizeSaturates: the ceiling is decided from a count, and
// that count saturates. A doubling chain reaches 2^40 in forty lines of text.
// A sum that kept adding would be judged on a number the walk cannot hold, and
// nothing is built to find out.
func TestExpandReferencesSizeSaturates(t *testing.T) {
	r := mustParse(t, expandDoublingSchema(40)).Root()
	x := &expander{
		table:  map[string]*SchemaNode{},
		cyclic: map[string]bool{},
		onPath: map[string]bool{},
		done:   map[string]bool{},
		sizes:  map[string]int{},
	}
	collectNamedTypes(r, x.table)
	x.markCycles(r, "", 0)
	if got := x.sizeOf(r, "", 0); got != maxExpandedNodes+1 {
		t.Errorf("sized the 2^40 expansion at %d, want the saturation value %d", got, maxExpandedNodes+1)
	}
	// And a tree that fits is sized exactly, so saturation is not just "always
	// return the cap".
	small := mustParse(t, expandDoublingSchema(3)).Root()
	y := &expander{
		table:  map[string]*SchemaNode{},
		cyclic: map[string]bool{},
		onPath: map[string]bool{},
		done:   map[string]bool{},
		sizes:  map[string]int{},
	}
	collectNamedTypes(small, y.table)
	y.markCycles(small, "", 0)
	y.expand = true
	if got, want := y.sizeOf(small, "", 0), expandCountNodes(small.ExpandReferences(), 0); got != want {
		t.Errorf("sized the expansion at %d, but it built %d nodes", got, want)
	}
}

// TestExpandReferencesDeepTree: nesting past the supported limit stops the
// walk rather than the stack, and everything above the limit is still copied.
func TestExpandReferencesDeepTree(t *testing.T) {
	deep := &SchemaNode{Type: "int"}
	for range maxSchemaJSONDepth + 200 {
		deep = &SchemaNode{Type: "array", Items: deep}
	}
	done := make(chan *SchemaNode, 1)
	go func() { done <- deep.ExpandReferences() }()
	var got *SchemaNode
	select {
	case got = <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("ExpandReferences did not return on a deeply nested tree")
	}
	a, b := got, deep
	for i := range 100 {
		if a == nil || a.Type != "array" || a.Items == nil {
			t.Fatalf("level %d: got %v, want an array", i, a)
		}
		if a == b || a.Items == b.Items {
			t.Fatalf("level %d shares a node with the receiver", i)
		}
		a, b = a.Items, b.Items
	}
}

// TestExpandReferencesExtractedSubtree: a subtree lifted out of a Root tree
// carries the stamp Root left on its references. So it expands even though the
// definition lives outside it, the same resolution Schema splices with.
func TestExpandReferencesExtractedSubtree(t *testing.T) {
	s := mustParse(t, `{"type":"record","name":"Top","namespace":"ns","fields":[
		{"name":"a","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}},
		{"name":"b","type":"Inner"}]}`)
	sub := s.Root().Fields[1].Type // just the reference node
	if sub.Type != "ns.Inner" && sub.Type != "Inner" {
		t.Fatalf("expected a reference node, got %s", expandJSON(t, &sub))
	}
	e := sub.ExpandReferences()
	if e.Type != "record" || e.Name != "Inner" || len(e.Fields) != 1 {
		t.Fatalf("extracted reference did not expand: %s", expandJSON(t, e))
	}
	if _, err := e.Schema(); err != nil {
		t.Errorf("expanded extraction does not rebuild: %v", err)
	}
}

// TestExpandReferencesWrappedReferenceKept: a reference carrying usage-site
// attributes stays as written. A definition cannot hold a second doc, and
// Schema would collapse the expanded copy back to a reference and lose the
// custom properties that rode on it.
func TestExpandReferencesWrappedReferenceKept(t *testing.T) {
	s := mustParse(t, `{"type":"record","name":"Top","fields":[
		{"name":"a","type":{"type":"record","name":"I","fields":[{"name":"x","type":"int"}]}},
		{"name":"b","type":{"type":"I","tag":"keep"}}]}`)
	e := s.Root().ExpandReferences()
	b := e.Fields[1].Type
	if b.Type != "I" || len(b.Fields) != 0 {
		t.Errorf("wrapped reference expanded: %s", expandJSON(t, &b))
	}
	if b.Props["tag"] != "keep" {
		t.Errorf("wrapped reference lost its property: %s", expandJSON(t, &b))
	}
}

func TestExpandReferencesNil(t *testing.T) {
	var n *SchemaNode
	if got := n.ExpandReferences(); got != nil {
		t.Errorf("got %v, want nil", got)
	}
}
