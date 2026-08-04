package ocf

import (
	"bytes"
	"fmt"
	"go/ast"
	"io"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Nil has two spellings, and an option is only "supported" if BOTH of them
// behave the same on EVERY constructor.
//
// WithCodec takes an interface. A caller writes nil two ways without meaning
// anything different by them:
//
//	WithCodec(nil)               // nil interface
//	WithCodec(newMyCodec())      // non-nil interface holding a nil *myCodec,
//	                             // when newMyCodec has a concrete return type
//
// The second is the ordinary Go typed-nil shape — a constructor that returns
// *T and yields nil on a path the caller did not check. It passes c != nil.
//
// A method call on either is a crash, so the library must not make one. The
// severity a CALLER sees, however, is not uniform, and this file records that
// rather than leaving it to be rediscovered: whether an unwanted Close PANICS
// depends on the caller's own code. A Close with a pointer receiver that touches
// a field dies on a nil receiver; a Close written nil-safe returns cleanly and
// the wrong call is INVISIBLE. So a matrix built only from dereferencing codecs
// would measure "does it crash" and would pass, vacuously, against any nil-safe
// codec. The nilSafe rows exist to make the library's mistake observable
// WITHOUT a crash: they count the calls the library had no business making.
// ---------------------------------------------------------------------------

// nilSafeCloses counts Close calls that arrived on a NIL *nilSafeCodec. A nil
// receiver has no field to record into, so the counter is package level. This is
// the whole point of the type: it converts "the library called Close on a codec
// it should have ignored" from a segfault into a number, so the cell reports the
// defect on its own instead of depending on the caller's Close being fragile.
var nilSafeCloses int

type nilSafeCodec struct {
	name   string
	closes *int
}

func (c *nilSafeCodec) Name() string {
	if c == nil {
		return "nil-safe"
	}
	return c.name
}
func (c *nilSafeCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (c *nilSafeCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (c *nilSafeCodec) Close() error {
	if c == nil {
		nilSafeCloses++
		return nil
	}
	*c.closes++
	return nil
}

// derefCodec is the common shape: a pointer receiver whose Close touches the
// receiver. Calling Close on a nil one is a segfault, which is what a caller
// hitting the typed-nil trap actually experiences.
type derefCodec struct {
	name   string
	closes *int
}

func (c *derefCodec) Name() string                          { return c.name }
func (c *derefCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (c *derefCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (c *derefCodec) Close() error                          { *c.closes++; return nil }

// mapKindCodec is nil in a kind that is neither an interface nor a pointer, and
// is additionally UNCOMPARABLE — the two properties the release path's repeat
// bookkeeping branches on, since an uncomparable codec cannot be a map key and
// is therefore tracked by TYPE instead. That makes a nil and a REAL codec of
// this one type the combination where mis-recording the nil is destructive: the
// nil poisons the seen-type list and the real codec of the same type then reads
// as a repeat and is silently never closed.
//
// Close counts go to package-level counters because a nil receiver has no field
// to record into, and because the nil and non-nil values here are the same type.
type mapKindCodec map[string]string

var mapKindCloses, mapKindNilCloses int

func (c mapKindCodec) Name() string {
	if c == nil {
		return "nil-map"
	}
	return c["name"]
}
func (c mapKindCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (c mapKindCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (c mapKindCodec) Close() error {
	if c == nil {
		mapKindNilCloses++
	} else {
		mapKindCloses++
	}
	return nil
}

// nilSpelling is one way of writing a nil codec, plus the REAL codec the cell
// pairs it with and how to read that real codec's close count.
type nilSpelling struct {
	label string
	make  func() Codec
	// observable is true when an unwanted Close on the NIL is COUNTABLE rather
	// than fatal. Those rows carry the extra assertion; the fatal ones can only
	// assert that no crash happened.
	observable bool
	// nilCloses reads the count of Close calls that landed on the nil. Only
	// meaningful when observable.
	nilCloses func() int
	// makeReal builds the non-nil codec in the SAME TYPE FAMILY as the nil.
	// Pairing them is what reaches the uncomparable-type bookkeeping: a cell
	// whose real codec is always a comparable pointer cannot tell whether the
	// nil was wrongly recorded as a seen TYPE, because no real codec shares
	// that type. realCloses reads its count.
	makeReal   func(name string) Codec
	realCloses func() int
	reset      func()
}

func nilSpellings() []nilSpelling {
	derefCloses := 0
	realDeref := func(name string) Codec { return &derefCodec{name: name, closes: &derefCloses} }
	derefCount := func() int { return derefCloses }
	resetDeref := func() { derefCloses = 0 }

	return []nilSpelling{
		// The spelling the package already pinned as working, on the one
		// constructor that happened to choose by position.
		{label: "untypedNil", make: func() Codec { return nil },
			makeReal: realDeref, realCloses: derefCount, reset: resetDeref},
		// The typed-nil trap, in the receiver shape that crashes.
		{label: "typedNilDerefClose", make: func() Codec { return (*derefCodec)(nil) },
			makeReal: realDeref, realCloses: derefCount, reset: resetDeref},
		// The same trap with a nil-safe Close: no crash either way, so this
		// row's verdict rests on the nil's own count, not on survival.
		{label: "typedNilSafeClose", make: func() Codec { return (*nilSafeCodec)(nil) },
			observable: true, nilCloses: func() int { return nilSafeCloses },
			makeReal: func(name string) Codec {
				return &nilSafeCodec{name: name, closes: &nilSafeRealCloses}
			},
			realCloses: func() int { return nilSafeRealCloses },
			reset:      func() { nilSafeCloses, nilSafeRealCloses = 0, 0 }},
		// Nil in a non-pointer, uncomparable kind, paired with a REAL codec of
		// that same uncomparable type.
		{label: "typedNilMapKind", make: func() Codec { return mapKindCodec(nil) },
			observable: true, nilCloses: func() int { return mapKindNilCloses },
			makeReal:   func(name string) Codec { return mapKindCodec{"name": name} },
			realCloses: func() int { return mapKindCloses },
			reset:      func() { mapKindCloses, mapKindNilCloses = 0, 0 }},
	}
}

var nilSafeRealCloses int

// offerLayout places the nil offer among the real ones. Position is a real axis
// here: the writer chooses by POSITION (last wins) and the readers choose by
// NAME, so the same layout puts the nil in a different role on each.
type offerLayout struct {
	label string
	// nilAt lists the offer positions that get the nil spelling; every other
	// position gets the real codec.
	nilAt []int
	total int
}

func offerLayouts() []offerLayout {
	return []offerLayout{
		{label: "nilOnly", nilAt: []int{0}, total: 1},
		{label: "nilTwice", nilAt: []int{0, 1}, total: 2},
		{label: "nilFirstRealLast", nilAt: []int{0}, total: 2},
		{label: "realFirstNilLast", nilAt: []int{1}, total: 2},
		{label: "nilBetweenReals", nilAt: []int{1}, total: 3},
	}
}

// ctorRunner builds one constructor's inputs and runs it. headerCodec is the
// name written into the file the reader-side constructors open, which is what
// decides whether the real offer is adopted or declined there.
type ctorRunner struct {
	name string
	// usesHeader is true when the file's avro.codec decides adoption. The two
	// reader-side constructors read it; NewWriter chooses by position and never
	// sees one, which is why the same offer layout means different things to
	// them and why the layout axis is not redundant with the spelling axis.
	usesHeader bool
	run        func(t *testing.T, headerCodec string, opts []Opt) (io.Closer, error)
}

// TestMatrix_NilCodecOfferIgnoredEverySpelling is the class eliminator.
//
// Axes, and why each is one:
//
//	spelling  x  constructor  x  offer layout  x  reader-side adoption
//
// spelling: the axis the defect turned on and the one the report was NOT
// written in — the reported instance was a typed nil, and the untyped nil was
// already pinned as working, on one constructor.
//
// constructor: DERIVED, not listed — the set comes from codecOwningConstructors'
// go/ast walk, and the cross-check at the end fails if the source grows a
// constructor this matrix does not drive.
//
// offer layout: where the nil sits. The writer adopts by position and the
// readers adopt by name, so "the nil is last" is a superseded offer on one and
// an ordinary declined offer on the others; an all-nil layout must fall through
// to the built-in on all three.
//
// reader-side adoption: the header names the real codec or it does not, which
// crosses adopted against declined underneath every nil spelling.
//
// The oracle is not read off current behavior. WithCodec documents that a nil
// offer behaves as though it were not written, and Codec.Close is documented to
// release the codec's resources — so the two facts asserted are that the
// constructor produces a usable object (a nil offer cannot break a call that
// would otherwise work) and that the library never calls Close on a codec the
// caller did not effectively supply. The real codec's own count is the CONTROL:
// it must still be closed exactly once, so a fix that ignored everything would
// fail here.
func TestMatrix_NilCodecOfferIgnoredEverySpelling(t *testing.T) {
	var drivenCtors []string

	for _, sp := range nilSpellings() {
		for _, layout := range offerLayouts() {
			for _, headerAdopts := range []bool{true, false} {
				for _, ctor := range nilMatrixConstructors() {
					if !slices.Contains(drivenCtors, ctor.name) {
						drivenCtors = append(drivenCtors, ctor.name)
					}
					cell := fmt.Sprintf("%s/%s/%s/headerAdopts=%v",
						sp.label, ctor.name, layout.label, headerAdopts)
					t.Run(cell, func(t *testing.T) {
						runNilSpellingCell(t, sp, layout, headerAdopts, ctor)
					})
				}
			}
		}
	}

	// The constructor axis is the derived set, or this matrix is a list.
	derived := codecOwningConstructors(t)
	slices.Sort(drivenCtors)
	if !slices.Equal(derived, drivenCtors) {
		t.Errorf("matrix drives %v but the source derives %v; a codec-owning constructor "+
			"is exempt from the nil-spelling rule", drivenCtors, derived)
	}
}

func runNilSpellingCell(t *testing.T, sp nilSpelling, layout offerLayout, headerAdopts bool, ctor ctorRunner) {
	t.Helper()

	const realName = "real-codec"
	headerCodec := realName
	if !headerAdopts {
		headerCodec = "null"
	}

	sp.reset()
	real := sp.makeReal(realName)

	var opts []Opt
	nilCount := 0
	for i := range layout.total {
		if slices.Contains(layout.nilAt, i) {
			opts = append(opts, WithCodec(sp.make()))
			nilCount++
			continue
		}
		opts = append(opts, WithCodec(real))
	}
	allNil := nilCount == layout.total

	// The defect is a segfault, so the cell must catch it and REPORT rather
	// than let it kill the binary: a panicking test process cannot be told
	// apart from another panicking test process, and every neuter below has to
	// produce a red set that names its own mechanism.
	var obj io.Closer
	var err error
	func() {
		defer func() {
			if p := recover(); p != nil {
				t.Fatalf("%s panicked on a nil codec offer: %v", ctor.name, p)
			}
		}()
		obj, err = ctor.run(t, headerCodec, opts)
	}()

	// One arm legitimately fails, and it is not a nil-handling failure: every
	// offer is nil AND the file names a codec no built-in provides, so after
	// ignoring the nils there is nothing left that can decompress it. Ignoring a
	// nil offer means behaving as though it were not written, and not writing
	// WithCodec at all against such a file is an unknown-codec error. The nil
	// must not turn that diagnosis into a crash or into a silent success.
	if allNil && ctor.usesHeader && headerAdopts {
		if err == nil {
			t.Fatalf("%s accepted a file naming %q with no codec supplying it", ctor.name, realName)
		}
		if !strings.Contains(err.Error(), "unknown codec") {
			t.Errorf("%s: error %q does not name the unknown codec", ctor.name, err)
		}
		if sp.observable && sp.nilCloses() != 0 {
			t.Errorf("Close called %d time(s) on a nil codec on the error path", sp.nilCloses())
		}
		return
	}

	if err != nil {
		t.Fatalf("%s returned an error for an offer set whose only defect is a nil: %v", ctor.name, err)
	}
	if obj == nil {
		t.Fatalf("%s returned no object and no error", ctor.name)
	}

	// The library must never have called Close on the nil. Only the nil-safe
	// spelling can observe this without dying, which is exactly why it is here.
	if sp.observable && sp.nilCloses() != 0 {
		t.Errorf("Close called %d time(s) on a nil codec the caller never effectively supplied",
			sp.nilCloses())
	}

	// CONTROL: a real offer alongside a nil is still governed by the ordinary
	// rule — closed exactly once by the time the caller is done. Without this a
	// fix that simply skipped every codec would pass every cell above.
	if !allNil {
		if err := obj.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if got := sp.realCloses(); got != 1 {
			t.Errorf("real codec closed %d times, want exactly 1 "+
				"(the nil offer must not change the real one's fate)", got)
		}
		return
	}

	// All offers nil: the constructor must behave as though WithCodec were
	// never written, which means falling through to the built-in the header
	// names rather than adopting nothing and failing.
	if err := obj.Close(); err != nil {
		t.Fatalf("Close after an all-nil offer set: %v", err)
	}
	if sp.observable && sp.nilCloses() != 0 {
		t.Errorf("Close called %d time(s) on a nil codec after the object was closed",
			sp.nilCloses())
	}
}

// TestRegression_NilCodecOfferUnknownCodecRatherThanPanic pins the one arm where
// a nil offer still changes the outcome: it is the ONLY offer, and the file
// names a codec no built-in provides. Nothing can decompress that file, so the
// constructor must say so — an unknown-codec error, not a crash and not a
// silent read of nothing.
func TestRegression_NilCodecOfferUnknownCodecRatherThanPanic(t *testing.T) {
	for _, sp := range nilSpellings() {
		t.Run(sp.label, func(t *testing.T) {
			data := ocfWithHeaderCodec(t, "no-such-codec")
			var err error
			func() {
				defer func() {
					if p := recover(); p != nil {
						t.Fatalf("NewReader panicked on a nil offer: %v", p)
					}
				}()
				_, err = NewReader(bytes.NewReader(data), WithCodec(sp.make()))
			}()
			if err == nil {
				t.Fatal("NewReader accepted a file whose codec nothing supplies")
			}
			if !strings.Contains(err.Error(), "unknown codec") {
				t.Errorf("error %q does not name the unknown codec", err)
			}
		})
	}
}

// TestInvariant_NilCodecAskedThroughOnePredicate is the source-level half: the
// behavioral matrix proves the constructors agree TODAY, and this keeps them
// agreeing by construction, because the two sites that reach into a caller's
// offers must ask the same question.
//
// Derived, not listed: every non-test function that REACHES INTO a []Codec —
// ranging over one or indexing one — is handling caller-supplied offers and must
// consult isNilCodec. The set comes from the declared TYPE, so a function added
// later is caught by taking offers rather than by being remembered.
//
// Indexing counts, not just ranging, and that is not a detail: the site whose
// missing check split the constructors was NewWriter's adoption, which walks the
// offers backwards by index rather than ranging over them. A derivation that saw
// only `range` would have reported full coverage of the exact class it exists to
// catch. Functions that merely APPEND to such a slice and hand it on (the two
// reader-side constructors) are correctly outside: they delegate the question.
//
// It also rejects the specific shape the regression had: comparing a codec
// drawn from such a slice against nil with ==, which reads only one of nil's
// two spellings.
//
// Where this stops, and why: the scope is "functions ranging over a []Codec",
// which the TYPE decides, not the spelling of a name. A function handed a single
// Codec by some future helper is outside it — that is an author's scope, not
// this guard's, and the behavioral matrix above is what covers the constructors
// regardless of how they are written internally.
func TestInvariant_NilCodecAskedThroughOnePredicate(t *testing.T) {
	files, names := parsePackageFiles(t, false)

	var ranging []string
	for fi, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			// Identifiers in this function declared as []Codec, whether as a
			// parameter or as a local var.
			slices := map[string]bool{}
			collect := func(fl *ast.FieldList) {
				if fl == nil {
					return
				}
				for _, fld := range fl.List {
					if isCodecSliceType(fld.Type) {
						for _, n := range fld.Names {
							slices[n.Name] = true
						}
					}
				}
			}
			collect(fd.Type.Params)
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				vs, ok := n.(*ast.ValueSpec)
				if ok && isCodecSliceType(vs.Type) {
					for _, id := range vs.Names {
						slices[id.Name] = true
					}
				}
				return true
			})
			if len(slices) == 0 {
				continue
			}

			var reachesIntoOffers bool
			var asksPredicate bool
			var comparesToNil []string
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.Ident:
					if x.Name == "isNilCodec" {
						asksPredicate = true
					}
				case *ast.RangeStmt:
					if id, ok := x.X.(*ast.Ident); ok && slices[id.Name] {
						reachesIntoOffers = true
						if v, ok := x.Value.(*ast.Ident); ok && v.Name != "_" {
							comparesToNil = append(comparesToNil,
								nilComparisons(x.Body, v.Name)...)
						}
					}
				case *ast.IndexExpr:
					if id, ok := x.X.(*ast.Ident); ok && slices[id.Name] {
						reachesIntoOffers = true
					}
				}
				return true
			})
			if !reachesIntoOffers {
				continue
			}
			ranging = append(ranging, fd.Name.Name)
			if !asksPredicate {
				t.Errorf("%s (%s) reaches into caller-supplied codecs without asking isNilCodec",
					fd.Name.Name, names[fi])
			}
			for _, c := range comparesToNil {
				t.Errorf("%s (%s) tests a supplied codec with %s; that reads only the "+
					"interface spelling of nil — ask isNilCodec", fd.Name.Name, names[fi], c)
			}
		}
	}

	// Fails the other way too: if the derivation stops finding the sites, the
	// guard has gone blind rather than the package having gotten simpler.
	slices2 := append([]string(nil), ranging...)
	slices.Sort(slices2)
	want := []string{"NewWriter", "releaseUnadopted", "resolveCodec"}
	if !slices.Equal(slices2, want) {
		t.Errorf("derivation found %v reaching into caller-supplied codecs, want %v; "+
			"a site was added or the walk stopped seeing them", slices2, want)
	}
}

// chanCodec and funcCodec exist so the predicate's nilable-kind list is driven
// rather than read. A Codec is any type with the four methods, and Go lets that
// be a channel or a func as readily as a pointer; each is nil-able and each
// would crash the same way.
type chanCodec chan int

func (c chanCodec) Name() string                          { return "chan" }
func (c chanCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (c chanCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (c chanCodec) Close() error                          { return nil }

type funcCodec func()

func (c funcCodec) Name() string                          { return "func" }
func (c funcCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (c funcCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (c funcCodec) Close() error                          { return nil }

type sliceCodec []byte

func (c sliceCodec) Name() string                          { return "slice" }
func (c sliceCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (c sliceCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (c sliceCodec) Close() error                          { return nil }

// TestIsNilCodecAnswersEveryNilableKind drives the predicate directly, across
// every reflect kind a Codec implementation can have. The switch inside it is a
// list of kinds, and a list is only as good as the cases someone thought of —
// so the nil and non-nil value of each kind are both asked here, which is what
// makes a missing case fail rather than merely be absent.
//
// It also EXECUTES the claim the predicate's comment makes about why
// reflect.Interface is not in that list: reflect.ValueOf takes an any and
// resolves it to the dynamic value, so a Codec interface value never presents
// as Kind Interface no matter what was stored in it. Asserting that rather than
// reasoning about it is the difference between a checked fact and a plausible
// one.
func TestIsNilCodecAnswersEveryNilableKind(t *testing.T) {
	nilCases := []struct {
		kind string
		c    Codec
	}{
		{"untyped interface", nil},
		{"pointer", (*derefCodec)(nil)},
		{"map", mapKindCodec(nil)},
		{"chan", chanCodec(nil)},
		{"func", funcCodec(nil)},
		{"slice", sliceCodec(nil)},
	}
	for _, tc := range nilCases {
		if !isNilCodec(tc.c) {
			t.Errorf("isNilCodec(nil %s) = false; a method call on it would crash", tc.kind)
		}
	}

	nonNil := []struct {
		kind string
		c    Codec
	}{
		{"pointer", &derefCodec{name: "p", closes: new(int)}},
		{"map", mapKindCodec{"name": "m"}},
		{"chan", chanCodec(make(chan int))},
		{"func", funcCodec(func() {})},
		{"slice", sliceCodec{}},
		{"struct (never nilable)", nullCodec{}},
		{"struct with fields", deflateCodec{level: 1}},
	}
	for _, tc := range nonNil {
		if isNilCodec(tc.c) {
			t.Errorf("isNilCodec(non-nil %s) = true; a usable codec would be silently ignored", tc.kind)
		}
	}

	// The omitted-kind claim, executed.
	for _, tc := range append(nilCases[1:], nonNil...) {
		if k := reflect.ValueOf(tc.c).Kind(); k == reflect.Interface {
			t.Errorf("a Codec holding %s presented as Kind Interface; the predicate's "+
				"switch omits that case on the grounds it cannot happen", tc.kind)
		}
	}
}

func isCodecSliceType(e ast.Expr) bool {
	at, ok := e.(*ast.ArrayType)
	if !ok || at.Len != nil {
		return false
	}
	id, ok := at.Elt.(*ast.Ident)
	return ok && id.Name == "Codec"
}

// nilComparisons returns the source of every `name == nil` / `name != nil` in
// body, which is the exact test that misses a typed nil.
func nilComparisons(body *ast.BlockStmt, name string) []string {
	var out []string
	ast.Inspect(body, func(n ast.Node) bool {
		be, ok := n.(*ast.BinaryExpr)
		if !ok {
			return true
		}
		op := be.Op.String()
		if op != "==" && op != "!=" {
			return true
		}
		x, xok := be.X.(*ast.Ident)
		y, yok := be.Y.(*ast.Ident)
		if xok && yok && x.Name == name && y.Name == "nil" {
			out = append(out, name+" "+op+" nil")
		}
		return true
	})
	return out
}

// ocfWithHeaderCodec writes a one-datum OCF whose avro.codec metadata names
// codec, without needing an implementation of it: the file is produced with the
// null codec and the header rewritten, which is what a foreign producer's file
// looks like to this package.
func ocfWithHeaderCodec(t *testing.T, codec string) []byte {
	t.Helper()
	var buf bytes.Buffer
	c := &derefCodec{name: codec, closes: new(int)}
	w, err := NewWriter(&buf, avro.MustParse(`"long"`), WithCodec(c))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Encode(int64(1)); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

func nilMatrixConstructors() []ctorRunner {
	schema := avro.MustParse(`"long"`)
	return []ctorRunner{
		{
			name: "NewWriter",
			run: func(t *testing.T, _ string, opts []Opt) (io.Closer, error) {
				var buf bytes.Buffer
				wopts := make([]WriterOpt, len(opts))
				for i, o := range opts {
					wopts[i] = o.(WriterOpt)
				}
				return NewWriter(&buf, schema, wopts...)
			},
		},
		{
			name:       "NewReader",
			usesHeader: true,
			run: func(t *testing.T, headerCodec string, opts []Opt) (io.Closer, error) {
				data := ocfWithHeaderCodec(t, headerCodec)
				ropts := make([]ReaderOpt, len(opts))
				for i, o := range opts {
					ropts[i] = o.(ReaderOpt)
				}
				return NewReader(bytes.NewReader(data), ropts...)
			},
		},
		{
			name:       "NewAppendWriter",
			usesHeader: true,
			run: func(t *testing.T, headerCodec string, opts []Opt) (io.Closer, error) {
				data := ocfWithHeaderCodec(t, headerCodec)
				f, err := os.CreateTemp(t.TempDir(), "ocf")
				if err != nil {
					t.Fatalf("temp file: %v", err)
				}
				t.Cleanup(func() { f.Close() })
				if _, err := f.Write(data); err != nil {
					t.Fatalf("writing fixture: %v", err)
				}
				if _, err := f.Seek(0, 0); err != nil {
					t.Fatalf("seek: %v", err)
				}
				wopts := make([]WriterOpt, len(opts))
				for i, o := range opts {
					wopts[i] = o.(WriterOpt)
				}
				return NewAppendWriter(f, wopts...)
			},
		},
	}
}
