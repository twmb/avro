package ocf

import (
	"bytes"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// A constructor that adopts the caller's Codec owns it from that point on: a
// failure hands back no Writer or Reader, so there is nothing left for the
// caller to Close, and when the codec was built inline in the call —
// WithCodec(MustZstdCodec(nil, nil)), the form the doc example uses — the
// caller has no handle either. The release is therefore the constructor's, and
// the rule is uniform across every constructor that can reach it.
//
// The rule has two halves, and a codec reaches a constructor without an owner
// under either one. The codec a constructor ADOPTS and then fails on is the
// first. The codec a constructor is OFFERED and declines is the second: at most
// one offer is taken — the first whose Name matches the header for the two
// reader-side constructors, the last WithCodec written for NewWriter — and the
// constructor then SUCCEEDS, so nothing at all signals the caller that their
// codec went unused. Both halves are the same argument about ownership, so a
// row that covers one and not the other leaves a member of the set unguarded.
//
// The set of such constructors is DERIVED from source below rather than listed,
// so a constructor added later cannot quietly skip the rule; this table records
// which tests drive each member's arms.
type codecOwnerRow struct {
	// ctor is the constructor as declared in the package source.
	ctor string
	// coveredBy names the tests that assert release of the ADOPTED codec on
	// that constructor's error arms. A row whose test no longer exists fails
	// the guard.
	coveredBy []string
	// declinedCoveredBy names the tests that assert release of a supplied
	// codec the constructor did NOT adopt — the success-path half, which no
	// error-arm test can reach.
	declinedCoveredBy []string
}

var codecOwnerRows = []codecOwnerRow{
	{
		ctor:      "NewWriter",
		coveredBy: []string{"TestConstructorErrorReleasesCodec"},
		declinedCoveredBy: []string{
			"TestMatrix_SuppliedCodecClosedExactlyOnce",
			"TestRegression_OCFNewWriterReleasesSupersededCodec",
			"TestRegression_OCFNilCodecOfferIsNeverClosed",
			"TestRegression_OCFUncomparableCodecOfferReleasedOnce",
		},
	},
	{
		ctor:      "NewAppendWriter",
		coveredBy: []string{"TestConstructorErrorReleasesCodec"},
		declinedCoveredBy: []string{
			"TestMatrix_SuppliedCodecClosedExactlyOnce",
			"TestRegression_OCFAppendWriterReleasesUnmatchedCodec",
		},
	},
	{
		ctor: "NewReader",
		coveredBy: []string{
			"TestRegression_OCFNewReaderClosesCodecOnReaderSchemaFnError",
			"TestRegression_OCFNewReaderClosesCodecOnResolveError",
		},
		declinedCoveredBy: []string{
			"TestMatrix_SuppliedCodecClosedExactlyOnce",
			"TestRegression_OCFNewReaderReleasesUnmatchedCodec",
		},
	},
}

// codecOwningConstructors derives the constructor set from the package source
// in two steps, neither of which reads a name: a struct that has a field of the
// Codec interface type is codec-owning, and a top-level function returning a
// pointer to such a struct alongside an error is a constructor that can fail
// after adopting one. Asking go/ast for the shape rather than matching a "New"
// prefix keeps the derivation from depending on how a future constructor is
// spelled.
//
// Scope, stated so the next reader knows what it cannot see: the package's own
// non-test .go files. A codec-owning constructor living in another package, or
// one that hands the codec to a struct built by a helper it calls rather than
// returning it directly, is outside this derivation.
func codecOwningConstructors(t *testing.T) []string {
	t.Helper()
	files, _ := parsePackageFiles(t, false)

	owners := map[string]bool{}
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok {
				return true
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok || st.Fields == nil {
				return true
			}
			for _, fld := range st.Fields.List {
				// Named field or embedded — both hold the codec.
				if id, ok := fld.Type.(*ast.Ident); ok && id.Name == "Codec" {
					owners[ts.Name.Name] = true
				}
			}
			return true
		})
	}
	if len(owners) == 0 {
		t.Fatal("derivation found no struct holding a Codec; the walk is broken, not the package")
	}

	var ctors []string
	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Recv != nil || fd.Type.Results == nil {
				continue
			}
			var ownsResult, errResult bool
			for _, res := range fd.Type.Results.List {
				switch rt := res.Type.(type) {
				case *ast.StarExpr:
					if id, ok := rt.X.(*ast.Ident); ok && owners[id.Name] {
						ownsResult = true
					}
				case *ast.Ident:
					if rt.Name == "error" {
						errResult = true
					}
				}
			}
			if ownsResult && errResult {
				ctors = append(ctors, fd.Name.Name)
			}
		}
	}
	slices.Sort(ctors)
	return ctors
}

// parsePackageFiles parses the package's .go files, test files or not.
func parsePackageFiles(t *testing.T, tests bool) ([]*ast.File, []string) {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading package dir: %v", err)
	}
	var files []*ast.File
	var names []string
	fset := token.NewFileSet()
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") != tests {
			continue
		}
		f, err := parser.ParseFile(fset, n, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", n, err)
		}
		files = append(files, f)
		names = append(names, n)
	}
	if len(files) == 0 {
		t.Fatalf("no source files found (tests=%v)", tests)
	}
	return files, names
}

// TestCodecOwningConstructorsAreRowed fails in both directions: a constructor
// the source grows with no row, and a row naming a constructor or a covering
// test that no longer exists. Either way the release rule would be unguarded
// for some member of the set.
func TestCodecOwningConstructorsAreRowed(t *testing.T) {
	derived := codecOwningConstructors(t)

	rowed := make(map[string]codecOwnerRow, len(codecOwnerRows))
	for _, r := range codecOwnerRows {
		if _, dup := rowed[r.ctor]; dup {
			t.Errorf("duplicate row for %s", r.ctor)
		}
		rowed[r.ctor] = r
	}

	for _, c := range derived {
		if _, ok := rowed[c]; !ok {
			t.Errorf("%s returns a codec-owning value and an error but has no row: "+
				"it must release the adopted codec on every error return, and a test must assert it", c)
		}
	}
	for _, r := range codecOwnerRows {
		if !slices.Contains(derived, r.ctor) {
			t.Errorf("row names %s, which the source no longer declares as a codec-owning constructor", r.ctor)
		}
	}

	// Every covering test must exist, so deleting a pin surfaces here rather
	// than silently leaving a member undriven.
	testFiles, _ := parsePackageFiles(t, true)
	declared := map[string]bool{}
	for _, f := range testFiles {
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Recv == nil {
				declared[fd.Name.Name] = true
			}
		}
	}
	for _, r := range codecOwnerRows {
		if len(r.coveredBy) == 0 {
			t.Errorf("row %s names no test for the adopted-codec release", r.ctor)
		}
		if len(r.declinedCoveredBy) == 0 {
			t.Errorf("row %s names no test for the declined-codec release; a constructor that "+
				"succeeds without adopting a supplied codec must still close it", r.ctor)
		}
		for _, name := range slices.Concat(r.coveredBy, r.declinedCoveredBy) {
			if !declared[name] {
				t.Errorf("row %s names covering test %s, which is not declared in this package", r.ctor, name)
			}
		}
	}
}

// TestEveryCodecOfferingConstructorReleasesUnadopted is the half of the rule a
// table cannot hold: whether the SOURCE actually routes each constructor through
// the shared release. Rows record which tests drive a constructor; this asks the
// package itself which constructors take codec options and which of those call
// releaseUnadopted, and reds on any that takes offers without releasing the ones
// it declines.
//
// Deriving it this way is what makes the guard survive a constructor added
// later: the new one is caught by taking optCodec, not by being remembered. The
// two predicates are read off the constructor's own body, so extracting the
// release into a helper that the constructor calls would ALSO have to be spelled
// as a call named here — stated as the scope this cannot see, along with a
// constructor in another package and one that hands its options to a collector
// it calls rather than switching on them itself.
func TestEveryCodecOfferingConstructorReleasesUnadopted(t *testing.T) {
	files, _ := parsePackageFiles(t, false)
	derived := codecOwningConstructors(t)

	type facts struct{ offers, releases bool }
	got := map[string]facts{}
	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Recv != nil || !slices.Contains(derived, fd.Name.Name) {
				continue
			}
			var fa facts
			ast.Inspect(fd, func(n ast.Node) bool {
				id, ok := n.(*ast.Ident)
				if !ok {
					return true
				}
				switch id.Name {
				case "optCodec":
					fa.offers = true
				case "releaseUnadopted":
					fa.releases = true
				}
				return true
			})
			got[fd.Name.Name] = fa
		}
	}

	var offering int
	for _, c := range derived {
		fa, ok := got[c]
		if !ok {
			t.Errorf("derived constructor %s was not found again when reading bodies; the walk is broken", c)
			continue
		}
		if !fa.offers {
			continue
		}
		offering++
		if !fa.releases {
			t.Errorf("%s accepts WithCodec but never calls releaseUnadopted: a supplied codec it "+
				"declines is dropped with no owner, and the constructor succeeds so nothing tells "+
				"the caller", c)
		}
	}
	// A derivation that matched nothing would pass silently, which is the way
	// this kind of guard usually fails.
	if offering == 0 {
		t.Fatal("no derived constructor was found to accept codec options; the derivation is broken, not the package")
	}
}

// TestConstructorErrorReleasesCodec crosses constructor × error arm × option
// order. The expectation is not read off the code: a caller that is handed an
// error owns no closable object, so "closed exactly once" is the only state in
// which the codec's own Close contract ("releases any resources held by the
// codec") has been honored — and the success cells pin the other side, that a
// constructor which returns a usable Writer must NOT have closed the codec it
// is about to use.
//
// The option-order axis is the one the arms behave differently on: a
// reserved-key rejection raised while the option loop is still running fires
// before or after WithCodec depending on where the caller wrote it, so the
// codec is adopted in one spelling and not the other. Validating after the loop
// makes both spellings adopt, which is what makes the release uniform — and
// what makes it observable at all, since a codec that was never adopted is
// indistinguishable from a leaked one by watching the codec.
func TestConstructorErrorReleasesCodec(t *testing.T) {
	intSchema := avro.MustParse(`"int"`)
	reserved := map[string][]byte{"avro.reserved": []byte("x")}

	// A complete null-codec OCF: the append-writer arms need a header to read,
	// and its absent avro.codec key resolves to the name the observer answers.
	var valid bytes.Buffer
	vw, err := NewWriter(&valid, intSchema)
	if err != nil {
		t.Fatalf("building fixture: %v", err)
	}
	if err := vw.Encode(int32(1)); err != nil {
		t.Fatalf("building fixture: %v", err)
	}
	if err := vw.Close(); err != nil {
		t.Fatalf("building fixture: %v", err)
	}

	// failWrites(0) fails every write, so the header write fails.
	failWrites := func(n int) *failAfterNWrites { return &failAfterNWrites{n: n} }

	cells := []struct {
		ctor string
		arm  string
		// order describes where WithCodec sits among the options.
		order string
		// wantErr is false for the success cells (the boundary that must
		// still pass), true for the failure cells.
		wantErr bool
		run     func(t *testing.T, c *leakDetectCodec) (*Writer, error)
	}{
		{
			ctor: "NewWriter", arm: "header-write", order: "codec-first", wantErr: true,
			run: func(t *testing.T, c *leakDetectCodec) (*Writer, error) {
				return NewWriter(failWrites(0), intSchema, WithCodec(c))
			},
		},
		{
			ctor: "NewWriter", arm: "sync-marker", order: "codec-only", wantErr: true,
			run: func(t *testing.T, c *leakDetectCodec) (*Writer, error) {
				orig := randRead
				randRead = func([]byte) (int, error) { return 0, errors.New("synthetic rand failure") }
				defer func() { randRead = orig }()
				return NewWriter(&bytes.Buffer{}, intSchema, WithCodec(c))
			},
		},
		{
			ctor: "NewWriter", arm: "reserved-metadata-key", order: "codec-first", wantErr: true,
			run: func(t *testing.T, c *leakDetectCodec) (*Writer, error) {
				return NewWriter(&bytes.Buffer{}, intSchema, WithCodec(c), WithMetadata(reserved))
			},
		},
		{
			// The order-swapped twin of the cell above. Rejecting inside the
			// option loop leaves this spelling with an un-adopted codec, which
			// looks identical to a leak from outside; rejecting after the loop
			// gives both spellings the same release.
			ctor: "NewWriter", arm: "reserved-metadata-key", order: "codec-last", wantErr: true,
			run: func(t *testing.T, c *leakDetectCodec) (*Writer, error) {
				return NewWriter(&bytes.Buffer{}, intSchema, WithMetadata(reserved), WithCodec(c))
			},
		},
		{
			ctor: "NewAppendWriter", arm: "seek", order: "codec-only", wantErr: true,
			run: func(t *testing.T, c *leakDetectCodec) (*Writer, error) {
				return NewAppendWriter(&failSeekRWS{data: slices.Clone(valid.Bytes())}, WithCodec(c))
			},
		},
		{
			// Boundary that must still pass: a constructor that succeeds hands
			// back a usable Writer, so the codec it is about to compress with
			// must be open.
			ctor: "NewWriter", arm: "success", order: "codec-first", wantErr: false,
			run: func(t *testing.T, c *leakDetectCodec) (*Writer, error) {
				return NewWriter(&bytes.Buffer{}, intSchema, WithCodec(c))
			},
		},
		{
			ctor: "NewAppendWriter", arm: "success", order: "codec-only", wantErr: false,
			run: func(t *testing.T, c *leakDetectCodec) (*Writer, error) {
				return NewAppendWriter(&seekBuf{data: slices.Clone(valid.Bytes())}, WithCodec(c))
			},
		},
	}

	seen := map[string]bool{}
	for _, c := range cells {
		seen[c.ctor] = true
		t.Run(c.ctor+"/"+c.arm+"/"+c.order, func(t *testing.T) {
			codec := &leakDetectCodec{name: "null"}
			w, err := c.run(t, codec)
			if c.wantErr {
				if err == nil {
					t.Fatalf("%s: expected the constructor to fail", c.ctor)
				}
				if codec.closes != 1 {
					t.Fatalf("%s failed with %v but closed the adopted codec %d times, want exactly 1: "+
						"the caller has no Writer to Close and, for an inline codec, no handle at all",
						c.ctor, err, codec.closes)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", c.ctor, err)
			}
			if codec.closes != 0 {
				t.Fatalf("%s succeeded but already closed the codec %d times; the Writer still needs it",
					c.ctor, codec.closes)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if codec.closes != 1 {
				t.Fatalf("%s: Writer.Close closed the codec %d times, want exactly 1", c.ctor, codec.closes)
			}
		})
	}

	// Keep the cell set and the derived constructor set from drifting apart:
	// every constructor this test claims to cover must still be one.
	derived := codecOwningConstructors(t)
	for ctor := range seen {
		if !slices.Contains(derived, ctor) {
			t.Errorf("cells drive %s, which is no longer a codec-owning constructor", ctor)
		}
	}
}

// suppliedCodec is one WithCodec argument in a matrix cell, paired with whether
// the constructor is expected to take the offer.
type suppliedCodec struct {
	name string
	// adopted is the disposition under test: true when this codec's Name
	// matches the file header's avro.codec (reader side) or it is the last
	// WithCodec written (writer side), false when the constructor declines it.
	adopted bool
	// alias, when non-zero, means this entry is the SAME codec object as the
	// entry that many positions later: one codec offered from two positions,
	// rather than two codecs that merely answer the same name. Position and
	// identity disagree here, which is the whole point of the cell.
	alias int
	// wrapNopCloser offers the codec through NopCloser — the form WithCodec's
	// doc points a caller sharing one codec at.
	wrapNopCloser bool
	codec         *leakDetectCodec
}

// TestMatrix_SuppliedCodecClosedExactlyOnce crosses the axes a supplied codec's
// fate can turn on:
//
//	constructor  x  disposition  x  outcome  x  offer count and position
//
// constructor: every member of the derived codec-owning set, asserted against
// that derivation at the end so the two cannot drift.
//
// disposition: adopted vs declined — the axis the defect turned on, and the one
// no error-arm test reaches, since declining happens on the SUCCESS path.
//
// outcome: the constructor succeeds, fails after it has chosen a codec, or fails
// before it has chosen one at all. The three differ in how many codecs are
// unowned at the moment of return.
//
// offer count and position: one offer, and two with the adopted one first vs
// last, so "release everything except index k" is driven with k at both ends and
// with k absent entirely.
//
// The expectation is not read off the code. Codec.Close is documented to release
// the codec's resources, and a codec the caller passed to a constructor has
// exactly one moment where that can happen — so "closed exactly once by the time
// the caller is done with what the constructor returned" is the only state in
// which the contract has been honored. Twice is a defect of its own, which is why
// the count is asserted rather than a boolean. The adopted cells are the control:
// they must NOT be closed while the returned Writer or Reader is still using
// them, which is what keeps "adopted" and "declined" from being made to converge
// by a later refactor that simply releases everything.
func TestMatrix_SuppliedCodecClosedExactlyOnce(t *testing.T) {
	longSchema := avro.MustParse(`"long"`)

	// A complete OCF with no avro.codec key, so the header names "null": a
	// supplied codec named "null" is adopted, any other name is declined.
	nullFile := func(t *testing.T) []byte {
		t.Helper()
		var buf bytes.Buffer
		w, err := NewWriter(&buf, longSchema)
		if err != nil {
			t.Fatalf("building fixture: %v", err)
		}
		if err := w.Encode(int64(1)); err != nil {
			t.Fatalf("building fixture: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("building fixture: %v", err)
		}
		return buf.Bytes()
	}

	type cell struct {
		ctor string
		// desc names the disposition/outcome combination under test.
		desc string
		// supplied is in the order the options are written.
		supplied []suppliedCodec
		wantErr  bool
		// run receives the WithCodec options already built from supplied.
		run func(t *testing.T, file []byte, opts []Opt) (io.Closer, error)
	}

	// Both product types are closed through io.Closer so one assertion block
	// covers Writer and Reader without knowing which it holds.
	newWriter := func(t *testing.T, _ []byte, opts []Opt) (io.Closer, error) {
		wopts := make([]WriterOpt, len(opts))
		for i, o := range opts {
			wopts[i] = o.(WriterOpt)
		}
		return NewWriter(&bytes.Buffer{}, longSchema, wopts...)
	}

	cells := []cell{
		// ---- NewReader: header says "null" ----
		{
			ctor: "NewReader", desc: "declined/success",
			supplied: []suppliedCodec{{name: "zippy"}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith(file, opts)
			},
		},
		{
			ctor: "NewReader", desc: "adopted/success (control)",
			supplied: []suppliedCodec{{name: "null", adopted: true}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith(file, opts)
			},
		},
		{
			ctor: "NewReader", desc: "two offers, adopted first/success",
			supplied: []suppliedCodec{{name: "null", adopted: true}, {name: "zippy"}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith(file, opts)
			},
		},
		{
			ctor: "NewReader", desc: "two offers, adopted last/success",
			supplied: []suppliedCodec{{name: "zippy"}, {name: "null", adopted: true}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith(file, opts)
			},
		},
		{
			// Fails AFTER the codec is chosen: the adopted one is released by
			// the error defer, the declined one by the sweep.
			ctor: "NewReader", desc: "adopted+declined/failure after choice", wantErr: true,
			supplied: []suppliedCodec{{name: "null", adopted: true}, {name: "zippy"}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith(file, opts, WithReaderSchemaFunc(func(*Reader) (*avro.Schema, error) {
					return nil, errors.New("synthetic reader-schema failure")
				}))
			},
		},
		{
			// Fails BEFORE any codec is chosen (the mutually-exclusive
			// reader-schema options are rejected ahead of the header read), so
			// nothing is adopted and every offer must be released.
			ctor: "NewReader", desc: "none adopted/failure before choice", wantErr: true,
			supplied: []suppliedCodec{{name: "null"}, {name: "zippy"}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith(file, opts,
					WithReaderSchema(longSchema),
					WithReaderSchemaFunc(func(*Reader) (*avro.Schema, error) { return nil, nil }))
			},
		},
		{
			// A header that cannot be read fails before the choice too, on a
			// different arm.
			ctor: "NewReader", desc: "none adopted/failure on header read", wantErr: true,
			supplied: []suppliedCodec{{name: "null"}},
			run: func(t *testing.T, _ []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith([]byte("not an avro file"), opts)
			},
		},

		// ---- NewAppendWriter ----
		{
			// The same codec on both sides of the name match: the reader adopts
			// index 0 and must not release index 1, which is the same object.
			ctor: "NewReader", desc: "same codec offered twice/success",
			supplied: []suppliedCodec{{name: "null", adopted: true, alias: 1}, {name: "null", adopted: true}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith(file, opts)
			},
		},
		{
			// NopCloser is what WithCodec's doc points a sharing caller at, so
			// the declined path must leave a wrapped codec untouched — the
			// wrapper's Close is a no-op, and Name is promoted through it, so
			// the wrapped codec is still matchable afterwards.
			ctor: "NewReader", desc: "declined NopCloser/success",
			supplied: []suppliedCodec{{name: "zippy", wrapNopCloser: true}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith(file, opts)
			},
		},
		{
			// The adopted twin of the cell above, which is what proves Name is
			// promoted through the wrapper: if it were not, this codec would be
			// declined instead of adopted and the cell would be the previous one
			// over again rather than its opposite.
			ctor: "NewReader", desc: "adopted NopCloser/success",
			supplied: []suppliedCodec{{name: "null", adopted: true, wrapNopCloser: true}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newReaderWith(file, opts)
			},
		},

		// ---- NewAppendWriter ----
		{
			ctor: "NewAppendWriter", desc: "declined/success",
			supplied: []suppliedCodec{{name: "zippy"}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newAppendWith(&seekBuf{data: slices.Clone(file)}, opts)
			},
		},
		{
			ctor: "NewAppendWriter", desc: "adopted/success (control)",
			supplied: []suppliedCodec{{name: "null", adopted: true}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newAppendWith(&seekBuf{data: slices.Clone(file)}, opts)
			},
		},
		{
			ctor: "NewAppendWriter", desc: "adopted+declined/failure after choice", wantErr: true,
			supplied: []suppliedCodec{{name: "zippy"}, {name: "null", adopted: true}},
			run: func(t *testing.T, file []byte, opts []Opt) (io.Closer, error) {
				return newAppendWith(&failSeekRWS{data: slices.Clone(file)}, opts)
			},
		},
		{
			ctor: "NewAppendWriter", desc: "none adopted/failure on header read", wantErr: true,
			supplied: []suppliedCodec{{name: "null"}, {name: "zippy"}},
			run: func(t *testing.T, _ []byte, opts []Opt) (io.Closer, error) {
				return newAppendWith(&seekBuf{data: []byte("not an avro file")}, opts)
			},
		},

		// ---- NewWriter: the last WithCodec written is adopted ----
		{
			ctor: "NewWriter", desc: "adopted/success (control)",
			supplied: []suppliedCodec{{name: "null", adopted: true}},
			run:      newWriter,
		},
		{
			ctor: "NewWriter", desc: "superseded+adopted/success",
			supplied: []suppliedCodec{{name: "first"}, {name: "null", adopted: true}},
			run:      newWriter,
		},
		{
			ctor: "NewWriter", desc: "three offers, last adopted/success",
			supplied: []suppliedCodec{{name: "a"}, {name: "b"}, {name: "null", adopted: true}},
			run:      newWriter,
		},
		{
			// One codec offered twice. Position alone says index 0 is unadopted,
			// but closing it would release the very codec the returned Writer is
			// about to compress with, so identity has to beat position.
			ctor: "NewWriter", desc: "same codec offered twice/success",
			supplied: []suppliedCodec{{name: "null", adopted: true, alias: 1}, {name: "null", adopted: true}},
			run:      newWriter,
		},
		{
			// Three offers of one codec with a different codec adopted last: the
			// repeats must collapse to a single Close, not one per position.
			ctor: "NewWriter", desc: "declined codec offered twice/success",
			supplied: []suppliedCodec{{name: "a", alias: 1}, {name: "a"}, {name: "null", adopted: true}},
			run:      newWriter,
		},
		{
			ctor: "NewWriter", desc: "superseded+adopted/failure on header write", wantErr: true,
			supplied: []suppliedCodec{{name: "first"}, {name: "null", adopted: true}},
			run: func(t *testing.T, _ []byte, opts []Opt) (io.Closer, error) {
				wopts := []WriterOpt{}
				for _, o := range opts {
					wopts = append(wopts, o.(WriterOpt))
				}
				return NewWriter(&failAfterNWrites{n: 0}, longSchema, wopts...)
			},
		},
		{
			// The reserved-key arm returns after the option loop, so both the
			// superseded and the adopted codec have been collected by then.
			ctor: "NewWriter", desc: "superseded+adopted/failure on reserved key", wantErr: true,
			supplied: []suppliedCodec{{name: "first"}, {name: "null", adopted: true}},
			run: func(t *testing.T, _ []byte, opts []Opt) (io.Closer, error) {
				wopts := []WriterOpt{}
				for _, o := range opts {
					wopts = append(wopts, o.(WriterOpt))
				}
				wopts = append(wopts, WithMetadata(map[string][]byte{"avro.reserved": []byte("x")}))
				return NewWriter(&bytes.Buffer{}, longSchema, wopts...)
			},
		},
	}

	file := nullFile(t)
	seen := map[string]bool{}
	for _, c := range cells {
		seen[c.ctor] = true
		t.Run(c.ctor+"/"+c.desc, func(t *testing.T) {
			supplied := slices.Clone(c.supplied)
			// Two passes so an aliased entry can point at an object built for a
			// later position.
			for i := range supplied {
				if supplied[i].alias == 0 {
					supplied[i].codec = &leakDetectCodec{name: supplied[i].name}
				}
			}
			for i := range supplied {
				if k := supplied[i].alias; k != 0 {
					supplied[i].codec = supplied[i+k].codec
				}
			}
			opts := make([]Opt, 0, len(supplied))
			for _, s := range supplied {
				var c Codec = s.codec
				if s.wrapNopCloser {
					c = NopCloser(c)
				}
				opts = append(opts, WithCodec(c))
			}

			// Expectations are per DISTINCT codec object, not per offer: one
			// codec offered from several positions is closed once or not at
			// all, never once per position. An object counts as adopted if any
			// of its positions was.
			type objExp struct {
				name              string
				adopted, shielded bool
			}
			objs := map[*leakDetectCodec]*objExp{}
			var order []*leakDetectCodec
			for _, s := range supplied {
				e, ok := objs[s.codec]
				if !ok {
					e = &objExp{name: s.name}
					objs[s.codec] = e
					order = append(order, s.codec)
				}
				e.adopted = e.adopted || s.adopted
				// A NopCloser at ANY position shields the object: the wrapper
				// absorbs the Close instead of forwarding it.
				e.shielded = e.shielded || s.wrapNopCloser
			}
			check := func(when string, want func(*objExp) int) {
				t.Helper()
				for _, obj := range order {
					e := objs[obj]
					if got := obj.closes; got != want(e) {
						t.Errorf("%s codec %q closed %d times %s, want %d",
							dispositionOf(e.adopted, e.shielded), e.name, got, when, want(e))
					}
				}
			}

			product, err := c.run(t, file, opts)

			if c.wantErr {
				if err == nil {
					t.Fatalf("expected the constructor to fail")
				}
				// The caller was handed no closable object, so every codec it
				// passed must already be released — shielded ones excepted,
				// since NopCloser is exactly a refusal to be released.
				check("after a failed constructor", func(e *objExp) int {
					if e.shielded {
						return 0
					}
					return 1
				})
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Before Close: a declined codec is already released (nothing will
			// ever use it), an adopted one must still be open.
			check("right after a successful constructor", func(e *objExp) int {
				if e.adopted || e.shielded {
					return 0
				}
				return 1
			})

			if err := product.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			check("after Close", func(e *objExp) int {
				if e.shielded {
					return 0
				}
				return 1
			})
		})
	}

	derived := codecOwningConstructors(t)
	for ctor := range seen {
		if !slices.Contains(derived, ctor) {
			t.Errorf("cells drive %s, which is no longer a codec-owning constructor", ctor)
		}
	}
	for _, c := range derived {
		if !seen[c] {
			t.Errorf("%s is a codec-owning constructor with no cell in this matrix", c)
		}
	}
}

func dispositionOf(adopted, shielded bool) string {
	switch {
	case shielded:
		return "NopCloser-shielded"
	case adopted:
		return "adopted"
	}
	return "declined"
}

// newReaderWith turns the cell's WithCodec options (which satisfy both option
// interfaces) into reader options and appends any reader-only ones the cell adds
// to drive a particular error arm.
func newReaderWith(file []byte, opts []Opt, extra ...ReaderOpt) (io.Closer, error) {
	ropts := make([]ReaderOpt, 0, len(opts)+len(extra))
	for _, o := range opts {
		ropts = append(ropts, o.(ReaderOpt))
	}
	ropts = append(ropts, extra...)
	return NewReader(bytes.NewReader(file), ropts...)
}

func newAppendWith(rws io.ReadWriteSeeker, opts []Opt) (io.Closer, error) {
	wopts := make([]WriterOpt, len(opts))
	for i, o := range opts {
		wopts[i] = o.(WriterOpt)
	}
	return NewAppendWriter(rws, wopts...)
}

// The three reported instances, pinned individually so each reads on its own
// next to the matrix that decides its whole class. Each is a constructor that
// SUCCEEDS: the caller gets a usable Writer or Reader and no indication that the
// codec they passed was never used, so the leak is invisible from the call site.

// TestRegression_OCFNewReaderReleasesUnmatchedCodec: the file header names
// "null" and the supplied codec answers a different name, so the reader resolves
// the built-in and the supplied one is never used by anything.
func TestRegression_OCFNewReaderReleasesUnmatchedCodec(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, avro.MustParse(`"long"`))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Encode(int64(7)); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	codec := &leakDetectCodec{name: "zippy"}
	rd, err := NewReader(bytes.NewReader(buf.Bytes()), WithCodec(codec))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if codec.closes != 1 {
		t.Fatalf("unmatched codec closed %d times, want exactly 1: NewReader resolved the "+
			"built-in null codec and nothing else will ever close this one", codec.closes)
	}
	// The reader must still work, and must not close the supplied codec a
	// second time on the way out.
	var got int64
	if err := rd.Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != 7 {
		t.Fatalf("decoded %d, want 7", got)
	}
	if err := rd.Close(); err != nil {
		t.Fatalf("Reader.Close: %v", err)
	}
	if codec.closes != 1 {
		t.Fatalf("unmatched codec closed %d times after Reader.Close, want exactly 1", codec.closes)
	}
}

// TestRegression_OCFAppendWriterReleasesUnmatchedCodec: same shape on the append
// path, where the codec name likewise comes from the header already on disk.
func TestRegression_OCFAppendWriterReleasesUnmatchedCodec(t *testing.T) {
	schema := avro.MustParse(`"long"`)
	var buf bytes.Buffer
	w, err := NewWriter(&buf, schema)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Encode(int64(1)); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	codec := &leakDetectCodec{name: "zippy"}
	aw, err := NewAppendWriter(&seekBuf{data: buf.Bytes()}, WithCodec(codec))
	if err != nil {
		t.Fatalf("NewAppendWriter: %v", err)
	}
	if codec.closes != 1 {
		t.Fatalf("unmatched codec closed %d times, want exactly 1", codec.closes)
	}
	if err := aw.Encode(int64(2)); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := aw.Close(); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}
	if codec.closes != 1 {
		t.Fatalf("unmatched codec closed %d times after Writer.Close, want exactly 1", codec.closes)
	}
}

// TestRegression_OCFNewWriterReleasesSupersededCodec: WithCodec written twice.
// The last one wins, which leaves the first adopted by nothing — and unlike the
// reader cases there is no name involved, so the only thing that distinguishes
// the two is their position in the option list.
func TestRegression_OCFNewWriterReleasesSupersededCodec(t *testing.T) {
	first := &leakDetectCodec{name: "null"}
	last := &leakDetectCodec{name: "null"}

	w, err := NewWriter(&bytes.Buffer{}, avro.MustParse(`"long"`), WithCodec(first), WithCodec(last))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if first.closes != 1 {
		t.Fatalf("superseded codec closed %d times, want exactly 1", first.closes)
	}
	if last.closes != 0 {
		t.Fatalf("adopted codec closed %d times before the Writer was used, want 0", last.closes)
	}
	if err := w.Encode(int64(1)); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if first.closes != 1 || last.closes != 1 {
		t.Fatalf("after Close: superseded closed %d times, adopted closed %d times, want 1 and 1",
			first.closes, last.closes)
	}
}

// TestRegression_OCFNilCodecOfferIsNeverClosed pins the one shape where a
// release would be a method call on a nil interface. WithCodec(nil) compiles,
// and when a later WithCodec supersedes it the constructor succeeds — the nil
// never reaches a call site — so this call works and must keep working. Closing
// the superseded offer without checking would turn a working call into a panic.
//
// Only the superseded position is asserted. A nil codec that is ADOPTED is a
// nil-method call in writeHeader (writer side) and in the name scan of
// resolveCodec (reader side), both of which are reached before any release and
// are not what this pins.
func TestRegression_OCFNilCodecOfferIsNeverClosed(t *testing.T) {
	real := &leakDetectCodec{name: "null"}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, avro.MustParse(`"long"`), WithCodec(nil), WithCodec(real))
	if err != nil {
		t.Fatalf("NewWriter with a superseded nil codec: %v", err)
	}
	if real.closes != 0 {
		t.Fatalf("adopted codec closed %d times by a successful constructor, want 0", real.closes)
	}
	if err := w.Encode(int64(1)); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if real.closes != 1 {
		t.Fatalf("adopted codec closed %d times after Close, want exactly 1", real.closes)
	}
}

// mapCodec is deliberately UNCOMPARABLE: a struct holding a map cannot be
// compared with ==, so a Codec interface value carrying one panics any direct
// equality test and cannot be a map key. Nothing forbids such a codec — the
// interface only asks for four methods — so the release path has to recognize
// repeats of it without ever comparing it the easy way.
type mapCodec struct {
	name   string
	notes  map[string]string
	closes *int
}

func (c mapCodec) Name() string                          { return c.name }
func (c mapCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (c mapCodec) Decompress(src []byte) ([]byte, error) { return src, nil }
func (c mapCodec) Close() error                          { *c.closes++; return nil }

// TestRegression_OCFUncomparableCodecOfferReleasedOnce drives the arm the
// comparable fast path cannot reach. The counts are the same rule as everywhere
// else — each distinct supplied codec closed exactly once unless it was adopted
// — but reaching it here requires recognizing a repeat WITHOUT the equality
// operator, so a release path that reached for == would panic instead of
// answering.
func TestRegression_OCFUncomparableCodecOfferReleasedOnce(t *testing.T) {
	schema := avro.MustParse(`"long"`)
	mk := func(name string, n *int) mapCodec {
		return mapCodec{name: name, notes: map[string]string{"k": "v"}, closes: n}
	}

	t.Run("declined, offered twice", func(t *testing.T) {
		var declined int
		c := mk("zippy", &declined)
		w, err := NewWriter(&bytes.Buffer{}, schema,
			WithCodec(c), WithCodec(c), WithCodec(&leakDetectCodec{name: "null"}))
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		if declined != 1 {
			t.Fatalf("uncomparable codec offered twice closed %d times, want exactly 1", declined)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if declined != 1 {
			t.Fatalf("uncomparable codec closed %d times after Close, want exactly 1", declined)
		}
	})

	t.Run("adopted, offered twice", func(t *testing.T) {
		var n int
		c := mk("null", &n)
		w, err := NewWriter(&bytes.Buffer{}, schema, WithCodec(c), WithCodec(c))
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		if n != 0 {
			t.Fatalf("adopted uncomparable codec closed %d times by a successful constructor, want 0: "+
				"the Writer is about to compress with it", n)
		}
		if err := w.Encode(int64(1)); err != nil {
			t.Fatalf("Encode: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if n != 1 {
			t.Fatalf("adopted uncomparable codec closed %d times after Close, want exactly 1", n)
		}
	})

	t.Run("declined next to a comparable codec", func(t *testing.T) {
		var un int
		comparable := &leakDetectCodec{name: "deflate"}
		w, err := NewWriter(&bytes.Buffer{}, schema,
			WithCodec(mk("zippy", &un)), WithCodec(comparable), WithCodec(&leakDetectCodec{name: "null"}))
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		if un != 1 || comparable.closes != 1 {
			t.Fatalf("mixed offers: uncomparable closed %d times, comparable closed %d times, want 1 and 1",
				un, comparable.closes)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
}
