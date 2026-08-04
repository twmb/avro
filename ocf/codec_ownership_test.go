package ocf

import (
	"bytes"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
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
// The set of such constructors is DERIVED from source below rather than listed,
// so a constructor added later cannot quietly skip the rule; this table records
// which tests drive each member's failure arms.
type codecOwnerRow struct {
	// ctor is the constructor as declared in the package source.
	ctor string
	// coveredBy names the tests that assert release on that constructor's
	// error arms. A row whose test no longer exists fails the guard.
	coveredBy []string
}

var codecOwnerRows = []codecOwnerRow{
	{ctor: "NewWriter", coveredBy: []string{"TestConstructorErrorReleasesCodec"}},
	{ctor: "NewAppendWriter", coveredBy: []string{"TestConstructorErrorReleasesCodec"}},
	{ctor: "NewReader", coveredBy: []string{
		"TestRegression_OCFNewReaderClosesCodecOnReaderSchemaFnError",
		"TestRegression_OCFNewReaderClosesCodecOnResolveError",
	}},
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
		for _, name := range r.coveredBy {
			if !declared[name] {
				t.Errorf("row %s names covering test %s, which is not declared in this package", r.ctor, name)
			}
		}
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
