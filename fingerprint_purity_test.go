package avro

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"crypto/sha512"
	"go/ast"
	"go/parser"
	"go/token"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"os"
	"strings"
	"testing"
)

// A fingerprint is the digest of a schema's canonical form, so its value must
// depend on the schema and the algorithm alone. Nothing else a caller did with
// the hash beforehand may reach the answer: not a previous fingerprint taken
// with the same hash, and not bytes the caller wrote into it directly.
//
// The expectation is not read off Fingerprint. Each cell's oracle is a FRESH
// hash of the same algorithm fed the schema's canonical form — the definition
// of the digest, computed without the code under test.
func TestFingerprintIsAFunctionOfTheSchemaAlone(t *testing.T) {
	algos := []struct {
		name string
		mk   func() hash.Hash
	}{
		{"rabin", func() hash.Hash { return NewRabin() }},
		{"sha256", sha256.New},
		{"sha512", sha512.New},
		{"md5", md5.New},
		{"crc32-ieee", func() hash.Hash { return crc32.NewIEEE() }},
		{"crc64-ecma", func() hash.Hash { return crc64.New(crc64.MakeTable(crc64.ECMA)) }},
	}

	schemas := []struct {
		name string
		text string
	}{
		{"short", `"int"`},
		// Longer than every block size above, so the accumulated state spans
		// more than one compression block and a partial reset would show.
		{"multi-block", `{"type":"record","name":"com.example.Wide","fields":[` +
			`{"name":"alpha","type":"string"},{"name":"bravo","type":"long"},` +
			`{"name":"charlie","type":{"type":"array","items":"double"}},` +
			`{"name":"delta","type":{"type":"map","values":"bytes"}},` +
			`{"name":"echo","type":["null","string"]}]}`},
		{"recursive", `{"type":"record","name":"N","fields":[` +
			`{"name":"v","type":"int"},{"name":"next","type":["null","N"]}]}`},
	}

	// Prior states a caller's hash can be in when it reaches Fingerprint.
	priors := []struct {
		name string
		put  func(t *testing.T, h hash.Hash, self, other *Schema)
	}{
		{"fresh", func(*testing.T, hash.Hash, *Schema, *Schema) {}},
		{"after fingerprinting the same schema", func(t *testing.T, h hash.Hash, self, _ *Schema) {
			self.Fingerprint(h)
		}},
		{"after fingerprinting a different schema", func(t *testing.T, h hash.Hash, _, other *Schema) {
			other.Fingerprint(h)
		}},
		{"caller wrote bytes into it", func(t *testing.T, h hash.Hash, _, _ *Schema) {
			h.Write([]byte("caller's own payload"))
		}},
		{"caller wrote, then reset", func(t *testing.T, h hash.Hash, _, _ *Schema) {
			h.Write([]byte("caller's own payload"))
			h.Reset()
		}},
		{"fingerprinted twice over", func(t *testing.T, h hash.Hash, self, other *Schema) {
			self.Fingerprint(h)
			other.Fingerprint(h)
		}},
	}

	// A schema distinct from every cell's own, for the cross-contamination
	// priors.
	other := MustParse(`{"type":"enum","name":"Contaminant","symbols":["X","Y","Z"]}`)

	for _, a := range algos {
		for _, sc := range schemas {
			s := MustParse(sc.text)

			// Oracle: a fresh hash of this algorithm over the canonical form.
			oracle := a.mk()
			oracle.Write(s.Canonical())
			want := oracle.Sum(nil)

			for _, p := range priors {
				t.Run(a.name+"/"+sc.name+"/"+p.name, func(t *testing.T) {
					h := a.mk()
					p.put(t, h, s, other)
					got := s.Fingerprint(h)
					if !bytes.Equal(got, want) {
						t.Fatalf("fingerprint depends on the hash's prior state: got %x, want %x", got, want)
					}
					// The digest must also still be readable from the hash
					// after the call: callers take Sum64 off the hash they
					// passed, so the accumulated state stays put. Clearing on
					// the way out would buy determinism and break that.
					if after := h.Sum(nil); !bytes.Equal(after, want) {
						t.Fatalf("hash state after the call: got %x, want %x", after, want)
					}
				})
			}
		}
	}
}

// TestFingerprintRepeatsOnOneHash is the property in its plainest form: the
// same hash handed to the same schema twice answers the same thing.
func TestFingerprintRepeatsOnOneHash(t *testing.T) {
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	h := NewRabin()
	first := bytes.Clone(s.Fingerprint(h))
	second := s.Fingerprint(h)
	if !bytes.Equal(first, second) {
		t.Fatalf("reused hash gave %x then %x", first, second)
	}
}

// hashTakingAPIs rows every exported entry point that accepts a caller-owned
// hash. Such a parameter is a mutable accumulator the caller may have used, so
// each one owes the same purity rule; a new one must be rowed and driven.
var hashTakingAPIs = []string{"Schema.Fingerprint"}

// TestHashTakingAPIsAreRowed derives the set from source rather than trusting
// the list: any exported function or method with a parameter from the hash
// package is one. Fails in both directions — an unrowed entry point, and a row
// naming one the source no longer has.
func TestHashTakingAPIsAreRowed(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading package dir: %v", err)
	}
	fset := token.NewFileSet()
	var derived []string
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, n, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", n, err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || !fd.Name.IsExported() || fd.Type.Params == nil {
				continue
			}
			takesHash := false
			for _, p := range fd.Type.Params.List {
				// hash.Hash, hash.Hash32, hash.Hash64 — any type from the
				// hash package, however the parameter is spelled.
				if sel, ok := p.Type.(*ast.SelectorExpr); ok {
					if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "hash" {
						takesHash = true
					}
				}
			}
			if !takesHash {
				continue
			}
			name := fd.Name.Name
			if fd.Recv != nil && len(fd.Recv.List) > 0 {
				name = recvTypeName(fd.Recv.List[0].Type) + "." + name
			}
			derived = append(derived, name)
		}
	}
	if len(derived) == 0 {
		t.Fatal("derivation found no hash-taking entry point; the walk is broken, not the package")
	}

	rowed := map[string]bool{}
	for _, r := range hashTakingAPIs {
		rowed[r] = true
	}
	seen := map[string]bool{}
	for _, d := range derived {
		seen[d] = true
		if !rowed[d] {
			t.Errorf("%s takes a caller-owned hash but has no row: its result must not depend on "+
				"what the caller did with that hash beforehand, and a test must assert it", d)
		}
	}
	for _, r := range hashTakingAPIs {
		if !seen[r] {
			t.Errorf("row names %s, which the source no longer declares as a hash-taking entry point", r)
		}
	}
}

// recvTypeName renders a method receiver's type name, pointer or not.
func recvTypeName(e ast.Expr) string {
	if star, ok := e.(*ast.StarExpr); ok {
		e = star.X
	}
	if id, ok := e.(*ast.Ident); ok {
		return id.Name
	}
	return "?"
}
