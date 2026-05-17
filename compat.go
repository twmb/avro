package avro

import (
	"fmt"
	"strings"
)

// CheckCompatibility reports whether data written with the writer schema can
// be read by the reader schema. It returns nil on success or a
// [*CompatibilityError] describing the first incompatibility.
//
// See [Resolve] for a note on argument order.
func CheckCompatibility(writer, reader *Schema) error {
	return checkCompat(reader.node, writer.node, "", make(map[nodePair]bool))
}

type nodePair struct {
	r, w *schemaNode
}

// compatErr builds a *CompatibilityError, factoring the construction
// shape repeated at every site in this file.
func compatErr(path, readerType, writerType, detail string) error {
	return &CompatibilityError{
		Path:       pathOrRoot(path),
		ReaderType: readerType,
		WriterType: writerType,
		Detail:     detail,
	}
}

func checkCompat(r, w *schemaNode, path string, seen map[nodePair]bool) error {
	if r == nil || w == nil {
		return compatErr(path, nodeKind(r), nodeKind(w), "nil schema")
	}

	pair := nodePair{r, w}
	if seen[pair] {
		return nil
	}
	seen[pair] = true

	// Writer is union: every branch must match something in the reader.
	if w.kind == "union" {
		return checkWriterUnion(r, w, path, seen)
	}
	// Reader is union: at least one branch must match the writer.
	if r.kind == "union" {
		return checkReaderUnion(r, w, path, seen)
	}

	// Same kind: recurse for complex types.
	if r.kind == w.kind {
		return checkSameKind(r, w, path, seen)
	}

	// Different kinds: check promotion.
	if promotionDeser(w.kind, r.kind) != nil {
		return nil
	}

	return compatErr(path, r.kind, w.kind, "incompatible types")
}

// checkWriterUnion validates that a writer-union schema is compatible
// with a reader schema. Every writer branch must be compatible with the
// reader (whether the reader is a union or not). The first incompatible
// branch yields an eager CompatibilityError.
//
// This is a deliberate divergence from Java's Resolver.WriterUnion and
// fastavro's read_union, both of which defer per-branch failures to
// decode time via ErrorAction sentinels — a writer who narrowed during
// evolution but never emits the dropped branch can still be consumed
// there. We choose fail-fast at resolve/CheckCompatibility time
// instead, matching the rest of this package (resolveEnum,
// resolveReaderUnion, resolveNode, validateDefault, etc., all eagerly
// reject incompatibilities). Trade-off: a "compatible-on-actual-data
// only" producer must update its schema before resolution will accept
// the pair. The benefit is that callers see schema problems before any
// data flows rather than at decode time.
func checkWriterUnion(r, w *schemaNode, path string, seen map[nodePair]bool) error {
	for i, wb := range w.branches {
		if r.kind == "union" {
			rb := findMatchingBranch(r, wb)
			if rb == nil {
				return compatErr(path, r.kind, fmt.Sprintf("union[%d]:%s", i, wb.kind),
					"writer union branch has no matching reader branch")
			}
			if err := checkCompat(rb, wb, path, seen); err != nil {
				return err
			}
			continue
		}
		if err := checkCompat(r, wb, path, seen); err != nil {
			return err
		}
	}
	return nil
}

func checkReaderUnion(r, w *schemaNode, path string, seen map[nodePair]bool) error {
	rb := findMatchingBranch(r, w)
	if rb == nil {
		return compatErr(path, "union", w.kind, "writer type matches no reader union branch")
	}
	return checkCompat(rb, w, path, seen)
}

func checkSameKind(r, w *schemaNode, path string, seen map[nodePair]bool) error {
	switch r.kind {
	case "record":
		if !namesMatch(r, w) {
			return compatErr(path, r.name, w.name, "record names do not match")
		}
		return checkRecordCompat(r, w, path, seen)
	case "enum":
		if !namesMatch(r, w) {
			return compatErr(path, r.name, w.name, "enum names do not match")
		}
		return checkEnumCompat(r, w, path)
	case "array":
		return checkCompat(r.items, w.items, path+".items", seen)
	case "map":
		return checkCompat(r.values, w.values, path+".values", seen)
	case "fixed":
		if !namesMatch(r, w) {
			return compatErr(path, r.name, w.name, "fixed names do not match")
		}
		if r.size != w.size {
			return compatErr(path,
				fmt.Sprintf("fixed(%d)", r.size),
				fmt.Sprintf("fixed(%d)", w.size),
				"fixed sizes differ")
		}
	}
	// Decimal logical types must match on precision and scale.
	if r.logical == "decimal" && w.logical == "decimal" {
		if r.precision != w.precision || r.scale != w.scale {
			return compatErr(path,
				fmt.Sprintf("decimal(%d,%d)", r.precision, r.scale),
				fmt.Sprintf("decimal(%d,%d)", w.precision, w.scale),
				"decimal precision or scale differ")
		}
	}
	return nil
}

func checkRecordCompat(r, w *schemaNode, path string, seen map[nodePair]bool) error {
	// Build writer field lookup by name.
	writerFields := make(map[string]*fieldNode, len(w.fields))
	for i := range w.fields {
		writerFields[w.fields[i].name] = &w.fields[i]
	}

	for _, rf := range r.fields {
		wf := findWriterField(rf, writerFields)
		if wf == nil {
			// Reader field not in writer: must have default.
			if !rf.hasDefault {
				return &CompatibilityError{
					Path:       fieldPath(path, rf.name),
					ReaderType: rf.node.kind,
					Detail:     "reader field has no default and is missing from writer",
				}
			}
			continue
		}
		if err := checkCompat(rf.node, wf.node, fieldPath(path, rf.name), seen); err != nil {
			return err
		}
	}
	return nil
}

func checkEnumCompat(r, w *schemaNode, path string) error {
	readerSymbols := make(map[string]bool, len(r.symbols))
	for _, s := range r.symbols {
		readerSymbols[s] = true
	}
	for _, ws := range w.symbols {
		if !readerSymbols[ws] && !r.hasEnumDef {
			return compatErr(path, r.name, w.name,
				fmt.Sprintf("writer enum symbol %q not in reader and reader has no default", ws))
		}
	}
	return nil
}

// namesMatch checks if two named types match by fully-qualified name,
// unqualified name, or aliases. Per the Avro spec, named types in
// different namespaces match if their unqualified names are the same.
func namesMatch(r, w *schemaNode) bool {
	if r.name == w.name {
		return true
	}
	if unqualified(r.name) == unqualified(w.name) {
		return true
	}
	for _, a := range r.aliases {
		if a == w.name || unqualified(a) == unqualified(w.name) {
			return true
		}
	}
	return false
}

// unqualified returns the unqualified portion of a possibly dot-separated name.
func unqualified(name string) string {
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// findWriterField finds the writer field matching a reader field by name or
// reader field aliases.
func findWriterField(rf fieldNode, writerFields map[string]*fieldNode) *fieldNode {
	if wf, ok := writerFields[rf.name]; ok {
		return wf
	}
	for _, alias := range rf.aliases {
		if wf, ok := writerFields[alias]; ok {
			return wf
		}
	}
	return nil
}

// findMatchingBranch finds the best reader union branch for the writer
// node. Three tiers, matching Java/fastavro: full-name (or alias-full-
// name) for named types beats unqualified-name match, which beats
// promotion. The unqualified-name tier preserves the lenient match that
// CheckCompatibility's simple writer-vs-reader case relies on (different
// namespaces, same logical type). Exact-match must win over it because
// the spec permits a union to contain multiple named types with the
// same unqualified name and different namespaces.
//
// Single-pass best-tier scan: equivalent to three sequential walks but
// shorter; ties resolve by declaration order.
func findMatchingBranch(r *schemaNode, w *schemaNode) *schemaNode {
	var bestTier matchTier
	var best *schemaNode
	for _, rb := range r.branches {
		if t := kindsMatchTier(rb, w); t > bestTier {
			bestTier = t
			best = rb
		}
	}
	return best
}

type matchTier int

const (
	matchNone matchTier = iota
	matchPromotion
	matchUnqualifiedName
	matchExact
)

// kindsMatchTier classifies how strongly r and w match for union-branch
// selection. matchExact = same kind plus full-name (or alias-fullname)
// for named types, OR same kind for primitives/array/map/union;
// matchUnqualifiedName = same kind, named, sharing only the unqualified
// portion (different namespaces); matchPromotion = different kinds with
// a valid Avro promotion (int→long/float/double, etc.); matchNone
// otherwise.
func kindsMatchTier(r, w *schemaNode) matchTier {
	if r.kind == w.kind {
		switch r.kind {
		case "record", "enum", "fixed":
			if r.name == w.name {
				return matchExact
			}
			for _, a := range r.aliases {
				if a == w.name {
					return matchExact
				}
			}
			if unqualified(r.name) == unqualified(w.name) {
				return matchUnqualifiedName
			}
			for _, a := range r.aliases {
				if unqualified(a) == unqualified(w.name) {
					return matchUnqualifiedName
				}
			}
			return matchNone
		default:
			return matchExact
		}
	}
	if promotionDeser(w.kind, r.kind) != nil {
		return matchPromotion
	}
	return matchNone
}


func pathOrRoot(path string) string {
	if path == "" {
		return "(root)"
	}
	return path
}

func fieldPath(parent, field string) string {
	if parent == "" {
		return field
	}
	return parent + "." + field
}

func nodeKind(n *schemaNode) string {
	if n == nil {
		return "<nil>"
	}
	return n.kind
}
