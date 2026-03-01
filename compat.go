package avro

import "fmt"

// CheckCompatibility checks whether data written with the writer schema can be
// read using the reader schema, following Avro's schema resolution rules. It
// returns nil if the schemas are compatible. If not, it returns a
// [*CompatibilityError] (extractable via [errors.As]) describing the first
// incompatibility found, including the path within the schema tree.
//
// Compatibility rules: record fields present in the reader but missing from
// the writer must have defaults; named types (records, enums, fixed) must
// match by name or alias; enum symbols in the writer but not the reader
// require the reader to have a default; fixed types must have equal sizes;
// different primitive types must be promotable (e.g. int to long).
func CheckCompatibility(reader, writer *Schema) error {
	return checkCompat(reader.node, writer.node, "", make(map[nodePair]bool))
}

type nodePair struct {
	r, w *schemaNode
}

func checkCompat(r, w *schemaNode, path string, seen map[nodePair]bool) error {
	if r == nil || w == nil {
		return &CompatibilityError{Path: pathOrRoot(path), ReaderType: nodeKind(r), WriterType: nodeKind(w), Detail: "nil schema"}
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

	return &CompatibilityError{
		Path:       pathOrRoot(path),
		ReaderType: r.kind,
		WriterType: w.kind,
		Detail:     "incompatible types",
	}
}

func checkWriterUnion(r, w *schemaNode, path string, seen map[nodePair]bool) error {
	if r.kind == "union" {
		// Both writer and reader are unions: every writer branch must
		// match a reader branch by kind, then recurse for deep check.
		for i, wb := range w.branches {
			rb := findMatchingBranch(r, wb)
			if rb == nil {
				return &CompatibilityError{
					Path:       pathOrRoot(path),
					ReaderType: r.kind,
					WriterType: fmt.Sprintf("union[%d]:%s", i, wb.kind),
					Detail:     "writer union branch has no matching reader branch",
				}
			}
			if err := checkCompat(rb, wb, path, seen); err != nil {
				return err
			}
		}
	} else {
		// Writer is union, reader is not: every branch must match reader.
		for _, wb := range w.branches {
			if err := checkCompat(r, wb, path, seen); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkReaderUnion(r, w *schemaNode, path string, seen map[nodePair]bool) error {
	rb := findMatchingBranch(r, w)
	if rb == nil {
		return &CompatibilityError{
			Path:       pathOrRoot(path),
			ReaderType: "union",
			WriterType: w.kind,
			Detail:     "writer type matches no reader union branch",
		}
	}
	return checkCompat(rb, w, path, seen)
}

func checkSameKind(r, w *schemaNode, path string, seen map[nodePair]bool) error {
	switch r.kind {
	case "record":
		if !namesMatch(r, w) {
			return &CompatibilityError{
				Path:       pathOrRoot(path),
				ReaderType: r.name,
				WriterType: w.name,
				Detail:     "record names do not match",
			}
		}
		return checkRecordCompat(r, w, path, seen)
	case "enum":
		if !namesMatch(r, w) {
			return &CompatibilityError{
				Path:       pathOrRoot(path),
				ReaderType: r.name,
				WriterType: w.name,
				Detail:     "enum names do not match",
			}
		}
		return checkEnumCompat(r, w, path)
	case "array":
		return checkCompat(r.items, w.items, path+".items", seen)
	case "map":
		return checkCompat(r.values, w.values, path+".values", seen)
	case "fixed":
		if !namesMatch(r, w) {
			return &CompatibilityError{
				Path:       pathOrRoot(path),
				ReaderType: r.name,
				WriterType: w.name,
				Detail:     "fixed names do not match",
			}
		}
		if r.size != w.size {
			return &CompatibilityError{
				Path:       pathOrRoot(path),
				ReaderType: fmt.Sprintf("fixed(%d)", r.size),
				WriterType: fmt.Sprintf("fixed(%d)", w.size),
				Detail:     "fixed sizes differ",
			}
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
					WriterType: "",
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
			return &CompatibilityError{
				Path:       pathOrRoot(path),
				ReaderType: r.name,
				WriterType: w.name,
				Detail:     fmt.Sprintf("writer enum symbol %q not in reader and reader has no default", ws),
			}
		}
	}
	return nil
}

// namesMatch checks if two named types match by name or aliases.
func namesMatch(r, w *schemaNode) bool {
	if r.name == w.name {
		return true
	}
	for _, a := range r.aliases {
		if a == w.name {
			return true
		}
	}
	return false
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

// findMatchingBranch finds the first reader union branch that matches the writer node.
func findMatchingBranch(r *schemaNode, w *schemaNode) *schemaNode {
	for _, rb := range r.branches {
		if kindsMatch(rb, w) {
			return rb
		}
	}
	return nil
}

// kindsMatch checks if two schema nodes could be compatible (same kind, or promotable, or name-matched).
func kindsMatch(r, w *schemaNode) bool {
	if r.kind == w.kind {
		switch r.kind {
		case "record", "enum", "fixed":
			return namesMatch(r, w)
		default:
			return true
		}
	}
	return promotionDeser(w.kind, r.kind) != nil
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
