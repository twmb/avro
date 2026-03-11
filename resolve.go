package avro

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sync"
)

// Resolve returns a schema that decodes data written with the writer schema
// and produces values matching the reader schema's layout. The writer schema
// is what the data was encoded with (typically from an OCF file header or
// a schema registry); the reader schema is what your application expects now.
//
// Decoding with the returned schema handles field addition (defaults), field
// removal (skip), renaming (aliases), reordering, and type promotion.
// Encoding with it uses the reader's format.
//
// If the schemas have identical canonical forms, reader is returned as-is.
// Otherwise [CheckCompatibility] is called first and any incompatibility is
// returned as a [*CompatibilityError]. See the package-level documentation
// for a full example.
//
// Note: the argument order is (writer, reader), matching source-then-destination
// convention and Java's GenericDatumReader. This differs from the Avro spec
// text and hamba/avro, which put reader first.
func Resolve(writer, reader *Schema) (*Schema, error) {
	if bytes.Equal(reader.Canonical(), writer.Canonical()) {
		return reader, nil
	}
	if err := CheckCompatibility(writer, reader); err != nil {
		return nil, err
	}
	resolved, err := resolveNode(reader.node, writer.node, "", make(map[nodePair]*schemaNode))
	if err != nil {
		return nil, err
	}
	s := &Schema{
		ser:   reader.ser,
		deser: resolved.deser,
		c:     reader.c,
		node:  reader.node,
		full:  reader.full,
	}
	s.soe = reader.soe
	return s, nil
}

// resolvedRecord holds the compiled resolution between a reader and writer record.
type resolvedRecord struct {
	readerNames []string
	wireOps     []wireOp
	defaults    []defaultOp
	cache       sync.Map
}

// wireOp describes how to handle a single writer field during deserialization.
type wireOp struct {
	readerIdx int     // index in the reader's field list; -1 means skip
	read      deserfn // non-nil when readerIdx >= 0
	skip      skipfn  // non-nil when readerIdx == -1
}

// defaultOp fills in a reader field that is absent from the writer.
type defaultOp struct {
	readerIdx      int
	encodedDefault []byte
	deser          deserfn
}

// resolveNode resolves a (reader, writer) schema pair, handling cycles
// from self-referencing records (e.g. a linked list node). The three
// states in the seen map are:
//   - absent: not yet visited — proceed with resolution
//   - nil:    in-progress — a recursive call hit this pair, creating a cycle
//   - *node:  resolved — reuse the result
//
// On cycle detection, we create a placeholder node whose deser is a
// trampoline closure (calls n.deser through the pointer). After the
// real resolution completes, we copy the resolved node's contents into
// the placeholder so all holders of the placeholder pointer get the
// real implementation.
func resolveNode(r, w *schemaNode, path string, seen map[nodePair]*schemaNode) (*schemaNode, error) {
	pair := nodePair{r, w}
	if n, ok := seen[pair]; ok {
		if n == nil {
			// Cycle detected: create a placeholder with a
			// trampoline deser that will forward to the real
			// deserfn once resolution completes.
			n = &schemaNode{}
			n.deser = func(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
				return n.deser(src, v, sl)
			}
			seen[pair] = n
		}
		return n, nil
	}
	seen[pair] = nil // mark as in-progress

	resolved, err := doResolve(r, w, path, seen)
	if err != nil {
		return nil, err
	}

	// If a placeholder was created during cycle detection, copy the
	// resolved contents into it so the trampoline now calls the real deser.
	if placeholder := seen[pair]; placeholder != nil && placeholder != resolved {
		*placeholder = *resolved
		resolved = placeholder
	}
	seen[pair] = resolved
	return resolved, nil
}

func doResolve(r, w *schemaNode, path string, seen map[nodePair]*schemaNode) (*schemaNode, error) {
	// Writer union: unwrap if reader is not a union.
	if w.kind == "union" && r.kind != "union" {
		return resolveWriterUnion(r, w, path, seen)
	}
	// Reader union: wrap.
	if r.kind == "union" && w.kind != "union" {
		return resolveReaderUnion(r, w, path, seen)
	}
	// Both unions.
	if r.kind == "union" && w.kind == "union" {
		return resolveUnionUnion(r, w, path, seen)
	}

	// Same kind.
	if r.kind == w.kind {
		switch r.kind {
		case "record":
			return resolveRecord(r, w, path, seen)
		case "enum":
			return resolveEnum(r, w)
		case "array":
			return resolveArray(r, w, path, seen)
		case "map":
			return resolveMap(r, w, path, seen)
		case "fixed":
			return r, nil // names and sizes already verified by CheckCompatibility
		default:
			// Same primitive: use reader directly.
			return r, nil
		}
	}

	// Type promotion.
	pd := promotionDeser(w.kind, r.kind)
	if pd != nil {
		return &schemaNode{
			kind:  r.kind,
			ser:   r.ser,
			deser: pd,
		}, nil
	}

	return nil, &CompatibilityError{
		Path:       pathOrRoot(path),
		ReaderType: r.kind,
		WriterType: w.kind,
		Detail:     "incompatible types",
	}
}

func resolveRecord(r, w *schemaNode, path string, seen map[nodePair]*schemaNode) (*schemaNode, error) {
	// Build writer field lookup.
	type writerFieldInfo struct {
		idx  int
		node *fieldNode
	}
	writerByName := make(map[string]writerFieldInfo, len(w.fields))
	for i := range w.fields {
		writerByName[w.fields[i].name] = writerFieldInfo{i, &w.fields[i]}
	}

	rr := &resolvedRecord{
		readerNames: make([]string, len(r.fields)),
	}
	for i, rf := range r.fields {
		rr.readerNames[i] = rf.name
	}

	// Track which reader fields are matched.
	readerMatched := make([]bool, len(r.fields))

	// For each writer field (in wire order), find matching reader field.
	for _, wf := range w.fields {
		ri := findReaderFieldIndex(r, wf.name)
		if ri < 0 {
			// Writer field not in reader: skip it.
			rr.wireOps = append(rr.wireOps, wireOp{
				readerIdx: -1,
				skip:      buildSkip(wf.node),
			})
			continue
		}
		readerMatched[ri] = true
		rf := &r.fields[ri]
		resolved, err := resolveNode(rf.node, wf.node, fieldPath(path, rf.name), seen)
		if err != nil {
			return nil, err
		}
		rr.wireOps = append(rr.wireOps, wireOp{
			readerIdx: ri,
			read:      resolved.deser,
		})
	}

	// For unmatched reader fields, encode defaults.
	for i, rf := range r.fields {
		if readerMatched[i] {
			continue
		}
		encoded, err := encodeDefault(rf.defaultVal, rf.node)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", fieldPath(path, rf.name), err)
		}
		rr.defaults = append(rr.defaults, defaultOp{
			readerIdx:      i,
			encodedDefault: encoded,
			deser:          rf.node.deser,
		})
	}

	nd := &schemaNode{
		kind:        "record",
		name:        r.name,
		aliases:     r.aliases,
		fields:      r.fields,
		ser:         r.ser,
		deser:       rr.buildDeser(),
		serRecord:   r.serRecord,
		deserRecord: r.deserRecord,
	}
	return nd, nil
}

// findReaderFieldIndex finds a writer field name in reader fields by name or
// reader field aliases.
func findReaderFieldIndex(r *schemaNode, writerFieldName string) int {
	for i, rf := range r.fields {
		if rf.name == writerFieldName {
			return i
		}
	}
	for i, rf := range r.fields {
		if slices.Contains(rf.aliases, writerFieldName) {
			return i
		}
	}
	return -1
}

func (rr *resolvedRecord) buildDeser() deserfn {
	return func(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
		v = indirectAlloc(v)
		k := v.Kind()

		if k == reflect.Interface {
			return rr.deserInterface(src, v, sl)
		}
		t := v.Type()
		if k == reflect.Map && t.Key().Kind() == reflect.String {
			return rr.deserMap(src, v, t, sl)
		}
		if k == reflect.Struct {
			return rr.deserStruct(src, v, t, sl)
		}
		return nil, &SemanticError{GoType: t, AvroType: "record"}
	}
}

func (rr *resolvedRecord) deserInterface(src []byte, v reflect.Value, sl *slab) ([]byte, error) {
	m := make(map[string]any, len(rr.readerNames))
	var err error

	// Process wire fields.
	for _, op := range rr.wireOps {
		if op.readerIdx < 0 {
			if src, err = op.skip(src); err != nil {
				return nil, err
			}
			continue
		}
		elem := reflect.New(anyType).Elem()
		if src, err = op.read(src, elem, sl); err != nil {
			return nil, &SemanticError{AvroType: "record", Field: rr.readerNames[op.readerIdx], Err: err}
		}
		m[rr.readerNames[op.readerIdx]] = elem.Interface()
	}

	// Apply defaults.
	for _, d := range rr.defaults {
		elem := reflect.New(anyType).Elem()
		if _, err = d.deser(append([]byte(nil), d.encodedDefault...), elem, sl); err != nil {
			return nil, &SemanticError{AvroType: "record", Field: rr.readerNames[d.readerIdx], Err: err}
		}
		m[rr.readerNames[d.readerIdx]] = elem.Interface()
	}

	v.Set(reflect.ValueOf(m))
	return src, nil
}

func (rr *resolvedRecord) deserMap(src []byte, v reflect.Value, t reflect.Type, sl *slab) ([]byte, error) {
	if v.IsNil() {
		v.Set(reflect.MakeMap(t))
	}
	var err error

	for _, op := range rr.wireOps {
		if op.readerIdx < 0 {
			if src, err = op.skip(src); err != nil {
				return nil, err
			}
			continue
		}
		elem := reflect.New(t.Elem()).Elem()
		if src, err = op.read(src, elem, sl); err != nil {
			return nil, &SemanticError{AvroType: "record", Field: rr.readerNames[op.readerIdx], Err: err}
		}
		v.SetMapIndex(reflect.ValueOf(rr.readerNames[op.readerIdx]), elem)
	}

	for _, d := range rr.defaults {
		elem := reflect.New(t.Elem()).Elem()
		if _, err = d.deser(append([]byte(nil), d.encodedDefault...), elem, sl); err != nil {
			return nil, &SemanticError{AvroType: "record", Field: rr.readerNames[d.readerIdx], Err: err}
		}
		v.SetMapIndex(reflect.ValueOf(rr.readerNames[d.readerIdx]), elem)
	}

	return src, nil
}

func (rr *resolvedRecord) deserStruct(src []byte, v reflect.Value, t reflect.Type, sl *slab) ([]byte, error) {
	mapping, err := typeFieldMapping(rr.readerNames, &rr.cache, t)
	if err != nil {
		return nil, err
	}

	for _, op := range rr.wireOps {
		if op.readerIdx < 0 {
			if src, err = op.skip(src); err != nil {
				return nil, err
			}
			continue
		}
		fv := fieldByIndex(v, mapping.indices[op.readerIdx])
		if src, err = op.read(src, fv, sl); err != nil {
			return nil, &SemanticError{GoType: t, AvroType: "record", Field: rr.readerNames[op.readerIdx], Err: err}
		}
	}

	for _, d := range rr.defaults {
		fv := fieldByIndex(v, mapping.indices[d.readerIdx])
		if _, err = d.deser(append([]byte(nil), d.encodedDefault...), fv, sl); err != nil {
			return nil, &SemanticError{GoType: t, AvroType: "record", Field: rr.readerNames[d.readerIdx], Err: err}
		}
	}

	return src, nil
}

func resolveEnum(r, w *schemaNode) (*schemaNode, error) {
	// Build writer symbol index → reader symbol index mapping.
	readerIdx := make(map[string]int, len(r.symbols))
	for i, s := range r.symbols {
		readerIdx[s] = i
	}

	identity := len(r.symbols) == len(w.symbols)
	mapping := make([]int, len(w.symbols))
	for i, ws := range w.symbols {
		if ri, ok := readerIdx[ws]; ok {
			mapping[i] = ri
			if ri != i {
				identity = false
			}
		} else {
			identity = false
			// Writer symbol not in reader: use reader default.
			if !r.hasEnumDef {
				return nil, &CompatibilityError{
					Path:       r.name,
					ReaderType: r.name,
					WriterType: w.name,
					Detail:     fmt.Sprintf("writer symbol %q not in reader and no default", ws),
				}
			}
			defIdx, ok := readerIdx[r.enumDef]
			if !ok {
				return nil, fmt.Errorf("enum default %q not found in reader symbols", r.enumDef)
			}
			mapping[i] = defIdx
		}
	}

	if identity {
		return r, nil
	}

	readerSymbols := r.symbols
	return &schemaNode{
		kind:    "enum",
		name:    r.name,
		aliases: r.aliases,
		symbols: r.symbols,
		ser:     r.ser,
		deser: func(src []byte, v reflect.Value, _ *slab) ([]byte, error) {
			idx, src, err := readVarint(src)
			if err != nil {
				return nil, err
			}
			if idx < 0 || int(idx) >= len(mapping) {
				return nil, fmt.Errorf("enum index %d out of range [0, %d)", idx, len(mapping))
			}
			ri := mapping[idx]
			v = indirectAlloc(v)
			switch {
			case v.Kind() == reflect.Interface:
				v.Set(reflect.ValueOf(readerSymbols[ri]))
			case v.Kind() == reflect.String:
				v.SetString(readerSymbols[ri])
			case v.CanInt():
				v.SetInt(int64(ri))
			case v.CanUint():
				v.SetUint(uint64(ri))
			default:
				return nil, &SemanticError{GoType: v.Type(), AvroType: "enum"}
			}
			return src, nil
		},
	}, nil
}

func resolveArray(r, w *schemaNode, path string, seen map[nodePair]*schemaNode) (*schemaNode, error) {
	resolved, err := resolveNode(r.items, w.items, path+".items", seen)
	if err != nil {
		return nil, err
	}
	if resolved == r.items {
		return r, nil
	}
	return &schemaNode{
		kind:  "array",
		items: resolved,
		ser:   r.ser,
		deser: (&deserArray{deserItem: resolved.deser}).deser,
	}, nil
}

func resolveMap(r, w *schemaNode, path string, seen map[nodePair]*schemaNode) (*schemaNode, error) {
	resolved, err := resolveNode(r.values, w.values, path+".values", seen)
	if err != nil {
		return nil, err
	}
	if resolved == r.values {
		return r, nil
	}
	return &schemaNode{
		kind:   "map",
		values: resolved,
		ser:    r.ser,
		deser:  (&deserMap{deserItem: resolved.deser}).deser,
	}, nil
}

// resolveWriterUnion: writer is union, reader is not.
// All writer branches must be compatible with the single reader type.
func resolveWriterUnion(r, w *schemaNode, path string, seen map[nodePair]*schemaNode) (*schemaNode, error) {
	branchDesers := make([]deserfn, len(w.branches))
	for i, wb := range w.branches {
		resolved, err := resolveNode(r, wb, path, seen)
		if err != nil {
			return nil, err
		}
		branchDesers[i] = resolved.deser
	}
	du := &deserUnion{fns: branchDesers}
	return &schemaNode{
		kind:  r.kind,
		name:  r.name,
		ser:   r.ser,
		deser: du.deser,
	}, nil
}

// resolveReaderUnion: reader is union, writer is not.
// Find first matching reader branch.
func resolveReaderUnion(r, w *schemaNode, path string, seen map[nodePair]*schemaNode) (*schemaNode, error) {
	for _, rb := range r.branches {
		if kindsMatch(rb, w) {
			resolved, err := resolveNode(rb, w, path, seen)
			if err != nil {
				return nil, err
			}
			return &schemaNode{
				kind:     "union",
				branches: r.branches,
				ser:      r.ser,
				deser:    resolved.deser,
			}, nil
		}
	}
	return nil, &CompatibilityError{
		Path:       pathOrRoot(path),
		ReaderType: "union",
		WriterType: w.kind,
		Detail:     "writer type matches no reader union branch",
	}
}

// resolveUnionUnion: both reader and writer are unions.
// Map each writer branch to its best matching reader branch.
func resolveUnionUnion(r, w *schemaNode, path string, seen map[nodePair]*schemaNode) (*schemaNode, error) {
	branchDesers := make([]deserfn, len(w.branches))
	for i, wb := range w.branches {
		rb := findMatchingBranch(r, wb)
		if rb == nil {
			return nil, &CompatibilityError{
				Path:       pathOrRoot(path),
				ReaderType: "union",
				WriterType: fmt.Sprintf("union[%d]:%s", i, wb.kind),
				Detail:     "writer union branch has no matching reader branch",
			}
		}
		resolved, err := resolveNode(rb, wb, path, seen)
		if err != nil {
			return nil, err
		}
		branchDesers[i] = resolved.deser
	}
	du := &deserUnion{fns: branchDesers}
	deser := du.deser
	// Null-union optimization.
	if len(w.branches) == 2 && w.branches[0].kind == "null" {
		deser = deserNullUnion(du)
	}
	return &schemaNode{
		kind:     "union",
		branches: r.branches,
		ser:      r.ser,
		deser:    deser,
	}, nil
}

// defaultStringToBytes converts a JSON-decoded string to raw bytes for Avro
// bytes/fixed defaults. The Avro spec says code points 0-255 map to byte values
// 0-255. json.Unmarshal decodes \u00FF to multi-byte UTF-8, so we must convert
// each rune back to a single byte.
func defaultStringToBytes(s string) []byte {
	b := make([]byte, 0, len(s))
	for _, r := range s {
		b = append(b, byte(r))
	}
	return b
}

// encodeDefault converts a parsed JSON default value to Avro binary encoding.
func encodeDefault(val any, node *schemaNode) ([]byte, error) {
	return doEncodeDefault(nil, val, node)
}

func doEncodeDefault(dst []byte, val any, node *schemaNode) ([]byte, error) {
	switch node.kind {
	case "null":
		return dst, nil
	case "boolean":
		b, ok := val.(bool)
		if !ok {
			return nil, fmt.Errorf("expected bool for boolean default, got %T", val)
		}
		if b {
			return append(dst, 1), nil
		}
		return append(dst, 0), nil
	case "int":
		f, ok := val.(float64)
		if !ok {
			return nil, fmt.Errorf("expected number for int default, got %T", val)
		}
		if f != math.Trunc(f) || f < math.MinInt32 || f > math.MaxInt32 {
			return nil, fmt.Errorf("int default %v out of range", f)
		}
		return appendVarint(dst, int32(f)), nil
	case "long":
		f, ok := val.(float64)
		if !ok {
			return nil, fmt.Errorf("expected number for long default, got %T", val)
		}
		if f != math.Trunc(f) || f < -(1<<63) || f >= 1<<63 {
			return nil, fmt.Errorf("long default %v out of range", f)
		}
		return appendVarlong(dst, int64(f)), nil
	case "float":
		f, ok := val.(float64)
		if !ok {
			return nil, fmt.Errorf("expected number for float default, got %T", val)
		}
		return appendUint32(dst, math.Float32bits(float32(f))), nil
	case "double":
		f, ok := val.(float64)
		if !ok {
			return nil, fmt.Errorf("expected number for double default, got %T", val)
		}
		return appendUint64(dst, math.Float64bits(f)), nil
	case "string":
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("expected string for string default, got %T", val)
		}
		dst = appendVarlong(dst, int64(len(s)))
		return append(dst, s...), nil
	case "bytes":
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("expected string for bytes default, got %T", val)
		}
		b := defaultStringToBytes(s)
		dst = appendVarlong(dst, int64(len(b)))
		return append(dst, b...), nil
	case "enum":
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("expected string for enum default, got %T", val)
		}
		for i, sym := range node.symbols {
			if sym == s {
				return appendVarint(dst, int32(i)), nil
			}
		}
		return nil, fmt.Errorf("unknown enum symbol %q in default", s)
	case "fixed":
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("expected string for fixed default, got %T", val)
		}
		b := defaultStringToBytes(s)
		if len(b) != node.size {
			return nil, fmt.Errorf("fixed default length %d != size %d", len(b), node.size)
		}
		return append(dst, b...), nil
	case "array":
		arr, ok := val.([]any)
		if !ok {
			if val == nil {
				return appendVarlong(dst, 0), nil
			}
			return nil, fmt.Errorf("expected array for array default, got %T", val)
		}
		if len(arr) == 0 {
			return appendVarlong(dst, 0), nil
		}
		dst = appendVarlong(dst, int64(len(arr)))
		var err error
		for _, item := range arr {
			dst, err = doEncodeDefault(dst, item, node.items)
			if err != nil {
				return nil, err
			}
		}
		return append(dst, 0), nil
	case "map":
		m, ok := val.(map[string]any)
		if !ok {
			if val == nil {
				return appendVarlong(dst, 0), nil
			}
			return nil, fmt.Errorf("expected object for map default, got %T", val)
		}
		if len(m) == 0 {
			return appendVarlong(dst, 0), nil
		}
		dst = appendVarlong(dst, int64(len(m)))
		var err error
		for k, v := range m {
			dst = appendVarlong(dst, int64(len(k)))
			dst = append(dst, k...)
			dst, err = doEncodeDefault(dst, v, node.values)
			if err != nil {
				return nil, err
			}
		}
		return append(dst, 0), nil
	case "record":
		m, _ := val.(map[string]any)
		if m == nil {
			m = make(map[string]any)
		}
		var err error
		for _, f := range node.fields {
			fval, exists := m[f.name]
			if !exists {
				if !f.hasDefault {
					return nil, fmt.Errorf("record default missing field %q with no default", f.name)
				}
				fval = f.defaultVal
			}
			dst, err = doEncodeDefault(dst, fval, f.node)
			if err != nil {
				return nil, err
			}
		}
		return dst, nil
	case "union":
		// Default value must match the first branch (Avro spec).
		if len(node.branches) == 0 {
			return nil, fmt.Errorf("empty union")
		}
		dst = appendVarlong(dst, 0) // index of first branch
		return doEncodeDefault(dst, val, node.branches[0])
	default:
		return nil, fmt.Errorf("unsupported default encoding for type %q", node.kind)
	}
}
