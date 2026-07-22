package avro

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
)

// parseSchemaTree decodes a schema JSON string into the aschema parse tree
// in a single O(n) pass. The prior path had aschema and afield implement
// json.Unmarshaler, so the stdlib decoder did a full subtree re-scan
// (d.skip) to delimit each nested node before recursing into its
// UnmarshalJSON — O(depth) re-scans of O(size) content = O(depth*size)
// over a nested schema. Decoding once into a generic tree (no custom
// Unmarshaler, so no re-scan) and walking that tree is O(n).
//
// Scalar leaf fields (size, scale, precision, default) re-marshal their
// small already-decoded value and reuse the existing stdlib decode
// (laxInt, *int, json.RawMessage) so their accept/reject behavior is
// identical to the prior path by construction; the re-marshal is
// O(leaf) and summed over all leaves stays O(n).
func parseSchemaTree(schema string) (*aschema, error) {
	dec := json.NewDecoder(strings.NewReader(schema))
	dec.UseNumber() // preserve integer precision in defaults and extras
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, boundJSONErrorEcho(err)
	}
	// json.Unmarshal rejects trailing non-whitespace; replicate by
	// requiring the next decode to be EOF (trailing whitespace is
	// consumed, anything else is content). Mirrors SchemaCache.Parse's
	// normalization check.
	var tail json.RawMessage
	if err := dec.Decode(&tail); !errors.Is(err, io.EOF) {
		return nil, errors.New("invalid schema: unexpected trailing content")
	}
	var s aschema
	if err := aschemaFromAny(v, &s, nil); err != nil {
		return nil, err
	}
	return &s, nil
}

// aschemaFromAny populates s from a generic JSON value: a string is a
// primitive / name reference, an array is a union, an object is a complex
// type. Mirrors the dispatch the former aschema.UnmarshalJSON did on the
// first byte.
func aschemaFromAny(v any, s *aschema, memo strayShapeMemo) error {
	switch t := v.(type) {
	case string:
		s.primitive = t
		return nil
	case []any:
		s.union = make([]aschema, len(t))
		for i := range t {
			if err := aschemaFromAny(t[i], &s.union[i], memo); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		o, err := aobjectFromMap(t, memo)
		if err != nil {
			return err
		}
		s.object = o
		return nil
	default:
		return errors.New("invalid schema")
	}
}

// schemaTypeMismatch mirrors the error encoding/json produced when a
// schema key held the wrong JSON type (e.g. a non-string "type"), so
// downstream behavior and messages stay equivalent.
func schemaTypeMismatch(key, want string) error {
	return fmt.Errorf("invalid schema: %q must be a JSON %s", key, want)
}

func aobjectFromMap(m map[string]any, memo strayShapeMemo) (o *aobject, err error) {
	// memo is set ONLY on the metadata walker's stray-shape validity path
	// (strayBodyShapeOKMemo), never on a real parse. There the caller
	// discards the built aobject and only reads whether this subtree is a
	// valid schema shape, so a subtree already validated returns its cached
	// verdict without the O(subtree) rebuild — turning the walker's repeated
	// per-ancestor-level checks over a nested schema from O(depth^2) back
	// into one linear pass. The verdict is recorded by this same decode, so
	// it is identical to a fresh strayBodyShapeOK.
	if memo != nil {
		p := reflect.ValueOf(m).Pointer()
		if valid, ok := memo[p]; ok {
			if valid {
				return &aobject{}, nil
			}
			return nil, errStrayShapeCached
		}
		defer func() { memo[p] = err == nil }()
	}
	o = &aobject{}

	if v, ok := m["type"]; ok {
		ts, ok := v.(string)
		if !ok {
			return nil, schemaTypeMismatch("type", "string")
		}
		o.Type = ts
	}
	// Structural/naming keys shape-validate ONLY where the kind binds
	// them. On a non-binding kind a malformed body cannot define, scope,
	// or bind anything: the arm leaves the aobject field unset and the
	// extra loop below routes the raw value to props verbatim
	// (schemaReservedKeyForObject's shape-conditional arm) — the same
	// treatment unconsumed precision/scale already get, and the same
	// accept-and-ignore posture Java (SCHEMA_RESERVED skip,
	// Schema.java:175-176) and fastavro take. A shape-OK body still
	// parses into the aobject field even on a non-binding kind (the
	// documented as-written structural surfacing).
	nameIsString := false
	if v, ok := m["name"]; ok {
		if ns, ok := v.(string); ok {
			o.Name = ns
			nameIsString = true
		} else if strayKeyBinds(o.Type, "name") {
			return nil, schemaTypeMismatch("name", "string")
		}
	}
	if v, ok := m["namespace"]; ok {
		if ns, ok := v.(string); ok {
			o.Namespace = &ns
		} else if strayKeyBinds(o.Type, "namespace") {
			return nil, schemaTypeMismatch("namespace", "string")
		}
	}
	if ss, ok, err := stringSliceFrom(m, "symbols"); err != nil {
		if strayKeyBinds(o.Type, "symbols") {
			return nil, err
		}
	} else if ok {
		o.Symbols = ss
	}
	if ss, ok, err := stringSliceFrom(m, "aliases"); err != nil {
		if strayKeyBinds(o.Type, "aliases") {
			return nil, err
		}
	} else if ok {
		o.Aliases = ss
	}
	if v, ok := m["items"]; ok {
		it := &aschema{}
		if err := aschemaFromAny(v, it, memo); err != nil {
			if strayKeyBinds(o.Type, "items") {
				return nil, err
			}
		} else {
			o.Items = it
		}
	}
	if v, ok := m["values"]; ok {
		vs := &aschema{}
		if err := aschemaFromAny(v, vs, memo); err != nil {
			if strayKeyBinds(o.Type, "values") {
				return nil, err
			}
		} else {
			o.Values = vs
		}
	}
	if v, ok := m["size"]; ok {
		if raw, err := json.Marshal(v); err == nil {
			var l laxInt
			if err := l.UnmarshalJSON(raw); err == nil {
				o.Size = &l
			} else if strayKeyBinds(o.Type, "size") {
				return nil, err
			}
		} else if strayKeyBinds(o.Type, "size") {
			return nil, err
		}
	}
	if v, ok := m["logicalType"]; ok {
		ls, ok := v.(string)
		if !ok {
			return nil, schemaTypeMismatch("logicalType", "string")
		}
		o.Logical = ls
	}
	// precision/scale are consumed (and shape-validated) only on a
	// recognized decimal carrier; everywhere else a malformed value is
	// inert and rides to props like any other unconsumed placement.
	if p, err := intPtrFrom(m, "scale"); err != nil {
		if decimalConsumesPrecisionScale(o.Type, o.Logical) {
			return nil, err
		}
	} else {
		o.Scale = p
	}
	if p, err := intPtrFrom(m, "precision"); err != nil {
		if decimalConsumesPrecisionScale(o.Type, o.Logical) {
			return nil, err
		}
	} else {
		o.Precision = p
	}
	if v, ok := m["default"]; ok {
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		o.Default = json.RawMessage(raw)
	}
	if v, ok := m["fields"]; ok {
		if fs, ok := v.([]any); ok {
			fields := make([]afield, len(fs))
			ferr := error(nil)
			for i := range fs {
				if err := afieldFromAny(fs[i], &fields[i], memo); err != nil {
					ferr = err
					break
				}
			}
			if ferr == nil {
				o.Fields = fields
			} else if strayKeyBinds(o.Type, "fields") {
				return nil, ferr
			}
		} else if strayKeyBinds(o.Type, "fields") {
			return nil, schemaTypeMismatch("fields", "array")
		}
	}
	// Extra (non-reserved) properties. The tree was decoded with
	// UseNumber; normalizeJSONValue applies the same value-based numeric
	// normalization (json.Number → int64/float64, exponent-overflow →
	// ±Inf) the former unmarshalAnyPreservePrecision capture did.
	// precision/scale count as extra everywhere except on a recognized
	// decimal carrier (schemaReservedKeyForObject) — these extras feed
	// node.props, so the CustomType-callback SchemaNode surfaces stray
	// precision/scale in Props exactly like Root() does.
	// The stray-body verdict is already recorded: the arms above set
	// o.Items/o.Values/o.Fields/... exactly when the body parsed as the
	// key's schema shape, an exact mirror of strayBodyShapeOK. Route on
	// that recorded verdict so a stray body is decoded ONCE (by its arm),
	// never a second time here — a second decode re-enters aschemaFromAny,
	// which routes its own stray keys, so the two decodes per level
	// compound to O(2^depth) over a nested-stray schema. Reserved keys
	// match exact-lowercase only, so the verdict is consulted exactly for
	// the spelling the arms read; a case-variant spelling is an ordinary
	// custom property routed to extra verbatim.
	shapeOK := o.strayShapeRecorded(nameIsString)
	for k, v := range m {
		if schemaReservedKeyForObject(k, v, o.Type, o.Logical, shapeOK) {
			continue
		}
		if o.extra == nil {
			o.extra = make(map[string]any)
		}
		o.extra[k] = normalizeJSONValue(v)
	}
	return o, nil
}

func afieldFromAny(v any, f *afield, memo strayShapeMemo) error {
	m, ok := v.(map[string]any)
	if !ok {
		return errors.New("invalid record field: must be a JSON object")
	}
	if v, ok := m["name"]; ok {
		ns, ok := v.(string)
		if !ok {
			return schemaTypeMismatch("name", "string")
		}
		f.Name = ns
	}
	if v, ok := m["order"]; ok {
		os, ok := v.(string)
		if !ok {
			return schemaTypeMismatch("order", "string")
		}
		f.Order = os
	}
	if ss, ok, err := stringSliceFrom(m, "aliases"); err != nil {
		return err
	} else if ok {
		f.Aliases = ss
	}
	if v, ok := m["logicalType"]; ok {
		ls, ok := v.(string)
		if !ok {
			return schemaTypeMismatch("logicalType", "string")
		}
		f.Logical = ls
	}
	if p, err := intPtrFrom(m, "scale"); err != nil {
		return err
	} else {
		f.Scale = p
	}
	if p, err := intPtrFrom(m, "precision"); err != nil {
		return err
	} else {
		f.Precision = p
	}
	if v, ok := m["default"]; ok {
		raw, err := json.Marshal(v)
		if err != nil {
			return err
		}
		f.Default = json.RawMessage(raw)
	}
	if v, ok := m["type"]; ok {
		f.Type = &aschema{}
		if err := aschemaFromAny(v, f.Type, memo); err != nil {
			return err
		}
	}

	// Flat ("linkedin/goavro") field format and field-level logicalType
	// lift, mirroring the former afield.UnmarshalJSON. When "type" is a
	// string naming a complex kind and that kind's defining key (symbols
	// / items / values / fields / size) sits alongside the field's own
	// keys, lift those into a nested type object.
	if f.Type != nil && f.Type.primitive != "" && flatFieldNeedsLift(m, f.Type.primitive) {
		return f.liftFlatFieldType(m, f.Type.primitive)
	}
	f.liftFieldLogicalIntoType()
	return nil
}

// flatFieldNeedsLift reports whether the field JSON object m, whose "type"
// attribute is the bare string tp, is written in the flat (goavro-style)
// field format: tp names a complex kind and that kind's defining key
// (symbols / items / values / fields / size) sits alongside the field's own
// keys. "error" is the record alias, defined by the "fields" key like
// "record".
//
// Shared by the wire parser (afieldFromAny) and, via walkNodeChildren
// (schema_walk.go), every JSON-map walker — the SchemaCache
// self-containment walkers and the Root() metadata tree: all must lift
// the SAME fields, or a walker would describe a different schema than
// the one that encodes — sharing the predicate makes the agreement
// structural.
func flatFieldNeedsLift(m map[string]any, tp string) bool {
	switch tp {
	case "enum", "array", "map", "record", "error", "fixed":
	default:
		return false
	}
	for key, forType := range afieldComplexKeys {
		if _, ok := m[key]; ok && (forType == tp || (key == "fields" && tp == "error")) {
			return true
		}
	}
	return false
}

// flatLiftTypeMap builds the nested type object's JSON map from a flat
// field's own keys: "default" and "order" are field-only and never
// propagate; "aliases" belongs to the field (flat-format aliases are field
// aliases); "name" and "namespace" propagate only for named kinds;
// everything else — the defining key, "type" itself, logicalType /
// precision / scale, doc, and custom properties — moves into the type.
//
// Shared by the wire parser (liftFlatFieldType) and the JSON-map walkers'
// flatField callbacks (collectTreeDefs, metadataField's callers) so the
// sides cannot drift on WHAT the lift routes; flatFieldNeedsLift is the
// shared WHEN.
func flatLiftTypeMap(m map[string]any, tp string) map[string]any {
	named := tp == "record" || tp == "error" || tp == "enum" || tp == "fixed"
	typeMap := make(map[string]any, len(m))
	for k, v := range m {
		switch k {
		case "default", "order":
			// Field-only keys, do not propagate.
		case "aliases":
			// Flat-format aliases belong to the field, not the type.
		case "name", "namespace":
			if named {
				typeMap[k] = v
			}
		default:
			typeMap[k] = v
		}
	}
	return typeMap
}

// liftFlatFieldType builds the field's nested type object from the field's
// own JSON keys (excluding field-only keys), for the flat field format.
// Mirrors the former afield.UnmarshalJSON flat-form branch. The key routing
// lives in flatLiftTypeMap (shared with the metadata walker): logicalType /
// precision / scale flow into the type object (they are not field-only),
// so the field-level copies are cleared afterward.
func (f *afield) liftFlatFieldType(m map[string]any, tp string) error {
	// The lifted type is a freshly constructed map (flatLiftTypeMap), not a
	// node of the caller's tree, so no shape memo applies — pass nil.
	o, err := aobjectFromMap(flatLiftTypeMap(m, tp), nil)
	if err != nil {
		return err
	}
	f.Type = &aschema{object: o}
	f.Logical, f.Scale, f.Precision = "", nil, nil
	return nil
}

// stringSliceFrom reads m[key] as a []string. The second return reports
// presence; a present non-array or non-string element is an error,
// matching encoding/json's []string decode.
func stringSliceFrom(m map[string]any, key string) ([]string, bool, error) {
	v, ok := m[key]
	if !ok {
		return nil, false, nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, false, schemaTypeMismatch(key, "array")
	}
	out := make([]string, len(arr))
	for i := range arr {
		s, ok := arr[i].(string)
		if !ok {
			return nil, false, schemaTypeMismatch(key, "array of strings")
		}
		out[i] = s
	}
	return out, true, nil
}

// intPtrFrom reads m[key] as a *int by re-marshaling
// the small value and reusing stdlib int decode, so the accept/reject
// behavior (rejecting floats, strings, overflow) is identical to the
// former *int struct field.
func intPtrFrom(m map[string]any, key string) (*int, error) {
	v, ok := m[key]
	if !ok {
		return nil, nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var n int
	if err := json.Unmarshal(raw, &n); err != nil {
		return nil, boundJSONErrorEcho(err)
	}
	return &n, nil
}

// strayRoutedKeys are the structural/naming keys whose STRAY placements
// (on a kind that does not bind them) get shape-conditional routing: a
// body that parses as the key's schema shape surfaces on the matching
// SchemaNode structural field (as-written), anything else rides in Props
// verbatim — the same route unconsumed precision/scale already take.
// Only the exact lowercase spelling is a reserved key; a case-variant
// spelling is an ordinary custom property with no routing of its own.
var strayRoutedKeys = [...]string{
	"items", "values", "fields", "symbols", "size", "name", "namespace", "aliases",
}

// canonicalStrayKey returns k when it is one of the stray-routed keys
// (exact lowercase spelling, like every reserved-key match), else "".
func canonicalStrayKey(k string) string {
	for _, key := range strayRoutedKeys {
		if k == key {
			return key
		}
	}
	return ""
}

// strayKeyBinds reports whether a node of the given kind BINDS key — the
// parser's kind-keyed grammar. A binding kind shape-validates the key's
// value and consumes it; on any other kind the key is a stray the parse
// never binds, so a malformed body there cannot be an attempt to define,
// scope, or reference anything.
func strayKeyBinds(typ, key string) bool {
	switch key {
	case "items":
		return typ == "array"
	case "values":
		return typ == "map"
	case "fields":
		return isRecordKind(typ)
	case "symbols":
		return typ == "enum"
	case "size":
		return typ == "fixed"
	case "name", "namespace", "aliases":
		return isNamedKind(typ)
	}
	return false
}

// strayBodyShapeOK reports whether v parses as key's schema shape. It
// runs the SAME decodes the parser's own arms run (aschemaFromAny,
// afieldFromAny, the string-slice and laxInt reads), so the wire parse
// and the metadata walker cannot disagree on a stray body's surfacing
// route: shape-OK bodies surface on the matching structural field,
// anything else stays a Props entry, and the accept/reject boundary for
// BINDING kinds is untouched (their arms still propagate the error).
func strayBodyShapeOK(key string, v any) bool {
	switch key {
	case "name", "namespace":
		_, ok := v.(string)
		return ok
	case "symbols", "aliases":
		arr, ok := v.([]any)
		if !ok {
			return false
		}
		for _, e := range arr {
			if _, ok := e.(string); !ok {
				return false
			}
		}
		return true
	case "size":
		raw, err := json.Marshal(v)
		if err != nil {
			return false
		}
		var l laxInt
		return l.UnmarshalJSON(raw) == nil
	case "items", "values":
		var s aschema
		return aschemaFromAny(v, &s, nil) == nil
	case "fields":
		arr, ok := v.([]any)
		if !ok {
			return false
		}
		for i := range arr {
			var f afield
			if afieldFromAny(arr[i], &f, nil) != nil {
				return false
			}
		}
		return true
	}
	return false
}

// strayShapeMemo caches, by subtree pointer, whether a schema-position
// subtree parses as a valid schema shape. The metadata walker's stray gates
// consult a stray body's shape once per node, and a nested-stray schema
// nests those bodies, so without memoization each body is re-validated once
// per enclosing level — O(depth^2). One memo shared across a Root() walk
// makes it linear: the FIRST validation of a subtree records it and every
// nested subtree it decodes (aobjectFromMap defers the record), so a later
// level's check of an inner subtree is a cache hit. The recorded verdict is
// produced by the SAME parser decodes strayBodyShapeOK runs, so it is
// identical to a fresh check; the map is used only by the metadata walker,
// and a nil memo (every other caller) takes the un-memoized path.
type strayShapeMemo map[uintptr]bool

// errStrayShapeCached is the sentinel aobjectFromMap returns for a subtree
// the memo already recorded as an invalid schema shape. It never escapes
// the shape-validity path (the only caller that passes a non-nil memo reads
// only whether the error is nil), so its text is never surfaced.
var errStrayShapeCached = errors.New("avro: stray shape (cached invalid)")

// strayBodyShapeOKMemo is strayBodyShapeOK with per-subtree memoization for
// the recursive keys (items/values/fields), whose bodies can nest and so
// drive the O(depth^2). A miss runs the ordinary decode threaded with the
// memo (recording this body and every subtree it walks); a later query of
// an already-walked subtree short-circuits at aobjectFromMap. The
// non-recursive keys carry no compounding cost and take the plain check.
func strayBodyShapeOKMemo(memo strayShapeMemo, key string, v any) bool {
	if memo == nil {
		return strayBodyShapeOK(key, v)
	}
	switch key {
	case "items", "values":
		var s aschema
		return aschemaFromAny(v, &s, memo) == nil
	case "fields":
		arr, ok := v.([]any)
		if !ok {
			return false
		}
		for i := range arr {
			var f afield
			if afieldFromAny(arr[i], &f, memo) != nil {
				return false
			}
		}
		return true
	default:
		return strayBodyShapeOK(key, v)
	}
}

// strayShapeVerdict reports whether a stray-routed reserved key's body
// (canonical key spelling, raw value v) parsed as that key's schema shape.
// A caller that already decoded the body once passes its RECORDED verdict
// here so the props-routing (schemaReservedKeyForObject) and the metadata
// child-surfacing never re-decode a subtree the caller already walked —
// the re-decode is what turns a nested-stray schema into O(2^depth) work.
type strayShapeVerdict func(canonKey string, v any) bool

// strayShapeRecorded returns the aobject's own arm verdicts as a
// strayShapeVerdict: a stray-routed body parsed as its schema shape iff the
// matching arm set the aobject field (o.Items for "items", o.Values for
// "values", o.Fields for "fields", and so on). The arms run aschemaFromAny/
// afieldFromAny/the string-slice and laxInt reads — the SAME decodes
// strayBodyShapeOK runs — so field-set is an exact mirror of the shape
// check, and consulting it lets the extra-property loop skip a second
// decode of every already-walked body. nameIsString carries the name arm's
// string-ness (o.Name is a bare string with no present/absent flag of its
// own; the empty short name "" is a valid name shape).
func (o *aobject) strayShapeRecorded(nameIsString bool) strayShapeVerdict {
	return func(canonKey string, _ any) bool {
		switch canonKey {
		case "items":
			return o.Items != nil
		case "values":
			return o.Values != nil
		case "fields":
			return o.Fields != nil
		case "symbols":
			return o.Symbols != nil
		case "size":
			return o.Size != nil
		case "aliases":
			return o.Aliases != nil
		case "namespace":
			return o.Namespace != nil
		case "name":
			return nameIsString
		}
		return false
	}
}
