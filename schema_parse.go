package avro

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

// parseSchemaTree decodes a schema JSON string into the aschema parse tree in
// a single O(n) pass. A json.Unmarshaler on aschema/afield would instead make
// the stdlib decoder re-scan each nested subtree to delimit it, O(depth*size).
// We decode once into a generic tree and walk that, which is O(n).
//
// Scalar leaves (size, scale, precision, default) re-marshal their small
// decoded value and reuse the stdlib decode, so their accept/reject behavior
// matches the typed decode. That re-marshal is O(leaf) and
// stays O(n) summed.
func parseSchemaTree(schema string) (*aschema, error) {
	// decodeSchemaAnyStrict preserves every number as its literal. That is what
	// the arms below re-marshal into o.Default / f.Default, and what the size /
	// precision / scale reads decode. See that decoder for the two silent
	// failures a resolving decode produces here.
	v, err := decodeSchemaAnyStrict(schema)
	if err != nil {
		return nil, boundJSONErrorEcho(err)
	}
	var s aschema
	if err := aschemaFromAny(v, &s, nil); err != nil {
		return nil, err
	}
	return &s, nil
}

// aschemaFromAny populates s from a generic JSON value: a string is a
// primitive or name reference, an array a union, an object a complex type.
// Mirrors the dispatch the former aschema.UnmarshalJSON did on the first byte.
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
	// memo is set only on the metadata walker's stray-shape validity path,
	// never on a real parse. There the caller reads only whether this
	// subtree is a valid schema shape, so an already-validated subtree
	// returns its cached verdict without the O(subtree) rebuild, keeping a
	// nested-stray schema linear rather than O(depth^2).
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
	// Structural and naming keys shape-validate only where the kind binds
	// them. On a non-binding kind a malformed body cannot define, scope, or
	// bind anything, so the arm leaves the field unset and the extra loop
	// below routes the raw value to props verbatim, as Java and fastavro
	// do. A shape-OK body still parses into the field on a non-binding kind,
	// which is how a stray key is carried as written.
	//
	// Each arm that consumes a body records the fact in o.present, which is
	// the verdict the Props routing below reads for a key the kind does not
	// bind: a stray-routed body parsed as its shape iff its arm set the
	// field. Deciding it where the decode happens is what keeps a nested
	// stray schema from being decoded once per enclosing level.
	if v, ok := m["name"]; ok {
		if ns, ok := v.(string); ok {
			o.Name = ns
			o.present |= presName
		} else if strayKeyBinds(o.Type, "name") {
			return nil, schemaTypeMismatch("name", "string")
		}
	}
	if v, ok := m["namespace"]; ok {
		if ns, ok := v.(string); ok {
			o.Namespace = &ns
			o.present |= presNamespace
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
		o.present |= presSymbols
	}
	if ss, ok, err := stringSliceFrom(m, "aliases"); err != nil {
		if strayKeyBinds(o.Type, "aliases") {
			return nil, err
		}
	} else if ok {
		o.Aliases = ss
		o.present |= presAliases
	}
	if v, ok := m["items"]; ok {
		it := &aschema{}
		if err := aschemaFromAny(v, it, memo); err != nil {
			if strayKeyBinds(o.Type, "items") {
				return nil, err
			}
		} else {
			o.Items = it
			o.present |= presItems
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
			o.present |= presValues
		}
	}
	if v, ok := m["size"]; ok {
		if l, err := decodeLaxInt("size", v); err == nil {
			o.Size = &l
			o.present |= presSize
		} else if strayKeyBinds(o.Type, "size") {
			return nil, err
		}
	}
	// A non-string logicalType can never name a logical type, so it rides
	// to props verbatim, as in Java, fastavro, and goavro.
	if ls, ok := m["logicalType"].(string); ok {
		o.Logical = ls
	}
	// We consume and shape-validate precision and scale only on a decimal
	// carrier; anywhere else a malformed value rides to props.
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
				o.present |= presFields
			} else if strayKeyBinds(o.Type, "fields") {
				return nil, ferr
			}
		} else if strayKeyBinds(o.Type, "fields") {
			return nil, schemaTypeMismatch("fields", "array")
		}
	}
	// Extra (non-reserved) properties, normalized by normalizeJSONValue.
	for k, v := range m {
		if schemaReservedKeyForObject(k, v, o.Type, o.Logical, o.present) {
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
		f.orderSet = true
	}
	if ss, ok, err := stringSliceFrom(m, "aliases"); err != nil {
		return err
	} else if ok {
		f.Aliases = ss
	}
	// As on a type object, a non-string logicalType lifts nothing and rides
	// in the field's Props verbatim.
	if ls, ok := m["logicalType"].(string); ok {
		f.Logical = ls
	}
	// Whether precision and scale are consumed depends on the field-level
	// logicalType and the lift target's kind, which we do not know yet, so we
	// record the shape verdicts and decide after the type parses.
	var scaleErr, precisionErr error
	if p, err := intPtrFrom(m, "scale"); err != nil {
		scaleErr = err
	} else {
		f.Scale = p
	}
	if p, err := intPtrFrom(m, "precision"); err != nil {
		precisionErr = err
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
	// The decimal lift is the only field-level consumer of precision and
	// scale. Where it consumes them, a malformed body must reject rather than
	// read as absent: scale defaults to 0, so dropping a malformed scale
	// beside a valid precision would parse as decimal(p,0) and change what
	// every wire value means. Everywhere else the pair rides to props like
	// any custom property, as in Java and fastavro.
	if f.fieldDecimalLiftConsumesPrecisionScale() {
		if precisionErr != nil {
			return fmt.Errorf("invalid record field %q: %w", "precision", precisionErr)
		}
		if scaleErr != nil {
			return fmt.Errorf("invalid record field %q: %w", "scale", scaleErr)
		}
	}
	f.liftFieldLogicalIntoType()
	return nil
}

// flatFieldNeedsLift reports whether the field JSON object m is written in the
// flat (goavro-style) field format. Its "type" attribute is the bare string
// tp: the format needs tp to name a complex kind, with that kind's defining
// key (symbols / items / values / fields / size) sitting alongside the field's
// own keys. "error" is the record alias, defined by the "fields" key like
// "record".
//
// The wire parser and, through walkNodeChildren, every JSON-map walker share
// this predicate, so all of them lift the same fields.
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

// flatLiftTypeMap builds the nested type object's JSON map from a flat field's
// own keys. "default" and "order" are field-only and never propagate.
// "aliases" belongs to the field, since flat-format aliases are field aliases.
// "name" and "namespace" propagate only for named kinds. Everything else moves
// into the type: the defining key, "type" itself, logicalType / precision /
// scale, doc, and custom properties.
//
// The wire parser and the JSON-map walkers share this routing, as they share
// flatFieldNeedsLift for when to lift.
func flatLiftTypeMap(m map[string]any, tp string) map[string]any {
	named := isNamedKind(tp)
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

// liftFlatFieldType builds the field's nested type object from the field's own
// JSON keys (excluding field-only keys), for the flat field format. Mirrors
// the former afield.UnmarshalJSON flat-form branch. The key routing lives in
// flatLiftTypeMap (shared with the metadata walker): logicalType / precision /
// scale flow into the type object, since they are not field-only, so we clear
// the field-level copies afterward.
func (f *afield) liftFlatFieldType(m map[string]any, tp string) error {
	// The lifted type is a freshly built map (flatLiftTypeMap), not a node of
	// the caller's tree, so no shape memo applies and we pass nil.
	o, err := aobjectFromMap(flatLiftTypeMap(m, tp), nil)
	if err != nil {
		return err
	}
	f.Type = &aschema{object: o}
	f.Logical, f.Scale, f.Precision = "", nil, nil
	return nil
}

// stringSliceFrom reads m[key] as a []string. The second return reports
// presence; a present non-array or non-string element is an error, matching
// encoding/json's []string decode.
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

// jsonNullBody reports whether a decoded JSON body is the null literal. A
// typed decode accepts null in silence, leaving the zero value, and that zero
// is not neutral: a fixed of size 0 is a usable schema and a decimal of
// scale 0 changes what every wire value means. So every typed body read asks
// this first and treats null as malformed, rejecting where the kind binds the
// key and routing to props where it does not. Keys read by type assertion
// need no guard, since a nil any satisfies no assertion.
func jsonNullBody(v any) bool {
	return v == nil
}

// decodeLaxInt re-marshals a decoded JSON value and reads it back through
// laxInt, the schema grammar's integer decode (plain integer syntax or the
// quoted [INTEGERS] form, length-capped). It is the one integer predicate for
// "size", shared by the parse arm, the metadata capture, and the stray shape
// verdict, so a value that fails here rides to props on every path.
func decodeLaxInt(key string, v any) (laxInt, error) {
	if jsonNullBody(v) {
		return 0, schemaTypeMismatch(key, "integer")
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	var l laxInt
	if err := l.UnmarshalJSON(raw); err != nil {
		return 0, err
	}
	return l, nil
}

// intPtrFrom reads m[key] as a *int by re-marshaling the value and reusing the
// stdlib int decode, so floats, strings, and overflow reject as they would
// for an int field. A null body rejects rather than reading as absent: we
// consume precision and scale only on a decimal carrier, and a decimal whose
// parameter names no number is not a decimal.
func intPtrFrom(m map[string]any, key string) (*int, error) {
	v, ok := m[key]
	if !ok {
		return nil, nil
	}
	if jsonNullBody(v) {
		return nil, schemaTypeMismatch(key, "integer")
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

// strayRoutedKeys are the structural/naming keys whose stray placements (on a
// kind that does not bind them) get shape-conditional routing. A body that
// parses as the key's schema shape is carried on the matching SchemaNode
// structural field, as-written; anything else rides in Props verbatim, the
// route unconsumed precision/scale already take. Only the exact lowercase
// spelling is a reserved key: a case-variant spelling is an ordinary custom
// property with no routing of its own.
var strayRoutedKeys = [...]string{
	"items", "values", "fields", "symbols", "size", "name", "namespace", "aliases",
}

func canonicalStrayKey(k string) string {
	for _, key := range strayRoutedKeys {
		if k == key {
			return key
		}
	}
	return ""
}

// strayKeyBinds reports whether a node of the given kind binds key, the
// parser's kind-keyed grammar. A binding kind shape-validates the key's value
// and consumes it. On any other kind the key is a stray the parse never binds,
// so a malformed body there cannot be an attempt to define, scope, or
// reference anything.
//
// [schemaKeyBinds] wraps this and answers the keys whose binding also depends
// on the value or the logical type (logicalType, precision/scale). Every key
// the kind alone decides is answered here, so the two are one question over
// one kind-keyed table.
func strayKeyBinds(typ, key string) bool {
	switch key {
	case "type", "doc":
		// Read on every kind: "type" names the kind itself, and "doc" is
		// documentation the metadata carries on any node.
		return true
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
	case "default":
		// The enum evolution default is the only type-level binding of
		// "default". A record field binds it too, but a field object is
		// read by afieldFromAny against the field grammar, never by this
		// kind-keyed one.
		return typ == "enum"
	case "order":
		// A field-only sort attribute; no type-level kind binds it, in Java
		// either.
		return false
	}
	return false
}

// strayBodyShapeOK reports whether v parses as key's schema shape. We run the
// same decodes the parser's own arms run (aschemaFromAny, afieldFromAny, the
// string-slice and laxInt reads), so the wire parse and the metadata walker
// cannot disagree on where a stray body is carried. Shape-OK bodies go on
// the matching structural field, anything else stays a Props entry, and the
// accept/reject boundary for binding kinds is untouched (their arms still
// propagate the error).
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
		_, err := decodeLaxInt(key, v)
		return err == nil
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

// strayShapeMemo caches, by subtree pointer, whether a schema-position subtree
// parses as a valid schema shape. The metadata walker checks a stray body's
// shape once per node, and a nested-stray schema nests those bodies, so
// without the memo each body is re-validated once per enclosing level,
// O(depth^2). The first validation records every nested subtree it decodes,
// so a later level's check is a hit. Only the metadata walker passes one; a
// nil memo takes the plain path.
type strayShapeMemo map[uintptr]bool

// errStrayShapeCached is the sentinel aobjectFromMap returns for a subtree the
// memo already recorded as an invalid schema shape. It never escapes the
// shape-validity path (the only caller passing a non-nil memo reads only
// whether the error is nil), so we never report its text.
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

// strayKeyBit returns the presence bit a stray-routed key's decoding arm
// records, and false for any other key.
func strayKeyBit(k string) (presenceSet, bool) {
	switch k {
	case "items":
		return presItems, true
	case "values":
		return presValues, true
	case "fields":
		return presFields, true
	case "symbols":
		return presSymbols, true
	case "size":
		return presSize, true
	case "name":
		return presName, true
	case "namespace":
		return presNamespace, true
	case "aliases":
		return presAliases, true
	}
	return 0, false
}

// strayPresence returns the presence bit for a stray-routed key whose body
// parses as the key's shape, else 0. It is the verdict for a key nothing
// decoded before routing: a wrapped reference's props, which the cache
// splice and the metadata splice merge onto the definition. That set is
// flat, never a nested stray schema, so the one decode per key compounds
// nothing.
func strayPresence(k string, v any) presenceSet {
	bit, stray := strayKeyBit(k)
	if !stray || !strayBodyShapeOK(k, v) {
		return 0
	}
	return bit
}
