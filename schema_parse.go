package avro

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	if err := aschemaFromAny(v, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// aschemaFromAny populates s from a generic JSON value: a string is a
// primitive / name reference, an array is a union, an object is a complex
// type. Mirrors the dispatch the former aschema.UnmarshalJSON did on the
// first byte.
func aschemaFromAny(v any, s *aschema) error {
	switch t := v.(type) {
	case string:
		s.primitive = t
		return nil
	case []any:
		s.union = make([]aschema, len(t))
		for i := range t {
			if err := aschemaFromAny(t[i], &s.union[i]); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		o, err := aobjectFromMap(t)
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

func aobjectFromMap(m map[string]any) (*aobject, error) {
	o := &aobject{}

	if v, ok := lookupCI(m, "type"); ok {
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
	if v, ok := lookupCI(m, "name"); ok {
		if ns, ok := v.(string); ok {
			o.Name = ns
		} else if strayKeyBinds(o.Type, "name") {
			return nil, schemaTypeMismatch("name", "string")
		}
	}
	if v, ok := lookupCI(m, "namespace"); ok {
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
	if v, ok := lookupCI(m, "items"); ok {
		it := &aschema{}
		if err := aschemaFromAny(v, it); err != nil {
			if strayKeyBinds(o.Type, "items") {
				return nil, err
			}
		} else {
			o.Items = it
		}
	}
	if v, ok := lookupCI(m, "values"); ok {
		vs := &aschema{}
		if err := aschemaFromAny(v, vs); err != nil {
			if strayKeyBinds(o.Type, "values") {
				return nil, err
			}
		} else {
			o.Values = vs
		}
	}
	if v, ok := lookupCI(m, "size"); ok {
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
	if v, ok := lookupCI(m, "logicalType"); ok {
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
	if v, ok := lookupCI(m, "default"); ok {
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		o.Default = json.RawMessage(raw)
	}
	if v, ok := lookupCI(m, "fields"); ok {
		if fs, ok := v.([]any); ok {
			fields := make([]afield, len(fs))
			ferr := error(nil)
			for i := range fs {
				if err := afieldFromAny(fs[i], &fields[i]); err != nil {
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
	for k, v := range m {
		if schemaReservedKeyForObject(k, v, o.Type, o.Logical) {
			continue
		}
		if o.extra == nil {
			o.extra = make(map[string]any)
		}
		o.extra[k] = normalizeJSONValue(v)
	}
	return o, nil
}

func afieldFromAny(v any, f *afield) error {
	m, ok := v.(map[string]any)
	if !ok {
		return errors.New("invalid record field: must be a JSON object")
	}
	if v, ok := lookupCI(m, "name"); ok {
		ns, ok := v.(string)
		if !ok {
			return schemaTypeMismatch("name", "string")
		}
		f.Name = ns
	}
	if v, ok := lookupCI(m, "order"); ok {
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
	if v, ok := lookupCI(m, "logicalType"); ok {
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
	if v, ok := lookupCI(m, "default"); ok {
		raw, err := json.Marshal(v)
		if err != nil {
			return err
		}
		f.Default = json.RawMessage(raw)
	}
	if v, ok := lookupCI(m, "type"); ok {
		f.Type = &aschema{}
		if err := aschemaFromAny(v, f.Type); err != nil {
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
// keys, case-insensitively. "error" is the record alias, defined by the
// "fields" key like "record".
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
		if _, ok := lookupCI(m, key); ok && (forType == tp || (key == "fields" && tp == "error")) {
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
		switch {
		case strings.EqualFold(k, "default"), strings.EqualFold(k, "order"):
			// Field-only keys, do not propagate.
		case strings.EqualFold(k, "aliases"):
			// Flat-format aliases belong to the field, not the type.
		case strings.EqualFold(k, "name"), strings.EqualFold(k, "namespace"):
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
	o, err := aobjectFromMap(flatLiftTypeMap(m, tp))
	if err != nil {
		return err
	}
	f.Type = &aschema{object: o}
	f.Logical, f.Scale, f.Precision = "", nil, nil
	return nil
}

// stringSliceFrom reads m[key] (case-insensitive) as a []string. The
// second return reports presence; a present non-array or non-string
// element is an error, matching encoding/json's []string decode.
func stringSliceFrom(m map[string]any, key string) ([]string, bool, error) {
	v, ok := lookupCI(m, key)
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

// intPtrFrom reads m[key] (case-insensitive) as a *int by re-marshaling
// the small value and reusing stdlib int decode, so the accept/reject
// behavior (rejecting floats, strings, overflow) is identical to the
// former *int struct field.
func intPtrFrom(m map[string]any, key string) (*int, error) {
	v, ok := lookupCI(m, key)
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
// Reserved-key casing is matched case-insensitively, like every reserved
// key.
var strayRoutedKeys = [...]string{
	"items", "values", "fields", "symbols", "size", "name", "namespace", "aliases",
}

// canonicalStrayKey maps k (any letter case) to the canonical spelling of
// the stray-routed key it names, or "" when it is not one.
func canonicalStrayKey(k string) string {
	for _, key := range strayRoutedKeys {
		if strings.EqualFold(k, key) {
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
		return aschemaFromAny(v, &s) == nil
	case "fields":
		arr, ok := v.([]any)
		if !ok {
			return false
		}
		for i := range arr {
			var f afield
			if afieldFromAny(arr[i], &f) != nil {
				return false
			}
		}
		return true
	}
	return false
}
