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
	if v, ok := lookupCI(m, "name"); ok {
		ns, ok := v.(string)
		if !ok {
			return nil, schemaTypeMismatch("name", "string")
		}
		o.Name = ns
	}
	if v, ok := lookupCI(m, "namespace"); ok {
		ns, ok := v.(string)
		if !ok {
			return nil, schemaTypeMismatch("namespace", "string")
		}
		o.Namespace = &ns
	}
	if ss, ok, err := stringSliceFrom(m, "symbols"); err != nil {
		return nil, err
	} else if ok {
		o.Symbols = ss
	}
	if ss, ok, err := stringSliceFrom(m, "aliases"); err != nil {
		return nil, err
	} else if ok {
		o.Aliases = ss
	}
	if v, ok := lookupCI(m, "items"); ok {
		o.Items = &aschema{}
		if err := aschemaFromAny(v, o.Items); err != nil {
			return nil, err
		}
	}
	if v, ok := lookupCI(m, "values"); ok {
		o.Values = &aschema{}
		if err := aschemaFromAny(v, o.Values); err != nil {
			return nil, err
		}
	}
	if v, ok := lookupCI(m, "size"); ok {
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		var l laxInt
		if err := l.UnmarshalJSON(raw); err != nil {
			return nil, err
		}
		o.Size = &l
	}
	if v, ok := lookupCI(m, "logicalType"); ok {
		ls, ok := v.(string)
		if !ok {
			return nil, schemaTypeMismatch("logicalType", "string")
		}
		o.Logical = ls
	}
	if p, err := intPtrFrom(m, "scale"); err != nil {
		return nil, err
	} else {
		o.Scale = p
	}
	if p, err := intPtrFrom(m, "precision"); err != nil {
		return nil, err
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
		fs, ok := v.([]any)
		if !ok {
			return nil, schemaTypeMismatch("fields", "array")
		}
		o.Fields = make([]afield, len(fs))
		for i := range fs {
			if err := afieldFromAny(fs[i], &o.Fields[i]); err != nil {
				return nil, err
			}
		}
	}
	// Extra (non-reserved) properties. The tree was decoded with
	// UseNumber; normalizeJSONValue applies the same value-based numeric
	// normalization (json.Number → int64/float64, exponent-overflow →
	// ±Inf) the former unmarshalAnyPreservePrecision capture did.
	for k, v := range m {
		if schemaReservedKeyCI(k) {
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
