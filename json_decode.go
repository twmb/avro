package avro

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"time"
)

// decodeLogicalInt applies logical type conversion for int-backed logical types
// when decoding to *any targets.
func decodeLogicalInt(val int32, node *schemaNode) any {
	switch node.logical {
	case "date":
		return dateToTime(val)
	case "time-millis":
		return timeMillisToDuration(val)
	}
	return val
}

// decodeLogicalLong applies logical type conversion for long-backed logical types
// when decoding to *any targets.
func decodeLogicalLong(val int64, node *schemaNode) any {
	switch node.logical {
	case "timestamp-millis", "local-timestamp-millis":
		return timestampMillisToTime(val)
	case "timestamp-micros", "local-timestamp-micros":
		return timestampMicrosToTime(val)
	case "timestamp-nanos", "local-timestamp-nanos":
		return timestampNanosToTime(val)
	case "time-micros":
		return timeMicrosToDuration(val)
	}
	return val
}

// decodeLogicalBytes applies logical type conversion for bytes-backed logical types
// when decoding to *any targets.
func decodeLogicalBytes(b []byte, node *schemaNode) any {
	if node.logical == "decimal" {
		r := bytesToRat(b, node.scale)
		return json.Number(r.FloatString(node.scale))
	}
	return b
}

// decodeLogicalFixed applies logical type conversion for fixed-backed logical types
// when decoding to *any targets.
func decodeLogicalFixed(b []byte, node *schemaNode) any {
	switch node.logical {
	case "decimal":
		r := bytesToRat(b, node.scale)
		return json.Number(r.FloatString(node.scale))
	case "duration":
		if len(b) == 12 {
			return DurationFromBytes(b)
		}
	}
	return b
}

// assignAny sets a native Go value on a reflect.Value target,
// handling nil for interface targets.
func assignAny(v reflect.Value, val any) {
	if val == nil {
		if v.Kind() == reflect.Interface {
			v.Set(reflect.Zero(v.Type()))
		}
		return
	}
	v.Set(reflect.ValueOf(val))
}

// consumeSlabString consumes a JSON string and copies it into the slab
// allocator, batching small string allocations into one backing buffer.
func (ctx *jsonDecoder) consumeSlabString() (string, error) {
	start, end, hasEscapes, err := ctx.scanner.consumeStringRaw()
	if err != nil {
		return "", err
	}
	if hasEscapes {
		return resolveJSONEscapes(ctx.scanner.data[start:end])
	}
	return ctx.slab.string(ctx.scanner.data[start:], end-start), nil
}

// jsonDecoder is the state for schema-guided JSON decoding.
type jsonDecoder struct {
	scanner        *jsonScanner
	slab           *slab
	customDecoders map[*schemaNode][]func(any, *SchemaNode) (any, error)
	customSNs      map[*schemaNode]*SchemaNode
	wrapUnions     bool
	qualifyLogical bool
}

// decodeValue is the core recursive decoder. It reads the next JSON
// value from the scanner, guided by the schema node, and assigns to v.
//
// For interface (any) targets, it produces JSON-native or enriched Go
// values. For typed targets (struct, int, string, etc.), it assigns
// directly.
func (ctx *jsonDecoder) decodeValue(v reflect.Value, node *schemaNode) error {
	// For custom decoders, we must produce an any value first, pass
	// it through the decoder chain, then assign. This is the only
	// case where typed targets go through an intermediate.
	if decs := ctx.customDecoders[node]; len(decs) > 0 {
		return ctx.decodeWithCustom(v, node, decs)
	}

	// Unions handle pointer/nil targets specially (before indirectAlloc),
	// so dispatch early.
	if node.kind == "union" {
		return ctx.decodeUnion(v, node)
	}

	v = indirectAlloc(v)
	toAny := v.Kind() == reflect.Interface

	switch node.kind {
	case "null":
		return ctx.decodeNull(v, toAny)
	case "boolean":
		return ctx.decodeBool(v, toAny)
	case "int":
		return ctx.decodeInt(v, node, toAny)
	case "long":
		return ctx.decodeLong(v, node, toAny)
	case "float":
		return ctx.decodeFloat(v, toAny)
	case "double":
		return ctx.decodeDouble(v, toAny)
	case "string":
		return ctx.decodeString(v, toAny)
	case "enum":
		return ctx.decodeEnum(v, node, toAny)
	case "bytes":
		return ctx.decodeBytes(v, node, toAny)
	case "fixed":
		return ctx.decodeFixed(v, node, toAny)
	case "array":
		return ctx.decodeArray(v, node, toAny)
	case "map":
		return ctx.decodeMap(v, node, toAny)
	case "record":
		return ctx.decodeRecord(v, node, toAny)
	default:
		return fmt.Errorf("avro json: unsupported schema kind %q", node.kind)
	}
}

func (ctx *jsonDecoder) decodeWithCustom(v reflect.Value, node *schemaNode, decs []func(any, *SchemaNode) (any, error)) error {
	// Decode as-if to *any to get the enriched native value.
	// Temporarily clear custom decoders for this node to avoid
	// infinite recursion (decodeValue would call decodeWithCustom again).
	saved := ctx.customDecoders[node]
	delete(ctx.customDecoders, node)
	var tmp any
	tmpV := reflect.ValueOf(&tmp).Elem()
	err := ctx.decodeValue(tmpV, node)
	ctx.customDecoders[node] = saved
	if err != nil {
		return err
	}
	sn := ctx.customSNs[node]
	for _, dec := range decs {
		out, err := dec(tmp, sn)
		if err != nil {
			if errors.Is(err, ErrSkipCustomType) {
				continue
			}
			return err
		}
		assignAny(indirectAlloc(v), out)
		return nil
	}
	assignAny(indirectAlloc(v), tmp)
	return nil
}

func (ctx *jsonDecoder) decodeNull(v reflect.Value, toAny bool) error {
	if err := ctx.scanner.consumeNull(); err != nil {
		return err
	}
	if toAny {
		v.Set(reflect.Zero(v.Type()))
	} else {
		switch v.Kind() {
		case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Interface:
			v.Set(reflect.Zero(v.Type()))
		}
	}
	return nil
}

func (ctx *jsonDecoder) decodeBool(v reflect.Value, toAny bool) error {
	b, err := ctx.scanner.consumeBool()
	if err != nil {
		return err
	}
	if toAny {
		v.Set(reflect.ValueOf(b))
	} else if v.Kind() == reflect.Bool {
		v.SetBool(b)
	} else {
		return &SemanticError{GoType: v.Type(), AvroType: "boolean"}
	}
	return nil
}

func (ctx *jsonDecoder) decodeInt(v reflect.Value, node *schemaNode, toAny bool) error {
	nb, err := ctx.scanner.consumeNumberBytes()
	if err != nil {
		return err
	}
	val, err := parseJSONInt32(nb)
	if err != nil {
		return err
	}
	if toAny {
		v.Set(reflect.ValueOf(decodeLogicalInt(val, node)))
		return nil
	}
	// All DecodeJSON entry points produce addressable values
	// (Schema.DecodeJSON requires a pointer; recursive paths use
	// reflect.New().Elem() or addressable struct fields).
	if v.Type() == timeType && node.logical == "date" {
		*(*time.Time)(v.Addr().UnsafePointer()) = dateToTime(val)
		return nil
	}
	if v.Type() == durationType && node.logical == "time-millis" {
		*(*time.Duration)(v.Addr().UnsafePointer()) = timeMillisToDuration(val)
		return nil
	}
	if v.CanInt() {
		v.SetInt(int64(val))
		return nil
	}
	if v.CanUint() {
		v.SetUint(uint64(val))
		return nil
	}
	return &SemanticError{GoType: v.Type(), AvroType: "int"}
}

func (ctx *jsonDecoder) decodeLong(v reflect.Value, node *schemaNode, toAny bool) error {
	nb, err := ctx.scanner.consumeNumberBytes()
	if err != nil {
		return err
	}
	val, err := parseJSONInt64(nb)
	if err != nil {
		return err
	}
	if toAny {
		v.Set(reflect.ValueOf(decodeLogicalLong(val, node)))
		return nil
	}
	// All DecodeJSON entry points produce addressable values (see decodeInt).
	if v.Type() == timeType {
		p := (*time.Time)(v.Addr().UnsafePointer())
		switch node.logical {
		case "timestamp-millis", "local-timestamp-millis":
			*p = timestampMillisToTime(val)
			return nil
		case "timestamp-micros", "local-timestamp-micros":
			*p = timestampMicrosToTime(val)
			return nil
		case "timestamp-nanos", "local-timestamp-nanos":
			*p = timestampNanosToTime(val)
			return nil
		}
	}
	if v.Type() == durationType && node.logical == "time-micros" {
		*(*time.Duration)(v.Addr().UnsafePointer()) = timeMicrosToDuration(val)
		return nil
	}
	if v.CanInt() {
		v.SetInt(val)
		return nil
	}
	if v.CanUint() {
		v.SetUint(uint64(val))
		return nil
	}
	return &SemanticError{GoType: v.Type(), AvroType: "long"}
}

func (ctx *jsonDecoder) decodeFloat(v reflect.Value, toAny bool) error {
	var f float32
	p := ctx.scanner.peek()
	if p == '"' {
		// NaN/Infinity as string.
		s, err := ctx.scanner.consumeString()
		if err != nil {
			return err
		}
		v32, err := parseSpecialFloat32(s)
		if err != nil {
			return err
		}
		f = v32
	} else if p == 'n' {
		// null → NaN (goavro convention).
		if err := ctx.scanner.consumeNull(); err != nil {
			return err
		}
		f = float32(math.NaN())
	} else {
		nb, err := ctx.scanner.consumeNumberBytes()
		if err != nil {
			return err
		}
		f64, err := strconv.ParseFloat(string(nb), 32)
		if err != nil {
			// Accept ±Inf from overflow (e.g. 1e999, goavro convention).
			if math.IsInf(f64, 0) {
				f = float32(f64)
			} else {
				return fmt.Errorf("avro json: invalid float: %w", err)
			}
		} else {
			f = float32(f64)
		}
	}
	if toAny {
		v.Set(reflect.ValueOf(f))
	} else if v.CanFloat() {
		v.SetFloat(float64(f))
	} else {
		return &SemanticError{GoType: v.Type(), AvroType: "float"}
	}
	return nil
}

func (ctx *jsonDecoder) decodeDouble(v reflect.Value, toAny bool) error {
	var f float64
	p := ctx.scanner.peek()
	if p == '"' {
		s, err := ctx.scanner.consumeString()
		if err != nil {
			return err
		}
		var err2 error
		f, err2 = parseSpecialFloat(s)
		if err2 != nil {
			return err2
		}
	} else if p == 'n' {
		if err := ctx.scanner.consumeNull(); err != nil {
			return err
		}
		f = math.NaN()
	} else {
		nb, err := ctx.scanner.consumeNumberBytes()
		if err != nil {
			return err
		}
		var err2 error
		f, err2 = strconv.ParseFloat(string(nb), 64)
		if err2 != nil {
			// Accept ±Inf from overflow (e.g. 1e999, goavro convention).
			if !math.IsInf(f, 0) {
				return fmt.Errorf("avro json: invalid double: %w", err2)
			}
		}
	}
	if toAny {
		v.Set(reflect.ValueOf(f))
	} else if v.CanFloat() {
		v.SetFloat(f)
	} else {
		return &SemanticError{GoType: v.Type(), AvroType: "double"}
	}
	return nil
}

func (ctx *jsonDecoder) decodeString(v reflect.Value, toAny bool) error {
	s, err := ctx.consumeSlabString()
	if err != nil {
		return err
	}
	if toAny {
		v.Set(reflect.ValueOf(s))
	} else if v.Kind() == reflect.String {
		v.SetString(s)
	} else if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		v.SetBytes([]byte(s))
	} else {
		return &SemanticError{GoType: v.Type(), AvroType: "string"}
	}
	return nil
}

func (ctx *jsonDecoder) decodeEnum(v reflect.Value, node *schemaNode, toAny bool) error {
	s, err := ctx.consumeSlabString()
	if err != nil {
		return err
	}
	valid := false
	for _, sym := range node.symbols {
		if sym == s {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("avro json: unknown enum symbol %q", s)
	}
	if toAny {
		v.Set(reflect.ValueOf(s))
	} else if v.Kind() == reflect.String {
		v.SetString(s)
	} else {
		return &SemanticError{GoType: v.Type(), AvroType: "enum"}
	}
	return nil
}

func (ctx *jsonDecoder) decodeBytes(v reflect.Value, node *schemaNode, toAny bool) error {
	// Decimal logical type: accept JSON numbers (e.g. 0.33) in addition
	// to Avro JSON byte strings, for round-trip with EncodeJSON output.
	if node.logical == "decimal" {
		if c := ctx.scanner.peek(); c != '"' && c != 0 {
			nb, err := ctx.scanner.consumeNumberBytes()
			if err != nil {
				return err
			}
			r, ok := new(big.Rat).SetString(string(nb))
			if !ok {
				return fmt.Errorf("avro json: invalid decimal number %q", nb)
			}
			if toAny {
				v.Set(reflect.ValueOf(json.Number(r.FloatString(node.scale))))
				return nil
			}
			return assignDecimalRat(v, r, node)
		}
	}
	start, end, _, err := ctx.scanner.consumeStringRaw()
	if err != nil {
		return err
	}
	b, err := scanAvroJSONBytes(ctx.scanner.data[start:end])
	if err != nil {
		return err
	}
	if toAny {
		v.Set(reflect.ValueOf(decodeLogicalBytes(b, node)))
		return nil
	}
	return assignBytes(v, b, node)
}

func (ctx *jsonDecoder) decodeFixed(v reflect.Value, node *schemaNode, toAny bool) error {
	// Decimal logical type: accept JSON numbers, same as decodeBytes.
	if node.logical == "decimal" {
		if c := ctx.scanner.peek(); c != '"' && c != 0 {
			nb, err := ctx.scanner.consumeNumberBytes()
			if err != nil {
				return err
			}
			r, ok := new(big.Rat).SetString(string(nb))
			if !ok {
				return fmt.Errorf("avro json: invalid decimal number %q", nb)
			}
			if toAny {
				v.Set(reflect.ValueOf(json.Number(r.FloatString(node.scale))))
				return nil
			}
			return assignDecimalRat(v, r, node)
		}
	}
	start, end, _, err := ctx.scanner.consumeStringRaw()
	if err != nil {
		return err
	}
	b, err := scanAvroJSONBytes(ctx.scanner.data[start:end])
	if err != nil {
		return err
	}
	if toAny {
		v.Set(reflect.ValueOf(decodeLogicalFixed(b, node)))
		return nil
	}
	return assignBytes(v, b, node)
}

// assignBytes assigns decoded bytes to a typed target, handling decimal
// and duration logical types.
func assignBytes(v reflect.Value, b []byte, node *schemaNode) error {
	if node.logical == "decimal" {
		r := bytesToRat(b, node.scale)
		if v.Type() == jsonNumberType {
			v.Set(reflect.ValueOf(json.Number(r.FloatString(node.scale))))
			return nil
		}
		if v.Type() == bigRatType {
			v.Set(reflect.ValueOf(*r))
			return nil
		}
	}
	if node.logical == "duration" && v.Type() == avroDurationType {
		v.Set(reflect.ValueOf(DurationFromBytes(b)))
		return nil
	}
	if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		v.SetBytes(b)
		return nil
	}
	if v.Kind() == reflect.String {
		v.SetString(string(b))
		return nil
	}
	if v.Kind() == reflect.Array && v.Type().Elem().Kind() == reflect.Uint8 {
		reflect.Copy(v, reflect.ValueOf(b))
		return nil
	}
	return &SemanticError{GoType: v.Type(), AvroType: node.kind}
}

// assignDecimalRat assigns a *big.Rat to a typed target for decimal fields
// decoded from JSON numbers.
func assignDecimalRat(v reflect.Value, r *big.Rat, node *schemaNode) error {
	if v.Type() == jsonNumberType {
		v.Set(reflect.ValueOf(json.Number(r.FloatString(node.scale))))
		return nil
	}
	if v.Type() == bigRatType {
		v.Set(reflect.ValueOf(*r))
		return nil
	}
	return &SemanticError{GoType: v.Type(), AvroType: node.kind}
}

func (ctx *jsonDecoder) decodeArray(v reflect.Value, node *schemaNode, toAny bool) error {
	if err := ctx.scanner.expect('['); err != nil {
		return err
	}
	if toAny {
		var arr []any
		if ctx.scanner.peek() != ']' {
			var elem any
			elemV := reflect.ValueOf(&elem).Elem()
			for {
				if err := ctx.decodeValue(elemV, node.items); err != nil {
					return err
				}
				arr = append(arr, elem)
				elem = nil
				if ctx.scanner.peek() != ',' {
					break
				}
				ctx.scanner.pos++ // consume comma
			}
		}
		if arr == nil {
			arr = []any{}
		}
		if err := ctx.scanner.expect(']'); err != nil {
			return err
		}
		v.Set(reflect.ValueOf(arr))
		return nil
	}
	// Typed slice target.
	if v.Kind() != reflect.Slice {
		return &SemanticError{GoType: v.Type(), AvroType: "array"}
	}
	v.Set(reflect.MakeSlice(v.Type(), 0, 0))
	if ctx.scanner.peek() != ']' {
		elem := reflect.New(v.Type().Elem()).Elem()
		for {
			if err := ctx.decodeValue(elem, node.items); err != nil {
				return err
			}
			v.Set(reflect.Append(v, elem))
			elem.SetZero()
			if ctx.scanner.peek() != ',' {
				break
			}
			ctx.scanner.pos++
		}
	}
	return ctx.scanner.expect(']')
}

func (ctx *jsonDecoder) decodeMap(v reflect.Value, node *schemaNode, toAny bool) error {
	if err := ctx.scanner.expect('{'); err != nil {
		return err
	}
	if toAny {
		m := make(map[string]any)
		if ctx.scanner.peek() != '}' {
			var val any
			valV := reflect.ValueOf(&val).Elem()
			for {
				key, err := ctx.consumeSlabString()
				if err != nil {
					return err
				}
				if err := ctx.scanner.expect(':'); err != nil {
					return err
				}
				if err := ctx.decodeValue(valV, node.values); err != nil {
					return err
				}
				m[key] = val
				val = nil
				if ctx.scanner.peek() != ',' {
					break
				}
				ctx.scanner.pos++
			}
		}
		if err := ctx.scanner.expect('}'); err != nil {
			return err
		}
		v.Set(reflect.ValueOf(m))
		return nil
	}
	// Typed map target.
	if v.Kind() != reflect.Map || v.Type().Key().Kind() != reflect.String {
		return &SemanticError{GoType: v.Type(), AvroType: "map"}
	}
	if v.IsNil() {
		v.Set(reflect.MakeMap(v.Type()))
	}
	valType := v.Type().Elem()
	if ctx.scanner.peek() != '}' {
		elem := reflect.New(valType).Elem()
		for {
			key, err := ctx.consumeSlabString()
			if err != nil {
				return err
			}
			if err := ctx.scanner.expect(':'); err != nil {
				return err
			}
			if err := ctx.decodeValue(elem, node.values); err != nil {
				return err
			}
			v.SetMapIndex(reflect.ValueOf(key), elem)
			elem.SetZero()
			if ctx.scanner.peek() != ',' {
				break
			}
			ctx.scanner.pos++
		}
	}
	return ctx.scanner.expect('}')
}

func (ctx *jsonDecoder) decodeRecord(v reflect.Value, node *schemaNode, toAny bool) error {
	if err := ctx.scanner.expect('{'); err != nil {
		return err
	}
	if toAny {
		return ctx.decodeRecordAny(v, node)
	}
	// Typed target: struct or map[string]T.
	if v.Kind() == reflect.Map && v.Type().Key().Kind() == reflect.String {
		return ctx.decodeRecordMap(v, node)
	}
	if v.Kind() == reflect.Struct {
		return ctx.decodeRecordStruct(v, node)
	}
	return &SemanticError{GoType: v.Type(), AvroType: "record"}
}

// iterateRecordFields handles the common JSON object field loop for
// records: read each key, look up the field index, skip unknown fields,
// and call handle for known fields. After the loop, validates that all
// required fields were seen.
func (ctx *jsonDecoder) iterateRecordFields(node *schemaNode, handle func(idx int, key string) error) error {
	seen := make([]bool, len(node.fields))
	if ctx.scanner.peek() != '}' {
		for {
			// Zero-copy: key is used only for fieldIdx lookup and
			// error messages, never stored in output.
			key, err := ctx.scanner.consumeStringZeroCopy()
			if err != nil {
				return err
			}
			if err := ctx.scanner.expect(':'); err != nil {
				return err
			}
			idx := -1
			if node.fieldIdx != nil {
				if i, ok := node.fieldIdx[key]; ok {
					idx = i
				}
			}
			if idx < 0 {
				if err := ctx.scanner.skipValue(); err != nil {
					return err
				}
			} else {
				seen[idx] = true
				if err := handle(idx, key); err != nil {
					return err
				}
			}
			if ctx.scanner.peek() != ',' {
				break
			}
			ctx.scanner.pos++
		}
	}
	if err := ctx.scanner.expect('}'); err != nil {
		return err
	}
	for i, f := range node.fields {
		if !seen[i] && !f.hasDefault {
			return fmt.Errorf("avro json: record %q missing required field %q", node.name, f.name)
		}
	}
	return nil
}

func (ctx *jsonDecoder) decodeRecordAny(v reflect.Value, node *schemaNode) error {
	m := make(map[string]any, len(node.fields))
	var val any
	valV := reflect.ValueOf(&val).Elem()
	err := ctx.iterateRecordFields(node, func(idx int, key string) error {
		f := &node.fields[idx]
		if err := ctx.decodeValue(valV, f.node); err != nil {
			return fmt.Errorf("field %q: %w", key, err)
		}
		m[f.name] = val
		val = nil
		return nil
	})
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(m))
	return nil
}

func (ctx *jsonDecoder) decodeRecordMap(v reflect.Value, node *schemaNode) error {
	if v.IsNil() {
		v.Set(reflect.MakeMap(v.Type()))
	}
	elem := reflect.New(v.Type().Elem()).Elem()
	return ctx.iterateRecordFields(node, func(idx int, key string) error {
		f := &node.fields[idx]
		if err := ctx.decodeValue(elem, f.node); err != nil {
			return fmt.Errorf("field %q: %w", key, err)
		}
		v.SetMapIndex(f.nameVal, elem)
		elem.SetZero()
		return nil
	})
}

func (ctx *jsonDecoder) decodeRecordStruct(v reflect.Value, node *schemaNode) error {
	dr := node.deserRecord
	if dr == nil {
		return &SemanticError{GoType: v.Type(), AvroType: "record", Err: errors.New("no record metadata")}
	}
	mapping, err := typeFieldMapping(dr.names, &dr.cache, v.Type())
	if err != nil {
		return err
	}
	return ctx.iterateRecordFields(node, func(idx int, _ string) error {
		f := &node.fields[idx]
		fv := fieldByIndex(v, mapping.indices[idx])
		return ctx.decodeValue(fv, f.node)
	})
}

func (ctx *jsonDecoder) decodeUnion(v reflect.Value, node *schemaNode) error {
	p := ctx.scanner.peek()

	// JSON null → null branch. Handle before indirectAlloc so *T
	// pointer targets stay nil.
	if p == 'n' {
		if err := ctx.scanner.consumeNull(); err != nil {
			return err
		}
		switch v.Kind() {
		case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Interface:
			v.Set(reflect.Zero(v.Type()))
		}
		return nil
	}

	v = indirectAlloc(v)
	toAny := v.Kind() == reflect.Interface

	// JSON object → could be tagged union {"type": value} or bare record/map.
	if p == '{' {
		return ctx.decodeUnionObject(v, node, toAny)
	}

	// Bare non-object value — match by JSON token type.
	return ctx.decodeUnionBare(v, node, toAny, p)
}

func (ctx *jsonDecoder) decodeUnionObject(v reflect.Value, node *schemaNode, toAny bool) error {
	// Save position for backtracking.
	savedPos := ctx.scanner.pos

	// Try tagged union: {"branchName": value}.
	ctx.scanner.pos++ // consume '{'
	ctx.scanner.skipWhitespace()
	if ctx.scanner.peek() != '}' {
		// Zero-copy: key is used only for branch name lookup, never stored.
		key, err := ctx.scanner.consumeStringZeroCopy()
		if err == nil {
			if branch := findUnionBranch(node, key); branch != nil {
				if err := ctx.scanner.expect(':'); err == nil {
					if toAny {
						var val any
						if err := ctx.decodeValue(reflect.ValueOf(&val).Elem(), branch); err == nil {
							if ctx.scanner.peek() == '}' {
								ctx.scanner.pos++
								v.Set(reflect.ValueOf(ctx.wrapUnion(val, branch)))
								return nil
							}
						}
					} else {
						if err := ctx.decodeUnionBranchTyped(v, branch); err == nil {
							if ctx.scanner.peek() == '}' {
								ctx.scanner.pos++
								return nil
							}
						}
					}
				}
			}
		}
	}

	// Tagged interpretation failed — backtrack and try bare.
	ctx.scanner.pos = savedPos
	return ctx.decodeUnionBare(v, node, toAny, '{')
}

func (ctx *jsonDecoder) decodeUnionBare(v reflect.Value, node *schemaNode, toAny bool, p byte) error {
	// Match by JSON token type against branch kinds.
	for _, branch := range node.branches {
		if branch.kind == "null" {
			continue
		}
		if !jsonTokenMatchesBranch(p, branch) {
			continue
		}
		savedPos := ctx.scanner.pos
		if toAny {
			var val any
			if err := ctx.decodeValue(reflect.ValueOf(&val).Elem(), branch); err == nil {
				v.Set(reflect.ValueOf(ctx.wrapUnion(val, branch)))
				return nil
			}
		} else {
			if err := ctx.decodeValue(v, branch); err == nil {
				return nil
			}
		}
		ctx.scanner.pos = savedPos
	}
	return fmt.Errorf("avro json: no union branch matched at offset %d", ctx.scanner.pos)
}

func (ctx *jsonDecoder) decodeUnionBranchTyped(v reflect.Value, branch *schemaNode) error {
	return ctx.decodeValue(v, branch)
}

func (ctx *jsonDecoder) wrapUnion(val any, branch *schemaNode) any {
	if !ctx.wrapUnions || val == nil {
		return val
	}
	name := unionBranchName(branch)
	if ctx.qualifyLogical && branch.logical != "" {
		name = branch.kind + "." + branch.logical
	}
	return map[string]any{name: val}
}

// jsonTokenMatchesBranch returns true if a JSON token type could
// potentially match a given schema branch kind.
func jsonTokenMatchesBranch(p byte, branch *schemaNode) bool {
	switch p {
	case '"':
		switch branch.kind {
		case "string", "enum", "bytes", "fixed":
			return true
		case "float", "double":
			return true // NaN/Infinity strings
		}
	case 't', 'f':
		return branch.kind == "boolean"
	case '[':
		return branch.kind == "array"
	case '{':
		return branch.kind == "record" || branch.kind == "map"
	default: // digit or '-'
		switch branch.kind {
		case "int", "long", "float", "double":
			return true
		}
	}
	return false
}
