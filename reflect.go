package avro

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

type stringer interface {
	String() string
}

func indirect(v reflect.Value) (reflect.Value, error) {
start:
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return v, fmt.Errorf("invalid nil in non-union, non-null")
		}
		v = v.Elem()
		goto start
	case reflect.Interface:
		if v.IsNil() {
			return v, fmt.Errorf("invalid nil in non-union, non-null")
		}
		v = v.Elem()
		goto start
	default:
		return v, nil
	}
}

func indirectAlloc(v reflect.Value) reflect.Value {
	for {
		switch v.Kind() {
		case reflect.Pointer:
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		case reflect.Interface:
			if v.IsNil() {
				return v
			}
			v = v.Elem()
		default:
			return v
		}
	}
}

// fieldByIndex is like reflect.Value.FieldByIndex but allocates nil embedded
// pointer structs along the path, which is needed during deserialization.
func fieldByIndex(v reflect.Value, index []int) reflect.Value {
	for _, i := range index {
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v
}

// typeFieldMapping returns the field index paths for each schema field in the
// given Go type. It handles embedded (anonymous) structs by recursing into
// them. Avro-tagged fields take priority over name-matched fields, and
// shallower fields take priority over deeper ones.
//
// The result is cached in the provided sync.Map for subsequent calls with the
// same type.
func typeFieldMapping(fieldNames []string, cache *sync.Map, t reflect.Type) ([][]int, error) {
	if v, ok := cache.Load(t); ok {
		return v.([][]int), nil
	}

	type fieldInfo struct {
		name   string
		index  []int
		tagged bool
	}

	var fields []fieldInfo
	var collect func(t reflect.Type, index []int, visited map[reflect.Type]bool)
	collect = func(t reflect.Type, index []int, visited map[reflect.Type]bool) {
		if visited[t] {
			return
		}
		visited[t] = true
		for i := 0; i < t.NumField(); i++ {
			sf := t.Field(i)
			idx := make([]int, len(index)+1)
			copy(idx, index)
			idx[len(index)] = i

			if sf.Anonymous {
				ft := sf.Type
				if ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				// Recurse into embedded structs (even unexported
				// ones, since they can have exported fields).
				if ft.Kind() == reflect.Struct {
					tag := sf.Tag.Get("avro")
					if tag == "-" {
						continue
					}
					// If the embedded struct has an explicit avro
					// tag, treat it as a named field rather than
					// inlining its fields.
					if name := strings.Split(tag, ",")[0]; name != "" {
						fields = append(fields, fieldInfo{
							name:   name,
							index:  idx,
							tagged: true,
						})
						continue
					}
					collect(ft, idx, visited)
					continue
				}
				if !sf.IsExported() {
					continue
				}
			} else if !sf.IsExported() {
				continue
			}

			tag := sf.Tag.Get("avro")
			if tag == "-" {
				continue
			}
			name := strings.Split(tag, ",")[0]
			tagged := name != ""
			if name == "" {
				name = sf.Name
			}
			fields = append(fields, fieldInfo{
				name:   name,
				index:  idx,
				tagged: tagged,
			})
		}
	}
	collect(t, nil, make(map[reflect.Type]bool))

	// Build name -> index map. Tagged fields win over untagged, and
	// shallower fields win over deeper ones (first-seen wins within
	// the same priority since we collect top-down).
	type entry struct {
		index  []int
		tagged bool
	}
	m := make(map[string]entry, len(fields))
	for _, f := range fields {
		if existing, ok := m[f.name]; ok {
			// Tagged always beats untagged.
			if f.tagged && !existing.tagged {
				m[f.name] = entry{f.index, f.tagged}
			}
			// Otherwise first-seen (shallower) wins; skip.
			continue
		}
		m[f.name] = entry{f.index, f.tagged}
	}

	ats := make([][]int, 0, len(fieldNames))
	for _, name := range fieldNames {
		e, exists := m[name]
		if !exists {
			return nil, fmt.Errorf("record type %s is missing field %s", t, name)
		}
		ats = append(ats, e.index)
	}

	cache.Store(t, ats)
	return ats, nil
}
