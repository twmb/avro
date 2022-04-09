package main

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

var unixEpoch = time.Unix(0, 0)

type typeField struct {
	t reflect.Type
	o *aobject
}

func (o *aobject) typeFieldMapping(t reflect.Type) ([]int, error) {
	if m, ok := o.typeFields.Load(t); ok {
		r, _ := m.([]int)
		return r, nil
	}

	m := make(map[string]int)

	// First build direct avro names, then build any auto-mapping.
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		aname, exists := sf.Tag.Lookup("avro")
		if !exists || aname == "-" {
			continue
		}
		if comma := strings.IndexByte(aname, ','); comma != -1 {
			aname = aname[:comma]
		}
		m[aname] = i
	}
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if _, exists := sf.Tag.Lookup("avro"); exists {
			continue
		}
		if _, exists := m[sf.Name]; exists {
			continue
		}
		m[sf.Name] = i
	}

	ats := make([]int, 0, len(m))
	for _, f := range o.Fields {
		at, exists := m[f.Name]
		if !exists {
			return nil, fmt.Errorf("record type %s is missing field %s", t, f.Name)
		}
		ats = append(ats, at)
	}

	o.typeFields.Store(t, ats)
	return ats, nil
}
