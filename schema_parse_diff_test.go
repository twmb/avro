package avro

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

// buildFromAschema runs the build pipeline on an already-parsed aschema,
// mirroring parse()'s body after the unmarshal so the front-end (old
// json.Unmarshaler vs new parseSchemaTree) is the only variable.
func buildFromAschema(schema string, orig *aschema) (*Schema, error) {
	b := &builder{named: make(map[string]*namedType)}
	if err := b.build("", orig); err != nil {
		return nil, err
	}
	if err := b.finalize(); err != nil {
		return nil, err
	}
	s := &Schema{ser: b.ser, deser: b.deser, c: b.canon, node: b.node, full: schema, custom: b.custom}
	s.soe[0], s.soe[1] = 0xC3, 0x01
	h := NewRabin()
	h.Write(s.Canonical())
	for i, x := range uint64ToLE(h.Sum64()) {
		s.soe[2+i] = x
	}
	return s, nil
}

func uint64ToLE(v uint64) [8]byte {
	var b [8]byte
	for i := 0; i < 8; i++ {
		b[i] = byte(v >> (8 * i))
	}
	return b
}

// TestDiff_ParseFrontEndEquivalence proves the new O(n) parseSchemaTree
// front-end builds schemas indistinguishable from the old json.Unmarshaler
// front-end across a corpus spanning every schema shape — same Canonical
// form, same fingerprint, same String(). Run BEFORE switching parse() over.
func TestDiff_ParseFrontEndEquivalence(t *testing.T) {
	corpus := []string{
		`"int"`, `"string"`, `"null"`, `"bytes"`, `"boolean"`, `"long"`, `"float"`, `"double"`,
		`{"type":"int"}`,
		`{"type":"fixed","name":"f","size":4}`,
		`{"type":"fixed","name":"f","size":"4"}`,
		`{"type":"enum","name":"e","symbols":["A","B","C"]}`,
		`{"type":"enum","name":"e","symbols":["A"],"default":"A"}`,
		`{"type":"array","items":"int"}`,
		`{"type":"array","items":{"type":"long","logicalType":"timestamp-millis"}}`,
		`{"type":"map","values":"string"}`,
		`["null","int"]`,
		`["null","string","long"]`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`,
		`{"type":"record","name":"R","namespace":"com.x","fields":[{"name":"a","type":"int"}]}`,
		`{"type":"record","name":"R","namespace":"com.x","fields":[{"name":"c","type":{"type":"record","name":"Inner","namespace":"","fields":[{"name":"v","type":"int"}]}}]}`,
		`{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]},{"name":"v","type":"int"}]}`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int","default":42},{"name":"b","type":"string","default":"hi"}]}`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":"long","default":9223372036854775807}]}`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":["null","int"],"default":null}]}`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"map","values":"int"},"default":{"x":1,"y":2}}]}`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":"int"},"default":[1,2,3]}]}`,
		`{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}`,
		`{"type":"fixed","name":"d","size":8,"logicalType":"decimal","precision":18,"scale":4}`,
		`{"type":"string","logicalType":"uuid"}`,
		`{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`,
		// field-level logicalType lift (the Java/JDBC idiom)
		`{"type":"record","name":"R","fields":[{"name":"ts","type":"long","logicalType":"timestamp-millis"}]}`,
		`{"type":"record","name":"R","fields":[{"name":"ts","type":["null","long"],"logicalType":"timestamp-millis"}]}`,
		`{"type":"record","name":"R","fields":[{"name":"d","type":"bytes","logicalType":"decimal","precision":9,"scale":2}]}`,
		// flat (goavro) field format
		`{"type":"record","name":"R","fields":[{"name":"e","type":"enum","symbols":["A","B"]}]}`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":"array","items":"int"}]}`,
		`{"type":"record","name":"R","fields":[{"name":"m","type":"map","values":"long"}]}`,
		`{"type":"record","name":"R","fields":[{"name":"f","type":"fixed","size":4,"name":"Inner"}]}`,
		// extras / custom props
		`{"type":"record","name":"R","com.acme.tag":"hello","extra":123,"bignum":99999999999999999999,"fields":[{"name":"a","type":"int"}]}`,
		`{"type":"int","custom.float":1.5,"custom.bool":true,"custom.arr":[1,2],"custom.obj":{"k":"v"}}`,
		// case-insensitive keys
		`{"TYPE":"record","NAME":"R","FIELDS":[{"NAME":"a","TYPE":"int"}]}`,
		`{"Type":"fixed","Name":"f","Size":4}`,
		// doc (dropped by aobject), aliases, order
		`{"type":"record","name":"R","doc":"hi","aliases":["Old"],"fields":[{"name":"a","type":"int","doc":"fld","order":"descending","aliases":["x"]}]}`,
		// wrapped name ref
		`{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"fixed","name":"F","size":2}},{"name":"b","type":"F"}]}`,
		// duplicate keys (last-wins)
		`{"type":"int","type":"string"}`,
		// nested deep
		`{"type":"array","items":{"type":"array","items":{"type":"map","values":["null","int"]}}}`,
	}

	for _, schema := range corpus {
		t.Run(schema, func(t *testing.T) {
			// OLD front-end (current parse()).
			sOld, errOld := Parse(schema)
			// NEW front-end.
			treeNew, errNew := parseSchemaTree(schema)
			var sNew *Schema
			if errNew == nil {
				sNew, errNew = buildFromAschema(schema, treeNew)
			}

			if (errOld == nil) != (errNew == nil) {
				t.Fatalf("error mismatch: old=%v new=%v", errOld, errNew)
			}
			if errOld != nil {
				return // both errored; ok
			}
			if co, cn := string(sOld.Canonical()), string(sNew.Canonical()); co != cn {
				t.Errorf("canonical differs:\n old: %s\n new: %s", co, cn)
			}
			if fo, fn := fmt.Sprintf("%x", sOld.Fingerprint(sha256.New())), fmt.Sprintf("%x", sNew.Fingerprint(sha256.New())); fo != fn {
				t.Errorf("fingerprint differs: old=%s new=%s", fo, fn)
			}
			if so, sn := sOld.String(), sNew.String(); so != sn {
				t.Errorf("String differs:\n old: %s\n new: %s", so, sn)
			}
		})
	}
}
