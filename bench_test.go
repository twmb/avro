package avro

import (
	"fmt"
	"strings"
	"testing"
)

// benchSuperheroSchema is the Superhero record schema without a union wrapper.
const benchSuperheroSchema = `{
	"name": "Superhero",
	"type": "record",
	"fields": [
		{"name": "id", "type": "int"},
		{"name": "affiliation_id", "type": "int"},
		{"name": "name", "type": "string"},
		{"name": "life", "type": "float"},
		{"name": "energy", "type": "float"},
		{"name": "powers", "type": {
			"type": "array",
			"items": {
				"name": "Superpower",
				"type": "record",
				"fields": [
					{"name": "id", "type": "int"},
					{"name": "name", "type": "string"},
					{"name": "damage", "type": "float"},
					{"name": "energy", "type": "float"},
					{"name": "passive", "type": "boolean"}
				]
			}
		}}
	]
}`

func benchNewSuperhero() *Superhero {
	return &Superhero{
		ID:            234765,
		AffiliationID: 9867,
		Name:          "Wolverine",
		Life:          85.25,
		Energy:        32.75,
		Powers: []*Superpower{
			{ID: 2345, Name: "Bone Claws", Damage: 5, Energy: 1.15, Passive: false},
			{ID: 2346, Name: "Regeneration", Damage: -2, Energy: 0.55, Passive: true},
			{ID: 2347, Name: "Adamant skeleton", Damage: -10, Energy: 0, Passive: true},
		},
	}
}

func BenchmarkSerializeGeneric(b *testing.B) {
	super := map[string]any{
		"id":             int32(234765),
		"affiliation_id": int32(9867),
		"name":           "Wolverine",
		"life":           float32(85.25),
		"energy":         float32(32.75),
		"powers": []map[string]any{
			{"id": int32(2345), "name": "Bone Claws", "damage": float32(5), "energy": float32(1.15), "passive": false},
			{"id": int32(2346), "name": "Regeneration", "damage": float32(-2), "energy": float32(0.55), "passive": true},
			{"id": int32(2347), "name": "Adamant skeleton", "damage": float32(-10), "energy": float32(0), "passive": true},
		},
	}

	s, err := Parse(benchSuperheroSchema)
	if err != nil {
		b.Fatal(err)
	}
	dst, err := s.AppendEncode(nil, super)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, err = s.AppendEncode(dst[:0], super)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseSchema(b *testing.B) {
	b.Run("Primitives", func(b *testing.B) {
		schema := `{"type":"record","name":"prims","fields":[
			{"name":"b","type":"boolean"},
			{"name":"i","type":"int"},
			{"name":"l","type":"long"},
			{"name":"f","type":"float"},
			{"name":"d","type":"double"},
			{"name":"s","type":"string"},
			{"name":"bs","type":"bytes"}
		]}`
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := Parse(schema); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Complex", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := Parse(benchSuperheroSchema); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMapEncode(b *testing.B) {
	s, err := Parse(`{"type":"map","values":"string"}`)
	if err != nil {
		b.Fatal(err)
	}
	m := map[string]string{
		"key1": "value1", "key2": "value2", "key3": "value3",
		"key4": "value4", "key5": "value5",
	}
	dst, err := s.AppendEncode(nil, m)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, err = s.AppendEncode(dst[:0], m)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMapDecode(b *testing.B) {
	s, err := Parse(`{"type":"map","values":"string"}`)
	if err != nil {
		b.Fatal(err)
	}
	m := map[string]string{
		"key1": "value1", "key2": "value2", "key3": "value3",
		"key4": "value4", "key5": "value5",
	}
	encoded, err := s.AppendEncode(nil, m)
	if err != nil {
		b.Fatal(err)
	}
	var out map[string]string
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = nil
		if _, err = s.Decode(encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
	_ = out
}

func BenchmarkEnumEncode(b *testing.B) {
	s, err := Parse(`{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE","YELLOW"]}`)
	if err != nil {
		b.Fatal(err)
	}
	val := "GREEN"
	dst, err := s.AppendEncode(nil, val)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, err = s.AppendEncode(dst[:0], val)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEnumDecode(b *testing.B) {
	s, err := Parse(`{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE","YELLOW"]}`)
	if err != nil {
		b.Fatal(err)
	}
	encoded, err := s.AppendEncode(nil, "GREEN")
	if err != nil {
		b.Fatal(err)
	}
	var out string
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = ""
		if _, err = s.Decode(encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
	_ = out
}

func BenchmarkLargeArrayEncode(b *testing.B) {
	s, err := Parse(benchSuperheroSchema)
	if err != nil {
		b.Fatal(err)
	}
	hero := benchNewSuperhero()
	powers := make([]*Superpower, 100)
	for i := range powers {
		powers[i] = &Superpower{
			ID:      int32(i),
			Name:    fmt.Sprintf("Power-%d", i),
			Damage:  float32(i) * 1.5,
			Energy:  float32(i) * 0.3,
			Passive: i%2 == 0,
		}
	}
	hero.Powers = powers
	dst, err := s.AppendEncode(nil, hero)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, err = s.AppendEncode(dst[:0], hero)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLargeArrayDecode(b *testing.B) {
	s, err := Parse(benchSuperheroSchema)
	if err != nil {
		b.Fatal(err)
	}
	hero := benchNewSuperhero()
	powers := make([]*Superpower, 100)
	for i := range powers {
		powers[i] = &Superpower{
			ID:      int32(i),
			Name:    fmt.Sprintf("Power-%d", i),
			Damage:  float32(i) * 1.5,
			Energy:  float32(i) * 0.3,
			Passive: i%2 == 0,
		}
	}
	hero.Powers = powers
	encoded, err := s.AppendEncode(nil, hero)
	if err != nil {
		b.Fatal(err)
	}
	var out Superhero
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = Superhero{}
		if _, err = s.Decode(encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStringHeavyEncode(b *testing.B) {
	type StringRecord struct {
		S1  string `avro:"s1"`
		S2  string `avro:"s2"`
		S3  string `avro:"s3"`
		S4  string `avro:"s4"`
		S5  string `avro:"s5"`
		S6  string `avro:"s6"`
		S7  string `avro:"s7"`
		S8  string `avro:"s8"`
		S9  string `avro:"s9"`
		S10 string `avro:"s10"`
	}
	s, err := Parse(`{"type":"record","name":"strings","fields":[
		{"name":"s1","type":"string"},
		{"name":"s2","type":"string"},
		{"name":"s3","type":"string"},
		{"name":"s4","type":"string"},
		{"name":"s5","type":"string"},
		{"name":"s6","type":"string"},
		{"name":"s7","type":"string"},
		{"name":"s8","type":"string"},
		{"name":"s9","type":"string"},
		{"name":"s10","type":"string"}
	]}`)
	if err != nil {
		b.Fatal(err)
	}
	input := &StringRecord{
		S1: strings.Repeat("hello ", 20), S2: strings.Repeat("world ", 20),
		S3: strings.Repeat("avro ", 20), S4: strings.Repeat("bench ", 20),
		S5: strings.Repeat("test ", 20), S6: strings.Repeat("data ", 20),
		S7: strings.Repeat("schema ", 20), S8: strings.Repeat("encode ", 20),
		S9: strings.Repeat("decode ", 20), S10: strings.Repeat("string ", 20),
	}
	dst, err := s.AppendEncode(nil, input)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, err = s.AppendEncode(dst[:0], input)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStringHeavyDecode(b *testing.B) {
	type StringRecord struct {
		S1  string `avro:"s1"`
		S2  string `avro:"s2"`
		S3  string `avro:"s3"`
		S4  string `avro:"s4"`
		S5  string `avro:"s5"`
		S6  string `avro:"s6"`
		S7  string `avro:"s7"`
		S8  string `avro:"s8"`
		S9  string `avro:"s9"`
		S10 string `avro:"s10"`
	}
	s, err := Parse(`{"type":"record","name":"strings","fields":[
		{"name":"s1","type":"string"},
		{"name":"s2","type":"string"},
		{"name":"s3","type":"string"},
		{"name":"s4","type":"string"},
		{"name":"s5","type":"string"},
		{"name":"s6","type":"string"},
		{"name":"s7","type":"string"},
		{"name":"s8","type":"string"},
		{"name":"s9","type":"string"},
		{"name":"s10","type":"string"}
	]}`)
	if err != nil {
		b.Fatal(err)
	}
	input := &StringRecord{
		S1: strings.Repeat("hello ", 20), S2: strings.Repeat("world ", 20),
		S3: strings.Repeat("avro ", 20), S4: strings.Repeat("bench ", 20),
		S5: strings.Repeat("test ", 20), S6: strings.Repeat("data ", 20),
		S7: strings.Repeat("schema ", 20), S8: strings.Repeat("encode ", 20),
		S9: strings.Repeat("decode ", 20), S10: strings.Repeat("string ", 20),
	}
	encoded, err := s.AppendEncode(nil, input)
	if err != nil {
		b.Fatal(err)
	}
	var out StringRecord
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = StringRecord{}
		if _, err = s.Decode(encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}
