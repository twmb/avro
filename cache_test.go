package avro

import (
	"encoding/json"
	"testing"
)

func TestSchemaCacheBasic(t *testing.T) {
	cache := NewSchemaCache()

	// Parse a leaf schema.
	_, err := cache.Parse(`{
		"type": "record",
		"name": "Telephone",
		"fields": [
			{"name": "number", "type": "int"},
			{"name": "label", "type": "string"}
		]
	}`)
	if err != nil {
		t.Fatalf("parse Telephone: %v", err)
	}

	// Parse a parent that references the leaf.
	parent, err := cache.Parse(`{
		"type": "record",
		"name": "Person",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "phone", "type": "Telephone"}
		]
	}`)
	if err != nil {
		t.Fatalf("parse Person: %v", err)
	}

	// Encode and decode using the parent schema.
	input := map[string]any{
		"name": "alice",
		"phone": map[string]any{
			"number": float64(1234),
			"label":  "home",
		},
	}
	binary, err := parent.Encode(input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded any
	rest, err := parent.Decode(binary, &decoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("unexpected remaining bytes: %v", rest)
	}
	m := decoded.(map[string]any)
	if m["name"] != "alice" {
		t.Errorf("name: got %v", m["name"])
	}
	phone := m["phone"].(map[string]any)
	if phone["number"] != int32(1234) {
		t.Errorf("phone.number: got %v (%T)", phone["number"], phone["number"])
	}
	if phone["label"] != "home" {
		t.Errorf("phone.label: got %v", phone["label"])
	}
}

func TestSchemaCacheMultipleRefs(t *testing.T) {
	cache := NewSchemaCache()

	_, err := cache.Parse(`{
		"type": "record",
		"name": "Telephone",
		"fields": [
			{"name": "number", "type": "int"},
			{"name": "label", "type": "string"}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = cache.Parse(`{
		"type": "record",
		"name": "Address",
		"fields": [
			{"name": "street", "type": "string"},
			{"name": "city", "type": "string"}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}

	// Parent references both.
	parent, err := cache.Parse(`{
		"type": "record",
		"name": "Contact",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "phone", "type": "Telephone"},
			{"name": "address", "type": "Address"}
		]
	}`)
	if err != nil {
		t.Fatalf("parse Contact: %v", err)
	}

	input := map[string]any{
		"name": "bob",
		"phone": map[string]any{
			"number": float64(5678),
			"label":  "work",
		},
		"address": map[string]any{
			"street": "123 Main St",
			"city":   "Springfield",
		},
	}
	binary, err := parent.Encode(input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded any
	if _, err := parent.Decode(binary, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	m := decoded.(map[string]any)
	addr := m["address"].(map[string]any)
	if addr["city"] != "Springfield" {
		t.Errorf("address.city: got %v", addr["city"])
	}
}

func TestSchemaCacheNestedRefs(t *testing.T) {
	cache := NewSchemaCache()

	// Leaf: Owner.
	_, err := cache.Parse(`{
		"type": "record",
		"name": "Owner",
		"fields": [{"name": "lastname", "type": "string"}]
	}`)
	if err != nil {
		t.Fatal(err)
	}

	// Mid-level: TelephoneOwner references Owner.
	_, err = cache.Parse(`{
		"type": "record",
		"name": "TelephoneOwner",
		"fields": [
			{"name": "number", "type": "int"},
			{"name": "owner", "type": "Owner"}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}

	// Top-level: references TelephoneOwner.
	top, err := cache.Parse(`{
		"type": "record",
		"name": "Contact",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "phone", "type": "TelephoneOwner"}
		]
	}`)
	if err != nil {
		t.Fatalf("parse Contact: %v", err)
	}

	input := map[string]any{
		"name": "carol",
		"phone": map[string]any{
			"number": float64(9999),
			"owner": map[string]any{
				"lastname": "Smith",
			},
		},
	}
	binary, err := top.Encode(input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded any
	if _, err := top.Decode(binary, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	m := decoded.(map[string]any)
	owner := m["phone"].(map[string]any)["owner"].(map[string]any)
	if owner["lastname"] != "Smith" {
		t.Errorf("owner.lastname: got %v", owner["lastname"])
	}
}

func TestSchemaCacheSharedBase(t *testing.T) {
	// Multiple schemas sharing a common base type.
	cache := NewSchemaCache()

	_, err := cache.Parse(`{
		"type": "record",
		"name": "Base",
		"fields": [{"name": "id", "type": "int"}]
	}`)
	if err != nil {
		t.Fatal(err)
	}

	s1, err := cache.Parse(`{
		"type": "record",
		"name": "TypeA",
		"fields": [
			{"name": "base", "type": "Base"},
			{"name": "a", "type": "string"}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}

	s2, err := cache.Parse(`{
		"type": "record",
		"name": "TypeB",
		"fields": [
			{"name": "base", "type": "Base"},
			{"name": "b", "type": "long"}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}

	// Both schemas should work independently.
	b1, err := s1.Encode(map[string]any{
		"base": map[string]any{"id": float64(1)},
		"a":    "hello",
	})
	if err != nil {
		t.Fatalf("encode TypeA: %v", err)
	}
	var d1 any
	if _, err := s1.Decode(b1, &d1); err != nil {
		t.Fatalf("decode TypeA: %v", err)
	}

	b2, err := s2.Encode(map[string]any{
		"base": map[string]any{"id": float64(2)},
		"b":    float64(42),
	})
	if err != nil {
		t.Fatalf("encode TypeB: %v", err)
	}
	var d2 any
	if _, err := s2.Decode(b2, &d2); err != nil {
		t.Fatalf("decode TypeB: %v", err)
	}
}

func TestSchemaCacheUnresolvedRef(t *testing.T) {
	cache := NewSchemaCache()

	// Parsing a schema that references an unknown type should fail.
	_, err := cache.Parse(`{
		"type": "record",
		"name": "Bad",
		"fields": [{"name": "x", "type": "Unknown"}]
	}`)
	if err == nil {
		t.Fatal("expected error for unresolved reference")
	}

	// The cache should not be corrupted by the failed parse.
	// A subsequent valid parse should still work.
	_, err = cache.Parse(`{
		"type": "record",
		"name": "Good",
		"fields": [{"name": "x", "type": "int"}]
	}`)
	if err != nil {
		t.Fatalf("expected success after failed parse, got: %v", err)
	}
}

func TestSchemaCacheJSONRoundtrip(t *testing.T) {
	// End-to-end test matching rpk's usage pattern:
	// parse refs → parse parent → json.Unmarshal → Encode → Decode → json.Marshal
	cache := NewSchemaCache()

	_, err := cache.Parse(`{
		"type": "record",
		"name": "telephone",
		"fields": [
			{"name": "number", "type": "int"},
			{"name": "identifier", "type": "string"}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}

	parent, err := cache.Parse(`{
		"type": "record",
		"name": "test",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "telephone", "type": "telephone"}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}

	input := `{"name":"redpanda","telephone":{"number":12341234,"identifier":"home"}}`

	var native any
	if err := json.Unmarshal([]byte(input), &native); err != nil {
		t.Fatal(err)
	}

	binary, err := parent.Encode(native)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var decoded any
	rest, err := parent.Decode(binary, &decoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("remaining bytes: %v", rest)
	}

	// Marshal back to JSON and compare.
	out, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	// Compare by unmarshalling both to maps to avoid key-order issues.
	var got, want map[string]any
	json.Unmarshal(out, &got)
	json.Unmarshal([]byte(input), &want)

	if got["name"] != want["name"] {
		t.Errorf("name mismatch: got %v, want %v", got["name"], want["name"])
	}
	gotPhone := got["telephone"].(map[string]any)
	wantPhone := want["telephone"].(map[string]any)
	if gotPhone["identifier"] != wantPhone["identifier"] {
		t.Errorf("identifier mismatch: got %v, want %v", gotPhone["identifier"], wantPhone["identifier"])
	}
}

func TestSchemaCacheEnum(t *testing.T) {
	cache := NewSchemaCache()

	_, err := cache.Parse(`{
		"type": "enum",
		"name": "Color",
		"symbols": ["RED", "GREEN", "BLUE"]
	}`)
	if err != nil {
		t.Fatal(err)
	}

	s, err := cache.Parse(`{
		"type": "record",
		"name": "Item",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "color", "type": "Color"}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}

	input := map[string]any{"name": "shirt", "color": "GREEN"}
	binary, err := s.Encode(input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded any
	if _, err := s.Decode(binary, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	m := decoded.(map[string]any)
	if m["color"] != "GREEN" {
		t.Errorf("color: got %v", m["color"])
	}
}
