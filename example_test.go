package avro_test

import (
	"fmt"
	"log"

	"github.com/twmb/avro"
)

func Example() {
	schema := avro.MustParse(`{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age",  "type": "int"}
		]
	}`)

	type User struct {
		Name string `avro:"name"`
		Age  int32  `avro:"age"`
	}

	data, err := schema.Encode(&User{Name: "Alice", Age: 30})
	if err != nil {
		log.Fatal(err)
	}

	var u User
	if _, err := schema.Decode(data, &u); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s is %d\n", u.Name, u.Age)
	// Output: Alice is 30
}

func ExampleResolve() {
	// v1 wrote User with just a name.
	writerSchema := avro.MustParse(`{
		"type": "record", "name": "User",
		"fields": [{"name": "name", "type": "string"}]
	}`)

	// v2 added an email field with a default.
	readerSchema := avro.MustParse(`{
		"type": "record", "name": "User",
		"fields": [
			{"name": "name",  "type": "string"},
			{"name": "email", "type": "string", "default": ""}
		]
	}`)

	resolved, err := avro.Resolve(writerSchema, readerSchema)
	if err != nil {
		log.Fatal(err)
	}

	// Encode a v1 record (name only).
	v1Data, err := writerSchema.Encode(map[string]any{"name": "Alice"})
	if err != nil {
		log.Fatal(err)
	}

	// Decode old data into the new layout; email gets the default.
	type User struct {
		Name  string `avro:"name"`
		Email string `avro:"email"`
	}
	var u User
	if _, err := resolved.Decode(v1Data, &u); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("name=%s email=%q\n", u.Name, u.Email)
	// Output: name=Alice email=""
}

func ExampleSchema_AppendEncode() {
	schema := avro.MustParse(`"string"`)

	// AppendEncode reuses a buffer across calls, avoiding allocation.
	var buf []byte
	var err error
	for _, s := range []string{"hello", "world"} {
		buf, err = schema.AppendEncode(buf[:0], s)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("encoded %q: %d bytes\n", s, len(buf))
	}
	// Output:
	// encoded "hello": 6 bytes
	// encoded "world": 6 bytes
}

func ExampleSchemaCache() {
	cache := avro.NewSchemaCache()

	// Parse the Address type first.
	if _, err := cache.Parse(`{
		"type": "record",
		"name": "Address",
		"fields": [
			{"name": "street", "type": "string"},
			{"name": "city",   "type": "string"}
		]
	}`); err != nil {
		log.Fatal(err)
	}

	// User references Address by name.
	schema, err := cache.Parse(`{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "name",    "type": "string"},
			{"name": "address", "type": "Address"}
		]
	}`)
	if err != nil {
		log.Fatal(err)
	}

	type Address struct {
		Street string `avro:"street"`
		City   string `avro:"city"`
	}
	type User struct {
		Name    string  `avro:"name"`
		Address Address `avro:"address"`
	}

	data, err := schema.Encode(&User{
		Name:    "Alice",
		Address: Address{Street: "123 Main St", City: "Springfield"},
	})
	if err != nil {
		log.Fatal(err)
	}

	var u User
	if _, err := schema.Decode(data, &u); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s lives at %s, %s\n", u.Name, u.Address.Street, u.Address.City)
	// Output: Alice lives at 123 Main St, Springfield
}

func ExampleSchema_AppendSingleObject() {
	schema := avro.MustParse(`{
		"type": "record",
		"name": "Event",
		"fields": [
			{"name": "id",   "type": "long"},
			{"name": "name", "type": "string"}
		]
	}`)

	type Event struct {
		ID   int64  `avro:"id"`
		Name string `avro:"name"`
	}

	// Encode: 2-byte magic + 8-byte fingerprint + Avro payload.
	data, err := schema.AppendSingleObject(nil, &Event{ID: 1, Name: "click"})
	if err != nil {
		log.Fatal(err)
	}

	// Decode.
	var e Event
	if _, err := schema.DecodeSingleObject(data, &e); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("id=%d name=%s\n", e.ID, e.Name)
	// Output: id=1 name=click
}
