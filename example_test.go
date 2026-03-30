package avro_test

import (
	"fmt"
	"log"
	"time"

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
	cache := new(avro.SchemaCache)

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

func ExampleSchema_EncodeJSON() {
	schema := avro.MustParse(`{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "name",  "type": "string"},
			{"name": "email", "type": ["null", "string"]}
		]
	}`)

	type User struct {
		Name  string  `avro:"name"`
		Email *string `avro:"email"`
	}
	email := "alice@example.com"
	u := User{Name: "Alice", Email: &email}

	// Default: bare union values.
	bare, err := schema.EncodeJSON(&u)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(bare))

	// TaggedUnions: wrapped as {"type": value}.
	tagged, err := schema.EncodeJSON(&u, avro.TaggedUnions())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(tagged))
	// Output:
	// {"name":"Alice","email":"alice@example.com"}
	// {"name":"Alice","email":{"string":"alice@example.com"}}
}

func ExampleSchema_DecodeJSON() {
	schema := avro.MustParse(`{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "name",  "type": "string"},
			{"name": "email", "type": ["null", "string"]}
		]
	}`)

	type User struct {
		Name  string  `avro:"name"`
		Email *string `avro:"email"`
	}

	// DecodeJSON accepts both bare and tagged union formats.
	var u1, u2 User
	if err := schema.DecodeJSON([]byte(`{"name":"Alice","email":"a@b.com"}`), &u1); err != nil {
		log.Fatal(err)
	}
	if err := schema.DecodeJSON([]byte(`{"name":"Bob","email":{"string":"b@c.com"}}`), &u2); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s: %s\n", u1.Name, *u1.Email)
	fmt.Printf("%s: %s\n", u2.Name, *u2.Email)
	// Output:
	// Alice: a@b.com
	// Bob: b@c.com
}

func ExampleSchemaFor() {
	type Event struct {
		ID     int64     `avro:"id"`
		Name   string    `avro:"name,default=unnamed"`
		Source string    `avro:"source,default=web"`
		Time   time.Time `avro:"ts"`
		Meta   *string   `avro:"meta"` // *T becomes ["null", T] union
	}

	schema := avro.MustSchemaFor[Event](avro.WithNamespace("com.example"))

	// Encode, then decode back.
	meta := "test"
	data, err := schema.Encode(&Event{
		ID:     1,
		Name:   "click",
		Source: "mobile",
		Time:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Meta:   &meta,
	})
	if err != nil {
		log.Fatal(err)
	}

	var out Event
	if _, err := schema.Decode(data, &out); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("id=%d name=%s source=%s meta=%s\n", out.ID, out.Name, out.Source, *out.Meta)

	// Inspect the inferred schema.
	root := schema.Root()
	for _, f := range root.Fields {
		if f.HasDefault {
			fmt.Printf("field %s: default=%v\n", f.Name, f.Default)
		}
	}
	// Output:
	// id=1 name=click source=mobile meta=test
	// field name: default=unnamed
	// field source: default=web
}
