package ocf_test

import (
	"bytes"
	"fmt"
	"log"

	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

func ExampleNewWriter() {
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

	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, schema)
	if err != nil {
		log.Fatal(err)
	}
	for _, u := range []User{
		{Name: "Alice", Age: 30},
		{Name: "Bob", Age: 25},
	} {
		if err := w.Encode(&u); err != nil {
			log.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}

	// Read back.
	r, err := ocf.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()
	for {
		var u User
		if err := r.Decode(&u); err != nil {
			break
		}
		fmt.Printf("%s is %d\n", u.Name, u.Age)
	}
	// Output:
	// Alice is 30
	// Bob is 25
}

func ExampleNewReader_evolution() {
	// Write v1 data (name only).
	v1Schema := avro.MustParse(`{
		"type": "record", "name": "User",
		"fields": [{"name": "name", "type": "string"}]
	}`)

	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, v1Schema)
	if err != nil {
		log.Fatal(err)
	}
	for _, name := range []string{"Alice", "Bob"} {
		if err := w.Encode(map[string]any{"name": name}); err != nil {
			log.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}

	// Read with a v2 schema that added an age field with a default.
	v2Schema := avro.MustParse(`{
		"type": "record", "name": "User",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age",  "type": "int", "default": 0}
		]
	}`)

	type User struct {
		Name string `avro:"name"`
		Age  int32  `avro:"age"`
	}

	r, err := ocf.NewReader(bytes.NewReader(buf.Bytes()), ocf.WithReaderSchema(v2Schema))
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()
	for {
		var u User
		if err := r.Decode(&u); err != nil {
			break
		}
		fmt.Printf("%s age=%d\n", u.Name, u.Age)
	}
	// Output:
	// Alice age=0
	// Bob age=0
}
