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

// ExampleWithReaderSchemaFunc demonstrates choosing the reader schema based
// on state that's only available after the OCF header is parsed — for
// example, a metadata key that distinguishes between old and new file
// variants, or a writer-schema shape that changed between versions of the
// producer. The callback runs after NewReader has read the header, so
// rd.Schema() and rd.Metadata() are populated; whatever schema it returns
// becomes the reader schema for resolution against the writer schema.
func ExampleWithReaderSchemaFunc() {
	// Producer v1 wrote records with a legacy field name:
	v1Schema := avro.MustParse(`{
		"type": "record", "name": "Event",
		"fields": [{"name": "legacy_ts", "type": "long"}]
	}`)

	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, v1Schema,
		ocf.WithMetadata(map[string][]byte{"producer-version": []byte("1")}))
	if err != nil {
		log.Fatal(err)
	}
	if err := w.Encode(map[string]any{"legacy_ts": int64(1700000000)}); err != nil {
		log.Fatal(err)
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}

	// Our application reads with two reader schemas — one per producer
	// version — each using the spec-correct field name "ts" but declaring
	// the old name as an alias so records from either version decode into
	// the same struct without coalescing.
	v1Reader := avro.MustParse(`{
		"type": "record", "name": "Event",
		"fields": [{"name": "ts", "type": "long", "aliases": ["legacy_ts"]}]
	}`)
	v2Reader := avro.MustParse(`{
		"type": "record", "name": "Event",
		"fields": [{"name": "ts", "type": "long"}]
	}`)

	type Event struct {
		TS int64 `avro:"ts"`
	}

	r, err := ocf.NewReader(bytes.NewReader(buf.Bytes()),
		ocf.WithReaderSchemaFunc(func(rd *ocf.Reader) (*avro.Schema, error) {
			// Header has been parsed. Pick the reader schema based on
			// whichever producer wrote the file.
			if string(rd.Metadata()["producer-version"]) == "1" {
				return v1Reader, nil
			}
			return v2Reader, nil
		}))
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	var e Event
	if err := r.Decode(&e); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("ts=%d\n", e.TS)
	// Output:
	// ts=1700000000
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
