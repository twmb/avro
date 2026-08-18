package avro_test

// Go shapes declared identically by more than one test. A type used by
// sixteen tests is not sixteen types. A test that needs a different shape
// under the same name still declares its own, which shadows these.

type R struct {
	Name string `avro:"name,omitzero"`
}

type Rec struct {
	F *[]string `avro:"f"`
}

type rec struct {
	F *[]string `avro:"f"`
}
