package avro

// Go shapes declared identically by more than one test. A type used by
// sixteen tests is not sixteen types. A test that needs a different shape
// under the same name still declares its own, which shadows these.

type Embed struct {
	A int32 `avro:"a"`
}

type Inner struct {
	X int32 `avro:"x"`
}

type LongList struct {
	Value int64     `avro:"value"`
	Next  *LongList `avro:"next"`
}

type R struct {
	A int64  `avro:"a"`
	B string `avro:"b"`
}

type Rec struct {
	V int32 `avro:"v"`
}

type Record struct {
	Name  string  `avro:"name"`
	Email *string `avro:"email"`
}

type Wrapper struct {
	Vals []int32 `avro:"vals"`
}
