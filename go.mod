module github.com/twmb/avro

go 1.25

require github.com/klauspost/compress v1.18.4

retract (
	v1.2.0 // Realized API flaws during integration; fixed in v1.3.0 within a day
	v1.1.0 // Realized API flaws during integration; fixed in v1.3.0 within a day
	v1.0.1 // Realized API flaws during integration; fixed in v1.3.0 within a day
)
