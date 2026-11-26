# Differential-testing oracles

These let the test suite cross-check twmb/avro against an independent
reference implementation, instead of only against the author's own beliefs.

## fastavro (`fastavro_oracle.py`)

Drives `TestDifferentialFastavro` (in `conformance_test.go`). The test
**skips** when fastavro is not importable, so `go test ./...` stays green
without it.

Enable it:

```sh
python3 -m venv /tmp/avro_oracle_venv
/tmp/avro_oracle_venv/bin/pip install fastavro
AVRO_FASTAVRO_PYTHON=/tmp/avro_oracle_venv/bin/python go test -run TestDifferentialFastavro .
```

`AVRO_FASTAVRO_PYTHON` selects the interpreter (default `python3`). The script
is a long-lived subprocess: one JSON job per stdin line, one JSON response per
stdout line. Everything is compared as Avro bytes, so no cross-language value
coercion is needed.

## Apache Avro Java (`SchemaOracle.java`)

Drives the `TestDifferentialJava` tests (in `java_differential_test.go`),
which compare canonical forms, fingerprints, and encoded values with
avro-tools. The file is behind the `cisuite` build tag, so a plain
`go test ./...` does not compile it. CI runs it:

```sh
javac -cp avro-tools.jar -d testdata/oracle testdata/oracle/SchemaOracle.java
AVRO_TOOLS_JAR=$PWD/avro-tools.jar go test -tags=cisuite -run TestDifferentialJava .
```
