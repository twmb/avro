# Differential-testing oracles

These let the test suite cross-check twmb/avro against an independent
reference implementation, instead of only against the author's own beliefs
(see `../../CORRECTNESS_PLAN.md` for why that matters).

## fastavro (`fastavro_oracle.py`)

Drives `TestDifferentialFastavro` (in `differential_test.go`). The test
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

## Apache Avro Java (planned — T1d)

`SchemaNormalization` (fingerprints) and the resolution engine are the gold
standard. Planned as a second oracle via `avro-tools` or a small JVM shim.
