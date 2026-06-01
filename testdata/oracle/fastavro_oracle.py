#!/usr/bin/env python3
"""fastavro differential oracle for twmb/avro Tier-1 testing.

Long-lived subprocess. Reads one JSON job per line from stdin, writes one
JSON response line per job to stdout. Used by differential_test.go to
cross-check twmb's binary encode/decode against fastavro (an independent
reference impl), comparing everything as Avro bytes so no cross-language
value coercion is needed.

Jobs:
  {"op":"encode","schema":<avro schema>,"value":<json value>,"kind":<kind>}
      -> {"ok":true,"hex":"<schemaless-encoded bytes, hex>"}
  {"op":"decode","schema":<avro schema>,"hex":"<bytes hex>"}
      -> {"ok":true}                       (decodes without error)
On any failure: {"ok":false,"err":"<message>"}

"kind" (optional, default "") tells the oracle how to turn the JSON value into
the native Python type fastavro expects for binary- and logical-typed schemas:
  ""                 value used as-is (numbers, strings, bool, null, lists, maps)
  "bytes" / "fixed"  value is a base64 string -> bytes
  "decimal"          value is a decimal string (e.g. "123.45") -> decimal.Decimal
  "timestamp-millis" value is int milliseconds since the unix epoch -> UTC datetime
  "timestamp-micros" value is int microseconds since the unix epoch -> UTC datetime

Install: python3 -m venv venv && venv/bin/pip install fastavro
Point the test at it with AVRO_FASTAVRO_PYTHON=/path/to/venv/bin/python.
"""
import base64
import datetime
import decimal
import io
import json
import sys

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def _coerce(value, kind):
    # Reconstruct the native Python type for a Kind-tagged transport value so
    # fastavro encodes the same logical value twmb does. JSON can carry only
    # numbers/strings/bool/null/lists/maps, so binary and logical types arrive
    # encoded and are rebuilt here.
    if kind in ("bytes", "fixed"):
        return base64.b64decode(value)
    if kind == "decimal":
        return decimal.Decimal(value)
    if kind == "timestamp-millis":
        return _EPOCH + datetime.timedelta(milliseconds=value)
    if kind == "timestamp-micros":
        return _EPOCH + datetime.timedelta(microseconds=value)
    return value


def _parse(schema):
    # Fresh named-schema table per call so repeated named types across jobs
    # don't trip fastavro's "X already defined" module-level cache.
    try:
        return fastavro.parse_schema(schema, {})
    except TypeError:
        return fastavro.parse_schema(schema)


def handle(job):
    op = job.get("op")
    schema = _parse(job["schema"])
    if op == "encode":
        value = _coerce(job["value"], job.get("kind", ""))
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, schema, value)
        return {"ok": True, "hex": buf.getvalue().hex()}
    if op == "decode":
        buf = io.BytesIO(bytes.fromhex(job["hex"]))
        fastavro.schemaless_reader(buf, schema)
        return {"ok": True}
    return {"ok": False, "err": "unknown op %r" % op}


def main():
    global fastavro
    try:
        import fastavro as _fa
        fastavro = _fa
    except Exception as e:  # noqa: BLE001
        sys.stdout.write(json.dumps({"fatal": "fastavro import failed: %s" % e}) + "\n")
        sys.stdout.flush()
        return
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            resp = handle(json.loads(line))
        except Exception as e:  # noqa: BLE001
            resp = {"ok": False, "err": str(e)}
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
