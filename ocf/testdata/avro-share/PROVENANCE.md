# Vendored Apache Avro interop test data

These `.avro` Object Container Files are copied verbatim from Apache Avro's
`share/test/data/` (https://github.com/apache/avro), licensed Apache-2.0.
They are real, Java-produced OCF files used by `TestDifferentialOCFCorpus`
(../../differential_ocf_test.go) to verify twmb decodes reference output
across codecs (null/deflate/snappy/zstd). `weather*.avro` encode the records
in `weather.json`; `syncInMeta.avro` stores the sync marker in file metadata.
