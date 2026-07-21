# @effect-avro/schema

## 0.0.1

Initial release.

- Add Effect v4 native Avro schema compiler.
- Re-export schema-backed Avro model definitions from `@effect-avro/core`.
- Add Avro JSON schema to Effect Schema importer.
- Add Avro binary `Schema.Codec<A, Uint8Array>` via `avro(schema)` using `@effect-avro/core`.
- Support records, enums, arrays, maps, unions, nullable fields, recursive named references, bytes, fixed values, logical type annotations, and tagged record `_tag` omission/restoration.
- Use schema-backed tagged errors for schema conversion failures.
