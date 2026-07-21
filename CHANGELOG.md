# @effect-avro

## 0.0.1

Initial release.

- Add `@effect-avro/schema`, an Effect v4 native Avro schema compiler.
- Add Avro JSON schema to Effect Schema importer.
- Add Avro binary `Schema.Codec<A, Uint8Array>` via `avro(schema)`.
- Support records, enums, arrays, maps, unions, nullable fields, recursive named references, bytes, fixed values, logical type annotations, and tagged record `_tag` omission/restoration.
- Add `@effect-avro/core`, a native Avro runtime foundation with schema model types and binary encode/decode support.
- Wire `@effect-avro/schema` to the native `@effect-avro/core` runtime instead of `avro-js`.
- Add `@effect-avro/schema-registry`, a Confluent Schema Registry client and Avro wire framing package.
- Add `@effect-avro/kafka`, Kafka key/value serializer and deserializer helpers.
- Add `@effect-avro/node`, Node.js Avro object-container file helpers.
- Add Effect service and layer APIs for Schema Registry, Kafka Avro, and Node object-container workflows.
- Add schema-backed tagged errors for `Effect.catchTag` based error handling.
- Export public data models as Effect Schema values with matching TypeScript types.
- Add type-checked examples for Schema Registry, Effect Schema, Kafka events, schema evolution, dead-letter handling, and Node archives.
- Add package build, tests, and GitHub Actions Changesets release/snapshot workflows.
