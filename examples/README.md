# @effect-avro examples

These examples show how the packages fit together in common application workflows.

The examples are Kafka-client agnostic. They use `KafkaMessage` from `@effect-avro/kafka` as the message shape, so the same functions can be adapted to KafkaJS, node-rdkafka, Effect Streams, or a custom message bus.

## Files

| File | Use case |
| --- | --- |
| `domain.ts` | Shared Effect Schema domain events and Avro schemas. |
| `support/in-memory-registry.ts` | Small in-memory Confluent Schema Registry test double. |
| `registry-kafka-events.ts` | Effect Schema encoded events with Schema Registry ids and Kafka message values. |
| `kafkajs-publisher-adapter.ts` | KafkaJS-style producer service/layer with latest schema lookup, batch publishing, tombstones, and raw JSON fallback. |
| `local-kafka-decode.ts` | Local/base64 Lambda-style Kafka record decode using Confluent framing and Effect Schema. |
| `lambda-kafka-dedupe.ts` | Dedupe helpers for self-managed Kafka records and decoded projection updates. |
| `schema-evolution.ts` | Compatibility checks and registering a new event schema version. |
| `dead-letter.ts` | `Effect.catchTag` based decode handling for dead-letter workflows. |
| `node-container-archive.ts` | Node object-container archive files generated from Effect Schema. |

## Typecheck

```sh
pnpm exec tsc -p examples/tsconfig.json
```

## Main Pattern

Use Effect Schema for domain types and runtime conversion, Schema Registry for ids, and Kafka for transport:

1. Define domain events with `Schema.TaggedClass` / `Schema.Union`.
2. Compile the union to Avro JSON with `toAvroSchema`.
3. Create a `Schema.Codec<A, Uint8Array>` with `avro(schema, { avroSchema })`.
4. Register the Avro schema and frame the codec output with `encodeConfluentFrame`.
5. On consume, parse the frame, fetch the schema id from the registry, and decode through the Effect Schema codec.

This preserves Effect Schema behavior such as `_tag` restoration for tagged unions.

## Other Common Use Cases

- Generic consumers can build an Effect schema at runtime from `getById(...).schema` with `fromAvroSchema`.
- Producers can use `checkCompatibility` before registering a new schema version.
- Consumers can use `Effect.catchTag` for registry, framing, and domain decode failures.
- Node jobs can write object-container archives for replay, analytics ingestion, or long-term storage.
- Browser producers can use `@effect-avro/core`, `@effect-avro/schema`, `@effect-avro/schema-registry`, and `@effect-avro/kafka` because those packages stay platform-neutral.

## Migrating Kafka Helper Libraries

The existing packages replace the Avro, Confluent frame, schema-registry cache, subject naming, typed domain codec, local decode, and error-modeling parts of a Kafka helper library.

Keep concrete Kafka clients and cloud credentials in adapters:

- A KafkaJS producer can be wrapped as a `ProducerLike` and exposed through an Effect `Context.Service` plus `Layer`.
- AWS Secrets Manager, Confluent SASL credentials, HTTP proxy agents, and AWS X-Ray are Node/cloud concerns. They belong in application code today, or in a future companion package such as `@effect-avro/kafkajs` or `@effect-avro/aws-confluent`.
- Generic object mappers such as `undefined` to `null` and ISO strings to `Date` should usually be Effect Schema transformations on the domain schema instead of global Kafka middleware.
