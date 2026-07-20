# @avro-effect

Effect v4 packages for working with Apache Avro schemas and binary data.

This repository publishes five packages under the `@avro-effect` npm scope:

| Package | Purpose |
| --- | --- |
| `@avro-effect/core` | Native Avro schema model plus binary encoder/decoder. |
| `@avro-effect/schema` | Effect Schema v4 to Avro compiler, Avro to Effect Schema importer, and `Schema.Codec` integration. |
| `@avro-effect/schema-registry` | Confluent Schema Registry client and wire framing. |
| `@avro-effect/kafka` | Kafka key/value serializers and deserializers. |
| `@avro-effect/node` | Node.js Avro object-container file helpers. |

The packages target the Effect v4 beta line and currently use `effect@4.0.0-beta.99`.

## Install

Use `@avro-effect/schema` when you want Effect Schema integration:

```sh
pnpm add @avro-effect/schema effect
```

Use `@avro-effect/core` directly when you only need Avro binary encoding and decoding:

```sh
pnpm add @avro-effect/core effect
```

Use the companion packages when integrating with Schema Registry, Kafka, or Node object-container files:

```sh
pnpm add @avro-effect/schema-registry @avro-effect/kafka @avro-effect/node effect
```

## Effect Schema Codec

```ts
import { Schema } from "effect"
import { avro, Long } from "@avro-effect/schema"

class User extends Schema.Class<User>("User")({
  id: Long,
  name: Schema.String
}) {}

const UserAvro = avro(User)
const encode = Schema.encodeSync(UserAvro)
const decode = Schema.decodeUnknownSync(UserAvro)

const buffer = encode(new User({ id: 1, name: "Ada" }))
const user = decode(buffer)
```

`@avro-effect/schema` can:

- compile Effect Schema v4 schemas to Avro JSON schemas
- import Avro JSON schemas as Effect schemas
- encode and decode Avro binary buffers through the native `@avro-effect/core` runtime
- handle records, enums, arrays, maps, unions, nullable fields, recursive named references, bytes, fixed values, and logical type annotations
- omit `_tag` literal fields from Avro records and restore them after decoding tagged Effect unions

## Native Avro Runtime

```ts
import { decode, encode } from "@avro-effect/core"

const schema = {
  type: "record",
  name: "Event",
  fields: [
    { name: "id", type: "long" },
    { name: "name", type: "string" }
  ]
} as const

const bytes = encode(schema, { id: 1, name: "created" })
const value = decode(schema, bytes)
```

`@avro-effect/core` is intentionally small and dependency-light. It replaces the previous `avro-js` runtime path for the schema package and exposes plain Avro union values rather than wrapper objects.

## Integrations

- `@avro-effect/schema-registry` handles Confluent Schema Registry HTTP APIs, schema id framing, and subject naming strategies.
- `@avro-effect/kafka` composes the registry package into Kafka key/value serializer and deserializer functions without depending on a Kafka client.
- `@avro-effect/node` reads and writes Avro object-container files with `null` and `deflate` codecs.

Public data models are exported as Effect Schema values plus matching types, for example `AvroSchema`, `RegisterSchemaRequest`, `KafkaMessage`, and `ContainerEncodeOptions`.

The integration packages expose Effect services as their primary API:

```ts
import { Effect, Layer } from "effect"
import { SchemaRegistry } from "@avro-effect/schema-registry"
import { KafkaAvro, decodeValue, serializeRegistryValue } from "@avro-effect/kafka"

const RegistryLive = SchemaRegistry.layer({
  endpoint: "http://localhost:8081"
})

const KafkaAvroLive = KafkaAvro.layer.pipe(
  Layer.provide(RegistryLive)
)

const schema = {
  type: "record",
  name: "Event",
  fields: [{ name: "id", type: "long" }]
} as const

const program = Effect.gen(function*() {
  const value = yield* serializeRegistryValue({ topic: "events", schema }, { id: 1 })
  return yield* decodeValue({ topic: "events", value })
}).pipe(
  Effect.provide(KafkaAvroLive)
)
```

For direct embedding or adapter code, each package also keeps lower-level constructors such as `makeClient`, `encodeWithRegistry`, and `makeAvroNode`.

## Examples

The [examples](./examples) directory includes type-checked workflows for:

- Effect Schema domain events encoded as Confluent-framed Kafka values
- Schema Registry compatibility checks and schema evolution
- `Effect.catchTag` based dead-letter handling
- Node object-container archive files generated from Effect Schema Avro schemas

## Development

```sh
pnpm install
pnpm check
pnpm check:examples
pnpm test -- --run
pnpm build
```

GitHub Actions mirrors the local flow:

- `check.yml` runs build, typecheck, and tests
- `release.yml` uses Changesets to open release PRs and publish to npm
- `snapshot.yml` publishes PR snapshots through `pkg-pr-new` when enabled

## Publishing

The first release is version `0.0.1`. Publishing requires an npm automation token stored as the GitHub repository secret `NPM_TOKEN`. The optional repository variable `PKG_PR_NEW_ENABLED=true` enables snapshot publishing for pull requests after the `pkg-pr-new` GitHub App is installed.
