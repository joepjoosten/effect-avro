# @effect-avro/schema-registry

Effect-friendly Confluent Schema Registry client and Avro wire framing.

## Install

```sh
pnpm add @effect-avro/schema-registry @effect-avro/core effect
```

## Usage

```ts
import { Effect } from "effect"
import { SchemaRegistry, decode, encode } from "@effect-avro/schema-registry"

const schema = {
  type: "record",
  name: "Event",
  fields: [{ name: "id", type: "long" }]
} as const

const RegistryLive = SchemaRegistry.layer({
  endpoint: "http://localhost:8081"
})

const program = Effect.gen(function*() {
  const encoded = yield* encode({
    subject: "events-value",
    schema,
    value: { id: 1 }
  })

  return yield* decode(encoded)
}).pipe(
  Effect.provide(RegistryLive)
)
```

## Features

- Effect service and layer API via `SchemaRegistry`.
- Effect Schema values for registry requests, responses, frames, auth, and subject naming input.
- Register and lookup Avro schemas by subject.
- Fetch schemas by id, subject version, and latest version.
- Confluent wire frame helpers: magic byte `0`, 4-byte schema id, Avro payload.
- Subject naming strategies: topic, record, and topic-record.
- Basic auth and bearer token support.
- In-memory schema id and subject/schema caches.
- Lower-level `makeClient`, `encodeWithRegistry`, and `decodeWithRegistry` helpers for custom adapters.
- Schema-backed `SchemaRegistryError`, `SchemaRegistryHttpError`, and `InvalidRegistryFrame` tagged errors.
