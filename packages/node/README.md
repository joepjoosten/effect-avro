# @effect-avro/node

Node.js Avro object container file helpers for `@effect-avro/core`.

The default `AvroNode.layer` is Node-specific and provides the Effect `FileSystem.FileSystem` service with `node:fs/promises`. Use `AvroNode.layerNoDeps` when an application wants to provide its own filesystem implementation.

## Install

```sh
pnpm add @effect-avro/node @effect-avro/core effect
```

## Usage

```ts
import { Effect } from "effect"
import { AvroNode, decodeContainer, encodeContainer, readFile, writeFile } from "@effect-avro/node"

const schema = {
  type: "record",
  name: "Event",
  fields: [{ name: "id", type: "long" }]
} as const

const file = encodeContainer(schema, [{ id: 1 }, { id: 2 }])
const decoded = decodeContainer(file)

const program = Effect.gen(function*() {
  yield* writeFile("events.avro", schema, [{ id: 1 }, { id: 2 }])
  return yield* readFile("events.avro")
}).pipe(
  Effect.provide(AvroNode.layer)
)
```

## Features

- Effect service and layer API via `AvroNode`.
- Effect Schema values for container encode options and typed container files.
- Avro object container file header, metadata, sync marker, and block handling.
- `null` and raw `deflate` codecs.
- Node-backed default filesystem layer plus `AvroNode.layerNoDeps` for custom runtimes and tests.
- Async iterable reader for Node streams.
- Schema-backed `AvroContainerError` tagged errors for `Effect.catchTag`.
