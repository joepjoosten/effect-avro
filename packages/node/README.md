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

Decoding accepts `limits` in parse options. Defaults are 128 levels of nesting, 1,000,000 decoded values and collection items, 64 MiB of input/decoded bytes, and 16 MiB per block. Counts include structural values such as arrays and records. Set explicit nonnegative safe-integer limits for trusted larger inputs. A shared `DecodeBudget` can bound multiple `Type.decodePartial` calls; container readers share a budget across records and blocks.

For bounded streaming, consume `decodeContainerRecords(input, parseOptions, signal?)` or use `decodeContainerEvents` to receive the header as well. `encodeContainerIterable(schema, values, options)` accepts sync/async values and yields a header followed by completed blocks with backpressure. Blocks are limited by `blockSize` and uncompressed `maxBlockBytes` (16 MiB by default for streaming); oversized individual records are rejected. Memory depends on the input chunk, one block, and the largest encoded record, not the complete archive. Increase the total `limits.maxBytes`/`maxValues` explicitly when processing trusted large archives.

The whole-file helpers still collect their results for convenience. `readContainerIterable` now parses incrementally and closes its source on Effect interruption. Breaking out of a streaming loop closes the upstream iterator. An arbitrary source awaiting an uncancellable operation may finish its own cleanup later; interruption requests `return()` without waiting indefinitely.
