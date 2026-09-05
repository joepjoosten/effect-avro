# @effect-avro/core

Native Avro runtime primitives and codecs for Effect v4 projects.

`@effect-avro/core` provides the lower-level runtime used by `@effect-avro/schema`. It handles Avro schema model types, named references, and binary encoding/decoding without depending on `avro-js`.

## Install

```sh
pnpm add @effect-avro/core effect
```

## Usage

```ts
import { decode, encode, parse } from "@effect-avro/core"

const schema = {
  type: "record",
  name: "Message",
  fields: [
    { name: "id", type: "long" },
    { name: "body", type: "string" },
    { name: "tags", type: { type: "array", items: "string" } }
  ]
} as const

const type = parse(schema)
const bytes = type.toUint8Array({ id: 1, body: "hello", tags: ["avro"] })
const message = type.fromUint8Array(bytes)

const bytes = encode(schema, message)
const decoded = decode(schema, bytes)
```

## Features

- Avro schema model types
- Effect Schema values for Avro schema model types
- Native Avro binary encoder and decoder using platform-neutral `Uint8Array`
- Primitive, record, enum, array, map, union, bytes, fixed, and logical type schema support
- Named type, alias, namespace, and recursive reference handling
- Plain union values rather than wrapper objects
- Partial decode support for block-oriented readers
- `Effect.try` based `encodeEffect` and `decodeEffect` helpers
- Schema-backed `AvroError` tagged errors for `Effect.catchTag`

Decoding accepts `limits` in parse options. Defaults are 128 levels of nesting, 1,000,000 decoded values and collection items, 64 MiB of input/decoded bytes, and 16 MiB per block. Counts include structural values such as arrays and records. Set explicit nonnegative safe-integer limits for trusted larger inputs. A shared `DecodeBudget` can bound multiple `Type.decodePartial` calls; container readers share a budget across records and blocks.
