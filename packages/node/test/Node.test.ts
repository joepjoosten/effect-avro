import { describe, expect, it } from "@effect/vitest"
import { randomBytes } from "node:crypto"
import * as Fs from "node:fs/promises"
import * as Os from "node:os"
import * as Path from "node:path"
import { Effect, FileSystem, PlatformError } from "effect"
import {
  AvroContainerError,
  AvroNode,
  decodeContainer,
  encodeContainer,
  readFile,
  readContainerFile,
  readContainerIterable,
  writeFile,
  writeContainerFile
} from "../src/index.js"

describe("@effect-avro/node", () => {
  const schema = {
    type: "record",
    name: "Event",
    fields: [
      { name: "id", type: "long" },
      { name: "name", type: "string" }
    ]
  } as const
  const values = [
    { id: 1, name: "created" },
    { id: 2, name: "updated" }
  ]

  it("round-trips object container buffers", () => {
    const syncMarker = Buffer.alloc(16, 1)
    const buffer = encodeContainer(schema, values, { syncMarker, blockSize: 1 })
    const decoded = decodeContainer(buffer)

    expect(decoded.schema).toEqual(schema)
    expect(decoded.codec).toBe("null")
    expect(decoded.syncMarker).toEqual(syncMarker)
    expect(decoded.values).toEqual(values)
  })

  it("round-trips deflate encoded blocks", () => {
    const buffer = encodeContainer(schema, values, {
      codec: "deflate",
      syncMarker: randomBytes(16)
    })

    expect(decodeContainer(buffer).values).toEqual(values)
  })

  it.effect("exposes container errors as tagged errors", () =>
    Effect.try({
      try: () => decodeContainer(new Uint8Array()),
      catch: (error) => error as AvroContainerError
    }).pipe(
      Effect.catchTag("AvroContainerError", (error) => Effect.succeed(error._tag)),
      Effect.map((tag) => {
        expect(tag).toBe("AvroContainerError")
      })
    ))

  it.effect("writes and reads files", () =>
    Effect.gen(function*() {
      const dir = yield* Effect.tryPromise(() => Fs.mkdtemp(Path.join(Os.tmpdir(), "effect-avro-")))
      const file = Path.join(dir, "events.avro")

      yield* writeContainerFile(file, schema, values)
      const decoded = yield* readContainerFile(file)

      expect(decoded.values).toEqual(values)
    }).pipe(Effect.provide(nodeFileSystem)))

  it.effect("exposes file helpers through an AvroNode service layer", () =>
    Effect.gen(function*() {
      const dir = yield* Effect.tryPromise(() => Fs.mkdtemp(Path.join(Os.tmpdir(), "effect-avro-")))
      const file = Path.join(dir, "events.avro")

      yield* writeFile(file, schema, values)
      const decoded = yield* readFile(file)

      expect(decoded.values).toEqual(values)
    }).pipe(Effect.provide(AvroNode.layer)))

  it.effect("reads async iterables", () =>
    Effect.gen(function*() {
      const buffer = encodeContainer(schema, values)
      const decoded = yield* readContainerIterable(async function*() {
        yield buffer.subarray(0, 8)
        yield buffer.subarray(8)
      }())

      expect(decoded.values).toEqual(values)
    }))
})

const nodeFileSystem = FileSystem.layerNoop({
  readFile: (path) =>
    Effect.tryPromise({
      try: () => Fs.readFile(path),
      catch: (error) => platformError("readFile", path, error)
    }),
  writeFile: (path, data) =>
    Effect.tryPromise({
      try: () => Fs.writeFile(path, data),
      catch: (error) => platformError("writeFile", path, error)
    })
})

const platformError = (method: string, path: string, cause: unknown) =>
  PlatformError.systemError({
    _tag: "Unknown",
    module: "FileSystem",
    method,
    pathOrDescriptor: path,
    cause
  })

it("preserves special metadata keys", () => {
  const metadata = JSON.parse('{"__proto__":"data","constructor":"ctor"}')
  const result = decodeContainer(encodeContainer("null", [], { metadata })).metadata
  expect(Object.getPrototypeOf(result)).toBe(Object.prototype)
  expect(Object.hasOwn(result, "__proto__")).toBe(true)
  expect(result["__proto__"].toString()).toBe("data")
})

it("shares decode budgets across container blocks and bounds inflation", () => {
  const file = encodeContainer({ type: "array", items: "null" }, [[null, null], [null, null]], { blockSize: 1 })
  expect(() => decodeContainer(file, { limits: { maxValues: 5 } })).toThrow("maxValues")
  const compressed = encodeContainer("string", ["a".repeat(1024)], { codec: "deflate" })
  expect(() => decodeContainer(compressed, { limits: { maxBlockBytes: 32 } })).toThrow()
  expect(decodeContainer(compressed, { limits: { maxBlockBytes: 2048 } }).values).toEqual(["a".repeat(1024)])
})
