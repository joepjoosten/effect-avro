import { describe, expect, it } from "@effect/vitest"
import { randomBytes } from "node:crypto"
import * as Fs from "node:fs/promises"
import * as Os from "node:os"
import * as Path from "node:path"
import { Effect, Fiber, FileSystem, PlatformError } from "effect"
import {
  AvroContainerError,
  AvroNode,
  decodeContainer,
  decodeContainerRecords,
  encodeContainerIterable,
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

it("streams records before EOF and closes an abandoned source", async () => {
  const file = encodeContainer("int", [1, 2], { blockSize: 1 })
  let pulls = 0
  let closed = false
  const source = async function*() {
    try { pulls++; yield file; pulls++; await new Promise(() => {}) }
    finally { closed = true }
  }
  const records = decodeContainerRecords<number>(source())
  expect(await records.next()).toMatchObject({ value: 1, done: false })
  expect(pulls).toBe(1)
  await records.return(undefined)
  expect(closed).toBe(true)
})

it("decodes arbitrary chunk boundaries and applies writer backpressure", async () => {
  for (const codec of ["null", "deflate"] as const) {
    let produced = 0
    let closed = false
    const source = async function*() { try { for (const value of [1, 2, 3]) { produced++; yield value } } finally { closed = true } }
    const output = encodeContainerIterable("int", source(), { blockSize: 1, codec })
    const header = await output.next()
    expect(produced).toBe(0)
    const block = await output.next()
    expect(produced).toBe(1)
    const chunks = [header.value!, block.value!]
    for await (const chunk of output) chunks.push(chunk)
    expect(closed).toBe(true)
    const file = Buffer.concat(chunks)
    const values: unknown[] = []
    for await (const value of decodeContainerRecords(async function*() {
      for (const byte of file) yield new Uint8Array([byte])
    }())) values.push(value)
    expect(values).toEqual([1, 2, 3])
    expect(decodeContainer(file).values).toEqual(values)
  }
})

it.effect("closes a pending input iterator when interrupted", () => Effect.gen(function*() {
  let started!: () => void
  const ready = new Promise<void>((resolve) => { started = resolve })
  let returned = 0
  const input: AsyncIterable<Uint8Array> = { [Symbol.asyncIterator]: () => ({
    next: () => { started(); return new Promise(() => {}) },
    return: async () => { returned++; return { done: true, value: undefined } }
  }) }
  const fiber = yield* readContainerIterable(input).pipe(Effect.forkChild)
  yield* Effect.promise(() => ready)
  yield* Fiber.interrupt(fiber)
  expect(returned).toBe(1)
}))

it("splits streaming blocks by bytes and closes a cancelled writer", async () => {
  for (const codec of ["null", "deflate"] as const) {
    const chunks: Buffer[] = []
    for await (const chunk of encodeContainerIterable("string", ["aa", "bb", "cc"], { maxBlockBytes: 4, codec })) chunks.push(chunk)
    expect(chunks).toHaveLength(4)
    expect(decodeContainer(Buffer.concat(chunks), { limits: { maxBlockBytes: 4 } }).values).toEqual(["aa", "bb", "cc"])
  }
  const oversized = encodeContainerIterable("string", ["hello"], { maxBlockBytes: 4 })
  await oversized.next()
  await expect(oversized.next()).rejects.toThrow("record exceeds maxBlockBytes")
  let closed = false
  const source = async function*() { try { yield 1; await new Promise(() => {}) } finally { closed = true } }
  const writer = encodeContainerIterable("int", source(), { blockSize: 1 })
  await writer.next()
  await writer.next()
  await writer.return(undefined)
  expect(closed).toBe(true)
})
