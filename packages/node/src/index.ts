import { Buffer } from "node:buffer"
import { randomBytes } from "node:crypto"
import * as Fs from "node:fs/promises"
import * as Avro from "@effect-avro/core"
import { Context, Effect, FileSystem, Layer, PlatformError, Schema } from "effect"

import {
  AvroContainerError, avroContainerError, magic, schemaMetadataKey, codecMetadataKey,
  syncMarkerSize, toBuffer, normalizeMetadata, BinaryWriter, BinaryReader,
  writeMetadata, readMetadata, writeBlock, parseSchema, parseCodec, decodeBlock,
  type ContainerCodec
} from "./internal/container.js"
import { decodeContainerEvents, type ContainerHeader } from "./stream.js"
export { AvroContainerError } from "./internal/container.js"
export type { ContainerCodec } from "./internal/container.js"
export { decodeContainerEvents, decodeContainerRecords, encodeContainerIterable } from "./stream.js"
export type { ContainerHeader, ContainerEvent } from "./stream.js"

const ContainerEncodeOptionsBase = Schema.Struct({
  codec: Schema.optionalKey(Schema.Literals(["null", "deflate"])),
  metadata: Schema.optionalKey(Schema.Record(
    Schema.String,
    Schema.Union([Schema.Uint8Array, Schema.String])
  )),
  syncMarker: Schema.optionalKey(Schema.Uint8Array),
  blockSize: Schema.optionalKey(Schema.Number),
  maxBlockBytes: Schema.optionalKey(Schema.Number),
  parseOptions: Schema.optionalKey(Avro.ParseOptions)
})
export const ContainerEncodeOptions = ContainerEncodeOptionsBase
export type ContainerEncodeOptions =
  Omit<typeof ContainerEncodeOptionsBase.Type, "metadata" | "syncMarker"> & {
    readonly metadata?: Record<string, Buffer | Uint8Array | string>
    readonly syncMarker?: Buffer | Uint8Array
  }

export const ContainerFile = <A>(value: Schema.Schema<A>) =>
  Schema.Struct({
    schema: Avro.AvroSchema,
    codec: Schema.Literals(["null", "deflate"]),
    metadata: Schema.Record(Schema.String, Schema.Uint8Array),
    syncMarker: Schema.Uint8Array,
    values: Schema.Array(value)
  })

export type ContainerFile<A = unknown> = {
  readonly schema: Avro.AvroSchema
  readonly codec: ContainerCodec
  readonly metadata: Record<string, Buffer>
  readonly syncMarker: Buffer
  readonly values: ReadonlyArray<A>
}


export interface AvroNodeService {
  readonly writeContainerFile: <A>(
    path: string,
    schema: Avro.AvroSchema,
    values: Iterable<A>,
    options?: ContainerEncodeOptions
  ) => Effect.Effect<void, AvroContainerError | PlatformError.PlatformError>
  readonly readContainerFile: <A = unknown>(
    path: string,
    options?: Avro.ParseOptions
  ) => Effect.Effect<ContainerFile<A>, AvroContainerError | PlatformError.PlatformError>
  readonly readContainerIterable: <A = unknown>(
    input: AsyncIterable<Buffer | Uint8Array>,
    options?: Avro.ParseOptions
  ) => Effect.Effect<ContainerFile<A>, AvroContainerError>
}

export const nodeFileSystemLayer: Layer.Layer<FileSystem.FileSystem> = FileSystem.layerNoop({
  readFile: (path) =>
    Effect.tryPromise({
      try: () => Fs.readFile(path),
      catch: (error) => nodePlatformError("readFile", path, error)
    }),
  writeFile: (path, data) =>
    Effect.tryPromise({
      try: () => Fs.writeFile(path, data),
      catch: (error) => nodePlatformError("writeFile", path, error)
    })
})

export class AvroNode extends Context.Service<AvroNode, AvroNodeService>()(
  "@effect-avro/node/AvroNode"
) {
  static readonly layerNoDeps: Layer.Layer<AvroNode, never, FileSystem.FileSystem> = Layer.effect(
    AvroNode,
    Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      return AvroNode.of(makeAvroNode(fs))
    })
  )

  static readonly layer: Layer.Layer<AvroNode> = this.layerNoDeps.pipe(
    Layer.provide(nodeFileSystemLayer)
  )
}

export const encodeContainer = <A>(
  schema: Avro.AvroSchema,
  values: Iterable<A>,
  options: ContainerEncodeOptions = {}
): Buffer => {
  const codec = options.codec ?? "null"
  const syncMarker = options.syncMarker === undefined ? randomBytes(syncMarkerSize) : toBuffer(options.syncMarker)
  if (syncMarker.length !== syncMarkerSize) {
    throw avroContainerError(`Avro sync marker must be ${syncMarkerSize} bytes`)
  }
  const blockSize = options.blockSize ?? 1000
  if (!Number.isInteger(blockSize) || blockSize <= 0) {
    throw avroContainerError(`Avro block size must be a positive integer, got ${blockSize}`)
  }

  const metadata = normalizeMetadata(options.metadata)
  metadata[schemaMetadataKey] = Buffer.from(JSON.stringify(schema), "utf8")
  metadata[codecMetadataKey] = Buffer.from(codec, "utf8")

  const type = Avro.parse<A>(schema, options.parseOptions)
  const writer = new BinaryWriter()
  writer.writeBuffer(magic)
  writeMetadata(metadata, writer)
  writer.writeBuffer(syncMarker)

  let block: Array<A> = []
  for (const value of values) {
    block.push(value)
    if (block.length >= blockSize) {
      writeBlock(block, type, codec, syncMarker, writer, options.maxBlockBytes)
      block = []
    }
  }
  if (block.length > 0) {
    writeBlock(block, type, codec, syncMarker, writer, options.maxBlockBytes)
  }

  return writer.toBuffer()
}

export const decodeContainer = <A = unknown>(
  input: Buffer | Uint8Array,
  options?: Avro.ParseOptions
): ContainerFile<A> => {
  const budget = new Avro.DecodeBudget(options?.limits)
  if (input.length > budget.limits.maxBytes) throw avroContainerError("Avro decode maxBytes exceeded")
  const reader = new BinaryReader(Buffer.from(input), budget)
  const actualMagic = reader.readFixed(magic.length)
  if (!actualMagic.equals(magic)) {
    throw avroContainerError("Invalid Avro object container magic header")
  }
  const metadata = readMetadata(reader)
  const schemaText = metadata[schemaMetadataKey]?.toString("utf8")
  if (schemaText === undefined) {
    throw avroContainerError("Avro object container is missing avro.schema metadata")
  }
  const schema = parseSchema(schemaText)
  const codec = parseCodec(metadata[codecMetadataKey]?.toString("utf8") ?? "null")
  const syncMarker = reader.readFixed(syncMarkerSize)
  const type = Avro.parse<A>(schema, options)
  const values: Array<A> = []

  while (!reader.done) {
    const count = reader.readLong()
    if (count <= 0) {
      throw avroContainerError(`Invalid Avro object container block count ${count}`)
    }
    budget.collection(count)
    const size = reader.readLong()
    if (size < 0 || size > budget.limits.maxBytes) {
      throw avroContainerError(`Invalid Avro object container block size ${size}`)
    }
    const compressedBlock = reader.readFixed(size)
    const block = decodeBlock(codec, compressedBlock, budget.limits.maxBlockBytes)
    let offset = 0
    for (let index = 0; index < count; index++) {
      const decoded = type.decodePartial(block, offset, budget)
      values.push(decoded.value)
      offset = decoded.offset
    }
    if (offset !== block.length) {
      throw avroContainerError("Avro object container block contains trailing bytes")
    }
    const blockSyncMarker = reader.readFixed(syncMarkerSize)
    if (!blockSyncMarker.equals(syncMarker)) {
      throw avroContainerError("Avro object container sync marker mismatch")
    }
  }

  return {
    schema,
    codec,
    metadata,
    syncMarker,
    values
  }
}

export const writeContainerFile = <A>(
  path: string,
  schema: Avro.AvroSchema,
  values: Iterable<A>,
  options?: ContainerEncodeOptions
) =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    yield* makeAvroNode(fs).writeContainerFile(path, schema, values, options)
  })

export const readContainerFile = <A = unknown>(
  path: string,
  options?: Avro.ParseOptions
) =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    return yield* makeAvroNode(fs).readContainerFile<A>(path, options)
  })

export const readContainerIterable = <A = unknown>(
  input: AsyncIterable<Buffer | Uint8Array>,
  options?: Avro.ParseOptions
) =>
  Effect.tryPromise({
    try: async (signal) => {
      let header: ContainerHeader | undefined
      const values: A[] = []
      for await (const event of decodeContainerEvents<A>(input, options, signal)) {
        if (event._tag === "Header") header = event.header
        else values.push(event.value)
      }
      if (header === undefined) throw avroContainerError("Missing Avro container header")
      return { ...header, values }
    },
    catch: (error) => error instanceof AvroContainerError
      ? error
      : avroContainerError(`Unable to read Avro container stream: ${message(error)}`, error)
  })

export const makeAvroNode = (fs: FileSystem.FileSystem): AvroNodeService => ({
  writeContainerFile: (path, schema, values, options) =>
    Effect.gen(function*() {
      const bytes = yield* Effect.try({
        try: () => encodeContainer(schema, values, options),
        catch: (error) => avroContainerError(`Unable to encode Avro container file: ${message(error)}`, error)
      })
      yield* fs.writeFile(path, bytes)
    }),
  readContainerFile: <A = unknown>(path: string, options?: Avro.ParseOptions) =>
    Effect.gen(function*() {
      const bytes = yield* fs.readFile(path)
      return yield* Effect.try({
        try: () => decodeContainer<A>(bytes, options),
        catch: (error) => error instanceof AvroContainerError
          ? error
          : avroContainerError(`Unable to decode Avro container file: ${message(error)}`, error)
      })
    }),
  readContainerIterable
})

export const writeFile = <A>(
  path: string,
  schema: Avro.AvroSchema,
  values: Iterable<A>,
  options?: ContainerEncodeOptions
): Effect.Effect<void, AvroContainerError | PlatformError.PlatformError, AvroNode> =>
  AvroNode.use((node) => node.writeContainerFile(path, schema, values, options))

export const readFile = <A = unknown>(
  path: string,
  options?: Avro.ParseOptions
): Effect.Effect<ContainerFile<A>, AvroContainerError | PlatformError.PlatformError, AvroNode> =>
  AvroNode.use((node) => node.readContainerFile<A>(path, options))

export const readIterable = <A = unknown>(
  input: AsyncIterable<Buffer | Uint8Array>,
  options?: Avro.ParseOptions
): Effect.Effect<ContainerFile<A>, AvroContainerError, AvroNode> =>
  AvroNode.use((node) => node.readContainerIterable<A>(input, options))

const nodePlatformError = (method: string, path: string | URL, cause: unknown) =>
  PlatformError.systemError({
    _tag: "Unknown",
    module: "FileSystem",
    method,
    pathOrDescriptor: String(path),
    cause
  })


const message = (error: unknown): string => error instanceof Error ? error.message : String(error)

