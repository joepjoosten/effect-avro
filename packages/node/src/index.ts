import { Buffer } from "node:buffer"
import { randomBytes } from "node:crypto"
import * as Fs from "node:fs/promises"
import * as Zlib from "node:zlib"
import * as Avro from "@effect-avro/core"
import { Context, Effect, FileSystem, Layer, PlatformError, Schema } from "effect"

export type ContainerCodec = "null" | "deflate"

const ContainerEncodeOptionsBase = Schema.Struct({
  codec: Schema.optionalKey(Schema.Literals(["null", "deflate"])),
  metadata: Schema.optionalKey(Schema.Record(
    Schema.String,
    Schema.Union([Schema.Uint8Array, Schema.String])
  )),
  syncMarker: Schema.optionalKey(Schema.Uint8Array),
  blockSize: Schema.optionalKey(Schema.Number),
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

export class AvroContainerError extends Schema.TaggedError<AvroContainerError>()("AvroContainerError", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect())
}) {}

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

const magic = Buffer.from([0x4f, 0x62, 0x6a, 0x01])
const schemaMetadataKey = "avro.schema"
const codecMetadataKey = "avro.codec"
const syncMarkerSize = 16

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
      writeBlock(block, type, codec, syncMarker, writer)
      block = []
    }
  }
  if (block.length > 0) {
    writeBlock(block, type, codec, syncMarker, writer)
  }

  return writer.toBuffer()
}

export const decodeContainer = <A = unknown>(
  input: Buffer | Uint8Array,
  options?: Avro.ParseOptions
): ContainerFile<A> => {
  const reader = new BinaryReader(Buffer.from(input))
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
    const size = reader.readLong()
    if (size < 0) {
      throw avroContainerError(`Invalid Avro object container block size ${size}`)
    }
    const compressedBlock = reader.readFixed(size)
    const block = decodeBlock(codec, compressedBlock)
    let offset = 0
    for (let index = 0; index < count; index++) {
      const decoded = type.decodePartial(block, offset)
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
    try: async () => {
      const chunks: Array<Buffer> = []
      for await (const chunk of input) {
        chunks.push(Buffer.from(chunk))
      }
      return decodeContainer<A>(Buffer.concat(chunks), options)
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

const writeBlock = <A>(
  values: ReadonlyArray<A>,
  type: Avro.Type<A>,
  codec: ContainerCodec,
  syncMarker: Buffer,
  writer: BinaryWriter
) => {
  const raw = Buffer.concat(values.map((value) => Buffer.from(type.toBuffer(value))))
  const encoded = encodeBlock(codec, raw)
  writer.writeLong(values.length)
  writer.writeLong(encoded.length)
  writer.writeBuffer(encoded)
  writer.writeBuffer(syncMarker)
}

const encodeBlock = (codec: ContainerCodec, block: Buffer): Buffer => {
  switch (codec) {
    case "null":
      return block
    case "deflate":
      return Zlib.deflateRawSync(block)
  }
}

const decodeBlock = (codec: ContainerCodec, block: Buffer): Buffer => {
  switch (codec) {
    case "null":
      return block
    case "deflate":
      return Zlib.inflateRawSync(block)
  }
}

const normalizeMetadata = (metadata: Record<string, Buffer | Uint8Array | string> = {}): Record<string, Buffer> => {
  const out: Record<string, Buffer> = {}
  for (const [key, value] of Object.entries(metadata)) {
    out[key] = typeof value === "string" ? Buffer.from(value, "utf8") : toBuffer(value)
  }
  return out
}

const writeMetadata = (metadata: Record<string, Buffer>, writer: BinaryWriter) => {
  const entries = Object.entries(metadata)
  if (entries.length > 0) {
    writer.writeLong(entries.length)
    for (const [key, value] of entries) {
      writer.writeString(key)
      writer.writeBytes(value)
    }
  }
  writer.writeLong(0)
}

const readMetadata = (reader: BinaryReader): Record<string, Buffer> => {
  const out: Record<string, Buffer> = {}
  while (true) {
    const count = reader.readLong()
    if (count === 0) {
      return out
    }
    const actualCount = count < 0 ? -count : count
    if (count < 0) {
      reader.readLong()
    }
    for (let index = 0; index < actualCount; index++) {
      out[reader.readString()] = reader.readBytes()
    }
  }
}

const parseSchema = (text: string): Avro.AvroSchema => {
  try {
    return JSON.parse(text) as Avro.AvroSchema
  } catch (error) {
    throw avroContainerError(`Unable to parse Avro object container schema: ${message(error)}`, error)
  }
}

const parseCodec = (codec: string): ContainerCodec => {
  if (codec === "null" || codec === "deflate") {
    return codec
  }
  throw avroContainerError(`Unsupported Avro object container codec ${JSON.stringify(codec)}`)
}

class BinaryWriter {
  private readonly chunks: Array<Buffer> = []

  writeLong(value: number) {
    if (!Number.isSafeInteger(value)) {
      throw avroContainerError(`Avro long value is outside the JavaScript safe integer range: ${value}`)
    }
    let encoded = (BigInt(value) << 1n) ^ (BigInt(value) >> 63n)
    const bytes: Array<number> = []
    while ((encoded & ~0x7fn) !== 0n) {
      bytes.push(Number((encoded & 0x7fn) | 0x80n))
      encoded >>= 7n
    }
    bytes.push(Number(encoded))
    this.chunks.push(Buffer.from(bytes))
  }

  writeBuffer(value: Buffer) {
    this.chunks.push(value)
  }

  writeBytes(value: Buffer) {
    this.writeLong(value.length)
    this.writeBuffer(value)
  }

  writeString(value: string) {
    this.writeBytes(Buffer.from(value, "utf8"))
  }

  toBuffer(): Buffer {
    return Buffer.concat(this.chunks)
  }
}

class BinaryReader {
  readonly buffer: Buffer
  offset = 0

  constructor(buffer: Buffer) {
    this.buffer = buffer
  }

  get done(): boolean {
    return this.offset === this.buffer.length
  }

  readLong(): number {
    let shift = 0n
    let value = 0n
    while (true) {
      const byte = this.readByte()
      value |= BigInt(byte & 0x7f) << shift
      if ((byte & 0x80) === 0) {
        break
      }
      shift += 7n
      if (shift > 63n) {
        throw avroContainerError("Invalid Avro variable-length integer")
      }
    }
    const decoded = (value >> 1n) ^ -(value & 1n)
    const number = Number(decoded)
    if (!Number.isSafeInteger(number)) {
      throw avroContainerError(`Decoded Avro long is outside the JavaScript safe integer range: ${decoded}`)
    }
    return number
  }

  readBytes(): Buffer {
    const size = this.readLong()
    if (size < 0) {
      throw avroContainerError(`Invalid negative bytes length ${size}`)
    }
    return this.readFixed(size)
  }

  readString(): string {
    return this.readBytes().toString("utf8")
  }

  readFixed(size: number): Buffer {
    this.ensure(size)
    const value = this.buffer.subarray(this.offset, this.offset + size)
    this.offset += size
    return value
  }

  private readByte(): number {
    this.ensure(1)
    return this.buffer[this.offset++]
  }

  private ensure(bytes: number) {
    if (this.offset + bytes > this.buffer.length) {
      throw avroContainerError("Truncated Avro object container data")
    }
  }
}

const toBuffer = (value: Buffer | Uint8Array): Buffer =>
  Buffer.isBuffer(value) ? value : Buffer.from(value.buffer, value.byteOffset, value.byteLength)

const nodePlatformError = (method: string, path: string | URL, cause: unknown) =>
  PlatformError.systemError({
    _tag: "Unknown",
    module: "FileSystem",
    method,
    pathOrDescriptor: String(path),
    cause
  })

const avroContainerError = (message: string, cause?: unknown): AvroContainerError =>
  cause === undefined ? new AvroContainerError({ message }) : new AvroContainerError({ message, cause })

const message = (error: unknown): string => error instanceof Error ? error.message : String(error)
