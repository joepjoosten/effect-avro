import { Buffer } from "node:buffer"
import * as Zlib from "node:zlib"
import * as Avro from "@effect-avro/core"
import { Schema } from "effect"
export type ContainerCodec = "null" | "deflate"

export class AvroContainerError extends Schema.TaggedError<AvroContainerError>()("AvroContainerError", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect())
}) {}
export const magic = Buffer.from([0x4f, 0x62, 0x6a, 0x01])
export const schemaMetadataKey = "avro.schema"
export const codecMetadataKey = "avro.codec"
export const syncMarkerSize = 16

export const writeBlock = <A>(
  values: ReadonlyArray<A>,
  type: Avro.Type<A>,
  codec: ContainerCodec,
  syncMarker: Buffer,
  writer: BinaryWriter,
  maxBlockBytes?: number
) => {
  const raw = Buffer.concat(values.map((value) => Buffer.from(type.toBuffer(value))))
  if (maxBlockBytes !== undefined && (!Number.isSafeInteger(maxBlockBytes) || maxBlockBytes < 0 || raw.length > maxBlockBytes)) {
    throw avroContainerError("Avro block exceeds maxBlockBytes")
  }
  const encoded = encodeBlock(codec, raw)
  writer.writeLong(values.length)
  writer.writeLong(encoded.length)
  writer.writeBuffer(encoded)
  writer.writeBuffer(syncMarker)
}

export const encodeBlock = (codec: ContainerCodec, block: Buffer): Buffer => {
  switch (codec) {
    case "null":
      return block
    case "deflate":
      return Zlib.deflateRawSync(block)
  }
}

export const decodeBlock = (codec: ContainerCodec, block: Buffer, maxOutputLength = Avro.defaultDecodeLimits.maxBlockBytes): Buffer => {
  switch (codec) {
    case "null":
      if (block.length > maxOutputLength) throw avroContainerError("Avro block exceeds maxBlockBytes")
      return block
    case "deflate":
      return Zlib.inflateRawSync(block, { maxOutputLength: Math.max(1, maxOutputLength) })
  }
}

export const normalizeMetadata = (metadata: Record<string, Buffer | Uint8Array | string> = {}): Record<string, Buffer> => {
  const out: Record<string, Buffer> = {}
  for (const [key, value] of Object.entries(metadata)) {
    setOwn(out, key, typeof value === "string" ? Buffer.from(value, "utf8") : toBuffer(value))
  }
  return out
}

export const writeMetadata = (metadata: Record<string, Buffer>, writer: BinaryWriter) => {
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

export const readMetadata = (reader: BinaryReader): Record<string, Buffer> => {
  const out: Record<string, Buffer> = {}
  while (true) {
    const count = reader.readLong()
    if (count === 0) {
      return out
    }
    const actualCount = count < 0 ? -count : count
    if (actualCount > reader.budget.limits.maxCollectionItems) throw avroContainerError("Avro metadata maxCollectionItems exceeded")
    if (count < 0) {
      reader.readLong()
    }
    for (let index = 0; index < actualCount; index++) {
      setOwn(out, reader.readString(), reader.readBytes())
    }
  }
}

export const parseSchema = (text: string): Avro.AvroSchema => {
  try {
    return JSON.parse(text) as Avro.AvroSchema
  } catch (error) {
    throw avroContainerError(`Unable to parse Avro object container schema: ${message(error)}`, error)
  }
}

export const parseCodec = (codec: string): ContainerCodec => {
  if (codec === "null" || codec === "deflate") {
    return codec
  }
  throw avroContainerError(`Unsupported Avro object container codec ${JSON.stringify(codec)}`)
}

export class BinaryWriter {
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

export class BinaryReader {
  readonly buffer: Buffer
  offset = 0

  constructor(buffer: Buffer, readonly budget = new Avro.DecodeBudget()) {
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

export const toBuffer = (value: Buffer | Uint8Array): Buffer =>
  Buffer.isBuffer(value) ? value : Buffer.from(value.buffer, value.byteOffset, value.byteLength)

export const avroContainerError = (message: string, cause?: unknown): AvroContainerError =>
  cause === undefined ? new AvroContainerError({ message }) : new AvroContainerError({ message, cause })

export const message = (error: unknown): string => error instanceof Error ? error.message : String(error)

export const setOwn = <A>(object: Record<string, A>, key: string, value: A): void => {
  Object.defineProperty(object, key, { value, enumerable: true, writable: true, configurable: true })
}
