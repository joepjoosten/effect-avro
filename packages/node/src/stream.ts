import { Buffer } from "node:buffer"
import { randomBytes } from "node:crypto"
import * as Avro from "@effect-avro/core"
import type { ContainerEncodeOptions, ContainerFile } from "./index.js"
import {
  BinaryWriter, avroContainerError, codecMetadataKey, decodeBlock, encodeBlock,
  magic, parseCodec, parseSchema, schemaMetadataKey, syncMarkerSize,
  normalizeMetadata, writeMetadata, toBuffer
} from "./internal/container.js"

export type ContainerHeader = Omit<ContainerFile<never>, "values">
export type ContainerEvent<A> =
  | { readonly _tag: "Header"; readonly header: ContainerHeader }
  | { readonly _tag: "Value"; readonly value: A }

/** Decode incrementally, retaining only the current input chunk and container block. */
export async function* decodeContainerEvents<A = unknown>(
  input: AsyncIterable<Buffer | Uint8Array>,
  options?: Avro.ParseOptions,
  signal?: AbortSignal
): AsyncGenerator<ContainerEvent<A>> {
  const budget = new Avro.DecodeBudget(options?.limits)
  const reader = new AsyncByteReader(input, budget.limits, signal)
  try {
    if (!(await reader.readFixed(magic.length)).equals(magic)) throw avroContainerError("Invalid Avro object container magic header")
    const metadata: Record<string, Buffer> = {}
    while (true) {
      const count = await reader.readLong()
      if (count === 0) break
      const actualCount = Math.abs(count)
      if (actualCount > budget.limits.maxCollectionItems) throw avroContainerError("Avro metadata maxCollectionItems exceeded")
      const size = count < 0 ? await reader.readLong() : undefined
      if (size !== undefined && (size < 0 || size > budget.limits.maxBytes)) throw avroContainerError("Invalid metadata block size")
      const end = size === undefined ? undefined : reader.position + size
      for (let i = 0; i < actualCount; i++) {
        const key = (await reader.readBytes()).toString("utf8")
        const value = await reader.readBytes()
        Object.defineProperty(metadata, key, { value, enumerable: true, writable: true, configurable: true })
      }
      if (end !== undefined && reader.position !== end) throw avroContainerError("Metadata block size mismatch")
    }
    const schemaText = metadata[schemaMetadataKey]?.toString("utf8")
    if (schemaText === undefined) throw avroContainerError("Avro object container is missing avro.schema metadata")
    const schema = parseSchema(schemaText)
    const codec = parseCodec(metadata[codecMetadataKey]?.toString("utf8") ?? "null")
    const syncMarker = await reader.readFixed(syncMarkerSize)
    const type = Avro.parse<A>(schema, options)
    yield { _tag: "Header", header: { schema, codec, metadata, syncMarker } }
    while (true) {
      const count = await reader.readLongOrEnd()
      if (count === undefined) return
      if (count <= 0) throw avroContainerError(`Invalid Avro object container block count ${count}`)
      budget.collection(count)
      const size = await reader.readLong()
      if (size < 0 || size > budget.limits.maxBytes) throw avroContainerError("Invalid or oversized container block")
      const compressed = await reader.readFixed(size)
      if (!(await reader.readFixed(syncMarkerSize)).equals(syncMarker)) throw avroContainerError("Avro object container sync marker mismatch")
      const block = decodeBlock(codec, compressed, budget.limits.maxBlockBytes)
      let offset = 0
      for (let i = 0; i < count; i++) {
        if (signal?.aborted) throw avroContainerError("Avro container stream interrupted", signal.reason)
        const decoded = type.decodePartial(block, offset, budget)
        offset = decoded.offset
        yield { _tag: "Value", value: decoded.value }
      }
      if (offset !== block.length) throw avroContainerError("Avro object container block contains trailing bytes")
    }
  } finally {
    await reader.close()
  }
}

export async function* decodeContainerRecords<A = unknown>(
  input: AsyncIterable<Buffer | Uint8Array>,
  options?: Avro.ParseOptions,
  signal?: AbortSignal
): AsyncGenerator<A> {
  for await (const event of decodeContainerEvents<A>(input, options, signal)) {
    if (event._tag === "Value") yield event.value
  }
}

/** Emits a header and completed blocks with upstream backpressure. */
export async function* encodeContainerIterable<A>(
  schema: Avro.AvroSchema,
  values: Iterable<A> | AsyncIterable<A>,
  options: ContainerEncodeOptions = {}
): AsyncGenerator<Buffer> {
  const codec = options.codec ?? "null"
  const syncMarker = options.syncMarker === undefined ? randomBytes(syncMarkerSize) : toBuffer(options.syncMarker)
  if (syncMarker.length !== syncMarkerSize) throw avroContainerError(`Avro sync marker must be ${syncMarkerSize} bytes`)
  const blockSize = options.blockSize ?? 1000
  const maxBlockBytes = options.maxBlockBytes ?? Avro.defaultDecodeLimits.maxBlockBytes
  if (!Number.isSafeInteger(blockSize) || blockSize <= 0) throw avroContainerError("Invalid Avro block size")
  if (!Number.isSafeInteger(maxBlockBytes) || maxBlockBytes < 0) throw avroContainerError("Invalid Avro maxBlockBytes")
  const type = Avro.parse<A>(schema, options.parseOptions)
  const metadata = normalizeMetadata(options.metadata)
  metadata[schemaMetadataKey] = Buffer.from(JSON.stringify(schema), "utf8")
  metadata[codecMetadataKey] = Buffer.from(codec, "utf8")
  const header = new BinaryWriter()
  header.writeBuffer(magic)
  writeMetadata(metadata, header)
  header.writeBuffer(syncMarker)
  yield header.toBuffer()
  let chunks: Buffer[] = []
  let bytes = 0
  const flush = () => {
    const encoded = encodeBlock(codec, Buffer.concat(chunks))
    const writer = new BinaryWriter()
    writer.writeLong(chunks.length)
    writer.writeLong(encoded.length)
    writer.writeBuffer(encoded)
    writer.writeBuffer(syncMarker)
    chunks = []
    bytes = 0
    return writer.toBuffer()
  }
  for await (const value of values) {
    const encoded = Buffer.from(type.toBuffer(value))
    if (encoded.length > maxBlockBytes) throw avroContainerError("Avro record exceeds maxBlockBytes")
    if (chunks.length > 0 && bytes + encoded.length > maxBlockBytes) yield flush()
    chunks.push(encoded)
    bytes += encoded.length
    if (chunks.length >= blockSize || bytes >= maxBlockBytes) yield flush()
  }
  if (chunks.length > 0) yield flush()
}

class AsyncByteReader {
  private readonly iterator: AsyncIterator<Buffer | Uint8Array>
  private current: Uint8Array = new Uint8Array()
  private offset = 0
  private received = 0
  private ended = false
  private closed = false
  position = 0
  private readonly onAbort = () => { void this.close().catch(() => undefined) }

  constructor(input: AsyncIterable<Buffer | Uint8Array>, private readonly limits: Required<Avro.DecodeLimits>, private readonly signal?: AbortSignal) {
    this.iterator = input[Symbol.asyncIterator]()
    signal?.addEventListener("abort", this.onAbort, { once: true })
  }

  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    this.signal?.removeEventListener("abort", this.onAbort)
    const cleanup = this.iterator.return?.()
    if (this.signal?.aborted) { void cleanup?.catch(() => undefined); return }
    await cleanup
  }

  private async nextChunk(): Promise<IteratorResult<Buffer | Uint8Array>> {
    const signal = this.signal
    if (signal?.aborted) throw avroContainerError("Avro container stream interrupted", signal.reason)
    const pending = this.iterator.next()
    if (signal === undefined) return pending
    return new Promise((resolve, reject) => {
      const abort = () => { signal.removeEventListener("abort", abort); reject(avroContainerError("Avro container stream interrupted", signal.reason)) }
      signal.addEventListener("abort", abort, { once: true })
      pending.then(
        (value) => { signal.removeEventListener("abort", abort); resolve(value) },
        (error) => { signal.removeEventListener("abort", abort); reject(error) }
      )
      if (signal.aborted) abort()
    })
  }

  private async available(): Promise<boolean> {
    if (this.signal?.aborted) throw avroContainerError("Avro container stream interrupted", this.signal.reason)
    while (this.offset === this.current.length) {
      if (this.ended) return false
      const next = await this.nextChunk()
      if (next.done) { this.ended = true; return false }
      this.current = next.value
      this.offset = 0
      this.received += next.value.length
      if (this.received > this.limits.maxBytes) throw avroContainerError("Avro decode maxBytes exceeded")
    }
    return true
  }

  async readFixed(size: number): Promise<Buffer> {
    if (!Number.isSafeInteger(size) || size < 0 || size > this.limits.maxBytes) throw avroContainerError("Invalid Avro read size")
    const result = Buffer.allocUnsafe(size)
    let offset = 0
    while (offset < size) {
      if (!(await this.available())) throw avroContainerError("Truncated Avro object container data")
      const count = Math.min(size - offset, this.current.length - this.offset)
      result.set(this.current.subarray(this.offset, this.offset + count), offset)
      this.offset += count
      this.position += count
      offset += count
    }
    return result
  }

  async readBytes(): Promise<Buffer> { return this.readFixed(await this.readLong()) }

  async readLong(): Promise<number> {
    const value = await this.readLongOrEnd()
    if (value === undefined) throw avroContainerError("Truncated Avro object container data")
    return value
  }

  async readLongOrEnd(): Promise<number | undefined> {
    if (!(await this.available())) return undefined
    let value = 0n
    let shift = 0n
    while (true) {
      const byte = (await this.readFixed(1))[0]
      value |= BigInt(byte & 0x7f) << shift
      if ((byte & 0x80) === 0) break
      shift += 7n
      if (shift > 63n) throw avroContainerError("Invalid Avro variable-length integer")
    }
    const result = Number((value >> 1n) ^ -(value & 1n))
    if (!Number.isSafeInteger(result)) throw avroContainerError("Avro long is outside the JavaScript safe integer range")
    return result
  }
}
