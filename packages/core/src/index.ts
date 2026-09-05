import { Effect, Schema } from "effect"

export const AvroPrimitive = Schema.Literals([
  "null",
  "boolean",
  "int",
  "long",
  "float",
  "double",
  "bytes",
  "string"
])
export type AvroPrimitive = typeof AvroPrimitive.Type

export type AvroRecordField = {
  readonly name: string
  readonly type: AvroSchema
  readonly doc?: string
  readonly default?: unknown
  readonly "x-effect-optional"?: boolean
  readonly order?: "ascending" | "descending" | "ignore"
  readonly aliases?: ReadonlyArray<string>
}

export type AvroRecordSchema = {
  readonly type: "record" | "error"
  readonly name: string
  readonly namespace?: string
  readonly doc?: string
  readonly aliases?: ReadonlyArray<string>
  readonly fields: ReadonlyArray<AvroRecordField>
  readonly [key: string]: unknown
}

export type AvroEnumSchema = {
  readonly type: "enum"
  readonly name: string
  readonly namespace?: string
  readonly doc?: string
  readonly aliases?: ReadonlyArray<string>
  readonly symbols: ReadonlyArray<string>
  readonly default?: string
}

export type AvroArraySchema = {
  readonly type: "array"
  readonly items: AvroSchema
}

export type AvroMapSchema = {
  readonly type: "map"
  readonly values: AvroSchema
}

export type AvroFixedSchema = {
  readonly type: "fixed"
  readonly name: string
  readonly namespace?: string
  readonly aliases?: ReadonlyArray<string>
  readonly size: number
  readonly logicalType?: string
}

export type AvroLogicalSchema = {
  readonly type: AvroSchema
  readonly logicalType: string
  readonly precision?: number
  readonly scale?: number
}

export type AvroNamedSchema = AvroRecordSchema | AvroEnumSchema | AvroFixedSchema
export type AvroUnionSchema = ReadonlyArray<AvroSchema>
export type AvroSchema =
  | AvroPrimitive
  | string
  | AvroRecordSchema
  | AvroEnumSchema
  | AvroArraySchema
  | AvroMapSchema
  | AvroFixedSchema
  | AvroLogicalSchema
  | AvroUnionSchema

export const AvroRecordField: Schema.Schema<AvroRecordField> = Schema.Struct({
  name: Schema.String,
  type: Schema.suspend((): Schema.Schema<AvroSchema> => AvroSchema),
  doc: Schema.optionalKey(Schema.String),
  default: Schema.optionalKey(Schema.Unknown),
  "x-effect-optional": Schema.optionalKey(Schema.Boolean),
  order: Schema.optionalKey(Schema.Literals(["ascending", "descending", "ignore"])),
  aliases: Schema.optionalKey(Schema.Array(Schema.String))
}) as Schema.Schema<AvroRecordField>

export const AvroRecordSchema: Schema.Schema<AvroRecordSchema> = Schema.Struct({
  type: Schema.Literals(["record", "error"]),
  name: Schema.String,
  namespace: Schema.optionalKey(Schema.String),
  doc: Schema.optionalKey(Schema.String),
  aliases: Schema.optionalKey(Schema.Array(Schema.String)),
  fields: Schema.Array(AvroRecordField)
}) as Schema.Schema<AvroRecordSchema>

export const AvroEnumSchema: Schema.Schema<AvroEnumSchema> = Schema.Struct({
  type: Schema.Literal("enum"),
  name: Schema.String,
  namespace: Schema.optionalKey(Schema.String),
  doc: Schema.optionalKey(Schema.String),
  aliases: Schema.optionalKey(Schema.Array(Schema.String)),
  symbols: Schema.Array(Schema.String),
  default: Schema.optionalKey(Schema.String)
}) as Schema.Schema<AvroEnumSchema>

export const AvroArraySchema: Schema.Schema<AvroArraySchema> = Schema.Struct({
  type: Schema.Literal("array"),
  items: Schema.suspend((): Schema.Schema<AvroSchema> => AvroSchema)
}) as Schema.Schema<AvroArraySchema>

export const AvroMapSchema: Schema.Schema<AvroMapSchema> = Schema.Struct({
  type: Schema.Literal("map"),
  values: Schema.suspend((): Schema.Schema<AvroSchema> => AvroSchema)
}) as Schema.Schema<AvroMapSchema>

export const AvroFixedSchema: Schema.Schema<AvroFixedSchema> = Schema.Struct({
  type: Schema.Literal("fixed"),
  name: Schema.String,
  namespace: Schema.optionalKey(Schema.String),
  aliases: Schema.optionalKey(Schema.Array(Schema.String)),
  size: Schema.Number,
  logicalType: Schema.optionalKey(Schema.String)
}) as Schema.Schema<AvroFixedSchema>

export const AvroLogicalSchema: Schema.Schema<AvroLogicalSchema> = Schema.Struct({
  type: Schema.suspend((): Schema.Schema<AvroSchema> => AvroSchema),
  logicalType: Schema.String,
  precision: Schema.optionalKey(Schema.Number),
  scale: Schema.optionalKey(Schema.Number)
}) as Schema.Schema<AvroLogicalSchema>

export const AvroSchema: Schema.Schema<AvroSchema> = Schema.suspend(() =>
  Schema.Union([
    Schema.String,
    AvroRecordSchema,
    AvroEnumSchema,
    AvroArraySchema,
    AvroMapSchema,
    AvroFixedSchema,
    AvroLogicalSchema,
    Schema.Array(AvroSchema)
  ])
) as Schema.Schema<AvroSchema>

type AvroObjectSchema = Exclude<AvroSchema, string | AvroUnionSchema>

export class AvroError extends Schema.TaggedError<AvroError>()("AvroError", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect())
}) {}

export interface Type<A = unknown> {
  readonly schema: AvroSchema
  readonly toUint8Array: (value: A) => Uint8Array
  readonly fromUint8Array: (buffer: Uint8Array) => A
  readonly toBuffer: (value: A) => Uint8Array
  readonly fromBuffer: (buffer: Uint8Array) => A
  readonly decodePartial: (buffer: Uint8Array, offset?: number) => DecodeResult<A>
  readonly encode: (value: A) => Uint8Array
  readonly decode: (buffer: Uint8Array) => A
  readonly isValid: (value: unknown) => value is A
  readonly getSchema: () => string
}

export const DecodeResult = <A>(value: Schema.Schema<A>) =>
  Schema.Struct({
    value,
    offset: Schema.Number
  })

export type DecodeResult<A = unknown> = {
  readonly value: A
  readonly offset: number
}

export const ParseOptions = Schema.Struct({
  namespace: Schema.optionalKey(Schema.String),
  restoreTags: Schema.optionalKey(Schema.Boolean)
})
export type ParseOptions = typeof ParseOptions.Type

type Node =
  | { readonly _tag: "null"; readonly schema: AvroSchema }
  | { readonly _tag: "boolean"; readonly schema: AvroSchema }
  | { readonly _tag: "int"; readonly schema: AvroSchema }
  | { readonly _tag: "long"; readonly schema: AvroSchema }
  | { readonly _tag: "float"; readonly schema: AvroSchema }
  | { readonly _tag: "double"; readonly schema: AvroSchema }
  | { readonly _tag: "bytes"; readonly schema: AvroSchema }
  | { readonly _tag: "string"; readonly schema: AvroSchema }
  | { readonly _tag: "array"; readonly schema: AvroArraySchema; readonly item: Node }
  | { readonly _tag: "map"; readonly schema: AvroMapSchema; readonly value: Node }
  | { readonly _tag: "enum"; readonly schema: AvroEnumSchema; readonly name: string; readonly symbols: ReadonlyArray<string> }
  | { readonly _tag: "fixed"; readonly schema: AvroFixedSchema; readonly name: string; readonly size: number }
  | { readonly _tag: "record"; readonly schema: AvroRecordSchema; readonly name: string; fields: ReadonlyArray<FieldNode> }
  | { readonly _tag: "union"; readonly schema: AvroUnionSchema; readonly branches: ReadonlyArray<Node> }
  | { readonly _tag: "ref"; readonly schema: string; readonly name: string; readonly registry: Registry }

type FieldNode = {
  readonly name: string
  readonly node: Node
  readonly defaultValue: unknown
  readonly hasDefault: boolean
}

type Registry = {
  readonly nodes: Map<string, Node>
  readonly aliases: Map<string, string>
}

const primitiveNames = new Set<AvroPrimitive>([
  "null",
  "boolean",
  "int",
  "long",
  "float",
  "double",
  "bytes",
  "string"
])

export const parse = <A = unknown>(schema: AvroSchema, options: ParseOptions = {}): Type<A> => {
  const registry: Registry = {
    nodes: new Map(),
    aliases: new Map()
  }
  const node = compile(schema, registry, options.namespace)

  const api: Type<A> = {
    schema,
    toUint8Array: (value) => {
      const writer = new BinaryWriter()
      writeNode(resolveNode(node), value, writer)
      return writer.toUint8Array()
    },
    fromUint8Array: (input) => {
      const reader = new BinaryReader(input, 0, options.restoreTags)
      const value = readNode(resolveNode(node), reader) as A
      if (!reader.done) {
        throw avroError(`Trailing Avro data at offset ${reader.offset}`)
      }
      return value
    },
    toBuffer(value) {
      return api.toUint8Array(value)
    },
    fromBuffer(input) {
      return api.fromUint8Array(input)
    },
    decodePartial: (input, offset = 0) => {
      const reader = new BinaryReader(input, offset, options.restoreTags)
      return {
        value: readNode(resolveNode(node), reader) as A,
        offset: reader.offset
      }
    },
    encode(value) {
      return api.toBuffer(value)
    },
    decode(buffer) {
      return api.fromBuffer(buffer)
    },
    isValid: (value): value is A => matchesNode(resolveNode(node), value),
    getSchema: () => JSON.stringify(schema)
  }
  return api
}

export const encode = <A>(schema: AvroSchema, value: A, options?: ParseOptions): Uint8Array =>
  parse<A>(schema, options).toBuffer(value)

export const decode = <A = unknown>(schema: AvroSchema, buffer: Uint8Array, options?: ParseOptions): A =>
  parse<A>(schema, options).fromBuffer(buffer)

export const decodePartial = <A = unknown>(
  schema: AvroSchema,
  buffer: Uint8Array,
  offset = 0,
  options?: ParseOptions
): DecodeResult<A> =>
  parse<A>(schema, options).decodePartial(buffer, offset)

export const encodeEffect = <A>(schema: AvroSchema, value: A, options?: ParseOptions) =>
  Effect.try({
    try: () => encode(schema, value, options),
    catch: (error) => avroError(`Unable to encode Avro value: ${message(error)}`, error)
  })

export const decodeEffect = <A = unknown>(schema: AvroSchema, buffer: Uint8Array, options?: ParseOptions) =>
  Effect.try({
    try: () => decode<A>(schema, buffer, options),
    catch: (error) => avroError(`Unable to decode Avro value: ${message(error)}`, error)
  })

const compile = (schema: AvroSchema, registry: Registry, namespace: string | undefined): Node => {
  if (typeof schema === "string") {
    if (primitiveNames.has(schema as AvroPrimitive)) {
      return primitive(schema as AvroPrimitive, schema)
    }
    return { _tag: "ref", schema, name: qualify(schema, namespace), registry }
  }
  if (Array.isArray(schema)) {
    return {
      _tag: "union",
      schema,
      branches: schema.map((branch) => compile(branch, registry, namespace))
    }
  }

  const objectSchema = schema as AvroObjectSchema
  const type = objectSchema.type
  if (typeof type !== "string") {
    return compile(type, registry, namespace)
  }
  if (primitiveNames.has(type as AvroPrimitive)) {
    return primitive(type as AvroPrimitive, objectSchema)
  }

  switch (type) {
    case "record":
    case "error": {
      const recordSchema = objectSchema as AvroRecordSchema
      const name = namedSchemaFullName(recordSchema.name, recordSchema.namespace ?? namespace)
      const existing = registry.nodes.get(name)
      if (existing !== undefined) {
        return existing
      }
      const record: Extract<Node, { readonly _tag: "record" }> = {
        _tag: "record",
        schema: recordSchema,
        name,
        fields: []
      }
      registerNamed(recordSchema, name, record, registry)
      record.fields = recordSchema.fields.map((field) => ({
        name: field.name,
        node: compile(field.type, registry, recordSchema.namespace ?? namespace),
        defaultValue: field.default,
        hasDefault: Object.hasOwn(field, "default")
      }))
      return record
    }
    case "enum": {
      const enumSchema = objectSchema as AvroEnumSchema
      const name = namedSchemaFullName(enumSchema.name, enumSchema.namespace ?? namespace)
      const existing = registry.nodes.get(name)
      if (existing !== undefined) {
        return existing
      }
      const node: Node = { _tag: "enum", schema: enumSchema, name, symbols: enumSchema.symbols }
      registerNamed(enumSchema, name, node, registry)
      return node
    }
    case "array": {
      const arraySchema = objectSchema as AvroArraySchema
      return { _tag: "array", schema: arraySchema, item: compile(arraySchema.items, registry, namespace) }
    }
    case "map": {
      const mapSchema = objectSchema as AvroMapSchema
      return { _tag: "map", schema: mapSchema, value: compile(mapSchema.values, registry, namespace) }
    }
    case "fixed": {
      const fixedSchema = objectSchema as AvroFixedSchema
      const name = namedSchemaFullName(fixedSchema.name, fixedSchema.namespace ?? namespace)
      const existing = registry.nodes.get(name)
      if (existing !== undefined) {
        return existing
      }
      const node: Node = { _tag: "fixed", schema: fixedSchema, name, size: fixedSchema.size }
      registerNamed(fixedSchema, name, node, registry)
      return node
    }
    default:
      return { _tag: "ref", schema: type, name: qualify(type, namespace), registry }
  }
}

const primitive = (type: AvroPrimitive, schema: AvroSchema): Node => {
  switch (type) {
    case "null":
      return { _tag: "null", schema }
    case "boolean":
      return { _tag: "boolean", schema }
    case "int":
      return { _tag: "int", schema }
    case "long":
      return { _tag: "long", schema }
    case "float":
      return { _tag: "float", schema }
    case "double":
      return { _tag: "double", schema }
    case "bytes":
      return { _tag: "bytes", schema }
    case "string":
      return { _tag: "string", schema }
  }
}

const registerNamed = (schema: AvroNamedSchema, name: string, node: Node, registry: Registry) => {
  registry.nodes.set(name, node)
  registry.nodes.set(unqualified(name), node)
  for (const alias of schema.aliases ?? []) {
    registry.aliases.set(alias, name)
    registry.aliases.set(unqualified(alias), name)
  }
}

const resolveNode = (node: Node): Node => {
  if (node._tag !== "ref") {
    return node
  }
  const alias = node.registry.aliases.get(node.name) ?? node.registry.aliases.get(unqualified(node.name))
  const resolved = node.registry.nodes.get(node.name) ??
    node.registry.nodes.get(unqualified(node.name)) ??
    (alias === undefined ? undefined : node.registry.nodes.get(alias))
  if (resolved === undefined) {
    throw avroError(`Unknown Avro type reference ${node.name}`)
  }
  return resolved
}

const readNode = (node: Node, reader: BinaryReader): unknown => {
  node = resolveNode(node)
  switch (node._tag) {
    case "null":
      return null
    case "boolean":
      return reader.readByte() === 1
    case "int":
      return reader.readLong()
    case "long":
      return reader.readLong()
    case "float":
      return reader.readFloat()
    case "double":
      return reader.readDouble()
    case "bytes":
      return reader.readBytes()
    case "string":
      return reader.readString()
    case "fixed":
      return reader.readFixed(node.size)
    case "enum": {
      const index = reader.readLong()
      const symbol = node.symbols[index]
      if (symbol === undefined) {
        throw avroError(`Invalid enum index ${index} for ${node.name}`)
      }
      return symbol
    }
    case "array": {
      const out: Array<unknown> = []
      readBlocks(reader, (count) => {
        for (let index = 0; index < count; index++) {
          out.push(readNode(node.item, reader))
        }
      })
      return out
    }
    case "map": {
      const out: Record<string, unknown> = {}
      readBlocks(reader, (count) => {
        for (let index = 0; index < count; index++) {
          out[reader.readString()] = readNode(node.value, reader)
        }
      })
      return out
    }
    case "record": {
      const out: Record<string, unknown> = {}
      for (const field of node.fields) {
        out[field.name] = readNode(field.node, reader)
      }
      if (reader.restoreTags && typeof node.schema["x-effect-tag"] === "string") {
        out._tag = node.schema["x-effect-tag"]
      }
      return out
    }
    case "union": {
      const index = reader.readLong()
      const branch = node.branches[index]
      if (branch === undefined) {
        throw avroError(`Invalid union branch index ${index}`)
      }
      return readNode(branch, reader)
    }
    case "ref":
      return readNode(resolveNode(node), reader)
  }
}

const writeNode = (node: Node, value: unknown, writer: BinaryWriter): void => {
  node = resolveNode(node)
  switch (node._tag) {
    case "null":
      if (value !== null) {
        throw expected(node, value)
      }
      return
    case "boolean":
      if (typeof value !== "boolean") {
        throw expected(node, value)
      }
      writer.writeByte(value ? 1 : 0)
      return
    case "int":
      if (!Number.isInteger(value)) {
        throw expected(node, value)
      }
      writer.writeLong(value as number)
      return
    case "long":
      if (typeof value !== "number" || !Number.isFinite(value)) {
        throw expected(node, value)
      }
      writer.writeLong(value)
      return
    case "float":
      if (typeof value !== "number") {
        throw expected(node, value)
      }
      writer.writeFloat(value)
      return
    case "double":
      if (typeof value !== "number") {
        throw expected(node, value)
      }
      writer.writeDouble(value)
      return
    case "bytes":
      writer.writeBytes(toBuffer(value, "bytes"))
      return
    case "fixed":
      writer.writeFixed(toBuffer(value, node.name), node.size)
      return
    case "string":
      if (typeof value !== "string") {
        throw expected(node, value)
      }
      writer.writeString(value)
      return
    case "enum": {
      if (typeof value !== "string") {
        throw expected(node, value)
      }
      const index = node.symbols.indexOf(value)
      if (index === -1) {
        throw expected(node, value)
      }
      writer.writeLong(index)
      return
    }
    case "array":
      if (!Array.isArray(value)) {
        throw expected(node, value)
      }
      if (value.length > 0) {
        writer.writeLong(value.length)
        for (const item of value) {
          writeNode(node.item, item, writer)
        }
      }
      writer.writeLong(0)
      return
    case "map": {
      if (!isRecordLike(value)) {
        throw expected(node, value)
      }
      const entries = Object.entries(value)
      if (entries.length > 0) {
        writer.writeLong(entries.length)
        for (const [key, item] of entries) {
          writer.writeString(key)
          writeNode(node.value, item, writer)
        }
      }
      writer.writeLong(0)
      return
    }
    case "record": {
      if (!isRecordLike(value)) {
        throw expected(node, value)
      }
      for (const field of node.fields) {
        const fieldValue = Object.hasOwn(value, field.name)
          ? value[field.name]
          : field.hasDefault
          ? field.defaultValue
          : undefined
        writeNode(field.node, fieldValue, writer)
      }
      return
    }
    case "union": {
      const index = node.branches.findIndex((branch) => matchesNode(branch, value))
      if (index === -1) {
        throw expected(node, value)
      }
      writer.writeLong(index)
      writeNode(node.branches[index], value, writer)
      return
    }
    case "ref":
      return writeNode(resolveNode(node), value, writer)
  }
}

const matchesNode = (node: Node, value: unknown): boolean => {
  node = resolveNode(node)
  switch (node._tag) {
    case "null":
      return value === null
    case "boolean":
      return typeof value === "boolean"
    case "int":
      return Number.isInteger(value)
    case "long":
    case "float":
    case "double":
      return typeof value === "number" && Number.isFinite(value)
    case "bytes":
      return value instanceof Uint8Array
    case "fixed":
      return value instanceof Uint8Array && value.byteLength === node.size
    case "string":
      return typeof value === "string"
    case "enum":
      return typeof value === "string" && node.symbols.includes(value)
    case "array":
      return Array.isArray(value)
    case "map":
      return isRecordLike(value)
    case "record":
      return isRecordLike(value) &&
        tagMatches(node, value) &&
        node.fields.every((field) => Object.hasOwn(value, field.name) || field.hasDefault)
    case "union":
      return node.branches.some((branch) => matchesNode(branch, value))
    case "ref":
      return matchesNode(resolveNode(node), value)
  }
}

const tagMatches = (node: Extract<Node, { readonly _tag: "record" }>, value: Record<string, unknown>) => {
  const tag = node.schema["x-effect-tag"]
  return typeof tag !== "string" || value._tag === tag
}

const readBlocks = (reader: BinaryReader, read: (count: number) => void) => {
  while (true) {
    const count = reader.readLong()
    if (count === 0) {
      return
    }
    if (count < 0) {
      const actualCount = Math.abs(count)
      reader.readLong() // block size in bytes; data is still consumed item-by-item
      read(actualCount)
    } else {
      read(count)
    }
  }
}

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

class BinaryWriter {
  private readonly chunks: Array<Uint8Array> = []

  writeByte(byte: number) {
    this.chunks.push(new Uint8Array([byte]))
  }

  writeLong(value: number) {
    if (!Number.isSafeInteger(value)) {
      throw avroError(`Avro long value is outside the JavaScript safe integer range: ${value}`)
    }
    let encoded = (BigInt(value) << 1n) ^ (BigInt(value) >> 63n)
    const bytes: Array<number> = []
    while ((encoded & ~0x7fn) !== 0n) {
      bytes.push(Number((encoded & 0x7fn) | 0x80n))
      encoded >>= 7n
    }
    bytes.push(Number(encoded))
    this.chunks.push(new Uint8Array(bytes))
  }

  writeFloat(value: number) {
    const buffer = new Uint8Array(4)
    new DataView(buffer.buffer).setFloat32(0, value, true)
    this.chunks.push(buffer)
  }

  writeDouble(value: number) {
    const buffer = new Uint8Array(8)
    new DataView(buffer.buffer).setFloat64(0, value, true)
    this.chunks.push(buffer)
  }

  writeBytes(value: Uint8Array) {
    this.writeLong(value.length)
    this.chunks.push(value)
  }

  writeFixed(value: Uint8Array, size: number) {
    if (value.length !== size) {
      throw avroError(`Expected fixed value of size ${size}, got ${value.length}`)
    }
    this.chunks.push(value)
  }

  writeString(value: string) {
    this.writeBytes(textEncoder.encode(value))
  }

  toUint8Array(): Uint8Array {
    return concatBytes(this.chunks)
  }
}

class BinaryReader {
  readonly buffer: Uint8Array
  offset: number

  constructor(buffer: Uint8Array, offset = 0, readonly restoreTags = false) {
    this.buffer = buffer
    this.offset = offset
  }

  get done(): boolean {
    return this.offset === this.buffer.length
  }

  readByte(): number {
    this.ensure(1)
    return this.buffer[this.offset++]
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
        throw avroError("Invalid Avro variable-length integer")
      }
    }
    const decoded = (value >> 1n) ^ -(value & 1n)
    const number = Number(decoded)
    if (!Number.isSafeInteger(number)) {
      throw avroError(`Decoded Avro long is outside the JavaScript safe integer range: ${decoded}`)
    }
    return number
  }

  readFloat(): number {
    this.ensure(4)
    const value = new DataView(this.buffer.buffer, this.buffer.byteOffset + this.offset, 4).getFloat32(0, true)
    this.offset += 4
    return value
  }

  readDouble(): number {
    this.ensure(8)
    const value = new DataView(this.buffer.buffer, this.buffer.byteOffset + this.offset, 8).getFloat64(0, true)
    this.offset += 8
    return value
  }

  readBytes(): Uint8Array {
    const length = this.readLong()
    if (length < 0) {
      throw avroError(`Invalid negative bytes length ${length}`)
    }
    return this.readFixed(length)
  }

  readFixed(size: number): Uint8Array {
    this.ensure(size)
    const value = this.buffer.subarray(this.offset, this.offset + size)
    this.offset += size
    return value
  }

  readString(): string {
    return textDecoder.decode(this.readBytes())
  }

  private ensure(bytes: number) {
    if (this.offset + bytes > this.buffer.length) {
      throw avroError("Truncated Avro buffer")
    }
  }
}

const toBuffer = (value: unknown, label: string): Uint8Array => {
  if (value instanceof Uint8Array) {
    return value
  }
  throw avroError(`Expected ${label} to be Uint8Array`)
}

const concatBytes = (chunks: ReadonlyArray<Uint8Array>): Uint8Array => {
  const size = chunks.reduce((sum, chunk) => sum + chunk.length, 0)
  const out = new Uint8Array(size)
  let offset = 0
  for (const chunk of chunks) {
    out.set(chunk, offset)
    offset += chunk.length
  }
  return out
}

const expected = (node: Node, value: unknown) =>
  avroError(`Expected Avro ${node._tag}, got ${JSON.stringify(value)}`)

const avroError = (message: string, cause?: unknown): AvroError =>
  cause === undefined ? new AvroError({ message }) : new AvroError({ message, cause })

const isRecordLike = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)

const message = (error: unknown): string => error instanceof Error ? error.message : String(error)

const qualify = (name: string, namespace: string | undefined): string =>
  name.includes(".") || namespace === undefined ? name : `${namespace}.${name}`

const namedSchemaFullName = (name: string, namespace: string | undefined): string => {
  if (name.includes(".")) {
    return name
  }
  return namespace === undefined ? name : `${namespace}.${name}`
}

const unqualified = (name: string): string => {
  const index = name.lastIndexOf(".")
  return index === -1 ? name : name.slice(index + 1)
}
