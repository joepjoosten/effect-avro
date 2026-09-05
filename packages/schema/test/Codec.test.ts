import { describe, expect, it } from "@effect/vitest"
import { Effect, Schema } from "effect"
import { avro, Bytes, fromAvroSchema, Long, toAvroSchema } from "../src/index.js"

class User extends Schema.Class<User>("User")({
  id: Long,
  name: Schema.String,
  email: Schema.String
}) {}

class Post extends Schema.TaggedClass<Post>()("Post", {
  id: Long,
  author: User,
  tags: Schema.Array(Schema.String),
  metadata: Schema.Record(Schema.String, Schema.String)
}) {}

describe("@effect-avro/schema", () => {
  it.effect("round-trips Effect classes through Avro binary", () =>
    Effect.gen(function*() {
      const PostAvro = avro(Post)
      const encode = Schema.encodeSync(PostAvro)
      const decode = Schema.decodeUnknownSync(PostAvro)
      const post = new Post({
        id: 1,
        author: new User({ id: 2, name: "Ada", email: "ada@example.com" }),
        tags: ["effect", "avro"],
        metadata: { source: "test" }
      })

      const buffer = encode(post)
      const decoded = decode(buffer)

      expect(buffer).toBeInstanceOf(Uint8Array)
      expect(decoded).toEqual(post)
    }))

  it.effect("round-trips tagged unions through native Avro unions", () =>
    Effect.gen(function*() {
      class Deleted extends Schema.TaggedClass<Deleted>()("Deleted", {
        id: Long
      }) {}
      const Event = Schema.Union([Post, Deleted])
      const EventAvro = avro(Event)
      const encode = Schema.encodeSync(EventAvro)
      const decode = Schema.decodeUnknownSync(EventAvro)
      const deleted = new Deleted({ id: 42 })

      expect(decode(encode(deleted))).toEqual(deleted)
    }))

  it("compiles schema metadata to Avro JSON", () => {
    const schema = toAvroSchema(Post)

    expect(schema).toMatchObject({
      type: "record",
      name: "Post",
      fields: [
        { name: "id", type: "long" },
        { name: "author" },
        { name: "tags", type: { type: "array", items: "string" } },
        { name: "metadata", type: { type: "map", values: "string" } }
      ],
      "x-effect-tag": "Post"
    })
  })

  it.effect("imports Avro JSON schemas into Effect schemas", () =>
    Effect.gen(function*() {
      const Imported = fromAvroSchema({
        type: "record",
        name: "Blob",
        fields: [
          { name: "id", type: "long" },
          { name: "payload", type: "bytes" },
          { name: "kind", type: { type: "enum", name: "Kind", symbols: ["A", "B"] } }
        ]
      })

      const BlobAvro = avro(Imported, { name: "Blob" })
      const encode = Schema.encodeSync(BlobAvro)
      const decode = Schema.decodeUnknownSync(BlobAvro)
      const value = { id: 1, payload: new Uint8Array([1, 2, 3]), kind: "A" }

      expect(decode(encode(value))).toEqual(value)
    }))

  it.effect("supports recursive Avro schemas imported to Effect", () =>
    Effect.gen(function*() {
      const Tree = fromAvroSchema({
        type: "record",
        name: "Tree",
        fields: [
          { name: "value", type: "string" },
          { name: "children", type: { type: "array", items: "Tree" } }
        ]
      })
      const TreeAvro = avro(Tree)
      const encode = Schema.encodeSync(TreeAvro)
      const decode = Schema.decodeUnknownSync(TreeAvro)
      const value = { value: "root", children: [{ value: "leaf", children: [] }] }

      expect(decode(encode(value))).toEqual(value)
    }))

  it.effect("encodes bytes from Uint8Array-compatible values", () =>
    Effect.gen(function*() {
      const Payload = Schema.Struct({
        data: Bytes
      }).annotate({ identifier: "Payload" })
      const PayloadAvro = avro(Payload)
      const encode = Schema.encodeSync(PayloadAvro)
      const decode = Schema.decodeUnknownSync(PayloadAvro)
      const value = { data: new Uint8Array([1, 2, 3]) }

      expect([...decode(encode(value)).data]).toEqual([1, 2, 3])
    }))
})

 it("preserves identical tagged union branches, including nested arrays", () => {
  const Event = Schema.Union([
    Schema.TaggedStruct("A", { id: Long }),
    Schema.TaggedStruct("B", { id: Long })
  ])
  for (const source of [Event, Schema.Array(Event)]) {
    const codec = avro(source)
    const value = source === Event ? { _tag: "B", id: 1 } : [{ _tag: "B", id: 1 }, { _tag: "A", id: 2 }]
    expect(Schema.decodeUnknownSync(codec)(Schema.encodeSync(codec)(value as never))).toEqual(value)
  }
})

it("normalizes missing optional fields through nested unions", () => {
  const Event = Schema.Union([
    Schema.TaggedStruct("Optional", { x: Schema.optionalKey(Schema.String) }),
    Schema.TaggedStruct("Other", { id: Long })
  ])
  const codec = avro(Schema.Struct({ events: Schema.Array(Event) }))
  const value = { events: [{ _tag: "Optional" as const }, { _tag: "Optional" as const, x: "ok" }] }
  expect(Schema.decodeUnknownSync(codec)(Schema.encodeSync(codec)(value))).toEqual(value)
  const nullable = avro(Schema.Struct({ x: Schema.optionalKey(Schema.NullOr(Schema.String)) }))
  expect(Schema.decodeUnknownSync(nullable)(Schema.encodeSync(nullable)({ x: null }))).toEqual({ x: null })
  const optional = avro(Schema.Struct({ x: Schema.optional(Schema.String) }))
  expect(Schema.decodeUnknownSync(optional)(Schema.encodeSync(optional)({ x: undefined }))).toEqual({})
})

it("keeps same-named records in separate namespaces through the schema adapter", () => {
  const schema = { type: "record", name: "a.Root", fields: [
    { name: "first", type: { type: "record", name: "Item", fields: [{ name: "x", type: "int" }] } },
    { name: "other", type: { type: "record", name: "b.Item", fields: [{ name: "x", type: "string" }] } },
    { name: "again", type: "Item" }
  ] } as const
  const codec = avro(fromAvroSchema(schema), { avroSchema: schema })
  const value = { first: { x: 1 }, other: { x: "other" }, again: { x: 2 } }
  expect(Schema.decodeUnknownSync(codec)(Schema.encodeSync(codec)(value))).toEqual(value)
})

it("preserves special record keys through imported codecs", () => {
  const schema = { type: "record", name: "Special", fields: [{ name: "__proto__", type: "string" }] } as const
  const codec = avro(fromAvroSchema(schema), { avroSchema: schema })
  const value = JSON.parse('{"__proto__":"data"}')
  const result = Schema.decodeUnknownSync(codec)(Schema.encodeSync(codec)(value))
  expect(Object.hasOwn(result as object, "__proto__")).toBe(true)
  expect(JSON.stringify(result)).toBe(JSON.stringify(value))
})
