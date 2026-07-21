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
