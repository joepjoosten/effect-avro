import { describe, expect, it } from "@effect/vitest"
import { Effect, Layer } from "effect"
import { makeClient, SchemaRegistry } from "@effect-avro/schema-registry"
import {
  avroDeserializer,
  avroSerializer,
  decodeValue,
  decodeMessageValue,
  KafkaAvro,
  KafkaAvroError,
  serializeRegistryValue,
  registryValueSerializer
} from "../src/index.js"

describe("@effect-avro/kafka", () => {
  const schema = {
    type: "record",
    name: "Event",
    fields: [{ name: "id", type: "long" }]
  } as const

  it("serializes plain Avro payloads", () => {
    const serialize = avroSerializer(schema)
    const deserialize = avroDeserializer(schema)

    expect(deserialize(serialize({ id: 1 }))).toEqual({ id: 1 })
  })

  it.effect("serializes and deserializes registry framed values", () =>
    Effect.gen(function*() {
      const requests: Array<{ readonly method: string; readonly path: string }> = []
      const client = makeClient({
        endpoint: "http://registry.test",
        fetch: async (input, init) => {
          const url = new URL(String(input))
          requests.push({ method: init?.method ?? "GET", path: url.pathname })
          if (url.pathname === "/subjects/events-value/versions") {
            return json({ id: 3, subject: "events-value", version: 1 })
          }
          if (url.pathname === "/schemas/ids/3") {
            return json({ schema: JSON.stringify(schema) })
          }
          return new Response("not found", { status: 404 })
        }
      })
      const serialize = registryValueSerializer(client, { topic: "events", schema })
      const payload = yield* serialize({ id: 2 })
      const decoded = yield* decodeMessageValue(client, {
        topic: "events",
        partition: 0,
        offset: "1",
        value: payload
      })

      expect(decoded).toEqual({ id: 2 })
      expect(requests).toEqual([
        { method: "POST", path: "/subjects/events-value/versions" }
      ])
    }))

  it.effect("exposes Kafka errors as tagged errors", () => {
    const client = makeClient({
      endpoint: "http://registry.test",
      fetch: async () => new Response("not found", { status: 404 })
    })

    return decodeMessageValue(client, { topic: "events", value: null }).pipe(
      Effect.catchTag("KafkaAvroError", (error: KafkaAvroError) => Effect.succeed(error.location)),
      Effect.catch(() => Effect.succeed(undefined)),
      Effect.map((location) => {
        expect(location).toBe("value")
      })
    )
  })

  it.effect("composes KafkaAvro with SchemaRegistry layers", () => {
    const requests: Array<{ readonly method: string; readonly path: string }> = []
    const fetch = async (input: string | URL, init?: RequestInit) => {
      const url = new URL(String(input))
      requests.push({ method: init?.method ?? "GET", path: url.pathname })
      if (url.pathname === "/subjects/events-value/versions") {
        return json({ id: 5, subject: "events-value", version: 1 })
      }
      if (url.pathname === "/schemas/ids/5") {
        return json({ schema: JSON.stringify(schema) })
      }
      return new Response("not found", { status: 404 })
    }

    const layer = KafkaAvro.layer.pipe(
      Layer.provide(SchemaRegistry.layer({ endpoint: "http://registry.test", fetch }))
    )

    return Effect.gen(function*() {
      const payload = yield* serializeRegistryValue({ topic: "events", schema }, { id: 4 })
      const decoded = yield* decodeValue({
        topic: "events",
        partition: 0,
        offset: "2",
        value: payload
      })

      expect(decoded).toEqual({ id: 4 })
      expect(requests).toEqual([
        { method: "POST", path: "/subjects/events-value/versions" }
      ])
    }).pipe(Effect.provide(layer))
  })
})

const json = (body: unknown) =>
  new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json" }
  })

it.effect("attaches message context while preserving the underlying decode error", () => Effect.gen(function*() {
  const cases = [
    { payload: new Uint8Array([1]), response: new Response("missing", { status: 404 }), tag: "InvalidRegistryFrame" },
    { payload: new Uint8Array([0, 0, 0, 0, 1]), response: new Response("missing", { status: 404 }), tag: "SchemaRegistryHttpError" },
    { payload: new Uint8Array([0, 0, 0, 0, 1, 2]), response: json({ schema: JSON.stringify("string") }), tag: "AvroError" }
  ]
  for (const entry of cases) {
    const client = makeClient({ endpoint: "http://test", fetch: async () => entry.response })
    const error = yield* decodeMessageValue(client, { topic: "t", partition: 2, offset: "3", value: entry.payload }).pipe(
      Effect.catchTag("KafkaAvroError", (error) => Effect.succeed(error))
    )
    expect(error).toMatchObject({ _tag: "KafkaAvroError", topic: "t", partition: 2, offset: "3", location: "value", cause: { _tag: entry.tag } })
  }
}))
