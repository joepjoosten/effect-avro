import { describe, expect, it } from "@effect/vitest"
import { Effect } from "effect"
import {
  decode,
  decodeConfluentFrame,
  decodeWithRegistry,
  encode,
  encodeConfluentFrame,
  encodeWithRegistry,
  InvalidRegistryFrame,
  makeClient,
  SchemaRegistry,
  subjectName
} from "../src/index.js"

describe("@effect-avro/schema-registry", () => {
  const schema = {
    type: "record",
    name: "Event",
    namespace: "example",
    fields: [{ name: "id", type: "long" }]
  } as const

  it("encodes and decodes Confluent frames", () => {
    const frame = encodeConfluentFrame(258, new Uint8Array([1, 2, 3]))

    expect([...frame.subarray(0, 5)]).toEqual([0, 0, 0, 1, 2])
    expect(decodeConfluentFrame(frame)).toEqual({
      schemaId: 258,
      payload: new Uint8Array([1, 2, 3])
    })
  })

  it("builds subject names", () => {
    expect(subjectName("TopicNameStrategy", { topic: "events", schema })).toBe("events-value")
    expect(subjectName("TopicNameStrategy", { topic: "events", isKey: true, schema })).toBe("events-key")
    expect(subjectName("RecordNameStrategy", { topic: "events", schema })).toBe("example.Event")
    expect(subjectName("TopicRecordNameStrategy", { topic: "events", schema })).toBe("events-example.Event")
  })

  it.effect("exposes frame errors as tagged errors", () =>
    Effect.try({
      try: () => decodeConfluentFrame(new Uint8Array()),
      catch: (error) => error as InvalidRegistryFrame
    }).pipe(
      Effect.catchTag("InvalidRegistryFrame", (error) => Effect.succeed(error._tag)),
      Effect.map((tag) => {
        expect(tag).toBe("InvalidRegistryFrame")
      })
    ))

  it.effect("registers schemas and decodes by schema id", () =>
    Effect.gen(function*() {
      const requests: Array<{ readonly method: string; readonly path: string }> = []
      const fetch = async (input: string | URL, init?: RequestInit) => {
        const url = new URL(String(input))
        requests.push({ method: init?.method ?? "GET", path: url.pathname })
        if (url.pathname === "/subjects/events-value/versions") {
          return json({ id: 7, subject: "events-value", version: 1 })
        }
        if (url.pathname === "/schemas/ids/7") {
          return json({ schema: JSON.stringify(schema) })
        }
        return new Response("not found", { status: 404 })
      }
      const client = makeClient({ endpoint: "http://registry.test", fetch })

      const encoded = yield* encodeWithRegistry(client, {
        subject: "events-value",
        schema,
        value: { id: 42 }
      })
      const decoded = yield* decodeWithRegistry(client, encoded)

      expect(decoded).toEqual({ id: 42 })
      expect(requests).toEqual([
        { method: "POST", path: "/subjects/events-value/versions" }
      ])
    }))

  it.effect("uses subject/schema cache for repeated registration", () =>
    Effect.gen(function*() {
      let calls = 0
      const client = makeClient({
        endpoint: "http://registry.test",
        fetch: async () => {
          calls += 1
          return json({ id: 9, subject: "events-value", version: 1 })
        }
      })

      yield* client.register({ subject: "events-value", schema })
      yield* client.register({ subject: "events-value", schema })

      expect(calls).toBe(1)
    }))

  it.effect("exposes the registry through a service layer", () => {
    const requests: Array<{ readonly method: string; readonly path: string }> = []
    const fetch = async (input: string | URL, init?: RequestInit) => {
      const url = new URL(String(input))
      requests.push({ method: init?.method ?? "GET", path: url.pathname })
      if (url.pathname === "/subjects/events-value/versions") {
        return json({ id: 11, subject: "events-value", version: 1 })
      }
      if (url.pathname === "/schemas/ids/11") {
        return json({ schema: JSON.stringify(schema) })
      }
      return new Response("not found", { status: 404 })
    }

    return Effect.gen(function*() {
      const encoded = yield* encode({
        subject: "events-value",
        schema,
        value: { id: 42 }
      })
      const decoded = yield* decode(encoded)

      expect(decoded).toEqual({ id: 42 })
      expect(requests).toEqual([
        { method: "POST", path: "/subjects/events-value/versions" }
      ])
    }).pipe(
      Effect.provide(SchemaRegistry.layer({ endpoint: "http://registry.test", fetch }))
    )
  })
})

const json = (body: unknown) =>
  new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json" }
  })

it.effect("reports invalid registry responses through the typed error channel", () => Effect.gen(function*() {
  for (const body of [
    { schema: "invalid" }, {}, { schema: "42" },
    { schema: JSON.stringify("string"), schemaType: "JSON" },
    { schema: JSON.stringify("string"), id: -1 },
    { schema: JSON.stringify("string"), id: 1.5 }
  ]) {
    const client = makeClient({ endpoint: "http://test", fetch: async () => json(body) })
    const result = yield* client.getById(1).pipe(Effect.catchTag("SchemaRegistryError", () => Effect.succeed("caught")))
    expect(result).toBe("caught")
  }
  const missingId = makeClient({ endpoint: "http://test", fetch: async () => json({}) })
  expect(yield* missingId.register({ subject: "s", schema: "string" }).pipe(
    Effect.catchTag("SchemaRegistryError", () => Effect.succeed("caught"))
  )).toBe("caught")
  const valid = makeClient({ endpoint: "http://test", fetch: async () => json({ schema: JSON.stringify("string") }) })
  expect((yield* valid.getById(1)).schemaType).toBe("AVRO")
}))

it.effect("keys registration caches by normalized references and reads caches lazily", () => Effect.gen(function*() {
  let calls = 0
  const client = makeClient({ endpoint: "http://test", fetch: async () => json({ id: ++calls }) })
  const base = { subject: "s", schema: "E" }
  const first = client.register({ ...base, references: [{ name: "E", subject: "e", version: 1 }] })
  expect((yield* first).id).toBe(1)
  expect((yield* first).id).toBe(1)
  expect((yield* client.getId({ ...base, references: [{ name: "E", subject: "e", version: 2 }] })).id).toBe(2)
  expect((yield* client.register({ ...base, references: [{ name: "E", subject: "other", version: 2 }] })).id).toBe(3)
  const references = [{ name: "E", subject: "e", version: 1 }, { name: "F", subject: "f", version: 1 }]
  const registered = yield* client.register({ ...base, references })
  expect((yield* client.getId({ ...base, references: [...references].reverse() })).id).toBe(registered.id)
  expect(calls).toBe(4)
}))
