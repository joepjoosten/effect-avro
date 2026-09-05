import { describe, expect, it } from "@effect/vitest"
import { Effect } from "effect"
import { decode, encode, encodeEffect, parse } from "../src/index.js"

describe("@effect-avro/core", () => {
  it("encodes and decodes primitives", () => {
    expect(decode("null", encode("null", null))).toEqual(null)
    expect(decode("boolean", encode("boolean", true))).toEqual(true)
    expect(decode("int", encode("int", -12))).toEqual(-12)
    expect(decode("long", encode("long", 123456))).toEqual(123456)
    expect(decode("float", encode("float", 1.5))).toEqual(1.5)
    expect(decode("double", encode("double", Math.PI))).toEqual(Math.PI)
    expect(decode("string", encode("string", "hello"))).toEqual("hello")
    expect([...decode<Uint8Array>("bytes", encode("bytes", new Uint8Array([1, 2, 3])))])
      .toEqual([1, 2, 3])
  })

  it("encodes and decodes records, arrays, maps, enums, fixed and unions", () => {
    const schema = {
      type: "record",
      name: "Envelope",
      fields: [
        { name: "id", type: "long" },
        { name: "kind", type: { type: "enum", name: "Kind", symbols: ["Created", "Deleted"] } },
        { name: "hash", type: { type: "fixed", name: "Hash", size: 3 } },
        { name: "tags", type: { type: "array", items: "string" } },
        { name: "metadata", type: { type: "map", values: "string" } },
        { name: "optional", type: ["null", "string"], default: null }
      ]
    } as const
    const type = parse(schema)
    const value = {
      id: 1,
      kind: "Created",
      hash: new Uint8Array([1, 2, 3]),
      tags: ["a", "b"],
      metadata: { x: "y" },
      optional: "ok"
    }

    expect(type.fromBuffer(type.toBuffer(value))).toEqual(value)
  })

  it("supports recursive named references", () => {
    const schema = {
      type: "record",
      name: "Tree",
      fields: [
        { name: "value", type: "string" },
        { name: "children", type: { type: "array", items: "Tree" } }
      ]
    } as const
    const value = { value: "root", children: [{ value: "leaf", children: [] }] }

    expect(decode(schema, encode(schema, value))).toEqual(value)
  })

  it("decodes values from a buffer prefix", () => {
    const first = encode("string", "a")
    const second = encode("string", "b")
    const type = parse<string>("string")
    const combined = new Uint8Array(first.length + second.length)
    combined.set(first, 0)
    combined.set(second, first.length)
    const decoded = type.decodePartial(combined)

    expect(decoded).toEqual({ value: "a", offset: first.length })
  })

  it.effect("exposes Avro errors as tagged errors", () =>
    encodeEffect("string", 1).pipe(
      Effect.catchTag("AvroError", (error) => Effect.succeed(error._tag)),
      Effect.map((tag) => {
        expect(tag).toBe("AvroError")
      })
    ))
})

it("validates nested data before choosing record union branches", () => {
  const type = parse([
    { type: "record", name: "Numbers", fields: [{ name: "v", type: { type: "array", items: "int" } }] },
    { type: "record", name: "Strings", fields: [{ name: "v", type: { type: "array", items: "string" } }] }
  ])
  expect(type.fromBuffer(type.toBuffer({ v: ["ok"] }))).toEqual({ v: ["ok"] })
  expect(type.isValid({ v: [false] })).toBe(false)
  expect(parse({ type: "map", values: "int" }).isValid({ x: "bad" })).toBe(false)
  const tree = parse({ type: "record", name: "Tree", fields: [{ name: "children", type: { type: "array", items: "Tree" } }] })
  const cyclic: { children: unknown[] } = { children: [] }
  cyclic.children.push(cyclic)
  expect(tree.isValid(cyclic)).toBe(false)
  const leaf = { children: [] }
  expect(tree.isValid({ children: [leaf, leaf] })).toBe(true)
})
