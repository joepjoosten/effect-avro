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

it("enforces integer ranges and handles floating point special values consistently", () => {
  for (const value of [-2147483648, 2147483647]) expect(decode("int", encode("int", value))).toBe(value)
  for (const value of [-2147483649, 2147483648]) {
    expect(parse("int").isValid(value)).toBe(false)
    expect(() => encode("int", value)).toThrow()
    expect(() => decode("int", encode("long", value))).toThrow()
  }
  expect(parse("long").isValid(1.5)).toBe(false)
  expect(parse("long").isValid(Number.MAX_SAFE_INTEGER + 1)).toBe(false)
  expect([...encode(["int", "long"], 2147483648)][0]).toBe(2)
  for (const value of [NaN, Infinity, -Infinity]) {
    expect(parse("double").isValid(value)).toBe(true)
    expect(decode(["null", "double"], encode(["null", "double"], value))).toBe(value)
  }
})

it("resolves dotted parent namespaces and qualified references without short-name collisions", () => {
  const schema = { type: "record", name: "a.Root", namespace: "ignored", fields: [
    { name: "local", type: { type: "record", name: "Item", aliases: ["Alias"], fields: [{ name: "x", type: "int" }] } },
    { name: "other", type: { type: "record", name: "b.Item", fields: [{ name: "x", type: "string" }] } },
    { name: "again", type: "a.Item" },
    { name: "alias", type: "Alias" }
  ] } as const
  const value = { local: { x: 1 }, other: { x: "other" }, again: { x: 2 }, alias: { x: 3 } }
  const bytes = new Uint8Array([2, 10, 111, 116, 104, 101, 114, 4, 6])
  expect(encode(schema, value)).toEqual(bytes)
  expect(decode(schema, bytes)).toEqual(value)
  expect(() => encode({ type: "record", name: "R", fields: [
    { name: "x", type: { type: "fixed", name: "b.F", size: 1 } },
    { name: "y", type: "a.F" }
  ] }, { x: new Uint8Array(1), y: new Uint8Array(1) })).toThrow("Unknown Avro type reference a.F")
})

it("preserves special keys as own data properties without changing prototypes", () => {
  const value = JSON.parse('{"__proto__":{"admin":true},"constructor":{"admin":false},"prototype":{"admin":true}}')
  const schemas = [
    { type: "map", values: { type: "map", values: "boolean" } },
    { type: "record", name: "Special", fields: Object.keys(value).map((name) => ({ name, type: { type: "map", values: "boolean" } } as const)) }
  ] as const
  for (const schema of schemas) {
    const result = decode<Record<string, unknown>>(schema, encode(schema, value))
    expect(Object.getPrototypeOf(result)).toBe(Object.prototype)
    expect(Object.hasOwn(result, "__proto__")).toBe(true)
    expect(result.admin).toBeUndefined()
    expect(JSON.stringify(result)).toBe(JSON.stringify(value))
  }
})
