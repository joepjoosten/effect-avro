import type { AvroSchema } from "@effect-avro/core"
import type { FetchLike } from "@effect-avro/schema-registry"
import { Schema } from "effect"

export const StoredSchema = Schema.Struct({
  id: Schema.Number,
  subject: Schema.String,
  version: Schema.Number,
  schema: Schema.Defect(),
  schemaText: Schema.String
})
export type StoredSchema = Omit<typeof StoredSchema.Type, "schema"> & {
  readonly id: number
  readonly subject: string
  readonly version: number
  readonly schema: AvroSchema
  readonly schemaText: string
}

export type InMemorySchemaRegistry = {
  readonly fetch: FetchLike
  readonly byId: ReadonlyMap<number, StoredSchema>
}

export const makeInMemorySchemaRegistry = (): InMemorySchemaRegistry => {
  let nextId = 1
  const byId = new Map<number, StoredSchema>()
  const bySubject = new Map<string, Array<StoredSchema>>()

  const fetch: FetchLike = async (input, init) => {
    const url = new URL(String(input))
    const method = init?.method ?? "GET"
    const segments = url.pathname.split("/").filter(Boolean).map(decodeURIComponent)

    if (method === "POST" && segments[0] === "subjects" && segments[2] === "versions") {
      const subject = segments[1]
      const request = parseRequest(init?.body)
      const registered = register(subject, request.schema)
      return json({
        id: registered.id,
        subject,
        version: registered.version,
        schema: registered.schemaText,
        schemaType: "AVRO"
      })
    }

    if (method === "POST" && segments[0] === "subjects" && segments.length === 2) {
      const subject = segments[1]
      const request = parseRequest(init?.body)
      const existing = findBySubjectAndSchema(subject, request.schema)
      return existing === undefined
        ? json({ message: "schema not found" }, 404)
        : json({
          id: existing.id,
          subject,
          version: existing.version,
          schema: existing.schemaText,
          schemaType: "AVRO"
        })
    }

    if (method === "GET" && segments[0] === "schemas" && segments[1] === "ids") {
      const schema = byId.get(Number(segments[2]))
      return schema === undefined
        ? json({ message: "schema id not found" }, 404)
        : json({ schema: schema.schemaText, schemaType: "AVRO" })
    }

    if (method === "GET" && segments[0] === "subjects" && segments[2] === "versions") {
      const subject = segments[1]
      const versions = bySubject.get(subject) ?? []
      const version = segments[3] === "latest"
        ? versions[versions.length - 1]
        : versions.find((schema) => schema.version === Number(segments[3]))
      return version === undefined
        ? json({ message: "subject version not found" }, 404)
        : json({
          id: version.id,
          subject,
          version: version.version,
          schema: version.schemaText,
          schemaType: "AVRO"
        })
    }

    if (method === "POST" && segments[0] === "compatibility") {
      return json({ is_compatible: true })
    }

    return json({ message: "not found" }, 404)
  }

  const register = (subject: string, schemaText: string): StoredSchema => {
    const existing = findBySubjectAndSchema(subject, schemaText)
    if (existing !== undefined) {
      return existing
    }

    const schemas = bySubject.get(subject) ?? []
    const registered: StoredSchema = {
      id: nextId++,
      subject,
      version: schemas.length + 1,
      schema: JSON.parse(schemaText) as AvroSchema,
      schemaText
    }
    schemas.push(registered)
    bySubject.set(subject, schemas)
    byId.set(registered.id, registered)
    return registered
  }

  const findBySubjectAndSchema = (subject: string, schemaText: string): StoredSchema | undefined =>
    bySubject.get(subject)?.find((schema) => schema.schemaText === schemaText)

  return { fetch, byId }
}

const parseRequest = (body: BodyInit | null | undefined): { readonly schema: string } => {
  if (typeof body !== "string") {
    throw new Error("Example registry expects JSON string request bodies")
  }
  return JSON.parse(body) as { readonly schema: string }
}

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" }
  })
