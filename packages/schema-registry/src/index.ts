import * as Avro from "@effect-avro/core"
import { Context, Effect, Layer, Schema } from "effect"

export const SchemaReference = Schema.Struct({
  name: Schema.String,
  subject: Schema.String,
  version: Schema.Number
})
export type SchemaReference = typeof SchemaReference.Type

export const RegisterSchemaRequest = Schema.Struct({
  subject: Schema.String,
  schema: Avro.AvroSchema,
  schemaType: Schema.optionalKey(Schema.Literal("AVRO")),
  references: Schema.optionalKey(Schema.Array(SchemaReference))
})
export type RegisterSchemaRequest = typeof RegisterSchemaRequest.Type

export const RegisteredSchema = Schema.Struct({
  id: Schema.Number,
  subject: Schema.optionalKey(Schema.String),
  version: Schema.optionalKey(Schema.Number),
  schema: Avro.AvroSchema,
  schemaType: Schema.Literal("AVRO"),
  references: Schema.Array(SchemaReference)
})
export type RegisteredSchema = typeof RegisteredSchema.Type

export const CompatibilityResult = Schema.Struct({
  isCompatible: Schema.Boolean
})
export type CompatibilityResult = typeof CompatibilityResult.Type

export const SchemaRegistryAuth = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("None") }),
  Schema.Struct({
    _tag: Schema.Literal("Basic"),
    username: Schema.String,
    password: Schema.String
  }),
  Schema.Struct({
    _tag: Schema.Literal("Bearer"),
    token: Schema.String
  })
])
export type SchemaRegistryAuth = typeof SchemaRegistryAuth.Type

export type FetchLike = (input: string | URL, init?: RequestInit) => Promise<Response>

const SchemaRegistryClientOptionsBase = Schema.Struct({
  endpoint: Schema.String,
  auth: Schema.optionalKey(SchemaRegistryAuth),
  headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  fetch: Schema.optionalKey(Schema.instanceOf(Function)),
  cache: Schema.optionalKey(Schema.Boolean)
})
export const SchemaRegistryClientOptions = SchemaRegistryClientOptionsBase
export type SchemaRegistryClientOptions =
  Omit<typeof SchemaRegistryClientOptionsBase.Type, "fetch"> & {
    readonly fetch?: FetchLike
  }

export interface SchemaRegistryClient {
  readonly register: (request: RegisterSchemaRequest) => Effect.Effect<RegisteredSchema, SchemaRegistryClientError>
  readonly getId: (request: RegisterSchemaRequest) => Effect.Effect<RegisteredSchema, SchemaRegistryClientError>
  readonly getById: (id: number) => Effect.Effect<RegisteredSchema, SchemaRegistryClientError>
  readonly getVersion: (subject: string, version: number | "latest") => Effect.Effect<RegisteredSchema, SchemaRegistryClientError>
  readonly getLatest: (subject: string) => Effect.Effect<RegisteredSchema, SchemaRegistryClientError>
  readonly checkCompatibility: (
    request: RegisterSchemaRequest & { readonly version?: number | "latest" }
  ) => Effect.Effect<CompatibilityResult, SchemaRegistryClientError>
}

export class SchemaRegistry extends Context.Service<SchemaRegistry, SchemaRegistryClient>()(
  "@effect-avro/schema-registry/SchemaRegistry"
) {
  static readonly layer = (options: SchemaRegistryClientOptions): Layer.Layer<SchemaRegistry, SchemaRegistryClientError> =>
    Layer.effect(
      SchemaRegistry,
      Effect.try({
        try: () => SchemaRegistry.of(makeClient(options)),
        catch: (error) => isSchemaRegistryClientError(error)
          ? error
          : schemaRegistryError(message(error), error)
      })
    )
}

export class SchemaRegistryError extends Schema.TaggedErrorClass<SchemaRegistryError>()("SchemaRegistryError", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect())
}) {}

export class SchemaRegistryHttpError extends Schema.TaggedErrorClass<SchemaRegistryHttpError>()(
  "SchemaRegistryHttpError",
  {
    message: Schema.String,
    status: Schema.Number,
    body: Schema.String
  }
) {}

export class InvalidRegistryFrame extends Schema.TaggedErrorClass<InvalidRegistryFrame>()("InvalidRegistryFrame", {
  message: Schema.String
}) {}

export type SchemaRegistryClientError = SchemaRegistryError | SchemaRegistryHttpError | InvalidRegistryFrame

export const ConfluentFrame = Schema.Struct({
  schemaId: Schema.Number,
  payload: Schema.Uint8Array
})
export type ConfluentFrame = typeof ConfluentFrame.Type

export const SubjectNameStrategy = Schema.Literals([
  "TopicNameStrategy",
  "RecordNameStrategy",
  "TopicRecordNameStrategy"
])
export type SubjectNameStrategy = typeof SubjectNameStrategy.Type

export const SubjectNameInput = Schema.Struct({
  topic: Schema.String,
  isKey: Schema.optionalKey(Schema.Boolean),
  schema: Avro.AvroSchema
})
export type SubjectNameInput = typeof SubjectNameInput.Type

const magicByte = 0

export const encodeConfluentFrame = (schemaId: number, payload: Uint8Array): Uint8Array => {
  if (!Number.isInteger(schemaId) || schemaId < 0 || schemaId > 0xffffffff) {
    throw invalidRegistryFrame(`Schema id must be an unsigned 32-bit integer, got ${schemaId}`)
  }
  const frame = new Uint8Array(5 + payload.length)
  const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength)
  view.setUint8(0, magicByte)
  view.setUint32(1, schemaId, false)
  frame.set(payload, 5)
  return frame
}

export const decodeConfluentFrame = (input: Uint8Array): ConfluentFrame => {
  const buffer = input
  if (buffer.length < 5) {
    throw invalidRegistryFrame(`Confluent frame must contain at least 5 bytes, got ${buffer.length}`)
  }
  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength)
  const magic = view.getUint8(0)
  if (magic !== magicByte) {
    throw invalidRegistryFrame(`Invalid Confluent magic byte ${magic}`)
  }
  return {
    schemaId: view.getUint32(1, false),
    payload: buffer.subarray(5)
  }
}

export const subjectName = (strategy: SubjectNameStrategy, input: SubjectNameInput): string => {
  switch (strategy) {
    case "TopicNameStrategy":
      return `${input.topic}-${input.isKey === true ? "key" : "value"}`
    case "RecordNameStrategy":
      return schemaFullName(input.schema)
    case "TopicRecordNameStrategy":
      return `${input.topic}-${schemaFullName(input.schema)}`
  }
}

export const makeClient = (options: SchemaRegistryClientOptions): SchemaRegistryClient => {
  const endpoint = options.endpoint.replace(/\/+$/, "")
  const fetchImpl = options.fetch ?? globalThis.fetch
  const useCache = options.cache ?? true
  const byId = new Map<number, RegisteredSchema>()
  const bySubjectSchema = new Map<string, RegisteredSchema>()

  if (fetchImpl === undefined) {
    throw schemaRegistryError("Schema Registry client requires a fetch implementation")
  }

  const request = <A>(method: string, path: string, body?: unknown) =>
    Effect.tryPromise({
      try: async () => {
        const init: RequestInit = {
          method,
          headers: makeHeaders(options, body !== undefined),
          ...(body === undefined ? {} : { body: JSON.stringify(body) })
        }
        const response = await fetchImpl(`${endpoint}${path}`, init)
        const text = await response.text()
        if (!response.ok) {
          throw schemaRegistryHttpError(response.status, text)
        }
        return (text.length === 0 ? {} : JSON.parse(text)) as A
      },
      catch: (error) => isSchemaRegistryClientError(error) ? error : schemaRegistryError(message(error), error)
    })

  const register = (requestBody: RegisterSchemaRequest): Effect.Effect<RegisteredSchema, SchemaRegistryClientError> => {
    const key = subjectSchemaCacheKey(requestBody.subject, requestBody.schema)
    const cached = bySubjectSchema.get(key)
    if (useCache && cached !== undefined) {
      return Effect.succeed(cached)
    }
    return Effect.gen(function*() {
      const response = yield* request<RegistryResponse>(
        "POST",
        `/subjects/${encodeURIComponent(requestBody.subject)}/versions`,
        registryRequestBody(requestBody)
      )
      const registered = normalizeRegisteredSchema(response, requestBody)
      cacheSchema(registered, byId, bySubjectSchema, useCache)
      return registered
    })
  }

  const getId = (requestBody: RegisterSchemaRequest): Effect.Effect<RegisteredSchema, SchemaRegistryClientError> => {
    const key = subjectSchemaCacheKey(requestBody.subject, requestBody.schema)
    const cached = bySubjectSchema.get(key)
    if (useCache && cached !== undefined) {
      return Effect.succeed(cached)
    }
    return Effect.gen(function*() {
      const response = yield* request<RegistryResponse>(
        "POST",
        `/subjects/${encodeURIComponent(requestBody.subject)}`,
        registryRequestBody(requestBody)
      )
      const registered = normalizeRegisteredSchema(response, requestBody)
      cacheSchema(registered, byId, bySubjectSchema, useCache)
      return registered
    })
  }

  const getById = (id: number): Effect.Effect<RegisteredSchema, SchemaRegistryClientError> => {
    const cached = byId.get(id)
    if (useCache && cached !== undefined) {
      return Effect.succeed(cached)
    }
    return Effect.gen(function*() {
      const response = yield* request<RegistryResponse>("GET", `/schemas/ids/${id}`)
      const registered = normalizeRegisteredSchema(response, { subject: "", schema: parseSchema(response.schema), schemaType: "AVRO" }, id)
      cacheSchema(registered, byId, bySubjectSchema, useCache)
      return registered
    })
  }

  const getVersion = (
    subject: string,
    version: number | "latest"
  ): Effect.Effect<RegisteredSchema, SchemaRegistryClientError> =>
    Effect.gen(function*() {
      const response = yield* request<RegistryResponse>(
        "GET",
        `/subjects/${encodeURIComponent(subject)}/versions/${version}`
      )
      const schema = parseSchema(response.schema)
      const registered = normalizeRegisteredSchema(response, { subject, schema, schemaType: "AVRO" })
      cacheSchema(registered, byId, bySubjectSchema, useCache)
      return registered
    })

  const checkCompatibility = (
    requestBody: RegisterSchemaRequest & { readonly version?: number | "latest" }
  ): Effect.Effect<CompatibilityResult, SchemaRegistryClientError> =>
    Effect.gen(function*() {
      const version = requestBody.version ?? "latest"
      const response = yield* request<{ readonly is_compatible?: boolean; readonly isCompatible?: boolean }>(
        "POST",
        `/compatibility/subjects/${encodeURIComponent(requestBody.subject)}/versions/${version}`,
        registryRequestBody(requestBody)
      )
      return { isCompatible: response.isCompatible ?? response.is_compatible ?? false }
    })

  return {
    register,
    getId,
    getById,
    getVersion,
    getLatest: (subject) => getVersion(subject, "latest"),
    checkCompatibility
  }
}

export const register = (
  request: RegisterSchemaRequest
): Effect.Effect<RegisteredSchema, SchemaRegistryClientError, SchemaRegistry> =>
  SchemaRegistry.use((registry) => registry.register(request))

export const getId = (
  request: RegisterSchemaRequest
): Effect.Effect<RegisteredSchema, SchemaRegistryClientError, SchemaRegistry> =>
  SchemaRegistry.use((registry) => registry.getId(request))

export const getById = (
  id: number
): Effect.Effect<RegisteredSchema, SchemaRegistryClientError, SchemaRegistry> =>
  SchemaRegistry.use((registry) => registry.getById(id))

export const getVersion = (
  subject: string,
  version: number | "latest"
): Effect.Effect<RegisteredSchema, SchemaRegistryClientError, SchemaRegistry> =>
  SchemaRegistry.use((registry) => registry.getVersion(subject, version))

export const getLatest = (
  subject: string
): Effect.Effect<RegisteredSchema, SchemaRegistryClientError, SchemaRegistry> =>
  SchemaRegistry.use((registry) => registry.getLatest(subject))

export const checkCompatibility = (
  request: RegisterSchemaRequest & { readonly version?: number | "latest" }
): Effect.Effect<CompatibilityResult, SchemaRegistryClientError, SchemaRegistry> =>
  SchemaRegistry.use((registry) => registry.checkCompatibility(request))

export const RegistryEncodeOptions = Schema.Struct({
  subject: Schema.String,
  schema: Avro.AvroSchema,
  schemaType: Schema.optionalKey(Schema.Literal("AVRO")),
  references: Schema.optionalKey(Schema.Array(SchemaReference)),
  value: Schema.Unknown,
  autoRegister: Schema.optionalKey(Schema.Boolean),
  parseOptions: Schema.optionalKey(Avro.ParseOptions)
})
export type RegistryEncodeOptions<A> =
  Omit<typeof RegistryEncodeOptions.Type, "value"> & {
    readonly value: A
  }

export const encodeWithRegistry = <A>(
  client: SchemaRegistryClient,
  options: RegistryEncodeOptions<A>
): Effect.Effect<Uint8Array, SchemaRegistryClientError | Avro.AvroError> =>
  Effect.gen(function*() {
    const registered = yield* (options.autoRegister === false ? client.getId(options) : client.register(options))
    return yield* Effect.try({
      try: () => encodeConfluentFrame(registered.id, Avro.encode(options.schema, options.value, options.parseOptions)),
      catch: registryOrAvroError
    })
  })

export const encode = <A>(
  options: RegistryEncodeOptions<A>
): Effect.Effect<Uint8Array, SchemaRegistryClientError | Avro.AvroError, SchemaRegistry> =>
  SchemaRegistry.use((registry) => encodeWithRegistry(registry, options))

export const decodeWithRegistry = <A = unknown>(
  client: SchemaRegistryClient,
  input: Uint8Array,
  options?: Avro.ParseOptions
): Effect.Effect<A, SchemaRegistryClientError | Avro.AvroError> =>
  Effect.gen(function*() {
    const frame = yield* Effect.try({
      try: () => decodeConfluentFrame(input),
      catch: registryOrAvroError
    })
    const registered = yield* client.getById(frame.schemaId)
    return yield* Effect.try({
      try: () => Avro.decode<A>(registered.schema, frame.payload, options),
      catch: registryOrAvroError
    })
  })

export const decode = <A = unknown>(
  input: Uint8Array,
  options?: Avro.ParseOptions
): Effect.Effect<A, SchemaRegistryClientError | Avro.AvroError, SchemaRegistry> =>
  SchemaRegistry.use((registry) => decodeWithRegistry<A>(registry, input, options))

const RegistryResponse = Schema.Struct({
  id: Schema.optionalKey(Schema.Number),
  subject: Schema.optionalKey(Schema.String),
  version: Schema.optionalKey(Schema.Number),
  schema: Schema.optionalKey(Schema.String),
  schemaType: Schema.optionalKey(Schema.String),
  references: Schema.optionalKey(Schema.Array(SchemaReference))
})
type RegistryResponse = typeof RegistryResponse.Type

const normalizeRegisteredSchema = (
  response: RegistryResponse,
  request: RegisterSchemaRequest,
  fallbackId?: number
): RegisteredSchema => {
  const id = response.id ?? fallbackId
  if (id === undefined) {
    throw schemaRegistryError("Schema Registry response did not include a schema id")
  }
  return {
    id,
    ...subjectProperty(response.subject ?? request.subject),
    ...(response.version === undefined ? {} : { version: response.version }),
    schema: response.schema === undefined ? request.schema : parseSchema(response.schema),
    schemaType: "AVRO",
    references: response.references ?? request.references ?? []
  }
}

const registryRequestBody = (request: RegisterSchemaRequest) => ({
  schema: stableStringify(request.schema),
  schemaType: request.schemaType ?? "AVRO",
  ...(request.references === undefined ? {} : { references: request.references })
})

const subjectProperty = (subject: string) => subject === "" ? {} : { subject }

const cacheSchema = (
  registered: RegisteredSchema,
  byId: Map<number, RegisteredSchema>,
  bySubjectSchema: Map<string, RegisteredSchema>,
  useCache: boolean
) => {
  if (!useCache) {
    return
  }
  byId.set(registered.id, registered)
  if (registered.subject !== undefined) {
    bySubjectSchema.set(subjectSchemaCacheKey(registered.subject, registered.schema), registered)
  }
}

const makeHeaders = (options: SchemaRegistryClientOptions, hasBody: boolean): Record<string, string> => {
  const headers: Record<string, string> = {
    Accept: "application/vnd.schemaregistry.v1+json, application/json",
    ...options.headers
  }
  if (hasBody) {
    headers["Content-Type"] = "application/vnd.schemaregistry.v1+json"
  }
  const auth = options.auth ?? { _tag: "None" }
  if (auth._tag === "Basic") {
    headers.Authorization = `Basic ${base64Encode(`${auth.username}:${auth.password}`)}`
  } else if (auth._tag === "Bearer") {
    headers.Authorization = `Bearer ${auth.token}`
  }
  return headers
}

const parseSchema = (schema: string | undefined): Avro.AvroSchema => {
  if (schema === undefined) {
    throw schemaRegistryError("Schema Registry response did not include a schema")
  }
  try {
    return JSON.parse(schema) as Avro.AvroSchema
  } catch (error) {
    throw schemaRegistryError(`Unable to parse registry schema JSON: ${message(error)}`, error)
  }
}

const subjectSchemaCacheKey = (subject: string, schema: Avro.AvroSchema) =>
  `${subject}:${stableStringify(schema)}`

const schemaFullName = (schema: Avro.AvroSchema): string => {
  const named = findNamedSchema(schema)
  if (named === undefined) {
    throw schemaRegistryError("Record-based subject name strategies require a named Avro schema")
  }
  return named.namespace === undefined || named.name.includes(".") ? named.name : `${named.namespace}.${named.name}`
}

const findNamedSchema = (schema: Avro.AvroSchema): Avro.AvroNamedSchema | undefined => {
  if (typeof schema === "string") {
    return undefined
  }
  if (Array.isArray(schema)) {
    for (const member of schema) {
      const named = findNamedSchema(member)
      if (named !== undefined) {
        return named
      }
    }
    return undefined
  }
  const objectSchema = schema as Exclude<Avro.AvroSchema, string | Avro.AvroUnionSchema>
  if (
    objectSchema.type === "record" ||
    objectSchema.type === "error" ||
    objectSchema.type === "enum" ||
    objectSchema.type === "fixed"
  ) {
    return objectSchema as Avro.AvroNamedSchema
  }
  return typeof objectSchema.type === "string" ? undefined : findNamedSchema(objectSchema.type)
}

const stableStringify = (value: unknown): string =>
  JSON.stringify(sortJson(value))

const sortJson = (value: unknown): unknown => {
  if (Array.isArray(value)) {
    return value.map(sortJson)
  }
  if (value !== null && typeof value === "object") {
    const out: Record<string, unknown> = {}
    for (const key of Object.keys(value).sort()) {
      out[key] = sortJson((value as Record<string, unknown>)[key])
    }
    return out
  }
  return value
}

const message = (error: unknown): string => error instanceof Error ? error.message : String(error)

const registryOrAvroError = (error: unknown): SchemaRegistryClientError | Avro.AvroError =>
  isSchemaRegistryClientError(error) || error instanceof Avro.AvroError
    ? error
    : schemaRegistryError(message(error), error)

const isSchemaRegistryClientError = (error: unknown): error is SchemaRegistryClientError =>
  error instanceof SchemaRegistryError ||
  error instanceof SchemaRegistryHttpError ||
  error instanceof InvalidRegistryFrame

const schemaRegistryError = (message: string, cause?: unknown): SchemaRegistryError =>
  cause === undefined ? new SchemaRegistryError({ message }) : new SchemaRegistryError({ message, cause })

const schemaRegistryHttpError = (status: number, body: string): SchemaRegistryHttpError =>
  new SchemaRegistryHttpError({
    message: `Schema Registry request failed with HTTP ${status}: ${body}`,
    status,
    body
  })

const invalidRegistryFrame = (message: string): InvalidRegistryFrame =>
  new InvalidRegistryFrame({ message })

const base64Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

const base64Encode = (value: string): string => {
  const bytes = new TextEncoder().encode(value)
  let out = ""
  let index = 0
  for (; index + 2 < bytes.length; index += 3) {
    const chunk = (bytes[index] << 16) | (bytes[index + 1] << 8) | bytes[index + 2]
    out += base64Alphabet[(chunk >> 18) & 63]
    out += base64Alphabet[(chunk >> 12) & 63]
    out += base64Alphabet[(chunk >> 6) & 63]
    out += base64Alphabet[chunk & 63]
  }
  if (index < bytes.length) {
    const remaining = bytes.length - index
    const chunk = (bytes[index] << 16) | (remaining === 2 ? bytes[index + 1] << 8 : 0)
    out += base64Alphabet[(chunk >> 18) & 63]
    out += base64Alphabet[(chunk >> 12) & 63]
    out += remaining === 2 ? base64Alphabet[(chunk >> 6) & 63] : "="
    out += "="
  }
  return out
}
