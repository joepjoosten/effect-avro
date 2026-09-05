import * as Avro from "@effect-avro/core"
import { Effect, Schema, SchemaAST, SchemaIssue, SchemaTransformation } from "effect"

export const AvroTypeAnnotationId = "@effect-avro/schema/type"
export const AvroNameAnnotationId = "@effect-avro/schema/name"
export const AvroNamespaceAnnotationId = "@effect-avro/schema/namespace"
export const AvroAliasesAnnotationId = "@effect-avro/schema/aliases"
export const AvroDefaultAnnotationId = "@effect-avro/schema/default"
export const AvroLogicalTypeAnnotationId = "@effect-avro/schema/logicalType"
export const AvroPrecisionAnnotationId = "@effect-avro/schema/precision"
export const AvroScaleAnnotationId = "@effect-avro/schema/scale"
export const AvroFixedSizeAnnotationId = "@effect-avro/schema/fixedSize"
export const AvroFieldOrderAnnotationId = "@effect-avro/schema/fieldOrder"
export const EffectTagMetadataKey = "x-effect-tag"

export const AvroPrimitive = Avro.AvroPrimitive
export type AvroPrimitive = Avro.AvroPrimitive

export const AvroRecordField = Avro.AvroRecordField
export type AvroRecordField = Avro.AvroRecordField

export const AvroRecordSchema = Avro.AvroRecordSchema as Schema.Schema<AvroRecordSchema>
export type AvroRecordSchema = Avro.AvroRecordSchema & {
  readonly [EffectTagMetadataKey]?: string
}

export const AvroEnumSchema = Avro.AvroEnumSchema
export type AvroEnumSchema = Avro.AvroEnumSchema

export const AvroArraySchema = Avro.AvroArraySchema
export type AvroArraySchema = Avro.AvroArraySchema

export const AvroMapSchema = Avro.AvroMapSchema
export type AvroMapSchema = Avro.AvroMapSchema

export const AvroFixedSchema = Avro.AvroFixedSchema
export type AvroFixedSchema = Avro.AvroFixedSchema

export const AvroLogicalSchema = Avro.AvroLogicalSchema
export type AvroLogicalSchema = Avro.AvroLogicalSchema

export const AvroSchema = Avro.AvroSchema
export type AvroSchema = Avro.AvroSchema

export type AvroNamedSchema = AvroRecordSchema | AvroEnumSchema | AvroFixedSchema
export type AvroUnionSchema = ReadonlyArray<AvroSchema>

export const ToAvroOptions = Schema.Struct({
  name: Schema.optionalKey(Schema.String),
  namespace: Schema.optionalKey(Schema.String),
  omitTags: Schema.optionalKey(Schema.Boolean)
})
export type ToAvroOptions = typeof ToAvroOptions.Type

export const FromAvroOptions = Schema.Struct({
  namespace: Schema.optionalKey(Schema.String)
})
export type FromAvroOptions = typeof FromAvroOptions.Type

export const AvroCodecOptions = Schema.Struct({
  name: Schema.optionalKey(Schema.String),
  namespace: Schema.optionalKey(Schema.String),
  omitTags: Schema.optionalKey(Schema.Boolean),
  avroSchema: Schema.optionalKey(AvroSchema)
})
export type AvroCodecOptions = typeof AvroCodecOptions.Type

export const CompiledAvroSchema = Schema.Struct({
  schema: AvroSchema,
  type: Schema.instanceOf(Object)
})
export type CompiledAvroSchema =
  Omit<typeof CompiledAvroSchema.Type, "type"> & {
    readonly type: Avro.Type
  }

export interface AvroCodec<S extends Schema.Constraint>
  extends Schema.Codec<S["Type"], Uint8Array, S["DecodingServices"], S["EncodingServices"]>
{
  readonly avro: AvroSchema
  readonly avroType: Avro.Type
  readonly schema: S
}

export const Int = Schema.Int.annotate({ [AvroTypeAnnotationId]: "int" })
export const Long = Schema.Int.annotate({ [AvroTypeAnnotationId]: "long" })
export const Float = Schema.Number.annotate({ [AvroTypeAnnotationId]: "float" })
export const Double = Schema.Number.annotate({ [AvroTypeAnnotationId]: "double" })
export const Bytes = Schema.Uint8Array.annotate({ [AvroTypeAnnotationId]: "bytes" })

export const Fixed = (name: string, size: number, namespace?: string) =>
  Schema.Uint8Array.annotate({
    [AvroTypeAnnotationId]: "fixed",
    [AvroNameAnnotationId]: name,
    [AvroFixedSizeAnnotationId]: size,
    ...(namespace === undefined ? {} : { [AvroNamespaceAnnotationId]: namespace })
  })

export const avroName = (name: string, namespace?: string) =>
  <S extends Schema.Constraint>(schema: S): S =>
    Schema.annotate({
      [AvroNameAnnotationId]: name,
      ...(namespace === undefined ? {} : { [AvroNamespaceAnnotationId]: namespace })
    })(schema as unknown as Schema.Top) as unknown as S

export const avroAnnotations = (annotations: Record<string, unknown>) =>
  <S extends Schema.Constraint>(schema: S): S =>
    Schema.annotate(annotations)(schema as unknown as Schema.Top) as unknown as S

class AvroSchemaError extends Schema.TaggedError<AvroSchemaError>()("AvroSchemaError", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect())
}) {}

type CompileState = {
  readonly options: Required<Pick<ToAvroOptions, "omitTags">> & Omit<ToAvroOptions, "omitTags">
  readonly names: Map<SchemaAST.AST, string>
  readonly schemas: Map<string, AvroSchema>
  readonly astByName: Map<string, SchemaAST.AST>
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

const namePattern = /^[A-Za-z_][A-Za-z0-9_]*$/

const BinarySchema = Schema.Uint8Array

export const toAvroSchema = <S extends Schema.Constraint>(schema: S, options: ToAvroOptions = {}): AvroSchema => {
  const rootName = options.name ?? schemaIdentifier(schema) ?? "Root"
  const state: CompileState = {
    options: { ...options, omitTags: options.omitTags ?? true },
    names: new Map(),
    schemas: new Map(),
    astByName: new Map()
  }
  return compileAst(SchemaAST.toEncoded(schema.ast), state, [rootName])
}

export const compileAvro = <S extends Schema.Constraint>(
  schema: S,
  options: AvroCodecOptions = {}
): CompiledAvroSchema => {
  const avroSchema = options.avroSchema ?? toAvroSchema(schema, options)
  return {
    schema: avroSchema,
    type: Avro.parse(avroSchema as Avro.AvroSchema, { restoreTags: true })
  }
}

export const avro = <S extends Schema.Constraint>(
  schema: S,
  options: AvroCodecOptions = {}
): AvroCodec<S> => {
  const compiled = compileAvro(schema, options)
  const registry = makeRuntimeRegistry(compiled.schema, options.namespace)

  const codec = BinarySchema.pipe(
    Schema.decodeTo(
      schema,
      SchemaTransformation.transformOrFail({
        decode: (buffer) =>
          Effect.try({
            try: () => fromAvroRuntime(compiled.type.fromBuffer(buffer), compiled.schema, registry),
            catch: (error) => avroIssue(buffer, `Unable to decode Avro buffer: ${message(error)}`)
          }),
        encode: (value) =>
          Effect.try({
            try: () => compiled.type.toBuffer(toAvroRuntime(value, compiled.schema, registry)),
            catch: (error) => avroIssue(value, `Unable to encode Avro buffer: ${message(error)}`)
          })
      })
    )
  ) as unknown as AvroCodec<S>

  return Object.assign(codec, {
    avro: compiled.schema,
    avroType: compiled.type,
    schema
  })
}

export const fromAvroSchema = (
  schema: AvroSchema,
  options: FromAvroOptions = {}
): Schema.Codec<unknown, unknown, never, never> => {
  const ctx: FromAvroContext = {
    namespace: options.namespace,
    schemas: new Map()
  }
  return buildEffectSchema(schema, ctx) as Schema.Codec<unknown, unknown, never, never>
}

const avroIssue = (input: unknown, text: string) =>
  new SchemaIssue.InvalidValue({ message: text }, input)

const avroSchemaError = (message: string, cause?: unknown): AvroSchemaError =>
  cause === undefined ? new AvroSchemaError({ message }) : new AvroSchemaError({ message, cause })

const message = (error: unknown): string => error instanceof Error ? error.message : String(error)

const schemaIdentifier = (schema: Schema.Constraint): string | undefined => {
  const value = schema as unknown as { readonly identifier?: unknown }
  return typeof value.identifier === "string" ? value.identifier : SchemaAST.resolveIdentifier(schema.ast)
}

const compileAst = (ast: SchemaAST.AST, state: CompileState, path: ReadonlyArray<string>): AvroSchema => {
  const annotatedType = SchemaAST.resolveAt<string>(AvroTypeAnnotationId)(ast)
  if (annotatedType !== undefined) {
    return compileAnnotatedType(ast, annotatedType, state, path)
  }

  switch (ast._tag) {
    case "Null":
    case "Undefined":
    case "Void":
      return "null"
    case "Boolean":
      return withLogicalAnnotations(ast, "boolean")
    case "String":
    case "TemplateLiteral":
      return withLogicalAnnotations(ast, "string")
    case "Number":
      return withLogicalAnnotations(ast, hasIntCheck(ast) ? "int" : "double")
    case "Literal":
      return compileLiteral(ast, state, path)
    case "Enum":
      return compileEnumAst(ast, state, path)
    case "Arrays":
      return compileArrayAst(ast, state, path)
    case "Objects":
      return compileObjectAst(ast, state, path)
    case "Union":
      return compileUnionAst(ast, state, path)
    case "Suspend":
      return compileAst(ast.thunk(), state, path)
    case "Declaration":
      return compileDeclarationAst(ast, state, path)
    case "BigInt":
      throw avroSchemaError("BigInt schemas require an explicit encoding to an Avro-supported type")
    default:
      throw unsupported(ast, path)
  }
}

const compileAnnotatedType = (
  ast: SchemaAST.AST,
  annotatedType: string,
  state: CompileState,
  path: ReadonlyArray<string>
): AvroSchema => {
  switch (annotatedType) {
    case "int":
    case "long":
    case "float":
    case "double":
    case "bytes":
    case "string":
    case "boolean":
    case "null":
      return withLogicalAnnotations(ast, annotatedType)
    case "fixed": {
      const name = resolveName(ast, state, path)
      const size = SchemaAST.resolveAt<number>(AvroFixedSizeAnnotationId)(ast)
      if (typeof size !== "number" || !Number.isInteger(size) || size <= 0) {
        throw avroSchemaError(`Fixed type ${name.fullName} requires a positive integer fixed size`)
      }
      const fixed: AvroFixedSchema = {
        type: "fixed",
        name: name.name,
        ...(name.namespace === undefined ? {} : { namespace: name.namespace }),
        size
      }
      return registerNamedSchema(ast, name.fullName, withLogicalAnnotations(ast, fixed), state)
    }
    default:
      throw avroSchemaError(`Unsupported Avro type annotation ${JSON.stringify(annotatedType)}`)
  }
}

const compileDeclarationAst = (
  ast: SchemaAST.Declaration,
  state: CompileState,
  path: ReadonlyArray<string>
): AvroSchema => {
  const typeConstructor = SchemaAST.resolveAt<{ readonly _tag?: string }>("typeConstructor")(ast)
  if (typeConstructor?._tag === "Uint8Array") {
    return withLogicalAnnotations(ast, "bytes")
  }
  if (ast.typeParameters.length === 1) {
    return compileAst(ast.typeParameters[0], state, path)
  }
  throw unsupported(ast, path)
}

const compileLiteral = (
  ast: SchemaAST.Literal,
  state: CompileState,
  path: ReadonlyArray<string>
): AvroSchema => {
  switch (typeof ast.literal) {
    case "string":
      return namePattern.test(ast.literal)
        ? compileStringEnum([ast.literal], ast, state, path)
        : "string"
    case "boolean":
      return "boolean"
    case "number":
      return Number.isInteger(ast.literal) ? "int" : "double"
    case "bigint":
      throw avroSchemaError("BigInt literals require an explicit encoding to an Avro-supported type")
  }
}

const compileEnumAst = (
  ast: SchemaAST.Enum,
  state: CompileState,
  path: ReadonlyArray<string>
): AvroSchema => {
  if (ast.enums.some(([, value]) => typeof value !== "string")) {
    throw avroSchemaError("Numeric enums require an explicit encoding to an Avro-supported type")
  }
  const symbols = ast.enums.map(([, value]) => String(value))
  return symbols.every((symbol) => namePattern.test(symbol))
    ? compileStringEnum(symbols, ast, state, path)
    : "string"
}

const compileStringEnum = (
  symbols: ReadonlyArray<string>,
  ast: SchemaAST.AST,
  state: CompileState,
  path: ReadonlyArray<string>
): AvroSchema => {
  const existing = state.names.get(ast)
  if (existing !== undefined) return existing
  const name = resolveName(ast, state, path)
  const schema: AvroEnumSchema = {
    type: "enum",
    name: name.name,
    ...(name.namespace === undefined ? {} : { namespace: name.namespace }),
    ...docAndAliases(ast),
    symbols
  }
  return registerNamedSchema(ast, name.fullName, schema, state)
}

const compileArrayAst = (
  ast: SchemaAST.Arrays,
  state: CompileState,
  path: ReadonlyArray<string>
): AvroSchema => {
  if (ast.elements.length > 0 || ast.rest.length !== 1) {
    throw avroSchemaError(`Avro arrays must be homogeneous at ${formatPath(path)}`)
  }
  return {
    type: "array",
    items: compileAst(ast.rest[0], state, [...path, "Item"])
  }
}

const compileObjectAst = (
  ast: SchemaAST.Objects,
  state: CompileState,
  path: ReadonlyArray<string>
): AvroSchema => {
  if (ast.indexSignatures.length > 0) {
    if (ast.propertySignatures.length === 0 && ast.indexSignatures.length === 1) {
      const index = ast.indexSignatures[0]
      if (index.parameter._tag !== "String" && index.parameter._tag !== "TemplateLiteral") {
        throw avroSchemaError(`Avro maps require string keys at ${formatPath(path)}`)
      }
      return {
        type: "map",
        values: compileAst(index.type, state, [...path, "Value"])
      }
    }
    throw avroSchemaError(`Avro records cannot mix fixed fields and index signatures at ${formatPath(path)}`)
  }

  const name = resolveName(ast, state, path)
  const existing = state.names.get(ast)
  if (existing !== undefined) {
    return existing
  }
  state.names.set(ast, name.fullName)

  const previousAst = state.astByName.get(name.fullName)
  if (previousAst !== undefined && previousAst !== ast) {
    throw avroSchemaError(`Duplicate Avro name ${name.fullName}`)
  }
  state.astByName.set(name.fullName, ast)

  let tag: string | undefined
  const fields: Array<AvroRecordField> = []
  for (const property of ast.propertySignatures) {
    if (typeof property.name !== "string") {
      throw avroSchemaError(`Avro field names must be strings at ${formatPath(path)}`)
    }
    if (
      state.options.omitTags &&
      property.name === "_tag" &&
      property.type._tag === "Literal" &&
      typeof property.type.literal === "string"
    ) {
      tag = property.type.literal
      continue
    }
    fields.push(compileField(property.name, property.type, state, [...path, property.name]))
  }

  const schema: AvroRecordSchema = {
    type: "record",
    name: name.name,
    ...(name.namespace === undefined ? {} : { namespace: name.namespace }),
    ...docAndAliases(ast),
    ...(tag === undefined ? {} : { [EffectTagMetadataKey]: tag }),
    fields
  }
  return registerNamedSchema(ast, name.fullName, schema, state)
}

const compileField = (
  name: string,
  ast: SchemaAST.AST,
  state: CompileState,
  path: ReadonlyArray<string>
): AvroRecordField => {
  const optional = SchemaAST.isOptional(ast)
  const compiled = compileAst(stripUndefined(ast), state, path)
  const type = optional ? nullable(compiled) : compiled
  const fieldAnnotations = ast.context?.annotations
  const doc = fieldAnnotations?.description ?? SchemaAST.resolveDescription(ast)
  const aliases = SchemaAST.resolveAt<ReadonlyArray<string>>(AvroAliasesAnnotationId)(ast)
  const fieldDefault = SchemaAST.resolveAt<unknown>(AvroDefaultAnnotationId)(ast)
  const order = SchemaAST.resolveAt<"ascending" | "descending" | "ignore">(AvroFieldOrderAnnotationId)(ast)

  return {
    name,
    type,
    ...(optional && !(Array.isArray(compiled) ? compiled : [compiled]).some((member) => branchName(member) === "null")
      ? { "x-effect-optional": true } : {}),
    ...(doc === undefined ? {} : { doc }),
    ...(aliases === undefined ? {} : { aliases }),
    ...(order === undefined ? {} : { order }),
    ...(optional ? { default: null } : fieldDefault === undefined ? {} : { default: fieldDefault })
  }
}

const compileUnionAst = (
  ast: SchemaAST.Union,
  state: CompileState,
  path: ReadonlyArray<string>
): AvroSchema => {
  const types = ast.types.map((type) => stripUndefined(type))
  const literals = types.filter((type): type is SchemaAST.Literal => type._tag === "Literal")
  if (
    literals.length === types.length &&
    literals.length > 0 &&
    literals.every((literal) => typeof literal.literal === "string" && namePattern.test(literal.literal))
  ) {
    return compileStringEnum(
      literals.map((literal) => String(literal.literal)),
      ast,
      state,
      path
    )
  }

  const union = uniqueUnion(types.map((type, index) => compileAst(type, state, [...path, `Member${index}`])))
  return union.length === 1 ? union[0] : union
}

const stripUndefined = (ast: SchemaAST.AST): SchemaAST.AST => {
  if (ast._tag !== "Union") {
    return ast._tag === "Undefined" || ast._tag === "Void" ? SchemaAST.null : ast
  }
  const types = ast.types.filter((type) => type._tag !== "Undefined" && type._tag !== "Void")
  if (types.length === 0) {
    return SchemaAST.null
  }
  if (types.length === 1) {
    return types[0]
  }
  return new SchemaAST.Union(types, ast.mode, ast.annotations, ast.checks, ast.encoding, ast.context)
}

const nullable = (schema: AvroSchema): AvroSchema => {
  const members = Array.isArray(schema) ? schema : [schema]
  return uniqueUnion(["null", ...members])
}

const uniqueUnion = (schemas: ReadonlyArray<AvroSchema>): ReadonlyArray<AvroSchema> => {
  const seen = new Map<string, AvroSchema>()
  const out: Array<AvroSchema> = []
  for (const schema of schemas.flatMap((member) => Array.isArray(member) ? member : [member])) {
    const key = branchName(schema)
    const existing = seen.get(key)
    if (existing !== undefined) {
      if (JSON.stringify(existing) !== JSON.stringify(schema)) {
        throw avroSchemaError(`Distinct ${key} alternatives cannot be represented faithfully in an Avro union`)
      }
      continue
    }
    seen.set(key, schema)
    out.push(schema)
  }
  return out
}

const withLogicalAnnotations = <A extends AvroSchema>(ast: SchemaAST.AST, schema: A): A | AvroLogicalSchema => {
  const logicalType = SchemaAST.resolveAt<string>(AvroLogicalTypeAnnotationId)(ast)
  if (logicalType === undefined) {
    return schema
  }
  const precision = SchemaAST.resolveAt<number>(AvroPrecisionAnnotationId)(ast)
  const scale = SchemaAST.resolveAt<number>(AvroScaleAnnotationId)(ast)
  return {
    type: schema,
    logicalType,
    ...(precision === undefined ? {} : { precision }),
    ...(scale === undefined ? {} : { scale })
  }
}

const hasIntCheck = (ast: SchemaAST.AST): boolean => {
  const checks = ast.checks
  if (checks === undefined) {
    return false
  }
  const visit = (check: SchemaAST.Check<unknown>): boolean => {
    if (check._tag === "Filter") {
      return check.annotations?.representation?.id === "effect/schema/isInt"
    }
    return check.checks.some(visit)
  }
  return checks.some(visit)
}

const docAndAliases = (ast: SchemaAST.AST) => {
  const doc = SchemaAST.resolveDescription(ast)
  const aliases = SchemaAST.resolveAt<ReadonlyArray<string>>(AvroAliasesAnnotationId)(ast)
  return {
    ...(doc === undefined ? {} : { doc }),
    ...(aliases === undefined ? {} : { aliases })
  }
}

const resolveName = (
  ast: SchemaAST.AST,
  state: CompileState,
  path: ReadonlyArray<string>
): { readonly name: string; readonly namespace?: string; readonly fullName: string } => {
  const annotatedName = SchemaAST.resolveAt<string>(AvroNameAnnotationId)(ast)
  const identifier = SchemaAST.resolveIdentifier(ast)
  const title = SchemaAST.resolveTitle(ast)
  const raw = annotatedName ?? identifier ?? title ?? path.join("_")
  const annotatedNamespace = SchemaAST.resolveAt<string>(AvroNamespaceAnnotationId)(ast)
  const split = splitName(raw)
  const namespace = split.namespace ?? annotatedNamespace ?? state.options.namespace
  let name = sanitizeName(split.name, "Type")
  if (annotatedName === undefined && identifier === undefined && title === undefined) {
    const base = name
    let suffix = 2
    while (state.astByName.has(fullName(name, namespace)) && state.astByName.get(fullName(name, namespace)) !== ast) {
      name = `${base}_${suffix++}`
    }
  }
  return {
    name,
    ...(namespace === undefined ? {} : { namespace }),
    fullName: namespace === undefined ? name : `${namespace}.${name}`
  }
}

const splitName = (name: string): { readonly name: string; readonly namespace?: string } => {
  const index = name.lastIndexOf(".")
  if (index === -1) {
    return { name }
  }
  return {
    namespace: name.slice(0, index),
    name: name.slice(index + 1)
  }
}

const sanitizeName = (name: string, fallback: string): string => {
  const sanitized = name.replace(/[^A-Za-z0-9_]/g, "_").replace(/^[^A-Za-z_]/, "_")
  return namePattern.test(sanitized) ? sanitized : fallback
}

const registerNamedSchema = (
  ast: SchemaAST.AST,
  fullName: string,
  schema: AvroSchema,
  state: CompileState
): AvroSchema => {
  const existing = state.schemas.get(fullName)
  if (existing !== undefined) {
    if (JSON.stringify(existing) !== JSON.stringify(schema)) throw avroSchemaError(`Conflicting Avro name ${fullName}`)
    state.names.set(ast, fullName)
    return fullName
  }
  state.names.set(ast, fullName)
  state.astByName.set(fullName, ast)
  state.schemas.set(fullName, schema)
  return schema
}

const unsupported = (ast: SchemaAST.AST, path: ReadonlyArray<string>) =>
  avroSchemaError(`Unsupported Effect Schema AST ${ast._tag} at ${formatPath(path)}`, ast)

const formatPath = (path: ReadonlyArray<string>) => path.join(".")

type RuntimeRegistry = {
  readonly named: Map<string, AvroNamedSchema>
  readonly namespace: string | undefined
}

const makeRuntimeRegistry = (schema: AvroSchema, namespace?: string): RuntimeRegistry => {
  const registry: RuntimeRegistry = {
    named: new Map(),
    namespace
  }
  collectRuntimeNames(schema, registry, namespace)
  return registry
}

const collectRuntimeNames = (schema: AvroSchema, registry: RuntimeRegistry, namespace?: string): void => {
  if (typeof schema === "string") {
    return
  }
  if (Array.isArray(schema)) {
    for (const member of schema) {
      collectRuntimeNames(member, registry, namespace)
    }
    return
  }
  const concrete = normalizeObjectType(schema)
  if (isNamedSchema(concrete)) {
    const name = fullName(concrete.name, concrete.namespace ?? namespace)
    registry.named.set(name, concrete)
    for (const alias of concrete.aliases ?? []) registry.named.set(fullName(alias, splitName(name).namespace), concrete)
    const childNamespace = splitName(name).namespace
    if (concrete.type === "record" || concrete.type === "error") {
      for (const field of concrete.fields) {
        collectRuntimeNames(field.type, registry, childNamespace)
      }
    }
    return
  }
  if (concrete.type === "array") {
    collectRuntimeNames(concrete.items, registry, namespace)
  } else if (concrete.type === "map") {
    collectRuntimeNames(concrete.values, registry, namespace)
  } else if (typeof concrete.type !== "string") {
    collectRuntimeNames(concrete.type, registry, namespace)
  }
}

const fromAvroRuntime = (value: unknown, schema: AvroSchema, registry: RuntimeRegistry, namespace = registry.namespace): unknown => {
  const resolved = resolveRuntimeSchema(schema, registry, namespace)
  if (Array.isArray(resolved)) {
    if (value === null) {
      return null
    }
    const branch = resolved.find((member) => branchName(member) !== "null" && matchesAvro(member, value, registry, namespace))
    return branch === undefined ? value : fromAvroRuntime(value, branch, registry, namespace)
  }
  if (typeof resolved === "string") {
    return value
  }

  const concrete = normalizeObjectType(resolved)
  switch (concrete.type) {
    case "record":
    case "error": {
      if (!isRecordLike(value)) {
        return value
      }
      const out: Record<string, unknown> = {}
      for (const field of concrete.fields) {
        if (field["x-effect-optional"] === true && value[field.name] === null) continue
        setOwn(out, field.name, fromAvroRuntime(value[field.name], field.type, registry, splitName(fullName(concrete.name, concrete.namespace ?? namespace)).namespace ?? ""))
      }
      const tag = concrete[EffectTagMetadataKey]
      if (tag !== undefined) {
        out._tag = tag
      }
      return out
    }
    case "array":
      return Array.isArray(value) ? value.map((item) => fromAvroRuntime(item, concrete.items, registry, namespace)) : value
    case "map":
      return isRecordLike(value)
        ? Object.fromEntries(Object.entries(value).map(([key, item]) => [key, fromAvroRuntime(item, concrete.values, registry, namespace)]))
        : value
    default:
      return value
  }
}

const toAvroRuntime = (value: unknown, schema: AvroSchema, registry: RuntimeRegistry, namespace = registry.namespace): unknown => {
  const resolved = resolveRuntimeSchema(schema, registry, namespace)
  if (Array.isArray(resolved)) {
    const branch = resolved.find((member) => matchesAvro(member, value, registry, namespace))
    return branch === undefined ? value : toAvroRuntime(value, branch, registry, namespace)
  }
  if (typeof resolved === "string") {
    return primitiveToAvroRuntime(value, resolved)
  }

  const concrete = normalizeObjectType(resolved)
  switch (concrete.type) {
    case "record":
    case "error": {
      if (!isRecordLike(value)) {
        return value
      }
      const out: Record<string, unknown> = {}
      for (const field of concrete.fields) {
        const item = value[field.name] === undefined && field.default === null ? null : value[field.name]
        setOwn(out, field.name, toAvroRuntime(item, field.type, registry, splitName(fullName(concrete.name, concrete.namespace ?? namespace)).namespace ?? ""))
      }
      if (concrete[EffectTagMetadataKey] !== undefined) out._tag = value._tag
      return out
    }
    case "array":
      return Array.isArray(value) ? value.map((item) => toAvroRuntime(item, concrete.items, registry, namespace)) : value
    case "map":
      return isRecordLike(value)
        ? Object.fromEntries(Object.entries(value).map(([key, item]) => [key, toAvroRuntime(item, concrete.values, registry, namespace)]))
        : value
    case "fixed":
      return toBytes(value)
    default:
      if (concrete.type === "bytes") {
        return toBytes(value)
      }
      return value
  }
}

const primitiveToAvroRuntime = (value: unknown, primitive: string): unknown =>
  primitive === "bytes" ? toBytes(value) : value

const toBytes = (value: unknown): unknown => {
  if (value instanceof Uint8Array) {
    return value
  }
  return value
}

const matchesAvro = (schema: AvroSchema, value: unknown, registry: RuntimeRegistry, namespace = registry.namespace): boolean => {
  const resolved = resolveRuntimeSchema(schema, registry, namespace)
  if (Array.isArray(resolved)) {
    return resolved.some((member) => matchesAvro(member, value, registry, namespace))
  }
  if (typeof resolved === "string") {
    switch (resolved) {
      case "null":
        return value === null
      case "boolean":
        return typeof value === "boolean"
      case "string":
        return typeof value === "string"
      case "int":
      case "long":
      case "float":
      case "double":
        return typeof value === "number"
      case "bytes":
        return value instanceof Uint8Array
      default:
        return matchesAvro(resolveRuntimeSchema(resolved, registry, namespace), value, registry, namespace)
    }
  }
  const concrete = normalizeObjectType(resolved)
  switch (concrete.type) {
    case "record":
    case "error":
      return isRecordLike(value) &&
        (
          concrete[EffectTagMetadataKey] === undefined ||
          value._tag === concrete[EffectTagMetadataKey] ||
          !Object.hasOwn(value, "_tag")
        ) &&
        concrete.fields.every((field: AvroRecordField) => field.default !== undefined || Object.hasOwn(value, field.name))
    case "enum":
      return typeof value === "string" && concrete.symbols.includes(value)
    case "array":
      return Array.isArray(value)
    case "map":
      return isRecordLike(value) && !Array.isArray(value)
    case "fixed":
      return value instanceof Uint8Array && value.byteLength === concrete.size
    default:
      return typeof concrete.type === "string" ? matchesAvro(concrete.type, value, registry, namespace) : false
  }
}

const resolveRuntimeSchema = (schema: AvroSchema, registry: RuntimeRegistry, namespace = registry.namespace): AvroSchema => {
  if (typeof schema === "string" && !primitiveNames.has(schema as AvroPrimitive)) {
    const resolved = registry.named.get(fullName(schema, namespace))
    if (resolved === undefined) throw avroSchemaError(`Unknown Avro type reference ${schema}`)
    return resolved
  }
  if (!Array.isArray(schema) && typeof schema !== "string") {
    const concrete = normalizeObjectType(schema)
    if (typeof concrete.type === "string" && !isComplexTypeName(concrete.type) && !primitiveNames.has(concrete.type as AvroPrimitive)) {
      return resolveRuntimeSchema(concrete.type, registry, namespace)
    }
  }
  return schema
}

const normalizeObjectType = (schema: AvroSchema): any => {
  if (typeof schema === "string" || Array.isArray(schema)) {
    return schema
  }
  const objectSchema = schema as Exclude<AvroSchema, string | AvroUnionSchema>
  return typeof objectSchema.type === "string" || Array.isArray(objectSchema.type) || typeof objectSchema.type === "object"
    ? objectSchema
    : { ...objectSchema, type: objectSchema.type }
}

const isNamedSchema = (schema: any): schema is AvroNamedSchema =>
  (schema.type === "record" || schema.type === "error" || schema.type === "enum" || schema.type === "fixed") &&
  typeof schema.name === "string"

const branchName = (schema: AvroSchema): string => {
  if (typeof schema === "string") {
    return schema
  }
  if (Array.isArray(schema)) {
    return "union"
  }
  const concrete = normalizeObjectType(schema)
  if (isNamedSchema(concrete)) {
    return fullName(concrete.name, concrete.namespace)
  }
  if (typeof concrete.type === "string") {
    return concrete.type
  }
  return branchName(concrete.type)
}

const fullName = (name: string, namespace?: string): string => {
  if (name.includes(".") || namespace === undefined || namespace === "" || primitiveNames.has(name as AvroPrimitive)) {
    return name
  }
  return `${namespace}.${name}`
}

const unqualified = (name: string): string => {
  const index = name.lastIndexOf(".")
  return index === -1 ? name : name.slice(index + 1)
}

const isComplexTypeName = (type: string) =>
  type === "record" || type === "error" || type === "enum" || type === "array" || type === "map" || type === "fixed"

const isRecordLike = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)

type FromAvroContext = {
  readonly namespace: string | undefined
  readonly schemas: Map<string, Schema.Constraint>
}

const buildEffectSchema = (schema: AvroSchema, ctx: FromAvroContext): Schema.Constraint => {
  if (typeof schema === "string") {
    return primitiveOrRef(schema, ctx)
  }
  if (Array.isArray(schema)) {
    return Schema.Union(schema.map((member) => buildEffectSchema(member, ctx)))
  }

  const concrete = normalizeObjectType(schema)
  if (typeof concrete.type !== "string") {
    return buildEffectSchema(concrete.type, ctx)
  }
  if (primitiveNames.has(concrete.type as AvroPrimitive)) {
    return primitiveOrRef(concrete.type, ctx)
  }

  switch (concrete.type) {
    case "record":
    case "error":
      return buildRecordSchema(concrete, ctx)
    case "enum":
      return registerImported(concrete, ctx, Schema.Literals(concrete.symbols).annotate({
        identifier: concrete.name,
        [AvroNameAnnotationId]: concrete.name,
        [AvroNamespaceAnnotationId]: concrete.namespace ?? ctx.namespace,
        ...(concrete.doc === undefined ? {} : { description: concrete.doc })
      }))
    case "array":
      return Schema.Array(buildEffectSchema(concrete.items, ctx))
    case "map":
      return Schema.Record(Schema.String, buildEffectSchema(concrete.values, ctx))
    case "fixed":
      return registerImported(concrete, ctx, Fixed(concrete.name, concrete.size, concrete.namespace ?? ctx.namespace))
    default:
      return primitiveOrRef(concrete.type, ctx)
  }
}

const buildRecordSchema = (schema: AvroRecordSchema, ctx: FromAvroContext): Schema.Constraint => {
  const name = fullName(schema.name, schema.namespace ?? ctx.namespace)
  const existing = ctx.schemas.get(name)
  if (existing !== undefined) {
    return existing
  }

  let lazy: Schema.Constraint
  lazy = Schema.suspend((): Schema.Constraint => {
    const resolved = ctx.schemas.get(name)
    if (resolved === undefined || resolved === lazy) {
      throw avroSchemaError(`Unresolved recursive Avro schema ${name}`)
    }
    return resolved
  })
  ctx.schemas.set(name, lazy)

  const fields: Record<string, Schema.Constraint> = {}
  const tag = schema[EffectTagMetadataKey]
  if (tag !== undefined) {
    fields._tag = Schema.tag(tag)
  }
  for (const field of schema.fields) {
    const fieldSchema = buildEffectSchema(field.type, {
      namespace: splitName(name).namespace,
      schemas: ctx.schemas
    })
    setOwn(fields, field.name, field.default === undefined ? fieldSchema : Schema.optionalKey(fieldSchema))
  }

  const struct = Schema.Struct(fields).annotate({
    identifier: unqualified(name),
    ...(schema.doc === undefined ? {} : { description: schema.doc }),
    [AvroNameAnnotationId]: schema.name,
    ...(schema.namespace === undefined ? {} : { [AvroNamespaceAnnotationId]: schema.namespace }),
    ...(schema.aliases === undefined ? {} : { [AvroAliasesAnnotationId]: schema.aliases })
  })
  ctx.schemas.set(name, struct)
  for (const alias of schema.aliases ?? []) ctx.schemas.set(fullName(alias, splitName(name).namespace), struct)
  return struct
}

const primitiveOrRef = (name: string, ctx: FromAvroContext): Schema.Constraint => {
  switch (name) {
    case "null":
      return Schema.Null
    case "boolean":
      return Schema.Boolean
    case "int":
      return Int
    case "long":
      return Long
    case "float":
      return Float
    case "double":
      return Double
    case "bytes":
      return Bytes
    case "string":
      return Schema.String
    default:
      return Schema.suspend(() => {
        const schema = ctx.schemas.get(fullName(name, ctx.namespace))
        if (schema === undefined) {
          throw avroSchemaError(`Unknown Avro type reference ${name}`)
        }
        return schema
      })
  }
}

const setOwn = <A>(object: Record<string, A>, key: string, value: A): void => {
  Object.defineProperty(object, key, { value, enumerable: true, writable: true, configurable: true })
}

const registerImported = (schema: AvroNamedSchema, ctx: FromAvroContext, value: Schema.Constraint): Schema.Constraint => {
  const name = fullName(schema.name, schema.namespace ?? ctx.namespace)
  ctx.schemas.set(name, value)
  for (const alias of schema.aliases ?? []) ctx.schemas.set(fullName(alias, splitName(name).namespace), value)
  return value
}
