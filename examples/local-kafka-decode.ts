import { Buffer } from "node:buffer"
import { Effect, Schema } from "effect"
import type { KafkaMessage } from "@effect-avro/kafka"
import { SchemaRegistry, type SchemaRegistryClientError } from "@effect-avro/schema-registry"
import { OrderPlaced, type OrderEvent } from "./domain.js"
import { decodeOrderMessage, encodeOrderMessage, type MissingKafkaValue, type OrderEventDecodeError } from "./registry-kafka-events.js"
import { SelfManagedKafkaRecord } from "./lambda-kafka-dedupe.js"
import { makeInMemorySchemaRegistry } from "./support/in-memory-registry.js"

export class Base64KafkaDecodeError extends Schema.TaggedError<Base64KafkaDecodeError>()(
  "Base64KafkaDecodeError",
  {
    message: Schema.String,
    cause: Schema.optional(Schema.Defect())
  }
) {}

export const decodeLocalKafkaRecord = (
  record: SelfManagedKafkaRecord
): Effect.Effect<
  OrderEvent,
  Base64KafkaDecodeError | MissingKafkaValue | OrderEventDecodeError | SchemaRegistryClientError,
  SchemaRegistry
> =>
  Effect.gen(function*() {
    const value = record.value === null || record.value === undefined
      ? null
      : yield* decodeBase64(record.value)
    const key = record.key === null || record.key === undefined
      ? undefined
      : yield* decodeBase64(record.key)

    const message: KafkaMessage = {
      topic: record.topic,
      partition: record.partition,
      offset: record.offset,
      value,
      ...(key === undefined ? {} : { key })
    }

    return yield* decodeOrderMessage(message)
  })

export const encodeLocalKafkaRecord = (
  message: KafkaMessage & { readonly value: Uint8Array }
): SelfManagedKafkaRecord => ({
  topic: message.topic,
  partition: message.partition ?? 0,
  offset: String(message.offset ?? "0"),
  key: message.key === null || message.key === undefined ? null : Buffer.from(message.key).toString("base64"),
  value: Buffer.from(message.value).toString("base64"),
  timestamp: Date.now()
})

const decodeBase64 = (value: string): Effect.Effect<Uint8Array, Base64KafkaDecodeError> =>
  Effect.try({
    try: () => Buffer.from(value, "base64"),
    catch: (cause) => new Base64KafkaDecodeError({
      message: "Unable to decode base64 Kafka payload",
      cause
    })
  })

const registry = makeInMemorySchemaRegistry()

const RegistryLive = SchemaRegistry.layer({
  endpoint: "http://registry.example",
  fetch: registry.fetch
})

export const program = Effect.gen(function*() {
  const produced = yield* encodeOrderMessage(new OrderPlaced({
    orderId: "ord_1000",
    customerId: "cus_123",
    totalCents: 4999,
    occurredAt: "2026-06-24T08:00:00.000Z"
  }))

  const localRecord = encodeLocalKafkaRecord(produced)
  const event = yield* decodeLocalKafkaRecord(localRecord)

  return {
    localRecord,
    event
  }
}).pipe(
  Effect.provide(RegistryLive)
)

if (import.meta.url === `file://${process.argv[1]}`) {
  Effect.runPromise(program).then((result) => {
    console.log(result.event)
  })
}
