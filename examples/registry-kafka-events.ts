import { Effect, Schema } from "effect"
import type { KafkaMessage } from "@effect-avro/kafka"
import {
  decodeConfluentFrame,
  encodeConfluentFrame,
  getById,
  InvalidRegistryFrame,
  register,
  SchemaRegistry,
  type SchemaRegistryClientError
} from "@effect-avro/schema-registry"
import {
  decodeOrderEvent,
  encodeOrderEvent,
  OrderEventAvroSchema,
  OrderPlaced,
  orderTopic,
  orderValueSubject,
  type OrderEvent
} from "./domain.js"
import { makeInMemorySchemaRegistry } from "./support/in-memory-registry.js"

export class MissingKafkaValue extends Schema.TaggedErrorClass<MissingKafkaValue>()("MissingKafkaValue", {
  topic: Schema.String
}) {}

export class OrderEventDecodeError extends Schema.TaggedErrorClass<OrderEventDecodeError>()("OrderEventDecodeError", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect())
}) {}

export const OrderKafkaMessage = Schema.Struct({
  topic: Schema.Literal(orderTopic),
  partition: Schema.optionalKey(Schema.Number),
  offset: Schema.optionalKey(Schema.Union([Schema.String, Schema.Number])),
  key: Schema.Uint8Array,
  value: Schema.Uint8Array,
  headers: Schema.optionalKey(Schema.Record(
    Schema.String,
    Schema.Union([Schema.Uint8Array, Schema.String, Schema.Undefined])
  ))
})
export type OrderKafkaMessage = typeof OrderKafkaMessage.Type

const textEncoder = new TextEncoder()

export const encodeOrderMessage = (
  event: OrderEvent
): Effect.Effect<OrderKafkaMessage, SchemaRegistryClientError | OrderEventDecodeError, SchemaRegistry> =>
  Effect.gen(function*() {
    const registered = yield* register({
      subject: orderValueSubject,
      schema: OrderEventAvroSchema
    })
    const payload = yield* Effect.try({
      try: () => encodeOrderEvent(event),
      catch: (cause) => new OrderEventDecodeError({
        message: "Unable to encode order event with Effect Schema",
        cause
      })
    })

    return {
      topic: orderTopic,
      key: textEncoder.encode(event.orderId),
      value: encodeConfluentFrame(registered.id, payload),
      headers: {
        "content-type": "application/vnd.apache.avro+binary",
        "schema-subject": orderValueSubject
      }
    }
  })

export const decodeOrderMessage = (
  message: KafkaMessage
): Effect.Effect<OrderEvent, MissingKafkaValue | SchemaRegistryClientError | OrderEventDecodeError, SchemaRegistry> =>
  Effect.gen(function*() {
    if (message.value === null || message.value === undefined) {
      return yield* new MissingKafkaValue({ topic: message.topic })
    }

    const frame = yield* Effect.try({
      try: () => decodeConfluentFrame(message.value as Uint8Array),
      catch: (cause) => cause instanceof InvalidRegistryFrame
        ? cause
        : new OrderEventDecodeError({ message: "Unable to decode Confluent frame", cause })
    })

    yield* getById(frame.schemaId)

    return yield* Effect.try({
      try: () => decodeOrderEvent(frame.payload) as OrderEvent,
      catch: (cause) => new OrderEventDecodeError({
        message: "Unable to decode order event with Effect Schema",
        cause
      })
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

  const consumed = yield* decodeOrderMessage(produced)

  return {
    produced,
    consumed
  }
}).pipe(
  Effect.provide(RegistryLive)
)

if (import.meta.url === `file://${process.argv[1]}`) {
  Effect.runPromise(program).then((result) => {
    console.log(result.consumed)
  })
}
