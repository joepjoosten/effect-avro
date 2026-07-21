import { Context, Effect, Layer, Schema } from "effect"
import {
  encodeConfluentFrame,
  register,
  SchemaRegistry,
  type SchemaRegistryClientError
} from "@effect-avro/schema-registry"
import {
  encodeOrderEvent,
  OrderCancelled,
  OrderEventAvroSchema,
  OrderPaid,
  OrderPlaced,
  orderTopic,
  orderValueSubject,
  type OrderEvent
} from "./domain.js"
import { makeInMemorySchemaRegistry } from "./support/in-memory-registry.js"

export const KafkaHeaderValue = Schema.Union([Schema.String, Schema.Uint8Array, Schema.Undefined])
export type KafkaHeaderValue = typeof KafkaHeaderValue.Type

export const KafkaHeaders = Schema.Record(Schema.String, KafkaHeaderValue)
export type KafkaHeaders = typeof KafkaHeaders.Type

export const ProducerMessage = Schema.Struct({
  key: Schema.optionalKey(Schema.NullOr(Schema.Uint8Array)),
  value: Schema.NullOr(Schema.Uint8Array),
  timestamp: Schema.optionalKey(Schema.String),
  headers: Schema.optionalKey(KafkaHeaders)
})
export type ProducerMessage = typeof ProducerMessage.Type

export const ProducerSendRequest = Schema.Struct({
  topic: Schema.String,
  messages: Schema.Array(ProducerMessage)
})
export type ProducerSendRequest = typeof ProducerSendRequest.Type

export const TopicMessages = Schema.Struct({
  topic: Schema.String,
  messages: Schema.Array(ProducerMessage)
})
export type TopicMessages = typeof TopicMessages.Type

export const ProducerSendBatchRequest = Schema.Struct({
  topicMessages: Schema.Array(TopicMessages)
})
export type ProducerSendBatchRequest = typeof ProducerSendBatchRequest.Type

export interface ProducerLike {
  readonly connect: () => Promise<void>
  readonly send: (request: ProducerSendRequest) => Promise<void>
  readonly sendBatch: (request: ProducerSendBatchRequest) => Promise<void>
}

export const PublisherSourceHeaders = Schema.Struct({
  domain: Schema.optionalKey(Schema.String),
  boundedContext: Schema.optionalKey(Schema.String),
  sourceApplication: Schema.optionalKey(Schema.String)
})
export type PublisherSourceHeaders = typeof PublisherSourceHeaders.Type

export const PublisherLayerOptions = Schema.Struct({
  sourceHeaders: Schema.optionalKey(PublisherSourceHeaders)
})
export type PublisherLayerOptions = typeof PublisherLayerOptions.Type

export const OrderPublishOptions = Schema.Struct({
  key: Schema.optionalKey(Schema.NullOr(Schema.String)),
  timestamp: Schema.optionalKey(Schema.String),
  headers: Schema.optionalKey(KafkaHeaders),
  valueSchemaVersion: Schema.optionalKey(Schema.Number)
})
export type OrderPublishOptions = typeof OrderPublishOptions.Type

export const OrderPublishMessage = Schema.Struct({
  value: Schema.Union([OrderPlaced, OrderPaid, OrderCancelled]),
  key: Schema.optionalKey(Schema.NullOr(Schema.String)),
  timestamp: Schema.optionalKey(Schema.String),
  headers: Schema.optionalKey(KafkaHeaders),
  valueSchemaVersion: Schema.optionalKey(Schema.Number)
})
export type OrderPublishMessage = typeof OrderPublishMessage.Type

export const CollectedKafkaSend = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("send"),
    request: ProducerSendRequest
  }),
  Schema.Struct({
    _tag: Schema.Literal("sendBatch"),
    request: ProducerSendBatchRequest
  })
])
export type CollectedKafkaSend = typeof CollectedKafkaSend.Type

export class KafkaProducerError extends Schema.TaggedErrorClass<KafkaProducerError>()("KafkaProducerError", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect())
}) {}

export class OrderEventEncodeError extends Schema.TaggedErrorClass<OrderEventEncodeError>()("OrderEventEncodeError", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect())
}) {}

export type KafkaPublisherError = KafkaProducerError | OrderEventEncodeError | SchemaRegistryClientError

export interface KafkaPublisherService {
  readonly publishWithLatestSchema: (
    event: OrderEvent,
    options?: Omit<OrderPublishOptions, "valueSchemaVersion">
  ) => Effect.Effect<void, KafkaPublisherError>
  readonly publish: (event: OrderEvent, options?: OrderPublishOptions) => Effect.Effect<void, KafkaPublisherError>
  readonly publishBatch: (messages: ReadonlyArray<OrderPublishMessage>) => Effect.Effect<void, KafkaPublisherError>
  readonly publishTombstone: (
    key: string,
    options?: Pick<OrderPublishOptions, "headers" | "timestamp">
  ) => Effect.Effect<void, KafkaPublisherError>
  readonly publishWithoutSchema: (
    value: unknown,
    options?: Pick<OrderPublishOptions, "headers" | "key" | "timestamp">
  ) => Effect.Effect<void, KafkaProducerError>
}

const textEncoder = new TextEncoder()

export class KafkaPublisher extends Context.Service<KafkaPublisher, KafkaPublisherService>()(
  "@effect-avro/examples/KafkaPublisher"
) {
  static readonly layer = (
    producer: ProducerLike,
    options: PublisherLayerOptions = {}
  ): Layer.Layer<KafkaPublisher, never, SchemaRegistry> =>
    Layer.effect(
      KafkaPublisher,
      Effect.gen(function*() {
        const registry = yield* SchemaRegistry
        let connection: Promise<void> | undefined

        const connectOnce = Effect.tryPromise({
          try: async () => {
            connection ??= producer.connect().catch((error) => {
              connection = undefined
              throw error
            })
            await connection
          },
          catch: (cause) => producerError("Unable to connect Kafka producer", cause)
        })

        const send = (request: ProducerSendRequest) =>
          Effect.gen(function*() {
            yield* connectOnce
            yield* Effect.tryPromise({
              try: () => producer.send(request),
              catch: (cause) => producerError(`Unable to publish to topic ${request.topic}`, cause)
            })
          })

        const sendBatch = (request: ProducerSendBatchRequest) =>
          Effect.gen(function*() {
            yield* connectOnce
            yield* Effect.tryPromise({
              try: () => producer.sendBatch(request),
              catch: (cause) => producerError("Unable to publish Kafka batch", cause)
            })
          })

        const encodeValue = (event: OrderEvent, version: number | undefined) =>
          Effect.gen(function*() {
            const registered = yield* (version === undefined
              ? registry.getLatest(orderValueSubject)
              : registry.getVersion(orderValueSubject, version))
            const payload = yield* Effect.try({
              try: () => encodeOrderEvent(event),
              catch: (cause) => new OrderEventEncodeError({
                message: "Unable to encode order event with Effect Schema",
                cause
              })
            })
            return encodeConfluentFrame(registered.id, payload)
          })

        const toProducerMessage = (
          event: OrderEvent,
          value: Uint8Array,
          publishOptions: Omit<OrderPublishOptions, "valueSchemaVersion"> = {}
        ): ProducerMessage => ({
          key: publishOptions.key === null ? null : textEncoder.encode(publishOptions.key ?? event.orderId),
          value,
          ...(publishOptions.timestamp === undefined ? {} : { timestamp: publishOptions.timestamp }),
          headers: mergeHeaders(options.sourceHeaders, publishOptions.headers)
        })

        return KafkaPublisher.of({
          publishWithLatestSchema: (event, publishOptions = {}) =>
            Effect.gen(function*() {
              const value = yield* encodeValue(event, undefined)
              yield* send({
                topic: orderTopic,
                messages: [toProducerMessage(event, value, publishOptions)]
              })
            }),

          publish: (event, publishOptions = {}) =>
            Effect.gen(function*() {
              const value = yield* encodeValue(event, publishOptions.valueSchemaVersion)
              yield* send({
                topic: orderTopic,
                messages: [toProducerMessage(event, value, publishOptions)]
              })
            }),

          publishBatch: (messages) =>
            Effect.gen(function*() {
              const encoded: Array<ProducerMessage> = []
              for (const message of messages) {
                const value = yield* encodeValue(message.value, message.valueSchemaVersion)
                encoded.push(toProducerMessage(message.value, value, message))
              }
              yield* sendBatch({
                topicMessages: [{
                  topic: orderTopic,
                  messages: encoded
                }]
              })
            }),

          publishTombstone: (key, publishOptions = {}) =>
            send({
              topic: orderTopic,
              messages: [{
                key: textEncoder.encode(key),
                value: null,
                ...(publishOptions.timestamp === undefined ? {} : { timestamp: publishOptions.timestamp }),
                headers: mergeHeaders(options.sourceHeaders, publishOptions.headers)
              }]
            }),

          publishWithoutSchema: (value, publishOptions = {}) =>
            Effect.gen(function*() {
              const payload = yield* Effect.try({
                try: () => textEncoder.encode(JSON.stringify(value)),
                catch: (cause) => producerError("Unable to serialize JSON Kafka payload", cause)
              })
              const key = publishOptions.key === undefined
                ? undefined
                : publishOptions.key === null
                ? null
                : textEncoder.encode(publishOptions.key)
              yield* send({
                topic: orderTopic,
                messages: [{
                  value: payload,
                  ...(key === undefined ? {} : { key }),
                  ...(publishOptions.timestamp === undefined ? {} : { timestamp: publishOptions.timestamp }),
                  headers: mergeHeaders(options.sourceHeaders, publishOptions.headers)
                }]
              })
            })
        })
      })
    )
}

export const makeCollectingProducer = (): {
  readonly producer: ProducerLike
  readonly sent: Array<CollectedKafkaSend>
} => {
  const sent: Array<CollectedKafkaSend> = []
  return {
    sent,
    producer: {
      connect: async () => {},
      send: async (request) => {
        sent.push({ _tag: "send", request })
      },
      sendBatch: async (request) => {
        sent.push({ _tag: "sendBatch", request })
      }
    }
  }
}

const mergeHeaders = (
  sourceHeaders: PublisherSourceHeaders | undefined,
  messageHeaders: KafkaHeaders | undefined
): KafkaHeaders => ({
  ...(sourceHeaders ?? {}),
  ...(messageHeaders ?? {})
})

const producerError = (message: string, cause?: unknown): KafkaProducerError =>
  cause === undefined
    ? new KafkaProducerError({ message })
    : new KafkaProducerError({ message, cause })

const registry = makeInMemorySchemaRegistry()
const collected = makeCollectingProducer()

const RegistryLive = SchemaRegistry.layer({
  endpoint: "http://registry.example",
  fetch: registry.fetch
})

const PublisherLive = KafkaPublisher.layer(collected.producer, {
  sourceHeaders: {
    domain: "commerce",
    boundedContext: "orders",
    sourceApplication: "order-service"
  }
})

export const program = Effect.gen(function*() {
  yield* register({
    subject: orderValueSubject,
    schema: OrderEventAvroSchema
  })

  const publisher = yield* KafkaPublisher

  yield* publisher.publishWithLatestSchema(new OrderPlaced({
    orderId: "ord_1000",
    customerId: "cus_123",
    totalCents: 4999,
    occurredAt: "2026-06-24T08:00:00.000Z"
  }), {
    headers: {
      eventType: "OrderPlaced",
      correlationId: "cor_1000"
    }
  })

  yield* publisher.publishBatch([
    {
      value: new OrderPaid({
        orderId: "ord_1000",
        paymentId: "pay_123",
        amountCents: 4999,
        occurredAt: "2026-06-24T08:05:00.000Z"
      }),
      headers: {
        eventType: "OrderPaid",
        correlationId: "cor_1000"
      }
    },
    {
      value: new OrderCancelled({
        orderId: "ord_1001",
        reason: "customer-request",
        occurredAt: "2026-06-24T08:10:00.000Z"
      }),
      headers: {
        eventType: "OrderCancelled",
        correlationId: "cor_1001"
      }
    }
  ])

  yield* publisher.publishTombstone("ord_1000")

  return collected.sent
}).pipe(
  Effect.provide(PublisherLive),
  Effect.provide(RegistryLive)
)

if (import.meta.url === `file://${process.argv[1]}`) {
  Effect.runPromise(program).then((sent) => {
    console.log(sent.map((entry) => entry._tag))
  })
}
