import { Effect, Schema } from "effect"
import { KafkaMessage } from "@effect-avro/kafka"
import { InvalidRegistryFrame, SchemaRegistryHttpError } from "@effect-avro/schema-registry"
import {
  decodeOrderMessage,
  MissingKafkaValue,
  OrderEventDecodeError
} from "./registry-kafka-events.js"

export const DeadLetterMessage = Schema.Struct({
  topic: Schema.String,
  original: KafkaMessage,
  reason: Schema.String
})
export type DeadLetterMessage = typeof DeadLetterMessage.Type

export class UnknownOrderEventFailure
  extends Schema.TaggedErrorClass<UnknownOrderEventFailure>()("UnknownOrderEventFailure", {
    cause: Schema.Defect()
  })
{}

export const decodeOrDeadLetter = (message: KafkaMessage) =>
  decodeOrderMessage(message).pipe(
    Effect.map((event) => ({ _tag: "Decoded" as const, event })),
    Effect.catchTag("MissingKafkaValue", (error: MissingKafkaValue) =>
      Effect.succeed(deadLetter(message, `Kafka message on ${error.topic} has no value`))
    ),
    Effect.catchTag("InvalidRegistryFrame", (error: InvalidRegistryFrame) =>
      Effect.succeed(deadLetter(message, error.message))
    ),
    Effect.catchTag("SchemaRegistryHttpError", (error: SchemaRegistryHttpError) =>
      Effect.succeed(deadLetter(message, `Registry lookup failed with HTTP ${error.status}`))
    ),
    Effect.catchTag("OrderEventDecodeError", (error: OrderEventDecodeError) =>
      Effect.succeed(deadLetter(message, error.message))
    ),
    Effect.catch((cause) => Effect.fail(new UnknownOrderEventFailure({ cause })))
  )

const deadLetter = (original: KafkaMessage, reason: string) => ({
  _tag: "DeadLetter" as const,
  message: {
    topic: `${original.topic}.dead-letter`,
    original,
    reason
  } satisfies DeadLetterMessage
})
