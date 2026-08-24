import { Effect, Schema } from "effect"
import type { AvroSchema } from "@effect-avro/core"
import { Long, toAvroSchema } from "@effect-avro/schema"
import { checkCompatibility, register, SchemaRegistry } from "@effect-avro/schema-registry"
import {
  OrderCancelled,
  OrderPaid,
  orderValueSubject
} from "./domain.js"
import { makeInMemorySchemaRegistry } from "./support/in-memory-registry.js"

export class OrderPlacedV2 extends Schema.TaggedClass<OrderPlacedV2>()("OrderPlaced", {
  orderId: Schema.String,
  customerId: Schema.String,
  totalCents: Long,
  occurredAt: Schema.String,
  promotionCode: Schema.optional(Schema.String)
}) {}

export const OrderEventV2 = Schema.Union([
  OrderPlacedV2,
  OrderPaid,
  OrderCancelled
])

export const OrderEventV2AvroSchema = toAvroSchema(OrderEventV2, {
  name: "OrderEvent",
  namespace: "example.order"
}) as AvroSchema

export class IncompatibleOrderEventSchema
  extends Schema.TaggedError<IncompatibleOrderEventSchema>()("IncompatibleOrderEventSchema", {
    subject: Schema.String
  })
{}

export const registerCompatibleOrderEventV2 = Effect.gen(function*() {
  const compatibility = yield* checkCompatibility({
    subject: orderValueSubject,
    schema: OrderEventV2AvroSchema
  })

  if (!compatibility.isCompatible) {
    return yield* new IncompatibleOrderEventSchema({ subject: orderValueSubject })
  }

  return yield* register({
    subject: orderValueSubject,
    schema: OrderEventV2AvroSchema
  })
})

const registry = makeInMemorySchemaRegistry()

export const program = registerCompatibleOrderEventV2.pipe(
  Effect.provide(SchemaRegistry.layer({
    endpoint: "http://registry.example",
    fetch: registry.fetch
  }))
)
