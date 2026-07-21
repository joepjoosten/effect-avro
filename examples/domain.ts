import { Schema } from "effect"
import type { AvroSchema } from "@effect-avro/core"
import { avro, Long, toAvroSchema } from "@effect-avro/schema"

export class OrderPlaced extends Schema.TaggedClass<OrderPlaced>()("OrderPlaced", {
  orderId: Schema.String,
  customerId: Schema.String,
  totalCents: Long,
  occurredAt: Schema.String
}) {}

export class OrderPaid extends Schema.TaggedClass<OrderPaid>()("OrderPaid", {
  orderId: Schema.String,
  paymentId: Schema.String,
  amountCents: Long,
  occurredAt: Schema.String
}) {}

export class OrderCancelled extends Schema.TaggedClass<OrderCancelled>()("OrderCancelled", {
  orderId: Schema.String,
  reason: Schema.String,
  occurredAt: Schema.String
}) {}

export const OrderEvent = Schema.Union([
  OrderPlaced,
  OrderPaid,
  OrderCancelled
])

export type OrderEvent = OrderPlaced | OrderPaid | OrderCancelled

export const orderTopic = "orders"
export const orderValueSubject = `${orderTopic}-value`

export const OrderEventAvroSchema = toAvroSchema(OrderEvent, {
  name: "OrderEvent",
  namespace: "example.order"
}) as AvroSchema

export const OrderEventCodec = avro(OrderEvent, {
  avroSchema: OrderEventAvroSchema
})

export const encodeOrderEvent = Schema.encodeSync(OrderEventCodec)
export const decodeOrderEvent = Schema.decodeUnknownSync(OrderEventCodec)

export class OrderSnapshot extends Schema.Class<OrderSnapshot>("OrderSnapshot")({
  orderId: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["placed", "paid", "cancelled"]),
  totalCents: Long,
  updatedAt: Schema.String
}) {}

export const OrderSnapshotAvroSchema = toAvroSchema(OrderSnapshot, {
  name: "OrderSnapshot",
  namespace: "example.order"
}) as AvroSchema
