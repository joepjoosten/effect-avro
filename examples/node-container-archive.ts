import { Effect } from "effect"
import { readFile, writeFile } from "@effect-avro/node"
import {
  OrderSnapshot,
  OrderSnapshotAvroSchema
} from "./domain.js"

const snapshots = [
  new OrderSnapshot({
    orderId: "ord_1000",
    customerId: "cus_123",
    status: "placed",
    totalCents: 4999,
    updatedAt: "2026-06-24T08:00:00.000Z"
  }),
  new OrderSnapshot({
    orderId: "ord_1001",
    customerId: "cus_456",
    status: "paid",
    totalCents: 2500,
    updatedAt: "2026-06-24T08:01:00.000Z"
  })
]

export const archiveSnapshots = (path: string) =>
  Effect.gen(function*() {
    yield* writeFile(path, OrderSnapshotAvroSchema, snapshots, {
      codec: "deflate",
      metadata: {
        source: "orders-service"
      }
    })

    return yield* readFile<OrderSnapshot>(path)
  })
