import { Schema } from "effect"

export const SelfManagedKafkaRecord = Schema.Struct({
  topic: Schema.String,
  partition: Schema.Number,
  offset: Schema.String,
  key: Schema.NullOr(Schema.String),
  value: Schema.NullOr(Schema.String),
  timestamp: Schema.Number
})
export type SelfManagedKafkaRecord = typeof SelfManagedKafkaRecord.Type

export const dedupeSelfManagedKafkaRecords = (
  records: ReadonlyArray<SelfManagedKafkaRecord>
): Array<SelfManagedKafkaRecord> => dedupeDecodedKafkaRecordsByValue(records, [["key"]], ["timestamp"])

export const dedupeDecodedKafkaRecordsByValue = <A>(
  records: ReadonlyArray<A>,
  propertyPaths: ReadonlyArray<ReadonlyArray<string>>,
  timestampPath?: ReadonlyArray<string>
): Array<A> => {
  const byKey = new Map<string, A>()

  for (const record of records) {
    const key = propertyPaths.map((path) => String(readPath(record, path))).join("|")
    const existing = byKey.get(key)

    if (existing === undefined || isNewer(record, existing, timestampPath)) {
      byKey.set(key, record)
    }
  }

  return [...byKey.values()]
}

export const orderProjectionDedupeExample = dedupeDecodedKafkaRecordsByValue(
  [
    {
      orderId: "ord_1000",
      customerId: "cus_123",
      status: "placed",
      updatedAt: "2026-06-24T08:00:00.000Z"
    },
    {
      orderId: "ord_1000",
      customerId: "cus_123",
      status: "paid",
      updatedAt: "2026-06-24T08:05:00.000Z"
    }
  ],
  [["orderId"], ["customerId"]],
  ["updatedAt"]
)

const isNewer = <A>(
  next: A,
  previous: A,
  timestampPath: ReadonlyArray<string> | undefined
): boolean => {
  if (timestampPath === undefined) {
    return true
  }

  const nextTimestamp = readPath(next, timestampPath)
  const previousTimestamp = readPath(previous, timestampPath)
  if (typeof nextTimestamp === "number" && typeof previousTimestamp === "number") {
    return nextTimestamp >= previousTimestamp
  }
  return String(nextTimestamp) >= String(previousTimestamp)
}

const readPath = (value: unknown, path: ReadonlyArray<string>): unknown => {
  let current = value
  for (const segment of path) {
    if (current === null || typeof current !== "object") {
      return undefined
    }
    current = (current as Record<string, unknown>)[segment]
  }
  return current
}
