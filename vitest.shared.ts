import type { ViteUserConfig } from "vitest/config"

const config: ViteUserConfig = {
  resolve: {
    alias: {
      "@effect-avro/core": new URL("./packages/core/src/index.ts", import.meta.url).pathname,
      "@effect-avro/kafka": new URL("./packages/kafka/src/index.ts", import.meta.url).pathname,
      "@effect-avro/node": new URL("./packages/node/src/index.ts", import.meta.url).pathname,
      "@effect-avro/schema": new URL("./packages/schema/src/index.ts", import.meta.url).pathname,
      "@effect-avro/schema-registry": new URL("./packages/schema-registry/src/index.ts", import.meta.url).pathname
    }
  },
  oxc: {
    target: "es2022"
  },
  test: {
    include: ["test/**/*.test.ts"],
    exclude: ["**/build/**", "**/dist/**", "**/node_modules/**"],
    sequence: {
      concurrent: true
    }
  }
}

export default config
