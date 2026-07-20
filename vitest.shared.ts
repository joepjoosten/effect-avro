import type { ViteUserConfig } from "vitest/config"

const config: ViteUserConfig = {
  resolve: {
    alias: {
      "@avro-effect/core": new URL("./packages/core/src/index.ts", import.meta.url).pathname,
      "@avro-effect/kafka": new URL("./packages/kafka/src/index.ts", import.meta.url).pathname,
      "@avro-effect/node": new URL("./packages/node/src/index.ts", import.meta.url).pathname,
      "@avro-effect/schema": new URL("./packages/schema/src/index.ts", import.meta.url).pathname,
      "@avro-effect/schema-registry": new URL("./packages/schema-registry/src/index.ts", import.meta.url).pathname
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
