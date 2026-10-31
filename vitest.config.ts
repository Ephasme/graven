import { defineConfig } from "vitest/config";

const alias = {
  "@ephasme/graven": new URL("packages/graven/src/index.ts", import.meta.url).pathname,
  "@ephasme/graven-sql": new URL("packages/graven-sql/src/index.ts", import.meta.url).pathname,
};

export default defineConfig({
  test: {
    coverage: {
      provider: "v8",
      reporter: ["text", "html", "lcov"],
      include: ["packages/*/src/**/*.ts"],
      exclude: ["packages/*/src/**/*.test.ts", "packages/*/src/index.ts"],
      thresholds: {
        lines: 90,
        statements: 90,
        functions: 90,
        branches: 90,
      },
    },
    projects: [
      {
        resolve: { alias },
        test: {
          name: "unit",
          include: ["packages/*/test/**/*.test.ts", "packages/*/src/**/*.test.ts"],
        },
      },
      {
        resolve: { alias },
        test: {
          name: "integration",
          include: ["packages/*/test-integration/**/*.test.ts"],
          testTimeout: 60_000,
          hookTimeout: 60_000,
          fileParallelism: false,
          // Non-UTC on purpose: a CI runner is UTC, where a driver reading a zone-less
          // datetime in local time is indistinguishable from reading it as UTC.
          env: { TZ: "Europe/Paris" },
        },
      },
    ],
  },
});
