import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    globals: true,
    setupFiles: ["./tests/setup.ts"],
    hookTimeout: 20000,
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
      include: ["src/*"]
    }
  }
});
