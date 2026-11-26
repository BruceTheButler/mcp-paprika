import { describe, expect, it } from "vitest";

import { buildEnvOverrides, paprikaConfigSchema } from "./config.js";

const paprika = { email: "test@example.com", password: "test-only" };
describe("lazy sync configuration", () => {
  it("preserves eager default and accepts configurable lazy freshness", () => {
    expect(paprikaConfigSchema.parse({ paprika }).sync.mode).toBe("eager");
    const config = paprikaConfigSchema.parse({ paprika, sync: { mode: "lazy", interval: "7m" } });
    expect(config.sync.interval).toBe(420_000);
    expect(config.paprika.requestTimeout).toBe(20_000);
  });
  it("routes environment settings and bounds request timeouts", () => {
    const config = paprikaConfigSchema.parse(
      buildEnvOverrides({
        PAPRIKA_EMAIL: paprika.email,
        PAPRIKA_PASSWORD: paprika.password,
        PAPRIKA_SYNC_MODE: "lazy",
        PAPRIKA_REQUEST_TIMEOUT: "3s",
      }),
    );
    expect(config.sync.mode).toBe("lazy");
    expect(config.paprika.requestTimeout).toBe(3000);
    expect(paprikaConfigSchema.safeParse({ paprika: { ...paprika, requestTimeout: "999ms" } }).success).toBe(false);
    expect(paprikaConfigSchema.safeParse({ paprika, sync: { mode: "unknown" } }).success).toBe(false);
  });
  it("rejects an uncoordinated derived semantic index in lazy mode", () => {
    const result = paprikaConfigSchema.safeParse({
      paprika,
      sync: { mode: "lazy" },
      features: { embeddings: { model: "test-model" } },
    });
    expect(result.success).toBe(false);
  });
});
