import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { err, ok, okAsync } from "neverthrow";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { DomainCtx, ErasedModule } from "./registry.js";

import { connectInMemoryMcp } from "../../test/support/in-memory-mcp.js";
import { makeKernelInfra } from "../../test/support/kernel-harness.js";
import { DiskCache } from "../cache/disk-cache.js";
import { reloadStore } from "../cache/hydrate.js";
import { EntityStore } from "../entity/store.js";
import { buildBrandedServer } from "../server/build.js";
import { paprikaConfigSchema } from "../utils/config.js";
import { buildKernel } from "./registry.js";
import { defineResource } from "./resource.js";
import { defineTool } from "./tool.js";

type Row = { uid: string; value: string };
class Store extends EntityStore<Row, string> {}
type State = { store: Store; cache: DiskCache<Row> };
const config = () =>
  paprikaConfigSchema.parse({
    paprika: { email: "test@example.com", password: "test-only" },
    sync: { mode: "lazy", interval: "15m" },
  });
let dir: string;
beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "lazy-kernel-"));
});
afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

describe("lazy MCP protocol and shared snapshots", () => {
  it("connects/lists/reads resources without syncing, then refreshes before guards and serves another session's writes", async () => {
    const reconcile = vi.fn();
    const remote: Row[] = [{ uid: "a", value: "from-cloud" }];
    const mod = {
      id: "synthetic",
      dependsOn: [],
      async build() {
        const store = new Store();
        const cache = new DiskCache<Row>({ subdir: join(dir, "rows"), getKey: (r) => r.uid, parse: (r) => r as Row });
        (await cache.init())._unsafeUnwrap();
        const state = { store, cache };
        return {
          state,
          api: {},
          tools: [
            defineTool(
              {
                name: "read_rows",
                title: "Rows",
                description: "Read rows",
                annotations: { readOnlyHint: true },
                inputSchema: {},
              },
              [
                (ctx: DomainCtx<State, never>) =>
                  ctx.state.store.hasSynced ? ok(undefined) : err({ content: [{ type: "text", text: "not synced" }] }),
              ],
              (ctx: DomainCtx<State, never>) => () => ({
                content: [{ type: "text", text: JSON.stringify(ctx.state.store.getAll()) }],
              }),
            ),
            defineTool(
              {
                name: "write_row",
                title: "Write",
                description: "Write a row",
                annotations: { readOnlyHint: false },
                inputSchema: {},
              },
              (ctx: DomainCtx<State, never>) => async () => {
                (await ctx.state.cache.put({ uid: "b", value: "local-write" }))._unsafeUnwrap();
                (await ctx.state.cache.flush())._unsafeUnwrap();
                ctx.state.store.set({ uid: "b", value: "local-write" });
                return { content: [{ type: "text", text: "saved" }] };
              },
            ),
          ],
          resources: [
            defineResource(
              { primary: { name: "test", uriTemplate: "ui://test/{id}", description: "test" } },
              (_ctx: DomainCtx<State, never>) => ({
                read: async (uri) => ({ contents: [{ uri: uri.href, text: "cached-resource" }] }),
              }),
            ),
          ],
          reload: () => reloadStore(cache, store),
          flush: () => cache.flush(),
          syncs: [
            {
              tier: "core",
              reconcile: () => {
                reconcile();
                return cache
                  .getAll()
                  .andThen((rows) => {
                    for (const row of rows) store.delete(row.uid);
                    store.load(remote);
                    return okAsync(undefined);
                  })
                  .andThen(() => cache.put(remote[0]!));
              },
            },
          ],
        };
      },
    } as unknown as ErasedModule;
    const aInfra = makeKernelInfra({ cacheDir: dir, config: config() });
    const bInfra = makeKernelInfra({ cacheDir: dir, config: config() });
    const aKernel = await buildKernel(aInfra, [mod]);
    const bKernel = await buildKernel(bInfra, [mod]);
    const aServer = buildBrandedServer(),
      bServer = buildBrandedServer();
    aKernel.registerAll(aServer);
    bKernel.registerAll(bServer);
    const a = await connectInMemoryMcp(aServer),
      b = await connectInMemoryMcp(bServer);
    try {
      await a.client.listTools();
      await a.client.listResources();
      await a.client.listResourceTemplates();
      await a.client.readResource({ uri: "ui://test/one" });
      expect(reconcile).not.toHaveBeenCalled();
      expect(aInfra.client.authenticate).not.toHaveBeenCalled();
      const first = await a.client.callTool({ name: "read_rows" });
      expect(first.isError).not.toBe(true);
      expect(JSON.stringify(first)).toContain("from-cloud");
      expect(reconcile).toHaveBeenCalledOnce();
      await a.client.callTool({ name: "write_row" });
      const other = await b.client.callTool({ name: "read_rows" });
      expect(JSON.stringify(other)).toContain("local-write");
      expect(reconcile).toHaveBeenCalledOnce();
      // Lazy shutdown/diagnostic flush must not publish this session's stale index.
      (await bKernel.flushAll())._unsafeUnwrap();
    } finally {
      await a.close();
      await b.close();
    }
  });
});
