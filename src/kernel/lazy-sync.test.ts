import { execFile, spawn } from "node:child_process";
import { mkdtemp, readFile, rm, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { promisify } from "node:util";

import { errAsync, okAsync } from "neverthrow";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { SyncCycle } from "./sync.js";

import { createLazySync } from "./lazy-sync.js";

const runFile = promisify(execFile);
let dir: string;
let now: number;
beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "paprika-lazy-"));
  now = 1_000_000;
});
afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});
function harness(extra = {}) {
  const sync = vi.fn(async (): Promise<SyncCycle> => ({ outcome: "ok", results: [] }));
  const auth = vi.fn(() => okAsync(undefined));
  const reload = vi.fn(() => okAsync(undefined));
  const ready = vi.fn(async () => {});
  const access = createLazySync({
    cacheDir: dir,
    account: "test@example.com",
    maxAgeMs: 900_000,
    writeGraceMs: 60_000,
    reload,
    authenticate: auth,
    sync,
    ready,
    now: () => now,
    ...extra,
  });
  return { access, sync, auth, reload, ready };
}
const state = async () =>
  JSON.parse(await readFile(join(dir, ".lazy-sync-state"), "utf8")) as { lastSuccess: number; dirty: boolean };

describe("demand-driven shared sync", () => {
  it("does no work on construction; resource access reloads but does not authenticate or sync", async () => {
    const h = harness();
    expect(h.reload).not.toHaveBeenCalled();
    (await h.access.run(() => "resource", true, false))._unsafeUnwrap();
    expect(h.reload).toHaveBeenCalledOnce();
    expect(h.auth).not.toHaveBeenCalled();
    expect(h.sync).not.toHaveBeenCalled();
  });
  it("syncs on first tool use and exactly at expiry, measuring freshness from completion", async () => {
    const h = harness();
    h.sync.mockImplementation(async () => {
      now += 100;
      return { outcome: "ok", results: [] };
    });
    (await h.access.run(() => "one", true))._unsafeUnwrap();
    expect((await state()).lastSuccess).toBe(now);
    now += 899_999;
    (await h.access.run(() => "two", true))._unsafeUnwrap();
    expect(h.sync).toHaveBeenCalledOnce();
    now++;
    (await h.access.run(() => "three", true))._unsafeUnwrap();
    expect(h.sync).toHaveBeenCalledTimes(2);
  });
  it("coalesces simultaneous stale callers from separate controllers", async () => {
    const a = harness(),
      b = harness();
    await Promise.all([
      a.access.run(async () => {
        await sleep(40);
        return "a";
      }, true),
      b.access.run(() => "b", true),
    ]);
    expect(a.sync.mock.calls.length + b.sync.mock.calls.length).toBe(1);
    expect(a.reload).toHaveBeenCalledOnce();
    expect(b.reload).toHaveBeenCalledOnce();
  });
  it.each<SyncCycle>([
    { outcome: "core_aborted", results: [] },
    { outcome: "core_partial", results: [] },
    { outcome: "flush_failed", results: [] },
    { outcome: "ok", complete: false, results: [] },
  ])("never marks a failed or degraded cycle fresh: $outcome", async (cycle) => {
    const h = harness();
    h.sync.mockResolvedValue(cycle);
    const body = vi.fn(() => "wrong");
    expect((await h.access.run(body, true))._unsafeUnwrapErr().message).toContain("did not complete");
    expect((await state()).lastSuccess).toBe(0);
    expect(body).not.toHaveBeenCalled();
    expect((await h.access.run(body, true))._unsafeUnwrapErr().message).toContain("recently failed");
    now += 30_000;
    h.sync.mockResolvedValue({ outcome: "ok", results: [] });
    expect((await h.access.run(body, true))._unsafeUnwrap()).toBe("wrong");
  });
  it("protects completed writes across processes without changing last successful sync", async () => {
    const a = harness();
    (await a.access.run(() => "read", true))._unsafeUnwrap();
    now += 899_999;
    (await a.access.run(() => ({ content: [] }), false))._unsafeUnwrap();
    const success = (await state()).lastSuccess;
    now += 2;
    const b = harness();
    (await b.access.run(() => "protected cache", true))._unsafeUnwrap();
    expect(b.sync).not.toHaveBeenCalled();
    expect((await state()).lastSuccess).toBe(success);
    now += 60_000;
    (await b.access.run(() => "fresh", true))._unsafeUnwrap();
    expect(b.sync).toHaveBeenCalledOnce();
  });
  it("checkpoints a failed or interrupted write and blocks reads until reconciliation", async () => {
    const a = harness();
    (await a.access.run(() => "read", true))._unsafeUnwrap();
    (await a.access.run(() => ({ isError: true }), false))._unsafeUnwrap();
    expect((await state()).dirty).toBe(true);
    const b = harness();
    expect((await b.access.run(() => "unsafe", true))._unsafeUnwrapErr().message).toContain("incomplete");
    now += 60_000;
    (await b.access.run(() => "reconciled", true))._unsafeUnwrap();
    expect((await state()).dirty).toBe(false);
  });
  it("releases the lock on callback failure and keeps the dirty checkpoint", async () => {
    const a = harness();
    const failed = await a.access.run(() => Promise.reject(new Error("interrupted write")), false);
    expect(failed._unsafeUnwrapErr().message).toContain("interrupted write");
    expect((await state()).dirty).toBe(true);
    (await harness().access.run(() => "resource", true, false))._unsafeUnwrap();
  });
  it("fails closed on reload errors, account mismatch and corrupt metadata", async () => {
    (await harness().access.run(() => "read", true))._unsafeUnwrap();
    const failedReload = harness({
      reload: () => errAsync({ context: "test", message: "broken cache", cause: undefined }),
    });
    expect((await failedReload.access.run(() => "wrong", true))._unsafeUnwrapErr().message).toBe("broken cache");
    const other = harness({ account: "other@example.com" });
    expect((await other.access.run(() => "wrong", true))._unsafeUnwrapErr().message).toContain("different");
    await writeFile(join(dir, ".lazy-sync-state"), "broken-json");
    expect((await harness().access.run(() => "wrong", true))._unsafeUnwrapErr()).toBeDefined();
  });
  it("treats a future success timestamp as stale after clock rollback", async () => {
    (await harness().access.run(() => "read", true))._unsafeUnwrap();
    now -= 100_000;
    const b = harness();
    (await b.access.run(() => "read", true))._unsafeUnwrap();
    expect(b.sync).toHaveBeenCalledOnce();
  });
});

describe("real process coordination", () => {
  const fixture = resolve("test/support/lazy-sync-process.ts");
  const worker = () => runFile(process.execPath, ["--import", "tsx", fixture, dir]);
  it("runs only one sync and gives both processes the new snapshot, including deletions", async () => {
    const rows = join(dir, "rows");
    const { mkdir } = await import("node:fs/promises");
    await mkdir(rows);
    await writeFile(join(rows, "old.json"), JSON.stringify({ uid: "old", value: "removed" }));
    const results = await Promise.all([worker(), worker()]);
    for (const result of results) expect(JSON.parse(result.stdout)).toEqual({ rows: [{ uid: "new", value: "fresh" }] });
    expect(await readFile(join(dir, "sync-events"), "utf8")).toBe("sync\n");
  }, 15_000);
  it("recovers an abandoned stale lease after the owner dies", async () => {
    const owner = spawn(process.execPath, ["--import", "tsx", fixture, dir, "hang"], { stdio: "ignore" });
    const exited = new Promise<void>((resolveExit) => owner.once("exit", () => resolveExit()));
    try {
      for (let i = 0; i < 100; i++) {
        const started = await readFile(join(dir, "started"), "utf8").catch(() => "");
        if (started) break;
        await sleep(20);
      }
      expect(await readFile(join(dir, "started"), "utf8")).toBe("started");
      owner.kill("SIGKILL");
      await exited;
      const old = new Date(Date.now() - 180_000);
      await utimes(join(dir, ".lazy-sync-state.lock"), old, old);
      // The killed owner's attempt backoff also expires before retrying.
      const path = join(dir, ".lazy-sync-state");
      const checkpoint = JSON.parse(await readFile(path, "utf8")) as Record<string, unknown>;
      checkpoint["lastAttempt"] = 0;
      await writeFile(path, JSON.stringify(checkpoint));
      const result = await worker();
      expect(JSON.parse(result.stdout)).toEqual({ rows: [{ uid: "new", value: "fresh" }] });
    } finally {
      owner.kill("SIGKILL");
    }
  }, 15_000);
});
