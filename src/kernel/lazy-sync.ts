import { createHash } from "node:crypto";
import { mkdir, readFile } from "node:fs/promises";
import { join } from "node:path";

import { Mutex } from "async-mutex";
import { err, ok, Result, ResultAsync } from "neverthrow";
import lockfile from "proper-lockfile";
import { z } from "zod";

import type { CacheError } from "../cache/disk-cache.js";
import type { SyncCycle } from "./sync.js";

import { cacheError, writeFileAtomic } from "../cache/disk-cache.js";
import { isNodeError } from "../utils/errors.js";

const State = z.object({
  version: z.literal(1),
  account: z.string(),
  lastSuccess: z.number().nonnegative(),
  lastAttempt: z.number().nonnegative(),
  writeUntil: z.number().nonnegative(),
  dirty: z.boolean(),
});
type SharedState = z.infer<typeof State>;

export interface ToolAccess {
  run<T>(body: () => T | Promise<T>, readOnly: boolean, sync?: boolean): Promise<Result<T, CacheError>>;
}

interface LazySyncOptions {
  readonly cacheDir: string;
  readonly account: string;
  readonly maxAgeMs: number;
  readonly writeGraceMs: number;
  readonly reload: () => ResultAsync<void, CacheError>;
  readonly authenticate: () => ResultAsync<void, { readonly message: string }>;
  readonly sync: () => Promise<SyncCycle>;
  readonly ready: () => Promise<void>;
  readonly now?: () => number;
}

/**
 * One local-filesystem cache root, one operation at a time. Hold the lease across
 * reload, sync AND tool commits: a sync-only lease lets stale recipe hash indexes
 * overwrite newer ones. Heartbeats keep slow cloud requests from expiring a live
 * owner; proper-lockfile handles abandoned locks after process death.
 */
export function createLazySync(options: LazySyncOptions): ToolAccess {
  const mutex = new Mutex();
  const path = join(options.cacheDir, ".lazy-sync-state");
  const account = createHash("sha256").update(options.account.trim().toLowerCase()).digest("hex");
  const now = options.now ?? Date.now;
  let authenticated = false;
  const failure = (message: string): CacheError => ({ context: "lazy sync", message, cause: undefined });
  const authenticate = async (): Promise<Result<void, CacheError>> => {
    if (authenticated) return ok(undefined);
    return (await options.authenticate())
      .map(() => {
        authenticated = true;
      })
      .mapErr((e) => failure(e.message));
  };
  const save = (state: SharedState): ResultAsync<void, CacheError> => writeFileAtomic(path, JSON.stringify(state));
  const readState = async (): Promise<Result<SharedState, CacheError>> => {
    const raw = await ResultAsync.fromPromise(readFile(path, "utf8"), cacheError("read lazy sync state"));
    return raw.match(
      (value) =>
        Result.fromThrowable(() => State.parse(JSON.parse(value) as unknown), cacheError("parse lazy sync state"))(),
      (e) =>
        isNodeError(e.cause) && e.cause.code === "ENOENT"
          ? ok({ version: 1 as const, account, lastSuccess: 0, lastAttempt: 0, writeUntil: 0, dirty: false })
          : err(e),
    );
  };

  return {
    run<T>(body: () => T | Promise<T>, readOnly: boolean, sync = true): Promise<Result<T, CacheError>> {
      return mutex.runExclusive(async () => {
        // This foreign-I/O boundary converts filesystem and callback rejections to
        // explicit errors; no caller may mistake a failed read for an empty plan.
        return ResultAsync.fromPromise(
          (async (): Promise<Result<T, CacheError>> => {
            await mkdir(options.cacheDir, { recursive: true, mode: 0o700 });
            let compromised: Error | undefined;
            const release = await lockfile.lock(path, {
              realpath: false,
              stale: 120_000,
              update: 10_000,
              retries: { retries: 250, factor: 1, minTimeout: 100, maxTimeout: 100 },
              onCompromised: (e) => {
                compromised = e;
              },
            });
            try {
              const loaded = await readState();
              const stateError = loaded.match(
                () => undefined,
                (e) => e,
              );
              if (stateError !== undefined) return err(stateError);
              let state = loaded.match(
                (v) => v,
                () => undefined,
              );
              if (state === undefined) return err(failure("Unable to load shared sync state."));
              if (state.account !== account)
                return err(failure("Cache belongs to a different Paprika account; use a separate cache directory."));
              const reload = await options.reload();
              const reloadError = reload.match(
                () => undefined,
                (e) => e,
              );
              if (reloadError !== undefined) return err(reloadError);
              if (compromised !== undefined) return err(failure("Shared cache lock was lost; retry the operation."));

              const stale =
                state.dirty ||
                state.lastSuccess === 0 ||
                now() < state.lastSuccess ||
                now() - state.lastSuccess >= options.maxAgeMs;
              if (sync && stale) {
                // A completed local write is protected for the cloud propagation
                // window across ALL processes. This is not a successful-sync stamp.
                if (now() < state.writeUntil) {
                  if (state.dirty || state.lastSuccess === 0)
                    return err(failure("A previous write may be incomplete; retry after its propagation window."));
                } else {
                  if (state.lastAttempt > 0 && now() - state.lastAttempt < 30_000)
                    return err(failure("Paprika sync recently failed; retry in 30 seconds."));
                  // Invalidate freshness BEFORE a reconcile can change files. A
                  // killed/partial cycle must not leave an apparently good snapshot.
                  state = { ...state, lastSuccess: 0, lastAttempt: now() };
                  const checkpoint = await save(state);
                  const checkpointError = checkpoint.match(
                    () => undefined,
                    (e) => e,
                  );
                  if (checkpointError !== undefined) return err(checkpointError);
                  const auth = await authenticate();
                  const authError = auth.match(
                    () => undefined,
                    (e) => e,
                  );
                  if (authError !== undefined) return err(authError);
                  const cycle = await options.sync();
                  if (cycle.outcome !== "ok" || cycle.complete === false)
                    return err(
                      failure("Paprika sync did not complete; cached data was not marked fresh. Retry shortly."),
                    );
                  if (compromised !== undefined) return err(failure("Shared cache lock was lost during sync."));
                  state = { ...state, lastSuccess: now(), lastAttempt: 0, dirty: false };
                  const saved = await save(state);
                  const savedError = saved.match(
                    () => undefined,
                    (e) => e,
                  );
                  if (savedError !== undefined) return err(savedError);
                }
              }
              if (sync) await options.ready();
              if (!readOnly) {
                const auth = await authenticate();
                const authError = auth.match(
                  () => undefined,
                  (e) => e,
                );
                if (authError !== undefined) return err(authError);
                state = { ...state, dirty: true, writeUntil: now() + options.writeGraceMs };
                const before = await save(state);
                const beforeError = before.match(
                  () => undefined,
                  (e) => e,
                );
                if (beforeError !== undefined) return err(beforeError);
              }
              const result = await body();
              if (compromised !== undefined)
                return err(
                  failure("Shared cache lock was lost during the operation; recheck before retrying a write."),
                );
              if (!readOnly) {
                const failed =
                  typeof result === "object" && result !== null && "isError" in result && result.isError === true;
                state = { ...state, dirty: failed, writeUntil: now() + options.writeGraceMs };
                const after = await save(state);
                const afterError = after.match(
                  () => undefined,
                  (e) => e,
                );
                if (afterError !== undefined) return err(afterError);
              }
              return ok(result);
            } finally {
              if (compromised === undefined) await release();
            }
          })(),
          cacheError("shared Paprika operation"),
        ).andThen((result) => result);
      });
    },
  };
}
