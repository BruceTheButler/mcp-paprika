/** Real-process fixture: isolated fake cloud and cache, never the user's account. */
import { appendFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

import { okAsync } from "neverthrow";

import { DiskCache } from "../../src/cache/disk-cache.js";
import { reloadStore } from "../../src/cache/hydrate.js";
import { EntityStore } from "../../src/entity/store.js";
import { createLazySync } from "../../src/kernel/lazy-sync.js";

const root = process.argv[2]!;
const mode = process.argv[3];
type Row = { uid: string; value: string };
class Store extends EntityStore<Row, string> {}
const store = new Store();
const cache = new DiskCache<Row>({ subdir: join(root, "rows"), getKey: (r) => r.uid, parse: (r) => r as Row });
(await cache.init())._unsafeUnwrap();
const access = createLazySync({
  cacheDir: root,
  account: "fixture@example.com",
  maxAgeMs: 900_000,
  writeGraceMs: 60_000,
  reload: () => reloadStore(cache, store),
  authenticate: () => okAsync(undefined),
  ready: async () => {},
  sync: async () => {
    await appendFile(join(root, "sync-events"), "sync\n");
    if (mode === "hang") {
      await writeFile(join(root, "started"), "started");
      await new Promise(() => {
        setInterval(() => {}, 1000);
      });
    }
    await sleep(200);
    for (const row of (await cache.getAll())._unsafeUnwrap()) (await cache.remove(row.uid))._unsafeUnwrap();
    const rows = [{ uid: "new", value: "fresh" }];
    (await cache.put(rows[0]!))._unsafeUnwrap();
    (await cache.flush())._unsafeUnwrap();
    store.load(rows);
    return { outcome: "ok" as const, results: [] };
  },
});
const result = await access.run(() => store.getAll(), true);
process.stdout.write(
  JSON.stringify(
    result.match(
      (rows) => ({ rows }),
      (e) => ({ error: e.message }),
    ),
  ),
);
