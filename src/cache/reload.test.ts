import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { makeRecipe } from "../../test/domains/recipe/__fixtures__/recipes.js";
import { makeRecipeCache, useTempDir } from "../../test/support/disk-caches.js";
import { RecipeStore } from "../domains/recipe/store.js";
import { reloadStore } from "./hydrate.js";

const tmp = useTempDir("paprika-reload-");
beforeEach(() => tmp.setup());
afterEach(() => tmp.teardown());

describe("shared cache reload", () => {
  it("reloads externally changed hashes before an unrelated writer flushes", async () => {
    const a = makeRecipeCache(tmp.dir()),
      b = makeRecipeCache(tmp.dir());
    (await a.init())._unsafeUnwrap();
    (await b.init())._unsafeUnwrap();
    const recipe = makeRecipe();
    (await a.put(recipe))._unsafeUnwrap();
    (await a.flush())._unsafeUnwrap();
    const store = new RecipeStore();
    (await reloadStore(b, store))._unsafeUnwrap();
    expect(store.get(recipe.uid)).toEqual(recipe);
    expect(b.diff([{ uid: recipe.uid, hash: recipe.hash }])._unsafeUnwrap().added).toEqual([]);
    (await b.flush())._unsafeUnwrap();
    const c = makeRecipeCache(tmp.dir());
    (await c.init())._unsafeUnwrap();
    expect(c.diff([{ uid: recipe.uid, hash: recipe.hash }])._unsafeUnwrap().added).toEqual([]);
  });

  it("drops a deleted last record, old hash and failed-operation buffer", async () => {
    const a = makeRecipeCache(tmp.dir()),
      b = makeRecipeCache(tmp.dir());
    (await a.init())._unsafeUnwrap();
    const recipe = makeRecipe();
    (await a.put(recipe))._unsafeUnwrap();
    (await a.flush())._unsafeUnwrap();
    (await b.init())._unsafeUnwrap();
    const store = new RecipeStore();
    (await reloadStore(b, store))._unsafeUnwrap();
    (await b.put({ ...recipe, name: "uncommitted stale buffer" }))._unsafeUnwrap();
    (await a.remove(recipe.uid))._unsafeUnwrap();
    (await a.flush())._unsafeUnwrap();
    (await reloadStore(b, store))._unsafeUnwrap();
    expect(store.size).toBe(0);
    expect(store.hasSynced).toBe(true);
    expect((await b.getAll())._unsafeUnwrap()).toEqual([]);
    expect(b.diff([])._unsafeUnwrap().removed).toEqual([]);
  });
});
