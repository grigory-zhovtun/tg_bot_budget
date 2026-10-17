import { afterEach, describe, expect, it, vi } from "vitest";
import { CACHE_KEY, FRESH_MS, Store } from "./store";

const TODAY = "2026-10-17";

function answer(value: number, today = TODAY) {
  return { today, value };
}

function deferred<T>() {
  let resolve: (value: T) => void = () => undefined;
  let reject: (reason: unknown) => void = () => undefined;
  const promise = new Promise<T>((ok, fail) => {
    resolve = ok;
    reject = fail;
  });
  return { promise, resolve, reject };
}

/** Дать отработать обработчикам уже выполненных обещаний. */
async function settle(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

afterEach(() => {
  vi.useRealTimers();
});

describe("Store", () => {
  it("shares a request that is already on its way", async () => {
    const store = new Store();
    const load = vi.fn(() => Promise.resolve(answer(1)));
    const first = store.fetch("dashboard", load);
    expect(store.fetch("dashboard", load)).toBe(first);
    expect(store.get("dashboard").loading).toBe(true);
    await first;
    expect(load).toHaveBeenCalledTimes(1);
    expect(store.get("dashboard")).toMatchObject({
      data: answer(1),
      live: true,
      loading: false,
    });
  });

  it("reads the table anew only on the first request of a launch", async () => {
    const store = new Store();
    const load = vi.fn((fresh: boolean) => Promise.resolve({ fresh }));
    await store.fetch("dashboard", load);
    await store.fetch("forecast", load);
    await store.fetch("forecast", load, { fresh: true });
    expect(load.mock.calls.map(([fresh]) => fresh)).toEqual([
      true,
      false,
      true,
    ]);
  });

  it("a finished request is not shared any more", async () => {
    const store = new Store();
    const load = vi.fn(() => Promise.resolve(answer(1)));
    await store.fetch("dashboard", load);
    await store.fetch("dashboard", load);
    expect(load).toHaveBeenCalledTimes(2);
  });

  it("a fresh read does not join a usual one on its way", () => {
    const store = new Store();
    const usual = store.fetch("dashboard", () => new Promise(() => undefined));
    expect(
      store.fetch("dashboard", () => Promise.resolve(answer(2)), {
        fresh: true,
      }),
    ).not.toBe(usual);
  });

  it("does not let a late answer overwrite a newer one", async () => {
    const store = new Store();
    const old = deferred<object>();
    const newer = deferred<object>();
    void store.fetch("dashboard", () => old.promise);
    void store.fetch("dashboard", () => newer.promise, { force: true });
    newer.resolve(answer(2));
    await settle();
    old.resolve(answer(1));
    await settle();
    expect(store.get("dashboard").data).toEqual(answer(2));
  });

  it("keeps an answer that came with a change over an older request", async () => {
    const store = new Store();
    const old = deferred<object>();
    void store.fetch("forecast", () => old.promise);
    store.put("forecast", answer(5));
    old.resolve(answer(1));
    await settle();
    expect(store.get("forecast")).toMatchObject({
      data: answer(5),
      loading: false,
    });
  });

  it("after a write drops numbers that could not be refreshed", async () => {
    const store = new Store();
    await store.fetch("dashboard", () => Promise.resolve(answer(1)));
    store.expire(["dashboard"]);
    // пока обновление в пути — прежние цифры на экране
    expect(store.get("dashboard").data).toEqual(answer(1));
    await expect(
      store.fetch("dashboard", () => Promise.reject(new Error("down"))),
    ).rejects.toThrow("down");
    expect(store.get("dashboard").data).toBeNull();
  });

  it("keeps the numbers when a usual refresh fails", async () => {
    const store = new Store();
    await store.fetch("dashboard", () => Promise.resolve(answer(1)));
    await expect(
      store.fetch("dashboard", () => Promise.reject(new Error("down")), {
        force: true,
      }),
    ).rejects.toThrow("down");
    expect(store.get("dashboard")).toMatchObject({
      data: answer(1),
      loading: false,
    });
  });

  it("a write does not join a request that started before it", async () => {
    const store = new Store();
    const old = deferred<object>();
    const first = store.fetch("dashboard", () => old.promise);
    store.expire(["dashboard"]);
    const second = store.fetch("dashboard", () => Promise.resolve(answer(2)));
    expect(second).not.toBe(first);
    old.resolve(answer(1));
    await second;
    await settle();
    expect(store.get("dashboard").data).toEqual(answer(2));
  });

  it("expires every plan month by the prefix", () => {
    const store = new Store();
    store.put("plan:2026-10", answer(1));
    store.put("plan:2026-11", answer(2));
    store.put("forecast", answer(3));
    store.expire(["plan:"]);
    expect(store.get("plan:2026-10").at).toBe(0);
    expect(store.get("plan:2026-11").at).toBe(0);
    expect(store.get("forecast").at).toBeGreaterThan(0);
  });

  it("gives fresh data of this launch without a request", async () => {
    const store = new Store();
    store.put("forecast", answer(1));
    const load = vi.fn(() => Promise.resolve(answer(2)));
    expect(await store.ensure("forecast", load)).toEqual(answer(1));
    expect(load).not.toHaveBeenCalled();
  });

  it("asks again for data older than half a minute", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-10-17T10:00:00"));
    const store = new Store();
    store.put("forecast", answer(1));
    vi.setSystemTime(Date.now() + FRESH_MS + 1);
    const load = vi.fn(() => Promise.resolve(answer(2)));
    expect(await store.ensure("forecast", load)).toEqual(answer(2));
    expect(load).toHaveBeenCalledTimes(1);
  });

  it("tells the screen when its key changes", () => {
    const store = new Store();
    const forecast = vi.fn();
    const dashboard = vi.fn();
    store.subscribe("forecast", forecast);
    const stop = store.subscribe("dashboard", dashboard);
    store.put("forecast", answer(1));
    stop();
    store.put("dashboard", answer(2));
    expect(forecast).toHaveBeenCalledTimes(1);
    expect(dashboard).not.toHaveBeenCalled();
  });
});

describe("Store on the phone", () => {
  afterEach(() => {
    localStorage.clear();
  });

  const options = { storage: localStorage, build: "b1", today: TODAY, user: 7 };

  it("keeps today's answers for the next launch, not as live ones", () => {
    new Store(options).put("dashboard", answer(1));
    const next = new Store(options);
    expect(next.get("dashboard")).toMatchObject({
      data: answer(1),
      live: false,
    });
  });

  it("restored answers are asked again at once", async () => {
    new Store(options).put("forecast", answer(1));
    const next = new Store(options);
    const load = vi.fn(() => Promise.resolve(answer(2)));
    expect(await next.ensure("forecast", load)).toEqual(answer(2));
  });

  it.each([
    ["another day", { ...options, today: "2026-10-18" }],
    ["another build", { ...options, build: "b2" }],
    ["another user", { ...options, user: 8 }],
  ])("forgets answers of %s", (_, other) => {
    new Store(options).put("dashboard", answer(1));
    expect(new Store(other).get("dashboard").data).toBeNull();
  });

  it("keeps only answers of today", () => {
    const store = new Store(options);
    store.put("dashboard", answer(1, "2026-10-16"));
    store.put("forecast", answer(2));
    const next = new Store(options);
    expect(next.get("dashboard").data).toBeNull();
    expect(next.get("forecast").data).toEqual(answer(2));
  });

  it("ignores a damaged copy", () => {
    localStorage.setItem(CACHE_KEY, "{not json");
    expect(new Store(options).get("dashboard").data).toBeNull();
    localStorage.setItem(CACHE_KEY, JSON.stringify({ build: "b1", user: 7 }));
    expect(new Store(options).get("dashboard").data).toBeNull();
  });

  it("works when the phone memory is closed", () => {
    const closed = {
      getItem: () => {
        throw new Error("denied");
      },
      setItem: () => {
        throw new Error("denied");
      },
    } as unknown as Storage;
    const store = new Store({ ...options, storage: closed });
    store.put("dashboard", answer(1));
    expect(store.get("dashboard").data).toEqual(answer(1));
  });
});
