import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, authHeader, request } from "./api";

describe("authHeader", () => {
  it("prefers Telegram initData", () => {
    expect(authHeader("query_id=1&hash=x", "?launch=abc")).toBe(
      "tma query_id=1&hash=x",
    );
  });
  it("falls back to the launch token of the keyboard button", () => {
    expect(authHeader("", "?launch=42.1.sig")).toBe("Launch 42.1.sig");
  });
  it("has nothing outside Telegram", () => {
    expect(authHeader("", "")).toBeNull();
  });
});

describe("request", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    window.history.replaceState(null, "", "/");
  });

  it("turns API errors into ApiError with the fields", async () => {
    const body = {
      error: {
        code: "validation",
        message: "Проверьте введённые данные",
        fields: { amount: "Сумма — больше нуля" },
      },
    };
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValue(new Response(JSON.stringify(body), { status: 422 })),
    );
    const error: unknown = await request("/expenses").catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ApiError);
    expect(error).toMatchObject({
      status: 422,
      code: "validation",
      fields: { amount: "Сумма — больше нуля" },
    });
  });

  it("reports a lost connection", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new TypeError("Failed")));
    await expect(request("/bootstrap")).rejects.toMatchObject({
      status: 0,
      code: "network",
    });
  });

  it("survives a server error that is not JSON", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValue(
          new Response("Internal Server Error", { status: 500 }),
        ),
    );
    await expect(request("/bootstrap")).rejects.toMatchObject({
      status: 500,
      code: "server",
    });
  });

  it("sends the launch token from the page address", async () => {
    window.history.replaceState(null, "", "/app/?launch=42.1.sig");
    const fetch = vi
      .fn()
      .mockResolvedValue(new Response("{}", { status: 200 }));
    vi.stubGlobal("fetch", fetch);
    await request("/bootstrap");
    expect(fetch).toHaveBeenCalledWith(
      "/api/bootstrap",
      expect.objectContaining({
        headers: expect.objectContaining({ Authorization: "Launch 42.1.sig" }),
      }),
    );
  });
});
