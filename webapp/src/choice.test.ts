import { afterEach, describe, expect, it, vi } from "vitest";
import { readChoice, saveChoice } from "./choice";

const KEY = "test-choice";
const ALLOWED = ["one", "two"] as const;

afterEach(() => {
  localStorage.clear();
});

describe("a choice kept on the phone", () => {
  it("falls back when nothing is stored", () => {
    expect(readChoice(KEY, ALLOWED, "one")).toBe("one");
  });

  it("reads a stored choice", () => {
    saveChoice(KEY, "two");
    expect(readChoice(KEY, ALLOWED, "one")).toBe("two");
  });

  it("ignores a value it does not know", () => {
    localStorage.setItem(KEY, "three");
    expect(readChoice(KEY, ALLOWED, "one")).toBe("one");
  });

  it("does without the phone memory when the browser closes it", () => {
    const closed = vi
      .spyOn(window, "localStorage", "get")
      .mockImplementation(() => {
        throw new Error("denied");
      });
    expect(readChoice(KEY, ALLOWED, "one")).toBe("one");
    expect(() => saveChoice(KEY, "two")).not.toThrow();
    closed.mockRestore();
  });
});
