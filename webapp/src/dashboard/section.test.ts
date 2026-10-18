import { afterEach, describe, expect, it, vi } from "vitest";
import { SECTION_KEY, storeSection, storedSection } from "./section";

afterEach(() => {
  localStorage.clear();
});

describe("the chosen section of «Сводка»", () => {
  it("starts on «Сегодня» when nothing is stored", () => {
    expect(storedSection()).toBe("today");
  });

  it("reads the stored section", () => {
    localStorage.setItem(SECTION_KEY, "savings");
    expect(storedSection()).toBe("savings");
  });

  it.each(["calendar", "", '{"scope":"UZS"}'])(
    "ignores %j from another build",
    (value) => {
      localStorage.setItem(SECTION_KEY, value);
      expect(storedSection()).toBe("today");
    },
  );

  it("keeps the choice for the next launch", () => {
    storeSection("ahead");
    expect(localStorage.getItem(SECTION_KEY)).toBe("ahead");
  });

  it("does without the phone memory when the browser closes it", () => {
    const closed = vi
      .spyOn(window, "localStorage", "get")
      .mockImplementation(() => {
        throw new Error("denied");
      });
    expect(storedSection()).toBe("today");
    expect(() => storeSection("spending")).not.toThrow();
    closed.mockRestore();
  });
});
