import { describe, expect, it } from "vitest";
import { noticeParts } from "./notice";

describe("noticeParts", () => {
  it("splits «head: details» like the bot's notes", () => {
    expect(noticeParts("✅ План сохранён: 2 изменения")).toEqual({
      head: "✅ План сохранён",
      rest: "2 изменения",
    });
  });

  it("keeps a sentence without a colon whole", () => {
    const text =
      "Не получилось сохранить. Правки не потеряны — сохраните ещё раз";
    expect(noticeParts(text)).toEqual({ head: null, rest: text });
  });
});
