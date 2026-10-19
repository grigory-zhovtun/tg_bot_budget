import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import SectionSwitch from "./SectionSwitch";

describe("SectionSwitch", () => {
  it("lists the sections and marks the chosen one", () => {
    render(<SectionSwitch section="today" onSection={vi.fn()} />);
    const group = screen.getByRole("radiogroup", { name: "Раздел сводки" });
    const radios = within(group).getAllByRole("radio");
    expect(radios.map((radio) => radio.textContent)).toEqual([
      "Сегодня",
      "Траты",
      "Накопления",
      "Впереди",
    ]);
    // два ряда по два: «Накопления» и «Впереди» не слипаются
    expect(group.className).toContain("grid-cols-2");
    const today = screen.getByRole("radio", { name: "Сегодня" });
    expect(today.getAttribute("aria-checked")).toBe("true");
    // выбор виден не только цветом: фон и жирность
    expect(today.className).toContain("font-extrabold");
    expect(today.className).toContain("bg-bg");
    const spending = screen.getByRole("radio", { name: "Траты" });
    expect(spending.getAttribute("aria-checked")).toBe("false");
    expect(spending.className).toContain("text-base");
    expect(spending.className).not.toContain("text-hint");
  });

  it("tells which section was pressed", async () => {
    const onSection = vi.fn();
    const user = userEvent.setup();
    render(<SectionSwitch section="today" onSection={onSection} />);
    await user.click(screen.getByRole("radio", { name: "Накопления" }));
    expect(onSection).toHaveBeenCalledWith("savings");
  });
});
