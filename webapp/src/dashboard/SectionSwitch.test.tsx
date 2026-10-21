import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { SECTIONS } from "./section";
import SectionSwitch from "./SectionSwitch";

describe("SectionSwitch", () => {
  it("lists the sections in one row and marks the chosen one", () => {
    render(
      <SectionSwitch
        label="Раздел сводки"
        sections={SECTIONS}
        section="today"
        onSection={vi.fn()}
      />,
    );
    const group = screen.getByRole("radiogroup", { name: "Раздел сводки" });
    const radios = within(group).getAllByRole("radio");
    expect(radios.map((radio) => radio.textContent)).toEqual([
      "Сегодня",
      "Траты",
      "Впереди",
    ]);
    // все разделы — в одну строку
    expect(group.className).toContain("grid-cols-3");
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

  it("puts two sections into two columns", () => {
    render(
      <SectionSwitch
        label="Раздел"
        sections={[
          { name: "a", title: "Первый" },
          { name: "b", title: "Второй" },
        ]}
        section="a"
        onSection={vi.fn()}
      />,
    );
    expect(
      screen.getByRole("radiogroup", { name: "Раздел" }).className,
    ).toContain("grid-cols-2");
  });

  it("tells which section was pressed", async () => {
    const onSection = vi.fn();
    const user = userEvent.setup();
    render(
      <SectionSwitch
        label="Раздел сводки"
        sections={SECTIONS}
        section="today"
        onSection={onSection}
      />,
    );
    await user.click(screen.getByRole("radio", { name: "Впереди" }));
    expect(onSection).toHaveBeenCalledWith("ahead");
  });
});
