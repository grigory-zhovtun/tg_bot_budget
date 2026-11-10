import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import QuickActions from "./QuickActions";

describe("QuickActions", () => {
  it("shows the actions it was given, in their order", () => {
    render(
      <QuickActions
        actions={["spend", "income", "transfer", "save"]}
        onAction={vi.fn()}
      />,
    );
    const row = screen.getByRole("group", { name: "Быстрые действия" });
    expect(
      within(row)
        .getAllByRole("button")
        .map((button) => button.textContent),
    ).toEqual(["Трата", "Доход", "Перевод", "Отложить"]);
  });

  it("tells which action was pressed", async () => {
    const onAction = vi.fn();
    const user = userEvent.setup();
    render(<QuickActions actions={["spend", "save"]} onAction={onAction} />);
    await user.click(screen.getByRole("button", { name: "Отложить" }));
    expect(onAction).toHaveBeenCalledWith("save");
  });

  it("has buttons a finger can hit, with labels that fit a narrow phone", () => {
    render(<QuickActions actions={["spend"]} onAction={vi.fn()} />);
    const button = screen.getByRole("button", { name: "Трата" });
    expect(button.className).toContain("min-h-16");
    expect(button.className).toContain("text-action");
    expect(button.className).toContain("break-words");
  });

  it("is not there without actions", () => {
    const { container } = render(
      <QuickActions actions={[]} onAction={vi.fn()} />,
    );
    expect(container.firstChild).toBeNull();
  });
});
