import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ApiError, type Api } from "../api";
import { dashboardFixture } from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
import DashboardScreen from "./DashboardScreen";

describe("DashboardScreen", () => {
  it("shows what is left today, the plan by groups and the chart", async () => {
    const { container } = render(<DashboardScreen api={fakeApi()} />);
    expect(await screen.findByText("Можно сегодня")).toBeTruthy();
    expect(screen.getByText("380 000")).toBeTruthy();
    expect(screen.getByText(/лимит 500 000 · потрачено 120 000/)).toBeTruthy();
    expect(container.querySelector('[data-status="over"]')).not.toBeNull();
    expect(
      screen.getByRole("img", { name: "Остаток по дням: план и факт" }),
    ).toBeTruthy();
    expect(screen.getByText("−650,00 USD")).toBeTruthy();
    expect(screen.getByText(/ждём/)).toBeTruthy();
  });

  it("opens a group to show its subcategories", async () => {
    const user = userEvent.setup();
    render(<DashboardScreen api={fakeApi()} />);
    await user.click(await screen.findByRole("button", { name: /ЕДА/ }));
    expect(screen.getByText(/кафе/)).toBeTruthy();
  });

  it("offers a retry when the sheet does not answer", async () => {
    const dashboard = vi
      .fn<Api["dashboard"]>()
      .mockRejectedValueOnce(
        new ApiError(
          503,
          "sheets_unavailable",
          "Таблица не ответила — попробуйте ещё раз",
        ),
      )
      .mockResolvedValueOnce(dashboardFixture);
    const user = userEvent.setup();
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    expect(
      await screen.findByText("Таблица не ответила — попробуйте ещё раз"),
    ).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "Повторить" }));
    expect(await screen.findByText("Можно сегодня")).toBeTruthy();
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("explains a missing month tab", async () => {
    const dashboard = vi
      .fn<Api["dashboard"]>()
      .mockResolvedValue({ ...dashboardFixture, status: "no_month_tab" });
    render(<DashboardScreen api={fakeApi({ dashboard })} />);
    expect(
      await screen.findByText(
        "Вкладки месяца нет — бот создаёт её 1-го числа.",
      ),
    ).toBeTruthy();
  });
});
