import { fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ApiError, type Api, type CalendarRecord } from "../api";
import { bootstrapFixture, forecastFixture } from "../mocks/fixtures";
import { fakeApi } from "../test/fakeApi";
import RecordSheet from "./RecordSheet";

/** Пробелы между разрядами — неразрывные, как в money(). */
const nb = (text: string) => text.replace(/(\d) (?=\d)/g, "$1\u00a0");

function fixtureRecord(id: string): CalendarRecord {
  const found = forecastFixture.calendar.find((record) => record.id === id);
  if (!found) throw new Error(`no record ${id}`);
  return found;
}

function open(record: CalendarRecord | null = null, api: Api = fakeApi()) {
  const onSaved = vi.fn();
  render(
    <RecordSheet
      api={api}
      boot={bootstrapFixture}
      data={forecastFixture}
      day={16}
      record={record}
      onSaved={onSaved}
      onClose={vi.fn()}
    />,
  );
  return { onSaved, sheet: screen.getByRole("dialog") };
}

describe("RecordSheet", () => {
  it("offers only spending groups and the currencies of the cards", () => {
    const { sheet } = open();
    expect(
      within(sheet)
        .getAllByRole("radio", { name: /^(UZS|USD)$/ })
        .map((radio) => radio.textContent),
    ).toEqual(["UZS", "USD"]);
    expect(within(sheet).getByRole("button", { name: "🍔 ЕДА" })).toBeTruthy();
    expect(within(sheet).queryByRole("button", { name: /ДОХОДЫ/ })).toBeNull();
    expect(
      within(sheet)
        .getByRole("button", { name: "Запланировать" })
        .hasAttribute("disabled"),
    ).toBe(true);
  });

  it("says when the article is not in the month plan", async () => {
    const user = userEvent.setup();
    const { sheet } = open();
    await user.click(within(sheet).getByRole("radio", { name: "USD" }));
    await user.click(within(sheet).getByRole("button", { name: "🍔 ЕДА" }));
    await user.click(within(sheet).getByRole("button", { name: "кофе" }));
    expect(within(sheet).getByText("Статьи нет в плане месяца")).toBeTruthy();
  });

  it("warns when the records of the article pass its rest", async () => {
    const user = userEvent.setup();
    const { sheet } = open();
    await user.click(within(sheet).getByRole("radio", { name: "Каждый день" }));
    await user.click(within(sheet).getByRole("button", { name: "🍔 ЕДА" }));
    await user.click(within(sheet).getByRole("button", { name: "кофе" }));
    await user.click(
      within(sheet).getByRole("button", {
        name: nb("в среднем в день 27 000"),
      }),
    );
    // 16–31 октября: 16 × 27 000 = 432 000 к записям 510 000 при остатке 480 000
    expect(
      within(sheet).getByText(
        "⚠️ План статьи «ЕДА / кофе» будет превышен на 462 000 сум",
      ),
    ).toBeTruthy();
  });

  it("picks the days of the week, starting with the day of the date", async () => {
    const addRecord = vi
      .fn<Api["addRecord"]>()
      .mockResolvedValue(forecastFixture);
    const user = userEvent.setup();
    const { sheet, onSaved } = open(null, fakeApi({ addRecord }));
    await user.click(
      within(sheet).getByRole("radio", { name: "По дням недели" }),
    );
    const days = within(sheet).getByRole("group", { name: "Дни недели" });
    expect(
      within(days)
        .getByRole("button", { name: "пт" })
        .getAttribute("aria-pressed"),
    ).toBe("true");
    await user.click(within(days).getByRole("button", { name: "пн" }));
    await user.click(within(sheet).getByRole("button", { name: "🏚️ ДОМ" }));
    await user.click(within(sheet).getByRole("button", { name: "продукты" }));
    await user.click(within(sheet).getByRole("button", { name: "5" }));
    await user.click(within(sheet).getByRole("button", { name: "000" }));
    await user.click(
      within(sheet).getByRole("button", { name: "Запланировать" }),
    );
    expect(addRecord).toHaveBeenCalledWith(
      expect.objectContaining({
        repeat: "weekdays",
        weekdays: [1, 5],
        amount: "5000",
      }),
    );
    expect(onSaved).toHaveBeenCalledWith(forecastFixture);
  });

  it("sends one request for a double tap", async () => {
    const addRecord = vi
      .fn<Api["addRecord"]>()
      .mockReturnValue(new Promise(() => {})); // сеть думает
    const user = userEvent.setup();
    const { sheet } = open(null, fakeApi({ addRecord }));
    await user.click(within(sheet).getByRole("button", { name: "🍔 ЕДА" }));
    await user.click(within(sheet).getByRole("button", { name: "кафе" }));
    await user.click(
      within(sheet).getByRole("button", { name: nb("обычно 160 000") }),
    );
    await user.dblClick(
      within(sheet).getByRole("button", { name: "Запланировать" }),
    );
    expect(addRecord).toHaveBeenCalledTimes(1);
  });

  it("keeps the date when the field is cleared", () => {
    const { sheet } = open();
    const field = within(sheet).getByLabelText("Дата");
    fireEvent.change(field, { target: { value: "" } }); // «Сбросить» в iOS
    expect(field).toHaveProperty("value", "2026-10-16");
    expect(
      within(sheet).getByRole("heading", { name: "Запись на 16 октября" }),
    ).toBeTruthy();
  });

  it("saves a repeat with its past start", async () => {
    const updateRecord = vi
      .fn<Api["updateRecord"]>()
      .mockResolvedValue(forecastFixture);
    const user = userEvent.setup();
    const { sheet } = open(fixtureRecord("m3a9zt"), fakeApi({ updateRecord }));
    expect(within(sheet).getByLabelText("Начало")).toHaveProperty(
      "value",
      "2026-10-01",
    );
    await user.click(within(sheet).getByRole("button", { name: "Сохранить" }));
    expect(updateRecord).toHaveBeenCalledWith("m3a9zt", {
      date: "2026-10-01",
      repeat: "daily",
      weekdays: [],
      group: "🍔 ЕДА",
      subcategory: "кофе",
      currency: "UZS",
      amount: "30000",
      name: "Кофе",
    });
  });

  it("asks once more before deleting", async () => {
    const deleteRecord = vi
      .fn<Api["deleteRecord"]>()
      .mockResolvedValue(forecastFixture);
    const user = userEvent.setup();
    const { sheet, onSaved } = open(
      fixtureRecord("p7r0dk"),
      fakeApi({ deleteRecord }),
    );
    await user.click(
      within(sheet).getByRole("button", { name: "Удалить запись" }),
    );
    expect(deleteRecord).not.toHaveBeenCalled();
    await user.click(
      within(sheet).getByRole("button", { name: "Удалить насовсем" }),
    );
    expect(deleteRecord).toHaveBeenCalledWith("p7r0dk");
    expect(onSaved).toHaveBeenCalledWith(forecastFixture);
  });

  it("shows field errors under the fields and others above the button", async () => {
    const updateRecord = vi
      .fn<Api["updateRecord"]>()
      .mockRejectedValueOnce(
        new ApiError(422, "validation", "Проверьте введённые данные", {
          date: "Дата — не раньше сегодня и не дальше чем через год",
        }),
      )
      .mockRejectedValueOnce(
        new ApiError(
          404,
          "record_not_found",
          "Запись уже удалили — обновите прогноз",
        ),
      );
    const user = userEvent.setup();
    const { sheet, onSaved } = open(
      fixtureRecord("p7r0dk"),
      fakeApi({ updateRecord }),
    );
    const save = within(sheet).getByRole("button", { name: "Сохранить" });
    await user.click(save);
    expect(
      await within(sheet).findByText(
        "Дата — не раньше сегодня и не дальше чем через год",
      ),
    ).toBeTruthy();
    expect(within(sheet).queryByRole("alert")).toBeNull();
    await user.click(save);
    expect((await within(sheet).findByRole("alert")).textContent).toBe(
      "Запись уже удалили — обновите прогноз",
    );
    expect(onSaved).not.toHaveBeenCalled();
  });
});
