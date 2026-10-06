"""Аналитика по листу fact: отчёт за 3 дня с графиками и цифры для /advice.

Все суммы приводятся к сумам по курсам из system!H2:I10, иначе доллары и рубли
складывались бы с сумами. Окно отчёта — календарные дни в часовом поясе владельца.
"""

import calendar
import io
import logging
import re
from datetime import date, timedelta
from typing import Any

import pandas as pd
from matplotlib.figure import Figure

from app import config
from app.domain import INCOME_GROUP, Rates, local_today
from app.services.google_sheets import GoogleSheetsService

logger = logging.getLogger(__name__)

TRANSFERS_GROUP = "💳 СЧЕТА"
OPENING_BALANCE = "нач остаток"
SERIAL_ZERO = date(1899, 12, 30)
COLUMNS = ["day", "group", "subcategory", "amount", "comment", "currency", "source"]
MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")  # fmt: skip
CAPTION_PIE = "📈 Расходы по категориям"
CAPTION_DAYS = "📊 Динамика по дням"

Chart = tuple[str, io.BytesIO]


def money(value: float) -> str:
    """12345678.9 → «12 345 679»."""
    return f"{value:,.0f}".replace(",", " ")


def md_escape(text: str) -> str:
    """Экранирование для Telegram Markdown (legacy): _ * ` [."""
    for char in ("_", "*", "`", "["):
        text = text.replace(char, f"\\{char}")
    return text


def to_day(value: Any) -> date | None:
    """Серийная дата Google Sheets или «ДД.ММ.ГГГГ» → date."""
    if isinstance(value, int | float) and not isinstance(value, bool) and value > 0:
        return SERIAL_ZERO + timedelta(days=int(value))
    try:
        parsed = pd.to_datetime(str(value), format="%d.%m.%Y")
    except (ValueError, TypeError):
        return None
    return None if pd.isna(parsed) else parsed.date()


def chart_label(text: str) -> str:
    """Подпись для графика без эмодзи: шрифт matplotlib их не рисует."""
    return re.sub(r"[^\w\s.,/()%-]", "", text).strip()


def transactions_frame(rows: list[list[Any]], rates: Rates) -> pd.DataFrame:
    """Строки fact (UNFORMATTED) → таблица с суммой в сумах (amount_uzs).

    amount — колонка D со знаком таблицы: расход и доход плюс, прочий приход минус.
    """
    records = []
    for row in rows[1:]:
        cells = list(row[:8]) + [""] * (8 - len(row[:8]))
        day, amount = to_day(cells[0]), cells[3]
        if day is None or not isinstance(amount, int | float):
            continue
        records.append(
            {
                "day": day,
                "group": str(cells[1]).strip(),
                "subcategory": str(cells[2]).strip(),
                "amount": float(amount),
                "comment": str(cells[5]).removeprefix("AI:").strip(),
                "currency": str(cells[6]).strip().upper() or "UZS",
                "source": str(cells[7]).strip(),
            }
        )
    frame = pd.DataFrame(records, columns=COLUMNS)
    rate = frame["currency"].map(lambda code: rates.to_uzs.get(code, 0.0))
    unknown = frame.loc[rate == 0, "currency"].unique()
    if len(unknown):
        logger.warning("No rate for currencies %s, their rows are skipped", unknown)
    frame["amount_uzs"] = frame["amount"] * rate
    return frame[rate > 0]


def expenses(frame: pd.DataFrame) -> pd.DataFrame:
    """Траты бюджета: без доходов и переводов между своими картами.

    Возврат (минус в колонке D) уменьшает траты своей категории.
    """
    return frame[~frame["group"].isin([INCOME_GROUP, TRANSFERS_GROUP])]


def income(frame: pd.DataFrame) -> pd.DataFrame:
    """Доходы без служебного «нач остаток» (выравнивание баланса)."""
    mask = (frame["group"] == INCOME_GROUP) & (frame["subcategory"] != OPENING_BALANCE)
    return frame[mask]


def _png(figure: Figure) -> io.BytesIO:
    buffer = io.BytesIO()
    figure.savefig(
        buffer, format="png", dpi=150, bbox_inches="tight", facecolor="white"
    )
    buffer.seek(0)
    return buffer


def pie_chart(totals: pd.Series) -> io.BytesIO:
    """Круговая диаграмма по положительным суммам: топ-6 и «Другое»."""
    totals = totals[totals > 0].sort_values(ascending=False)
    if len(totals) > 6:
        rest = pd.Series({"Другое": totals.iloc[6:].sum()})
        totals = pd.concat([totals.head(6), rest])
    figure = Figure(figsize=(10, 8))
    axes = figure.subplots()
    wedges, _, _ = axes.pie(
        totals.values,
        autopct=lambda pct: f"{pct:.1f}%" if pct > 5 else "",
        startangle=90,
        pctdistance=0.75,
    )
    axes.legend(
        wedges,
        [f"{chart_label(name)}: {money(value)}" for name, value in totals.items()],
        title="Категории (сум)",
        loc="center left",
        bbox_to_anchor=(1, 0, 0.5, 1),
        fontsize=9,
    )
    axes.set_title("Расходы по категориям (3 дня)", fontsize=14, fontweight="bold")
    return _png(figure)


def days_chart(days: list[date], spent: list[float], earned: list[float]) -> io.BytesIO:
    figure = Figure(figsize=(10, 6))
    axes = figure.subplots()
    positions = list(range(len(days)))
    width = 0.35
    for offset, values, label, color in (
        (-width / 2, spent, "Расходы", "#e74c3c"),
        (width / 2, earned, "Доходы", "#27ae60"),
    ):
        bars = axes.bar(
            [p + offset for p in positions], values, width, label=label, color=color
        )
        for bar in bars:
            if bar.get_height() > 0:
                axes.annotate(
                    money(bar.get_height()),
                    xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha="center",
                    fontsize=9,
                )
    axes.set_xticks(positions)
    axes.set_xticklabels([d.strftime("%d.%m") for d in days])
    axes.set_ylabel("Сум")
    axes.set_title("Доходы и расходы по дням", fontsize=14, fontweight="bold")
    axes.legend()
    axes.grid(axis="y", alpha=0.3)
    axes.yaxis.set_major_formatter(lambda value, _: money(value))
    return _png(figure)


class AnalyticsService:
    def __init__(self, gs_service: GoogleSheetsService) -> None:
        self.gs_service = gs_service

    def _frame(self) -> pd.DataFrame:
        rows = self.gs_service.get_values(config.FACT_SHEET_NAME)
        return transactions_frame(rows, self.gs_service.get_rates())

    def generate_3day_report(
        self, today: date | None = None
    ) -> tuple[str, list[Chart]]:
        """Отчёт за сегодня и два предыдущих дня (по Ташкенту) с графиками."""
        today = today or local_today(config.ANALYTICS_TIMEZONE)
        days = [today - timedelta(days=offset) for offset in (2, 1, 0)]
        frame = self._frame()
        window = frame[frame["day"].isin(days)]
        spent, earned = expenses(window), income(window)

        lines = [
            "📊 *АНАЛИТИКА ЗА 3 ДНЯ*",
            f"📅 {days[0]:%d.%m} – {days[-1]:%d.%m.%Y}",
            "",
        ]
        if window.empty:
            lines.append("Операций за эти дни нет.")
            return "\n".join(lines), []

        total_spent = spent["amount_uzs"].sum()
        lines += [
            "💰 *ИТОГИ* (в сумах по курсу таблицы):",
            f"   📈 Доходы: {money(earned['amount_uzs'].sum())}",
            f"   📉 Расходы: {money(total_spent)}",
            f"   📝 Операций: {len(spent) + len(earned)}",
            "",
        ]
        by_group = spent.groupby("group")["amount_uzs"].sum()
        top = by_group[by_group > 0].sort_values(ascending=False).head(5)
        if not top.empty:
            lines.append("📂 *ТОП КАТЕГОРИЙ РАСХОДОВ:*")
            for rank, (group, value) in enumerate(top.items(), 1):
                share = value / total_spent * 100 if total_spent > 0 else 0
                lines.append(
                    f"   {rank}. {md_escape(group)}: {money(value)} ({share:.1f}%)"
                )
            lines.append("")

        # возвраты уже уменьшили итоги и категории; день «в плюсе» показываем как 0
        daily_spent = [
            max(spent.loc[spent["day"] == d, "amount_uzs"].sum(), 0.0) for d in days
        ]
        daily_earned = [
            earned.loc[earned["day"] == d, "amount_uzs"].sum() for d in days
        ]
        lines.append("📅 *ПО ДНЯМ:*")
        for day, out, inc in zip(days, daily_spent, daily_earned, strict=True):
            lines.append(f"   {day:%d.%m}: −{money(out)} / +{money(inc)}")

        biggest = spent[spent["amount_uzs"] > 0].nlargest(3, "amount_uzs")
        if not biggest.empty:
            lines += ["", "🔝 *КРУПНЫЕ ТРАТЫ:*"]
            for _, row in biggest.iterrows():
                lines.append(
                    f"   • {money(row['amount_uzs'])} — {md_escape(row['subcategory'])}"
                    f" ({md_escape(row['comment'][:30])})"
                )

        charts: list[Chart] = []
        try:
            if (by_group > 0).any():
                charts.append((CAPTION_PIE, pie_chart(by_group)))
            charts.append((CAPTION_DAYS, days_chart(days, daily_spent, daily_earned)))
        except Exception:
            logger.exception("Charts failed, sending the text report only")
        return "\n".join(lines), charts

    def advice_context(self, today: date | None = None) -> str:
        """Цифры для /advice: план-факт месяца, темп и средние за 3 месяца.

        Считает Python, а не модель: LLM плохо складывает тысячи строк.
        """
        today = today or local_today(config.ANALYTICS_TIMEZONE)
        frame = expenses(self._frame())
        month_start = today.replace(day=1)
        current = frame[(frame["day"] >= month_start) & (frame["day"] <= today)]
        start = month_start
        for _ in range(3):
            start = (start - timedelta(days=1)).replace(day=1)
        previous = frame[(frame["day"] >= start) & (frame["day"] < month_start)]

        days_in_month = calendar.monthrange(today.year, today.month)[1]
        spent = current["amount_uzs"].sum()
        forecast = spent / today.day * days_in_month
        now_by_group = current.groupby("group")["amount_uzs"].sum()
        avg_by_group = previous.groupby("group")["amount_uzs"].sum() / 3

        lines = [
            f"Сегодня {today:%d.%m.%Y}, прошло {today.day} из {days_in_month} дней.",
            f"Траты месяца: {money(spent)} сум; прогноз на месяц: {money(forecast)}"
            f" сум; средний месяц за последние 3: {money(avg_by_group.sum())} сум.",
            "",
            "Категория | месяц сейчас | средний месяц (3 мес.)",
        ]
        for group in sorted(set(now_by_group.index) | set(avg_by_group.index)):
            lines.append(
                f"{group} | {money(now_by_group.get(group, 0))}"
                f" | {money(avg_by_group.get(group, 0))}"
            )
        lines += ["", *self._plan_fact_lines(today)]
        return "\n".join(lines)

    def _plan_fact_lines(self, today: date) -> list[str]:
        """Строки вкладки месяца («Oct 26»): план и факт в сумовом эквиваленте."""
        sheet = f"{MONTHS[today.month - 1]} {today:%y}"
        try:
            rows = self.gs_service.get_values(sheet)
        except Exception:
            logger.info("No plan sheet %s", sheet)
            return [f"Листа с планом «{sheet}» нет."]
        lines = [f"План-факт «{sheet}» (сум): статья | план | факт | факт/план"]
        for row in rows[1:]:
            cells = list(row[:9]) + [""] * (9 - len(row[:9]))
            kind, group, sub, currency = cells[0], cells[1], cells[2], cells[6]
            plan, fact = cells[7], cells[8]
            numbers = isinstance(plan, int | float) and isinstance(fact, int | float)
            if not group or not numbers or (plan == 0 and fact == 0):
                continue
            ratio = f"{abs(fact) / abs(plan) * 100:.0f}%" if plan else "вне плана"
            lines.append(
                f"{kind}: {group} / {sub} ({currency}) | {money(abs(plan))}"
                f" | {money(abs(fact))} | {ratio}"
            )
        return lines
