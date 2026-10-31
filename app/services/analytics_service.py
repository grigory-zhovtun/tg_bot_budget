"""Аналитика по листу fact: отчёт за 3 дня с графиками и цифры для /advice.

Все суммы приводятся к сумам по курсам из system!H2:I10, иначе доллары и рубли
складывались бы с сумами. Окно отчёта — календарные дни в часовом поясе владельца.
"""

import calendar
import io
import logging
import re
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any, NamedTuple

import pandas as pd
from gspread.exceptions import WorksheetNotFound
from matplotlib.figure import Figure

from app import config
from app.custom_icons import leading_emoji
from app.domain import INCOME_GROUP, Rates, local_today, month_title
from app.rich import NBSP, RULE, Report, heading, para, percent, table, whole
from app.services.google_sheets import GoogleSheetsService, read_month_sheets

if TYPE_CHECKING:
    from app.services.calendar import CalendarRecord
    from app.services.dashboard import Dashboard
    from app.services.day_budget import ForecastItem
    from app.services.forecast import MonthForecast
    from app.services.piggy import Catalog, Screen
    from app.services.savings import Savings

logger = logging.getLogger(__name__)


class Feedback(NamedTuple):
    """После записи: строки ответа (текст и приложение) и те же сведения блоками."""

    lines: list[str]
    blocks: list[str]


class SavingsTab(NamedTuple):
    """«Накопления»: копилки, план и подушка, норма сбережений, курсы таблицы."""

    screen: "Screen"
    savings: "Savings"
    rates: Rates


TRANSFERS_GROUP = "💳 СЧЕТА"
OPENING_BALANCE = "нач остаток"
SERIAL_ZERO = date(1899, 12, 30)
COLUMNS = ["day", "group", "subcategory", "amount", "comment", "currency", "source"]
MONTHS_RU = ("январь", "февраль", "март", "апрель", "май", "июнь", "июль",
             "август", "сентябрь", "октябрь", "ноябрь", "декабрь")  # fmt: skip
NEAR_LIMIT = 0.8  # с этой доли плана статья попадает в «почти всё»
PACE_SLACK = 0.05  # траты могут обгонять календарь на 5 п. п. без тревоги
STALE_CHECK_DAYS = 7  # остаток банка старше недели — напомнить в сводке
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
        try:
            return SERIAL_ZERO + timedelta(days=int(value))
        except (OverflowError, ValueError):  # позже 9999 года: число не в той колонке
            return None
    try:
        parsed = pd.to_datetime(str(value), format="%d.%m.%Y")
    except (ValueError, TypeError):
        return None
    return None if pd.isna(parsed) else parsed.date()


def chart_label(text: str) -> str:
    """Подпись для графика без эмодзи: шрифт matplotlib их не рисует."""
    return re.sub(r"[^\w\s.,/()%-]", "", text).strip()


def transactions_frame(
    rows: list[list[Any]], rates: Rates, keep_unknown: bool = False
) -> pd.DataFrame:
    """Строки fact (UNFORMATTED) → таблица с суммой в сумах (amount_uzs).

    amount — колонка D со знаком таблицы: расход и доход плюс, прочий приход минус.
    Строки валюты без курса пропускаются; keep_unknown оставляет их с amount_uzs 0 —
    прогноз считает каждую валюту в её собственных деньгах.
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
    if len(unknown) and not keep_unknown:
        logger.warning("No rate for currencies %s, their rows are skipped", unknown)
    frame["amount_uzs"] = frame["amount"] * rate
    return frame if keep_unknown else frame[rate > 0]


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


@dataclass
class PlanLine:
    """Статья вкладки месяца в сумах; по всем валютам вместе."""

    group: str
    subcategory: str
    plan: float = 0.0
    fact: float = 0.0

    @property
    def name(self) -> str:
        return f"{self.group} / {self.subcategory}"


def plan_lines(rows: list[list[Any]]) -> tuple[list[PlanLine], list[PlanLine]]:
    """Вкладка месяца (UNFORMATTED) → расходы и доходы, план и факт в сумах.

    Во вкладке и план, и факт отрицательные (факт = −SUMIFS по fact), берём модуль
    со знаком: возврат больше трат даёт отрицательный факт. Строки одной статьи
    в разных валютах складываются по сумовому эквиваленту (колонки H и I).
    """
    found: dict[tuple[str, str, str], PlanLine] = {}
    for row in rows[1:]:
        cells = list(row[:9]) + [""] * (9 - len(row[:9]))
        kind, group, sub = (str(c).strip() for c in cells[:3])
        plan, fact = cells[7], cells[8]
        if kind not in ("Расходы", "Доходы") or not group:
            continue
        if not (isinstance(plan, int | float) and isinstance(fact, int | float)):
            continue
        line = found.setdefault((kind, group, sub), PlanLine(group, sub))
        line.plan -= plan
        line.fact -= fact
    spend = [line for (kind, *_), line in found.items() if kind == "Расходы"]
    earn = [line for (kind, *_), line in found.items() if kind == "Доходы"]
    return spend, earn


def _share(part: float, whole: float) -> str:
    return f"{part / whole:.0%}" if whole else "—"


def _plan_groups(
    spend: list[PlanLine],
) -> tuple[list[PlanLine], list[PlanLine], list[PlanLine]]:
    """Статьи сверх плана, «почти всё» (от NEAR_LIMIT) и вне плана — по убыванию."""
    over = sorted(
        (line for line in spend if line.plan > 0 and line.fact > line.plan),
        key=lambda line: line.fact - line.plan,
        reverse=True,
    )
    near = sorted(
        (
            line
            for line in spend
            if line.plan > 0 and NEAR_LIMIT * line.plan <= line.fact <= line.plan
        ),
        key=lambda line: line.fact / line.plan,
        reverse=True,
    )
    outside = sorted(
        (line for line in spend if line.plan <= 0 and line.fact > 0),
        key=lambda line: line.fact,
        reverse=True,
    )
    return over, near, outside


def _icon_name(group: str, subcategory: str) -> str:
    """«🎉 ПРАЗДНИК» + «сезонный» → «🎉 сезонный»."""
    icon, _ = leading_emoji(group)
    return f"{icon} {subcategory}" if icon else subcategory


def _days_word(days: int) -> str:
    """«из 31 дня», «из 30 дней»."""
    return "дня" if days % 10 == 1 and days % 100 != 11 else "дней"


def week_title(start: date, end: date) -> str:
    """«12–18 октября»; неделя через границу месяцев — «28 сентября – 4 октября»."""
    from app.services.day_budget import MONTHS_GENITIVE

    last = f"{end.day} {MONTHS_GENITIVE[end.month - 1]}"
    if start.month == end.month:
        return f"{start.day}–{last}"
    return f"{start.day} {MONTHS_GENITIVE[start.month - 1]} – {last}"


def _no_tab(today: date) -> str:
    return f"Вкладки «{month_title(today)}» с планом нет — бот создаёт её 1-го числа."


def format_plan_report(rows: list[list[Any]], today: date) -> str:
    """План-факт месяца для чата: темп трат, остаток в день, перерасход."""
    days = calendar.monthrange(today.year, today.month)[1]
    elapsed = today.day / days
    spend, earn = plan_lines(rows)
    plan = sum(line.plan for line in spend)
    fact = sum(line.fact for line in spend)

    lines = [
        f"📋 План-факт: {MONTHS_RU[today.month - 1]} {today.year}, "
        f"прошло {today.day} из {days} дн. ({elapsed:.0%})",
        "",
    ]
    pace = (
        "✅ в графике"
        if plan and fact / plan <= elapsed + PACE_SLACK
        else "⚠️ быстрее графика"
    )
    lines.append(
        f"Расходы: {money(fact)} из {money(plan)} сум ({_share(fact, plan)}) {pace}"
    )
    left = plan - fact
    if left > 0:
        per_day = left / (days - today.day + 1)
        lines.append(f"Осталось {money(left)} сум ≈ {money(per_day)} в день")
    else:
        lines.append(f"План превышен на {money(-left)} сум")

    over, near, outside = _plan_groups(spend)
    if over:
        lines += ["", "🔴 Сверх плана:"]
        lines += [
            f"• {line.name}: {money(line.fact)} из {money(line.plan)} "
            f"({_share(line.fact, line.plan)})"
            for line in over
        ]
    if near:
        lines += ["", f"🟡 Почти всё (от {NEAR_LIMIT:.0%}):"]
        lines += [
            f"• {line.name}: {money(line.fact)} из {money(line.plan)} "
            f"({_share(line.fact, line.plan)})"
            for line in near
        ]
    if outside:
        lines += ["", "⚪ Вне плана:"]
        lines += [f"• {line.name}: {money(line.fact)}" for line in outside]

    earn_plan = sum(line.plan for line in earn)
    earn_fact = sum(line.fact for line in earn)
    if earn_plan or earn_fact:
        lines += [
            "",
            f"Доходы: {money(earn_fact)} из {money(earn_plan)} сум "
            f"({_share(earn_fact, earn_plan)})",
        ]
    return "\n".join(lines)


def rich_plan_report(rows: list[list[Any]], today: date) -> list[str]:
    """План-факт месяца блоками: расходы и остаток крупно, статьи таблицами."""
    days = calendar.monthrange(today.year, today.month)[1]
    elapsed = today.day / days
    spend, earn = plan_lines(rows)
    plan = sum(line.plan for line in spend)
    fact = sum(line.fact for line in spend)
    month = MONTHS_RU[today.month - 1].capitalize()
    blocks = [heading(f"📋 {month}: {today.day} из {days} {_days_word(days)}", 2)]
    if plan > 0:
        calm = fact / plan <= elapsed + PACE_SLACK
        pace = "в графике" if calm else "быстрее графика"
        blocks += [
            heading(f"{'✅' if calm else '⚠️'} Расходы {whole(fact)}"),
            para(
                f"из {whole(plan)} — {percent(fact / plan)}",
                f"{pace}: прошло {percent(elapsed)} месяца",
            ),
        ]
        left = plan - fact
        if left > 0:
            per_day = left / (days - today.day + 1)
            blocks += [
                heading(f"Осталось {whole(left)}"),
                para(f"≈{NBSP}{whole(per_day)} в день"),
            ]
        else:
            blocks.append(heading(f"⚠️ План превышен на {whole(-left)}"))
    else:
        blocks += [heading(f"Расходы {whole(fact)}"), para("плана на месяц нет")]

    over, near, outside = _plan_groups(spend)
    head = ["статья", "факт", "план"]

    def rows_of(lines: list[PlanLine]) -> list[list[str]]:
        return [[x.subcategory, whole(x.fact), whole(x.plan)] for x in lines]

    if over:
        blocks += [heading("🔴 Сверх плана"), table(rows_of(over), "lrr", head)]
    if near:
        blocks += [
            heading(f"🟡 Почти всё (от {percent(NEAR_LIMIT)})"),
            table(rows_of(near), "lrr", head),
        ]
    if outside:
        named = [[_icon_name(x.group, x.subcategory), whole(x.fact)] for x in outside]
        blocks += [heading("⚪ Вне плана"), table(named, "lr")]

    earn_plan = sum(line.plan for line in earn)
    earn_fact = sum(line.fact for line in earn)
    if earn_plan or earn_fact:
        share = (
            f"из {whole(earn_plan)} — {percent(earn_fact / earn_plan)} плана"
            if earn_plan
            else "плана на месяц нет"
        )
        blocks += [heading(f"💰 Доходы {whole(earn_fact)}"), para(share)]
    return blocks


class AnalyticsService:
    def __init__(
        self,
        gs_service: GoogleSheetsService,
        catalog: "Callable[[], Catalog] | None" = None,
    ) -> None:
        """catalog — справочник бота на момент чтения (bot_catalog): по нему «Сводка»,
        утро и /today читают копилки для «Заморожено»; без него копилки не читаются."""
        self.gs_service = gs_service
        self.catalog = catalog

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
        sheet = month_title(today)
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

    def _month_rows(self, today: date) -> list[list[Any]] | None:
        """Вкладка месяца для план-факта; None — её нет (или не прочиталась)."""
        sheet = month_title(today)
        try:
            return self.gs_service.get_values(sheet)
        except Exception:
            logger.info("No plan sheet %s", sheet)
            return None

    def _previous_rows(self, today: date) -> list[list[Any]] | None:
        """Вкладка прошлого месяца — для переноса недополученных поступлений; None —
        её нет или она не прочиталась (сообщение — без переноса)."""
        sheet = month_title(today.replace(day=1) - timedelta(days=1))
        try:
            return self.gs_service.get_values(sheet)
        except WorksheetNotFound:
            return None
        except Exception:
            logger.warning(
                "Last month's tab %s was not read: no carry-over", sheet, exc_info=True
            )
            return None

    def last_months_rest(self, today: date) -> tuple["ForecastItem", ...]:
        """Что прошлый месяц ещё ждёт на сейчас — для вопроса «за какой месяц?» после
        поступления; вкладки прошлого месяца нет — ()."""
        from app.services.day_budget import carried_items

        previous = self._previous_rows(today)
        if previous is None:
            return ()
        rates = self.gs_service.get_rates()
        frame = transactions_frame(
            self.gs_service.get_values(config.FACT_SHEET_NAME), rates
        )
        return carried_items(previous, frame, today.replace(day=1), today)

    def plan_report(self, today: date | None = None) -> Report:
        """/plan: план-факт текущего месяца по вкладке «Oct 26»."""
        today = today or local_today(config.ANALYTICS_TIMEZONE)
        rows = self._month_rows(today)
        if rows is None:
            return Report(_no_tab(today))
        return Report(
            format_plan_report(rows, today), "".join(rich_plan_report(rows, today))
        )

    def _records(self) -> "tuple[CalendarRecord, ...]":
        """Записи календаря: лист «Календарь» (бот держит его копию минуту)."""
        from app.services.calendar import read_calendar

        return read_calendar(self.gs_service.get_calendar())

    def _piggies_now(
        self,
        sheets: tuple[list[list[Any]], list[list[Any]]] | None,
        frame: pd.DataFrame,
        rates: Rates,
        today: date,
    ) -> float | None:
        """Копилки на сейчас для «Заморожено»; без справочника или листов — None. Сбой —
        в лог: «Заморожено» покажется без «не трогаем», экран и утро не падают."""
        from app.services.piggy import piggies_now

        if self.catalog is None:
            return None
        try:
            return piggies_now(
                sheets, self.catalog(), frame, rates, today, config.FROZEN_CURRENCY
            )
        except Exception:
            logger.warning(
                "Piggy banks were not counted: frozen money without them",
                exc_info=True,
            )
            return None

    def _piggy_sheets(self) -> tuple[list[list[Any]], list[list[Any]]] | None:
        """Листы копилок для утра и /today — заново; не прочитались — None."""
        try:
            return self.gs_service.get_piggy()
        except Exception:
            logger.warning(
                "Piggy sheets were not read: frozen money without piggies",
                exc_info=True,
            )
            return None

    def morning_brief(self, today: date | None = None, now: bool = False) -> Report:
        """Лимит на сегодня по вкладке месяца: утреннее сообщение и /today."""
        return self._brief(today, now)[0]

    def morning_message(self, today: date | None = None) -> tuple[Report, bool]:
        """Утреннее сообщение и нужен ли 🎉 (вчера уложились в лимит)."""
        from app.services.day_budget import celebrate

        report, budget = self._brief(today, now=False)
        return report, budget is not None and celebrate(budget)

    def _brief(self, today: date | None, now: bool) -> tuple[Report, Any]:
        # day_budget и recurring сами берут отсюда plan_lines и expenses
        from app.services.day_budget import (
            day_budget,
            format_day_budget,
            read_forecast,
            rich_day_budget,
        )
        from app.services.recurring import (
            find_series,
            morning_lines,
            outside_list,
            rich_morning_lines,
        )

        today = today or local_today(config.ANALYTICS_TIMEZONE)
        sheet = month_title(today)
        try:
            rows = self.gs_service.get_values(sheet)
        except WorksheetNotFound:
            # сбой сети — не «вкладки нет»: ошибка уйдёт в лог или ответ /today
            logger.info("No plan sheet %s", sheet)
            text = f"Вкладки «{sheet}» с планом нет — бот создаёт её 1-го числа."
            return Report(text), None
        rates = self.gs_service.get_rates()
        frame = transactions_frame(
            self.gs_service.get_values(config.FACT_SHEET_NAME), rates
        )
        budget = day_budget(
            frame,
            rows,
            today,
            rates,
            config.FROZEN_CURRENCY,
            self._records(),
            previous=self._previous_rows(today),
        )
        if budget is None:
            return (
                Report(
                    f"Во вкладке «{sheet}» нет списка поступлений и платежей (блок "
                    "прогноза справа) — лимит на день не посчитать.\n\n"
                    + format_plan_report(rows, today)
                ),
                None,
            )
        if budget.frozen is not None and self.catalog is not None:
            sheets = self._piggy_sheets()
            budget = replace(
                budget, piggies=self._piggies_now(sheets, frame, rates, today)
            )
        listed = read_forecast(rows)
        series = outside_list(find_series(frame, today), listed.items if listed else ())
        text = [format_day_budget(budget, now=now), *morning_lines(series, today)]
        html = [*rich_day_budget(budget, now), *rich_morning_lines(series, today)]
        return Report("\n".join(text), "".join(html)), budget

    def write_feedback(
        self,
        subcategories: list[str],
        icons: dict[str, str] | None = None,
        today: date | None = None,
        *,
        salary_day_only: bool = False,
    ) -> Feedback:
        """После записи: статус каждой статьи месяца и сколько осталось на сегодня.

        salary_day_only — ответ на запись дохода: только «💸 С завтра можно …» в день
        прихода зарплаты (спека 1.4), в другой день — ничего.
        """
        from app.services.day_budget import (
            day_budget,
            plan_status,
            rich_plan_status,
            rich_today_line,
            today_line,
        )

        today = today or local_today(config.ANALYTICS_TIMEZONE)
        try:
            rows = self.gs_service.get_values(month_title(today))
        except WorksheetNotFound:
            return Feedback([], [])
        spend, _ = plan_lines(rows)
        by_name = {line.subcategory: line for line in spend}
        lines = [plan_status(by_name.get(s), s, icons, today) for s in subcategories]
        blocks = [
            block
            for s in subcategories
            for block in rich_plan_status(by_name.get(s), s, today)
        ]
        rates = self.gs_service.get_rates()
        frame = transactions_frame(
            self.gs_service.get_values(config.FACT_SHEET_NAME), rates
        )
        # без вкладки прошлого месяца: перенос не трогает лимит в сумах
        budget = day_budget(
            frame, rows, today, rates, config.FROZEN_CURRENCY, self._records()
        )
        if salary_day_only and (budget is None or budget.tomorrow is None):
            return Feedback(lines, blocks)
        if budget is not None:
            lines.append(today_line(budget))
            blocks += rich_today_line(budget)
        return Feedback(lines, blocks)

    def subs_report(self, today: date | None = None) -> Report:
        """/subs: регулярные списания по истории fact и план подписок месяца."""
        from app.services.day_budget import read_forecast
        from app.services.recurring import (
            SUBSCRIPTION,
            find_series,
            format_subs,
            outside_list,
            rich_subs,
        )

        today = today or local_today(config.ANALYTICS_TIMEZONE)
        rates = self.gs_service.get_rates()
        frame = transactions_frame(
            self.gs_service.get_values(config.FACT_SHEET_NAME), rates
        )
        found = find_series(frame, today)
        if not found:
            return Report(
                "Регулярных списаний пока не нашёл — нужно хотя бы два месяца истории."
            )
        plan, listed = None, None
        try:
            rows = self.gs_service.get_values(month_title(today))
            spend, _ = plan_lines(rows)
            plan = next((x.plan for x in spend if x.subcategory == SUBSCRIPTION), None)
            listed = read_forecast(rows)
        except WorksheetNotFound:
            logger.info("No plan sheet for the subscriptions plan")
        found = outside_list(found, listed.items if listed else ())
        return Report(
            format_subs(found, today, plan), "".join(rich_subs(found, today, plan))
        )

    def dashboard(
        self, today: date | None = None, max_age: float = 0.0
    ) -> "Dashboard | None":
        """Экран «Сводка» Mini App; None — вкладки месяца нет.

        max_age > 0 — годятся копии листов не старше стольких секунд.
        """
        from app.services.calendar import read_calendar
        from app.services.cards import card_balances
        from app.services.dashboard import build_dashboard

        today = today or local_today(config.ANALYTICS_TIMEZONE)
        sheet = month_title(today)
        # копилки — для «Заморожено»: вместе с остальными листами, если есть справочник
        piggy = self.catalog is not None and config.FROZEN_CURRENCY is not None
        sheets = read_month_sheets(
            self.gs_service, sheet, max_age, previous=True, piggy=piggy
        )
        if sheets.month is None:
            logger.info("No plan sheet %s", sheet)
            return None
        rates, fact = sheets.rates, sheets.fact
        try:
            cards = card_balances(fact, rates)  # блок остатков — из того же чтения
        except Exception:
            # блок правят руками: его поломка не должна убрать всю «Сводку»
            logger.exception("Could not read the card balance block")
            cards = ()
        frame = transactions_frame(fact, rates)
        return build_dashboard(
            frame,
            sheets.month,
            today,
            rates,
            config.FROZEN_CURRENCY,
            read_calendar(sheets.calendar),
            cards,
            previous=sheets.previous,
            piggies=self._piggies_now(sheets.piggy, frame, rates, today),
        )

    def savings(
        self,
        today: date,
        currencies: Iterable[str],
        groups: Iterable[str],
        subcategories: dict[str, list[str]],
        max_age: float = 0.0,
        *,
        percent: float | None = None,
        order: Sequence[str] | None = None,
    ) -> "SavingsTab | None":
        """«Накопления» (раздел «Плана»); None — вкладки месяца нет. percent и order —
        предпросмотр окон «Процент» и «Порядок».

        currencies, groups, subcategories — валюты карт и справочник статей бота:
        по ним проверяются строки листа «Копилки».
        """
        from app.services.day_budget import day_budget
        from app.services.piggy import read_book, savings_screen
        from app.services.savings import month_savings

        sheet = month_title(today)
        sheets = read_month_sheets(self.gs_service, sheet, max_age, calendar=False)
        if sheets.month is None:
            logger.info("No plan sheet %s", sheet)
            return None
        piggies, journal = self.gs_service.get_piggy(max_age)
        rates = sheets.rates
        frame = transactions_frame(sheets.fact, rates)
        catalog = list(groups)
        book = read_book(piggies, journal, set(currencies), catalog, subcategories)
        budget = day_budget(frame, sheets.month, today, rates, config.FROZEN_CURRENCY)
        screen = savings_screen(
            book,
            frame,
            sheets.month,
            today,
            rates,
            budget,
            config.SAVE_SHARE,
            config.CUSHION_MONTHS,
            percent=percent,
            order=order,
            groups=catalog,
        )
        return SavingsTab(screen, month_savings(frame, today), rates)

    def forecast(
        self, today: date, currencies: Iterable[str], max_age: float = 0.0
    ) -> "MonthForecast | None":
        """Экран «Календарь» Mini App; None — вкладки месяца нет.

        max_age > 0 — годятся копии листов не старше стольких секунд.
        """
        from app.services.calendar import read_calendar
        from app.services.day_budget import read_forecast
        from app.services.forecast import month_forecast
        from app.services.plan import read_plan

        sheet = month_title(today)
        sheets = read_month_sheets(self.gs_service, sheet, max_age)
        if sheets.month is None:
            logger.info("No plan sheet %s", sheet)
            return None
        frame = transactions_frame(sheets.fact, sheets.rates, keep_unknown=True)
        listed = read_forecast(sheets.month)
        items = listed.items if listed is not None else ()
        return month_forecast(
            frame,
            read_plan(sheets.month).articles,
            items,
            today,
            sheets.rates,
            currencies,
            read_calendar(sheets.calendar),
        )

    def weekly_digest(self, today: date | None = None) -> Report:
        """Воскресная сводка: траты недели против прошлой и план-факт месяца."""
        today = today or local_today(config.ANALYTICS_TIMEZONE)
        week_start = today - timedelta(days=6)
        frame = expenses(self._frame())
        week = frame[(frame["day"] >= week_start) & (frame["day"] <= today)]
        before = frame[
            (frame["day"] >= week_start - timedelta(days=7))
            & (frame["day"] < week_start)
        ]
        spent, spent_before = week["amount_uzs"].sum(), before["amount_uzs"].sum()
        lines = [f"🗓 Неделя {week_start:%d.%m}–{today:%d.%m}"]
        change = (
            f", {spent / spent_before - 1:+.0%} к прошлой" if spent_before > 0 else ""
        )
        lines.append(
            f"Потрачено {money(spent)} сум (прошлая неделя {money(spent_before)}{change})"
        )
        top = (
            week.groupby(["group", "subcategory"])["amount_uzs"]
            .sum()
            .sort_values(ascending=False)
            .head(3)
        )
        top = top[top > 0]
        if not top.empty:
            lines.append(
                "Больше всего: "
                + ", ".join(f"{g} / {s} {money(v)}" for (g, s), v in top.items())
            )
        rows = self._month_rows(today)
        plan_text = format_plan_report(rows, today) if rows is not None else None
        reminder = self.balance_reminder(today)
        text = "\n".join([*lines, "", plan_text or _no_tab(today), "", reminder])

        about = ["потрачено за неделю"]
        if spent_before > 0:
            ratio = spent / spent_before - 1
            about.append(
                "столько же, сколько на прошлой"
                if round(ratio * 100) == 0
                else f"на {percent(abs(ratio))} "
                f"{'меньше' if ratio < 0 else 'больше'} прошлой ({whole(spent_before)})"
            )
        blocks = [
            para(f"🗓 Неделя {week_title(week_start, today)}"),
            heading(f"{whole(spent)} сум", 1),
            para(*about),
        ]
        if not top.empty:
            named = [[_icon_name(g, s), whole(v)] for (g, s), v in top.items()]
            blocks += [heading("Больше всего"), table(named, "lr")]
        blocks += [
            RULE,
            *(
                rich_plan_report(rows, today)
                if rows is not None
                else [para(_no_tab(today))]
            ),
            RULE,
            para(*reminder.splitlines()),
        ]
        return Report(text, "".join(blocks))

    def balance_reminder(self, today: date) -> str:
        """Напоминание прислать скрины банков; карты, которые давно не сверяли."""
        text = "📸 Пришлите скрины главных экранов банков — сверю остатки по картам."
        try:
            checks = self.gs_service.get_balance_checks()
        except Exception:
            logger.exception("Could not read balance checks")
            return text
        stale = [
            f"{source} ({(today - when.date()).days} дн.)"
            for source, when in checks.items()
            if when is not None and (today - when.date()).days > STALE_CHECK_DAYS
        ]
        return text + ("\nДавно не сверяли: " + ", ".join(stale) if stale else "")
