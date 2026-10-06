"""Импорт выписок в чате: сводка с кнопками, запись, отмена, повторные нажатия."""

from datetime import date
from itertools import count
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app.handlers import messages, statement_import
from app.services.google_sheets import row_values
from app.statements import Statement, Txn
from tests.test_documents import FakeFile, attach_document
from tests.test_messages import CATEGORIES, SOURCES, SUBCATEGORIES, make_chat
from tests.test_statements import LAYOUT

ids = count(500)
DAY = date(2026, 10, 7)


def serial(day: date) -> int:
    return (day - date(1899, 12, 30)).days


class StatementSheets:
    """Таблица для импорта: fact, отметки «выписка по», остатки."""

    def __init__(self, fact: list[list[Any]], marks: dict[str, date]) -> None:
        self.fact = [["Дата", "Категория", "Название", "Сумма", "", "", "", ""], *fact]
        self.marks = dict(marks)
        self.appended: list[list[Any]] = []
        self.adjusted: list[tuple[int, float, str]] = []
        self.deleted: list[int] = []

    def get_values(self, name: str) -> list[list[Any]]:
        assert name == "fact"
        return self.fact

    def get_statement_marks(self) -> dict[str, date]:
        return dict(self.marks)

    def set_statement_marks(self, marks: dict[str, date]) -> None:
        self.marks.update(marks)

    def append_transactions(self, rows: list[Any]) -> tuple[int, int]:
        self.appended += [row_values(row) for row in rows]
        return 4169, 4168 + len(rows)

    def update_fact_amounts(self, changes: list[tuple[int, float, str]]) -> None:
        self.adjusted += changes

    def delete_fact_rows(self, numbers: list[int], must_contain: str) -> list[int]:
        self.deleted += numbers
        return list(numbers)

    def get_table_balances(self) -> dict[str, float]:
        return {"VISA 9120 UZS": 14_000_000.0}


class FakeJobQueue:
    def __init__(self) -> None:
        self.jobs: list[SimpleNamespace] = []

    def get_jobs_by_name(self, name: str) -> list[SimpleNamespace]:
        return [job for job in self.jobs if job.name == name and not job.removed]

    def run_once(self, callback: Any, when: float, **kwargs: Any) -> None:
        job = SimpleNamespace(callback=callback, when=when, removed=False, **kwargs)
        job.schedule_removal = lambda: setattr(job, "removed", True)
        self.jobs.append(job)


class MerchantAI:
    enabled = True

    def __init__(self, answer: dict[str, tuple[str, str]]) -> None:
        self.answer = answer
        self.asked: list[list[str]] = []

    async def categorize_merchants(self, names: list[str], catalog: Any) -> Any:
        self.asked.append(names)
        return {k: v for k, v in self.answer.items() if k in names}


def sent(*args: Any, **kwargs: Any) -> SimpleNamespace:
    return SimpleNamespace(message_id=next(ids))


def make_context(sheets: StatementSheets, ai: Any = None) -> SimpleNamespace:
    return SimpleNamespace(
        bot_data={
            "gs_service": sheets,
            "ai_service": ai,
            "categories": CATEGORIES,
            "subcategories": SUBCATEGORIES,
            "sources": SOURCES,
        },
        user_data={"bot_messages": []},
        bot=SimpleNamespace(
            edit_message_text=AsyncMock(), send_message=AsyncMock(side_effect=sent)
        ),
        job_queue=FakeJobQueue(),
    )


def update_for(chat_id: int = 1) -> SimpleNamespace:
    return SimpleNamespace(
        effective_chat=SimpleNamespace(id=chat_id),
        effective_user=SimpleNamespace(id=42),
    )


def visa(*txns: Txn) -> Statement:
    return Statement("VISA 9120 UZS", list(txns), (date(2026, 10, 1), DAY), DAY)


def purchase(amount: float, name: str, uid: str = "a") -> Txn:
    details = f"Списание по операциям покупке {name} SLIP No 1 за 06.10.2026"
    return Txn(uid, "VISA 9120 UZS", DAY, date(2026, 10, 6), -amount, details)


def last_text(context: SimpleNamespace) -> str:
    return context.bot.edit_message_text.await_args.args[0]


def last_markup(context: SimpleNamespace) -> Any:
    return context.bot.edit_message_text.await_args.kwargs["reply_markup"]


async def receive(context: SimpleNamespace, statement: Statement) -> SimpleNamespace:
    status = SimpleNamespace(message_id=next(ids), delete=AsyncMock())
    context.user_data["bot_messages"].append(status.message_id)
    await statement_import.receive_statement(update_for(), context, statement, status)
    return status


async def run_preview(context: SimpleNamespace) -> None:
    [job] = [job for job in context.job_queue.jobs if not job.removed]
    context.job = job
    await job.callback(context)


def press(context: SimpleNamespace, data: str) -> SimpleNamespace:
    query = SimpleNamespace(
        data=data, answer=AsyncMock(), edit_message_text=AsyncMock()
    )
    return SimpleNamespace(
        callback_query=query,
        effective_chat=SimpleNamespace(id=1),
        effective_user=SimpleNamespace(id=42),
    )


async def test_statement_preview_then_write() -> None:
    temp = [
        serial(date(2026, 10, 6)),  # выравнивание до выписки, которая её покрывает
        "🚧 РАЗНОЕ",
        "неучтенка",
        500,
        "",
        "ВРЕМЕННАЯ",
        "UZS",
        "VISA 9120 UZS",
    ]
    sheets = StatementSheets([temp], {"VISA 9120 UZS": date(2026, 10, 5)})
    ai = MerchantAI({"NEW CAFE": ("🍔 ЕДА", "кафе")})
    context = make_context(sheets, ai)

    status = await receive(context, visa(purchase(48000, "NEW CAFE")))
    assert context.user_data["bot_messages"] == []  # сводку не сотрёт очистка чата
    assert "Готовлю сводку" in last_text(context)
    assert (
        context.bot.edit_message_text.await_args.kwargs["message_id"]
        == status.message_id
    )

    await run_preview(context)
    preview = last_text(context)
    assert "новых строк: 1" in preview
    assert "удалю временную строку 2" in preview
    assert "категория от AI: NEW Cafe" in preview
    assert ai.asked == [["NEW CAFE"]]
    buttons = last_markup(context).inline_keyboard[0]
    assert [b.callback_data for b in buttons] == ["import:ok:1", "import:no:1"]

    update = press(context, "import:ok:1")
    await statement_import.button(update, context)

    [row] = sheets.appended
    assert row[1:4] == ["🍔 ЕДА", "кафе", 48000.0]
    assert sheets.deleted == [2]
    assert sheets.marks["VISA 9120 UZS"] == date(2026, 10, 6)
    result = last_text(context)
    assert result.startswith("✅ Выписки загружены")
    assert "VISA 9120 UZS: 14 000 000" in result
    assert ai.asked == [["NEW CAFE"]]  # второй раз Gemini не спрашивали
    assert statement_import.BASKET not in context.user_data

    # повторное нажатие ничего не записывает
    await statement_import.button(press(context, "import:ok:1"), context)
    assert len(sheets.appended) == 1


async def test_cancel_changes_nothing() -> None:
    sheets = StatementSheets([], {})
    context = make_context(sheets)
    await receive(context, visa(purchase(48000, "NEW CAFE")))
    await run_preview(context)

    update = press(context, "import:no:1")
    await statement_import.button(update, context)
    assert sheets.appended == [] and sheets.marks == {}
    assert "отменён" in update.callback_query.edit_message_text.await_args.args[0]


async def test_second_statement_rebuilds_one_preview() -> None:
    sheets = StatementSheets([], {})
    context = make_context(sheets)
    await receive(context, visa(purchase(48000, "NEW CAFE", "a")))
    second = await receive(context, visa(purchase(9000, "OTHER", "b")))
    second.delete.assert_awaited()  # одно сообщение на весь импорт
    jobs = [job for job in context.job_queue.jobs if not job.removed]
    assert len(jobs) == 1 and jobs[0].data == 2

    # старая кнопка не срабатывает, пока сводка пересчитывается
    stale = press(context, "import:ok:1")
    await statement_import.button(stale, context)
    assert "обновляется" in stale.callback_query.answer.await_args.args[0]
    assert sheets.appended == []

    await run_preview(context)
    assert "новых строк: 2" in last_text(context)


async def test_nothing_new_only_moves_the_mark() -> None:
    known = [
        serial(date(2026, 10, 6)),
        "🍔 ЕДА",
        "кафе",
        48000,
        "",
        "AI: New Cafe",
        "UZS",
        "VISA 9120 UZS",
    ]
    sheets = StatementSheets([known], {})
    context = make_context(sheets)
    await receive(context, visa(purchase(48000, "NEW CAFE")))
    await run_preview(context)
    text = last_text(context)
    assert "Всё из выписок уже есть в таблице" in text
    assert last_markup(context) is None
    assert sheets.marks == {"VISA 9120 UZS": date(2026, 10, 6)}
    assert statement_import.BASKET not in context.user_data


async def test_ai_failure_does_not_block_the_import() -> None:
    class BrokenAI(MerchantAI):
        async def categorize_merchants(self, names: list[str], catalog: Any) -> Any:
            raise RuntimeError("503 overloaded")

    sheets = StatementSheets([], {})
    context = make_context(sheets, BrokenAI({}))
    await receive(context, visa(purchase(48000, "NEW CAFE")))
    await run_preview(context)
    assert "новый магазин: NEW Cafe" in last_text(context)


async def test_sheet_error_is_reported() -> None:
    sheets = StatementSheets([], {})
    sheets.get_values = lambda name: (_ for _ in ()).throw(
        ConnectionError("no network")
    )
    context = make_context(sheets)
    await receive(context, visa(purchase(48000, "NEW CAFE")))
    await run_preview(context)
    assert last_text(context) == "❌ Не разобрал выписки: no network"
    assert statement_import.BASKET not in context.user_data


async def test_statement_pdf_skips_gemini(monkeypatch: pytest.MonkeyPatch) -> None:
    received = AsyncMock()
    monkeypatch.setattr(messages, "pdf_text", lambda data: LAYOUT)
    monkeypatch.setattr(statement_import, "receive_statement", received)
    update, context, _ = make_chat("", {}, ai=None)
    attach_document(
        update,
        "statement.pdf",
        "application/pdf",
        FakeFile(lambda p: p.write_bytes(b"%PDF")),
    )

    await messages.document_handler(update, context)

    received.assert_awaited_once()
    statement = received.await_args.args[2]
    assert (statement.source, len(statement.txns)) == ("VISA 4058 USD", 2)
