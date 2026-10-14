"""Сверка «Пришло к утру»: по пометке раз в две минуты и каждую ночь."""

from datetime import date
from types import SimpleNamespace
from typing import Any

import pytest

from app.handlers import receipts


def stub(monkeypatch: pytest.MonkeyPatch, outcome: int | Exception = 1) -> list[Any]:
    seen: list[Any] = []

    def sync(gs: Any, today: date) -> int:
        seen.append(gs)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(receipts, "sync_receipts", sync)
    return seen


async def test_the_job_reconciles_only_after_a_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = stub(monkeypatch)
    context = SimpleNamespace(bot_data={"gs_service": "sheets"})
    await receipts.receipts_job(context)
    assert seen == []
    receipts.mark_dirty(context.bot_data)
    await receipts.receipts_job(context)
    await receipts.receipts_job(context)
    assert seen == ["sheets"]


async def test_a_failed_reconciliation_stays_in_the_log(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stub(monkeypatch, ConnectionError("Google is down"))
    assert await receipts.sync_now({"gs_service": "sheets"}, date(2026, 10, 16)) == 0
