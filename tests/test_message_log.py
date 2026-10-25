"""Журнал сообщений: последний номер каждого дня по местному времени."""

from datetime import UTC, date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from telegram.request import HTTPXRequest

from app.message_log import DAYS_KEPT, LoggingRequest, MessageLog

TASHKENT = ZoneInfo("Asia/Tashkent")


def at(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, day, hour, minute, tzinfo=TASHKENT)


def test_last_number_of_each_local_day() -> None:
    log = MessageLog("Asia/Tashkent")
    log.record(1, 100, at(25, 10))
    log.record(1, 105, at(25, 23, 50))
    log.record(1, 104, at(25, 23, 40))  # пришло позже по сети — номер меньше
    log.record(1, 106, at(26, 0, 5))  # ночное «📅 Новый месяц» — уже сегодня
    log.record(2, 50, at(25, 12))
    assert log.last_before(1, date(2026, 10, 26)) == 105
    assert log.last_before(1, date(2026, 10, 27)) == 106
    assert log.last_before(1, date(2026, 10, 25)) is None
    assert log.last_before(2, date(2026, 10, 26)) == 50
    assert log.last_before(3, date(2026, 10, 26)) is None


def test_utc_time_counts_by_the_local_day() -> None:
    log = MessageLog("Asia/Tashkent")
    log.record(1, 200, datetime(2026, 10, 25, 19, 30, tzinfo=UTC))  # 00:30 26-го
    assert log.last_before(1, date(2026, 10, 26)) is None
    assert log.last_before(1, date(2026, 10, 27)) == 200


def test_old_days_are_forgotten() -> None:
    log = MessageLog("Asia/Tashkent")
    for number, day in enumerate(range(20, 26)):
        log.record(1, 10 + number, at(day, 12))
    assert log.last_before(1, date(2026, 10, 26 - DAYS_KEPT)) is None
    assert log.last_before(1, date(2026, 10, 26)) == 15


def test_telegram_answers_with_messages_are_logged() -> None:
    log = MessageLog("Asia/Tashkent")
    sent = int(at(25, 9).timestamp())
    message = {"message_id": 7, "date": sent, "chat": {"id": 1, "type": "private"}}
    log.record_result(message)
    log.record_result([{**message, "message_id": 8}, {**message, "message_id": 9}])
    for other in (True, {"ok": True}, {"message_id": 20}, [1, 2], None):
        log.record_result(other)  # не сообщение — пропуск
    assert log.last_before(1, date(2026, 10, 26)) == 9


async def test_logging_request_passes_the_answer_to_the_log(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent = int(at(25, 9).timestamp())
    answer = {"message_id": 31, "date": sent, "chat": {"id": 4}}

    async def post(self: Any, url: str, *args: Any, **kwargs: Any) -> Any:
        return answer

    monkeypatch.setattr(HTTPXRequest, "post", post)
    log = MessageLog("Asia/Tashkent")
    request = LoggingRequest(log, connection_pool_size=1)
    assert await request.post("https://api.telegram.org/bot1:x/sendMessage") is answer
    assert log.last_before(4, date(2026, 10, 26)) == 31


async def test_a_broken_log_never_breaks_sending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def post(self: Any, url: str, *args: Any, **kwargs: Any) -> Any:
        return {"message_id": 1, "date": "not a time", "chat": {"id": 4}}

    monkeypatch.setattr(HTTPXRequest, "post", post)
    request = LoggingRequest(MessageLog("Asia/Tashkent"), connection_pool_size=1)
    assert (await request.post("https://api.telegram.org/bot1:x/sendMessage"))[
        "message_id"
    ] == 1
