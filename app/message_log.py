"""Журнал сообщений: последний номер каждого дня в каждом чате.

Утренняя уборка удаляет вчерашнее, а прочитать историю чата Bot API не даёт —
номера бот запоминает сам: пришедшие сообщения пишет обработчик всех обновлений
(app/handlers/cleanup.py, observe), отправленные — запрос к Telegram (LoggingRequest)
по ответу на любой метод отправки. Номера в личном чате идут подряд для обеих
сторон, поэтому последнего номера дня хватает, чтобы знать границу.
"""

import logging
from datetime import UTC, date, datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from telegram.request import HTTPXRequest

logger = logging.getLogger(__name__)

DAYS_KEPT = 3  # уборка смотрит на вчера; запас — если бот пропустил утро


class MessageLog:
    """Чат → день по местному времени → самый большой номер сообщения."""

    def __init__(self, timezone: str) -> None:
        try:
            self._zone = ZoneInfo(timezone)
        except ZoneInfoNotFoundError:
            logger.warning("Unknown timezone %r, the message log uses UTC", timezone)
            self._zone = ZoneInfo("UTC")
        self._days: dict[int, dict[date, int]] = {}

    def record(self, chat_id: int, message_id: int, when: datetime) -> None:
        day = when.astimezone(self._zone).date()
        days = self._days.setdefault(chat_id, {})
        days[day] = max(days.get(day, message_id), message_id)
        while len(days) > DAYS_KEPT:
            del days[min(days)]

    def record_result(self, result: Any) -> None:
        """Ответ Telegram: сообщение или список сообщений; остальное — пропуск."""
        for item in result if isinstance(result, list) else [result]:
            if not isinstance(item, dict):
                continue
            chat = item.get("chat")
            if "message_id" in item and "date" in item and isinstance(chat, dict):
                when = datetime.fromtimestamp(item["date"], UTC)
                self.record(chat["id"], item["message_id"], when)

    def last_before(self, chat_id: int, day: date) -> int | None:
        """Последний известный номер сообщения в чате до этого дня."""
        earlier = [
            number for when, number in self._days.get(chat_id, {}).items() if when < day
        ]
        return max(earlier) if earlier else None


class LoggingRequest(HTTPXRequest):
    """Запрос к Telegram, который отдаёт ответы в журнал сообщений."""

    def __init__(self, log: MessageLog, **options: Any) -> None:
        super().__init__(**options)
        self._log = log

    async def post(self, *args: Any, **kwargs: Any) -> Any:
        result = await super().post(*args, **kwargs)
        try:
            self._log.record_result(result)
        except Exception:  # журнал не мешает отправке
            logger.exception("Could not log a Telegram answer")
        return result
