import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from functools import partial
from io import BytesIO
from typing import Any, NamedTuple

from telegram import Bot, Message, Update
from telegram.error import BadRequest, EndPointNotFound, TelegramError
from telegram.ext import ContextTypes

from app import config
from app.errors import user_message
from app.rich import Report, whole

logger = logging.getLogger(__name__)

CHUNK_SIZE = 4000  # лимит Telegram — 4096 символов
DRAFT_EVERY_SECONDS = 1.0  # как часто обновлять черновик /advice

Send = Callable[..., Awaitable[Message]]
REFUSED = (BadRequest, EndPointNotFound)  # Telegram ответил «нет» — можно иначе


def split_text(text: str, limit: int = CHUNK_SIZE) -> list[str]:
    """Куски не длиннее limit по границам строк — так не рвётся Markdown-разметка."""
    chunks: list[str] = []
    current = ""
    for line in text.splitlines(keepends=True):
        while len(line) > limit:
            if current:
                chunks.append(current)
                current = ""
            chunks.append(line[:limit])
            line = line[limit:]
        if len(current) + len(line) > limit:
            chunks.append(current)
            current = ""
        current += line
    if current:
        chunks.append(current)
    return chunks


async def send_markdown(send: Send, text: str) -> None:
    """Каждый кусок с Markdown; если разметка сломана — этот же кусок без неё.

    Раньше при ошибке в середине весь текст отправлялся заново, и начало дублировалось.
    """
    for chunk in split_text(text, CHUNK_SIZE):
        try:
            await send(chunk, parse_mode="Markdown")
        except BadRequest:
            await send(chunk, parse_mode=None)


class Sent(NamedTuple):
    """Каким видом ушло сообщение («rich» или «text») и все отправленные куски."""

    kind: str
    messages: tuple[Message, ...]


def _rich_message(html: str | None) -> dict[str, Any]:
    return {"html": html, "skip_entity_detection": True}


async def send_report(
    bot: Bot,
    chat_id: int,
    report: Report,
    effect: str | None = None,
    reply_markup: Any = None,
    send: Send | None = None,
) -> Sent:
    """Сводка расширенным сообщением; Telegram отказал — тем же обычным текстом.

    Попытки по очереди, следующая — только после отказа: расширенное с эффектом,
    без эффекта, обычный текст с эффектом, без эффекта. Сеть и таймауты — не отказ:
    сообщение могло дойти, и повтор дал бы дубль. Клавиатура — у последнего куска
    текста, эффект — у первого. send — чем слать текст (chat.send_message,
    reply_text…); по умолчанию bot.send_message(chat_id, …).
    """
    effects = [effect, None] if effect else [None]
    attempts = (
        [("rich", current) for current in effects]
        if report.html is not None and config.RICH_MESSAGES
        else []
    )
    attempts += [("text", current) for current in effects]
    chunks = split_text(report.text) or [report.text]
    markup = {"reply_markup": reply_markup} if reply_markup is not None else {}
    send_text = send or partial(bot.send_message, chat_id)
    for number, (kind, current) in enumerate(attempts, 1):
        options = {"message_effect_id": current} if current else {}
        try:
            if kind == "rich":
                message = await bot.do_api_request(
                    "sendRichMessage",
                    api_kwargs={
                        "chat_id": chat_id,
                        "rich_message": _rich_message(report.html),
                        **options,
                        **markup,
                    },
                    return_type=Message,
                )
                return Sent(kind, (message,))
            last = len(chunks) == 1
            sent = [await send_text(chunks[0], **options, **(markup if last else {}))]
        except REFUSED as error:
            if number == len(attempts):
                raise
            logger.warning(
                "Telegram refused the %s message%s: %s",
                kind,
                " with the effect" if current else "",
                error,
            )
            continue
        for index, chunk in enumerate(chunks[1:], 2):
            last = index == len(chunks)
            sent.append(await send_text(chunk, **(markup if last else {})))
        return Sent(kind, tuple(sent))
    raise RuntimeError("no way to send the report")  # попытка с текстом есть всегда


async def edit_report(
    bot: Bot,
    chat_id: int,
    message_id: int,
    report: Report,
    reply_markup: Any = None,
    edit: Send | None = None,
) -> str:
    """Правка сообщения: расширенным, при отказе — текстом; «rich» или «text».

    Без reply_markup Telegram снимает кнопки — так и задумано у подтверждений.
    edit — чем править текст (query.edit_message_text); по умолчанию
    bot.edit_message_text по chat_id и message_id.
    """
    markup = {"reply_markup": reply_markup} if reply_markup is not None else {}
    if report.html is not None and config.RICH_MESSAGES:
        try:
            await bot.do_api_request(
                "editMessageText",
                api_kwargs={
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "rich_message": _rich_message(report.html),
                    **markup,
                },
            )
            return "rich"
        except REFUSED as error:
            logger.warning("Telegram refused the rich edit: %s", error)
    if edit is not None:
        await edit(report.text, reply_markup=reply_markup)
    else:
        await bot.edit_message_text(
            chat_id=chat_id, message_id=message_id, text=report.text, **markup
        )
    return "text"


async def _send_charts(send_photo: Send, charts: list[tuple[str, BytesIO]]) -> None:
    for caption, chart in charts:
        with chart:
            await send_photo(photo=chart, caption=caption)


async def _delete_quietly(message: Message) -> None:
    try:
        await message.delete()
    except BadRequest:
        logger.debug("Status message already gone")


async def advice_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/advice: выводы Gemini по цифрам месяца, текст появляется по мере генерации."""
    ai_service = context.bot_data.get("ai_service")
    analytics_service = context.bot_data.get("analytics_service")
    if not ai_service or not analytics_service:
        await update.message.reply_text("AI сервис не доступен.")
        return
    try:
        numbers = await asyncio.to_thread(analytics_service.advice_context)
    except Exception as e:
        logger.exception("Advice numbers failed")
        await update.message.reply_text(
            f"Произошла ошибка при анализе: {user_message(e)}"
        )
        return

    chat_id, draft_id = update.effective_chat.id, update.message.message_id
    text = ""
    try:
        await _draft(context.bot, chat_id, draft_id, "")  # «Думаю…»
        shown = 0.0
        async for text in ai_service.stream_analysis(numbers):
            if time.monotonic() - shown >= DRAFT_EVERY_SECONDS:
                await _draft(context.bot, chat_id, draft_id, text[:CHUNK_SIZE])
                shown = time.monotonic()
    except Exception:
        logger.warning(
            "Streaming advice failed, asking without streaming", exc_info=True
        )
        text = ""
    if not text:
        try:
            text = await ai_service.analyze_finances(numbers)
        except Exception as e:
            logger.exception("Advice failed")
            await update.message.reply_text(
                f"Произошла ошибка при анализе: {user_message(e)}"
            )
            return
    if len(text) > CHUNK_SIZE:
        text = text[: CHUNK_SIZE - 100] + "..."
    await send_markdown(update.message.reply_text, text)


async def _draft(bot: Bot, chat_id: int, draft_id: int, text: str) -> None:
    """Черновик ответа в чате; если клиент их не умеет — просто ждём итог."""
    try:
        await bot.send_message_draft(chat_id, draft_id, text)
    except TelegramError as error:
        logger.debug("Draft not shown: %s", error)


async def analytics_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for /analytics command: 3-day report with charts."""
    analytics_service = context.bot_data.get("analytics_service")
    if not analytics_service:
        await update.message.reply_text("Сервис аналитики не доступен.")
        return

    status_msg = await update.message.reply_text("📊 Формирую аналитику за 3 дня...")
    try:
        report_text, charts = await asyncio.to_thread(
            analytics_service.generate_3day_report
        )
    except Exception as e:
        logger.exception("Analytics failed")
        await status_msg.edit_text(
            f"Ошибка при формировании аналитики: {user_message(e)}"
        )
        return

    await _delete_quietly(status_msg)
    await send_markdown(update.message.reply_text, report_text)
    await _send_charts(update.message.reply_photo, charts)


async def send_daily_analytics(bot: Bot, chat_id: int, analytics_service: Any) -> None:
    """Ежедневный отчёт (JobQueue). Ошибки только в лог — пользователь ничего не ждёт."""
    try:
        report_text, charts = await asyncio.to_thread(
            analytics_service.generate_3day_report
        )
        await send_markdown(partial(bot.send_message, chat_id), report_text)
        await _send_charts(partial(bot.send_photo, chat_id), charts)
        logger.info("Daily analytics sent to chat %s", chat_id)
    except Exception:
        logger.exception("Failed to send daily analytics")


async def plan_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/plan: план-факт месяца без AI — сколько потрачено и сколько осталось."""
    analytics_service = context.bot_data.get("analytics_service")
    try:
        report = await asyncio.to_thread(analytics_service.plan_report)
    except Exception as e:
        logger.exception("Plan report failed")
        await update.message.reply_text(
            f"Не удалось собрать план-факт: {user_message(e)}"
        )
        return
    await send_report(context.bot, update.effective_chat.id, report)


async def send_morning_brief(bot: Bot, chat_id: int, analytics_service: Any) -> None:
    """Утреннее сообщение с лимитом (JobQueue); 🎉, если вчера уложились. Ошибки — в лог."""
    try:
        report, party = await asyncio.to_thread(analytics_service.morning_message)
        effect = config.CELEBRATE_EFFECT_ID if party else None
        sent = await send_report(bot, chat_id, report, effect)
        logger.info(
            "Morning brief sent as %s%s", sent.kind, " (party)" if effect else ""
        )
    except Exception:
        logger.exception("Failed to send the morning brief")


async def today_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/today: лимит на сегодня, сколько уже потрачено и сколько осталось."""
    analytics_service = context.bot_data.get("analytics_service")
    try:
        report = await asyncio.to_thread(analytics_service.morning_brief, now=True)
    except Exception as e:
        logger.exception("Today brief failed")
        await update.message.reply_text(
            f"Не удалось посчитать лимит: {user_message(e)}"
        )
        return
    await send_report(context.bot, update.effective_chat.id, report)


def _font_variants() -> list[tuple[str, str]]:
    """Временная проверка шрифтов: одна сумма разными способами (подпись, блок)."""
    number = whole(1_234_567_890)
    sample = f"💸 {number} сум"
    return [
        ("1 — заголовок, как сейчас", f"<h1>{sample}</h1>"),
        ("2 — заголовок, цифры моноширинным", f"<h1>💸 <code>{number}</code> сум</h1>"),
        ("3 — заголовок целиком моноширинным", f"<h1><code>{sample}</code></h1>"),
        ("4 — жирный текст обычного размера", f"<p><b>{sample}</b></p>"),
        ("5 — заголовок поменьше", f"<h4>{sample}</h4>"),
        ("6 — моноширинный блок", f"<pre>{sample}</pre>"),
        (
            "7 — таблица, жирным",
            f"<table bordered><tr><td><b>{sample}</b></td></tr></table>",
        ),
    ]


async def fonts_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/fonts — временная: одна сумма семью способами, чтобы выбрать вид без засечек;
    каждый вариант — своим сообщением: отказ Telegram в одном не скроет остальные."""
    for label, block in _font_variants():
        report = Report(f"Вариант {label}", f"<p>Вариант {label}</p>{block}")
        await send_report(context.bot, update.effective_chat.id, report)


async def subs_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/subs: подписки и другие платежи раз в месяц."""
    analytics_service = context.bot_data.get("analytics_service")
    try:
        report = await asyncio.to_thread(analytics_service.subs_report)
    except Exception as e:
        logger.exception("Subscriptions report failed")
        await update.message.reply_text(f"Не удалось собрать список: {user_message(e)}")
        return
    await send_report(
        context.bot, update.effective_chat.id, report, send=update.message.reply_text
    )


async def send_weekly_digest(bot: Bot, chat_id: int, analytics_service: Any) -> None:
    """Воскресная сводка (JobQueue). Ошибки только в лог."""
    try:
        report = await asyncio.to_thread(analytics_service.weekly_digest)
        sent = await send_report(bot, chat_id, report)
        logger.info("Weekly digest sent as %s", sent.kind)
    except Exception:
        logger.exception("Failed to send the weekly digest")
