"""/keyboards — включить и выключить клавиатуры групп и карт; /app — приложение.

Настройки — в таблице (system!Q:U, app/chat_state.py): очистка истории чата и
перезапуск бота их не меняют. Включили — клавиатура приходит сразу; выключили
карты — бот убирает их с экрана сообщением «⌨️ Карты скрыты», которое не
удаляет: без него приложение Telegram могло бы вернуть старую клавиатуру.
"""

from telegram import ReplyKeyboardRemove, Update
from telegram.ext import ContextTypes

from app import config
from app.chat_state import ChatState, state_of, update_state
from app.handlers.analytics import edit_report, send_report
from app.handlers.common import (
    ANCHOR,
    CHOOSE_SOURCE,
    premium_icons,
    sources_keyboard,
    track_message,
)
from app.rich import Report, heading, para
from app.utils.keyboards import (
    app_markup,
    category_prompt,
    generate_categories_keyboard,
    keyboards_markup,
)

TITLE = "⌨️ Клавиатуры"
NOT_SAVED = "⚠️ Не сохранил, попробуйте ещё раз"
HIDDEN = "⌨️ Карты скрыты"


def keyboards_report(state: ChatState, warning: str | None = None) -> Report:
    """Что включено — знаком и словом, чтобы не читать по цвету."""
    lines = [
        ("✅" if state.groups else "⬜")
        + " Группы под записью: "
        + ("включены" if state.groups else "выключены"),
        ("✅" if state.cards else "⬜")
        + " Карты внизу экрана: "
        + ("включены" if state.cards else "выключены"),
        *([warning] if warning else []),
    ]
    return Report("\n".join([TITLE, *lines]), heading(TITLE) + para(*lines))


async def keyboards_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    state = state_of(context.bot_data, chat.id)
    sent = await send_report(
        context.bot,
        chat.id,
        keyboards_report(state),
        reply_markup=keyboards_markup(state.groups, state.cards),
        send=chat.send_message,
    )
    for message in sent.messages:
        track_message(context, message)


async def keyboards_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """kb:groups:on|off, kb:cards:on|off — сохранить и сразу показать или убрать."""
    query = update.callback_query
    await query.answer()
    _, key, value = query.data.split(":")
    chat = update.effective_chat
    new = await update_state(context.bot_data, chat.id, **{key: value == "on"})
    state = new or state_of(context.bot_data, chat.id)
    await edit_report(
        context.bot,
        chat.id,
        query.message.message_id,
        keyboards_report(state, None if new else NOT_SAVED),
        reply_markup=keyboards_markup(state.groups, state.cards),
        edit=query.edit_message_text,
    )
    if new is None:
        return
    source = context.user_data.get("source") or context.bot_data.get("last_source")
    if key == "groups" and new.groups and source:
        menu = await chat.send_message(
            category_prompt(source),
            reply_markup=generate_categories_keyboard(
                context.bot_data.get("categories", []), premium_icons(context)
            ),
        )
        track_message(context, menu)
    elif key == "cards" and new.cards:
        anchor = await chat.send_message(
            f"💳 {source}" if source else CHOOSE_SOURCE,
            reply_markup=sources_keyboard(context, chat.id, source),
        )
        track_message(context, anchor)
        context.user_data[ANCHOR] = anchor.message_id
    elif key == "cards":
        hidden = await chat.send_message(HIDDEN, reply_markup=ReplyKeyboardRemove())
        context.user_data.pop(ANCHOR, None)
        await update_state(
            context.bot_data, chat.id, keep=(*new.keep, hidden.message_id)
        )


async def app_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/app — кнопка, которая открывает Mini App (когда клавиатура карт скрыта)."""
    chat = update.effective_chat
    if not config.WEBAPP_URL:
        await chat.send_message("Приложение не настроено")
        return
    message = await chat.send_message(
        "📱 Приложение бюджета", reply_markup=app_markup(config.WEBAPP_URL)
    )
    track_message(context, message)
