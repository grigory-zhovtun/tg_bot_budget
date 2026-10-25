"""Клавиатуры бота: карты внизу экрана, группы и подгруппы — под сообщением.

Кнопки под сообщением шириной с само сообщение, поэтому подписи над ними
называют карту и выбор: так кнопки шире, а пользователь видит, где он.
"""

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    WebAppInfo,
)
from telegram.constants import KeyboardButtonStyle

from app.custom_icons import CustomIcons, leading_emoji

BACK = "⬅️ Назад"
APP_BUTTON = "📱 Приложение"
# Кнопки под сводкой записи (обработчик app/handlers/last_write.py)
LAST_FIX, LAST_UNDO, LAST_CHECK = "last:fix", "last:undo", "last:check"
LAST_UNDO_YES, LAST_UNDO_NO = "last:undo:yes", "last:undo:no"
# Те же кнопки на самой сводке — когда клавиатуры групп и карт выключены
CARD = "card:"
CARD_FIX, CARD_UNDO, CARD_CHECK = "card:fix", "card:undo", "card:check"
CARD_UNDO_YES, CARD_UNDO_NO = "card:undo:yes", "card:undo:no"


def with_icon(name: str, icons: dict[str, str] | None) -> str:
    """«☕ кофе»: иконка подкатегории из system!D, если она есть."""
    icon = (icons or {}).get(name)
    return f"{icon} {name}" if icon else name


def action(
    text: str, callback_data: str, style: str | None = None
) -> InlineKeyboardButton:
    """Кнопка-действие; style — цвет: primary синий, success зелёный, danger красный."""
    return InlineKeyboardButton(text, callback_data=callback_data, style=style)


def _rows(
    buttons: list[InlineKeyboardButton], per_row: int
) -> list[list[InlineKeyboardButton]]:
    return [buttons[i : i + per_row] for i in range(0, len(buttons), per_row)]


def category_prompt(source: str) -> str:
    return f"💳 {source} — выберите категорию"


def menu_prompt(source: str | None, groups: bool) -> str:
    """Подпись меню под сводкой: с группами — «выберите категорию», без них — карта."""
    if not source:
        return "Категория:"
    return category_prompt(source) if groups else f"💳 {source}"


def subcategory_prompt(source: str, category: str) -> str:
    return f"💳 {source} · {category} — выберите подкатегорию"


def amount_prompt(
    source: str, category: str, subcategory: str, icons: dict[str, str] | None
) -> str:
    return (
        f"💳 {source} · {category} · {with_icon(subcategory, icons)}\n"
        "Введите сумму и комментарий, например «48000 латте»"
    )


def group_button(
    name: str, callback_data: str, custom: CustomIcons | None = None
) -> InlineKeyboardButton:
    """Группа; с картинкой из набора — без эмодзи в тексте, картинка вместо него."""
    custom_id = custom.groups.get(name) if custom else None
    if custom_id:
        text = leading_emoji(name)[1]
        return InlineKeyboardButton(
            text, callback_data=callback_data, icon_custom_emoji_id=custom_id
        )
    return InlineKeyboardButton(name, callback_data=callback_data)


def subcategory_button(
    name: str,
    callback_data: str,
    icons: dict[str, str] | None,
    custom: CustomIcons | None = None,
) -> InlineKeyboardButton:
    """Подкатегория с картинкой из набора или с обычной иконкой из system!D."""
    custom_id = custom.subcategories.get(name) if custom else None
    if custom_id:
        return InlineKeyboardButton(
            name, callback_data=callback_data, icon_custom_emoji_id=custom_id
        )
    return InlineKeyboardButton(with_icon(name, icons), callback_data=callback_data)


def generate_categories_keyboard(
    categories: list[str], custom: CustomIcons | None = None
) -> InlineKeyboardMarkup:
    """Группы по две в ряд: так кнопки шире, а названия не обрезаются."""
    buttons = [group_button(name, f"cat_{name}", custom) for name in categories]
    return InlineKeyboardMarkup(_rows(buttons, 2))


def record_actions(
    check: int = 0, on_card: bool = False
) -> list[list[InlineKeyboardButton]]:
    """«✏️ Исправить запись» и «↩️ Отменить запись»; над ними «🔎 Проверить (N)».

    check — сколько строк записи с пометкой 🆕/❓ ещё не проверено; on_card — кнопки
    на самой сводке (card:…), иначе — в меню под ней (last:…).
    """
    fix, undo, look = (
        (CARD_FIX, CARD_UNDO, CARD_CHECK)
        if on_card
        else (LAST_FIX, LAST_UNDO, LAST_CHECK)
    )
    rows = [
        [
            action("✏️ Исправить запись", fix, KeyboardButtonStyle.PRIMARY),
            action("↩️ Отменить запись", undo, KeyboardButtonStyle.DANGER),
        ]
    ]
    if check:
        label = f"🔎 Проверить ({check})"
        rows.insert(0, [action(label, look, KeyboardButtonStyle.PRIMARY)])
    return rows


def undo_confirm(on_card: bool = False) -> InlineKeyboardMarkup:
    """Подтверждение отмены; на сводке вопроса нет — кнопка называет запись."""
    if on_card:
        yes, no, label = CARD_UNDO_YES, CARD_UNDO_NO, "🗑 Да, удалить запись"
    else:
        yes, no, label = LAST_UNDO_YES, LAST_UNDO_NO, "🗑 Да, удалить"
    return InlineKeyboardMarkup(
        [[action(label, yes, KeyboardButtonStyle.DANGER), action("Нет", no)]]
    )


def categories_menu(
    categories: list[str],
    with_actions: bool = False,
    custom: CustomIcons | None = None,
    check: int = 0,
    groups: bool = True,
) -> InlineKeyboardMarkup:
    """Группы; сразу после записи сверху — «Исправить» и «Отменить» для неё.

    check — сколько строк записи ждут проверки («🔎 Проверить (N)» над действиями);
    groups=False — без групп (выключены в /keyboards), только кнопки записи.
    """
    rows = (
        [
            list(row)
            for row in generate_categories_keyboard(categories, custom).inline_keyboard
        ]
        if groups
        else []
    )
    if with_actions:
        rows = record_actions(check) + rows
    return InlineKeyboardMarkup(rows)


def generate_sources_keyboard(
    sources: list[str],
    current_source: str | None = None,
    app_url: str | None = None,
) -> ReplyKeyboardMarkup:
    """Карты внизу экрана, выбранная — с галочкой; последней строкой — Mini App."""
    if not sources:
        return ReplyKeyboardMarkup([["Нет доступных источников"]], resize_keyboard=True)
    labels = [f"✅ {s}" if s == current_source else s for s in sources]
    rows: list[list[str | KeyboardButton]] = [
        list(labels[i : i + 3]) for i in range(0, len(labels), 3)
    ]
    if app_url:
        app = KeyboardButton(
            APP_BUTTON, web_app=WebAppInfo(app_url), style=KeyboardButtonStyle.PRIMARY
        )
        rows.append([app])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def generate_subcategories_keyboard(
    subcategories: dict[str, list[str]],
    selected_category: str,
    icons: dict[str, str] | None = None,
    custom: CustomIcons | None = None,
) -> InlineKeyboardMarkup:
    """Подгруппы с иконками по две в ряд; в callback — имя без иконки."""
    buttons = [
        subcategory_button(name, f"sub_{name}", icons, custom)
        for name in subcategories.get(selected_category, [])
    ]
    back = [InlineKeyboardButton(BACK, callback_data="back_to_categories")]
    return InlineKeyboardMarkup([*_rows(buttons, 2), back])
