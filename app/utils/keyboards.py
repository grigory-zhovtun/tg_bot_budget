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
LAST_FIX, LAST_UNDO = "last:fix", "last:undo"
LAST_UNDO_YES, LAST_UNDO_NO = "last:undo:yes", "last:undo:no"


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


def categories_menu(
    categories: list[str],
    with_actions: bool = False,
    custom: CustomIcons | None = None,
) -> InlineKeyboardMarkup:
    """Группы; сразу после записи сверху — «Исправить» и «Отменить» для неё."""
    rows = [
        list(row)
        for row in generate_categories_keyboard(categories, custom).inline_keyboard
    ]
    if with_actions:
        rows.insert(
            0,
            [
                action("✏️ Исправить запись", LAST_FIX, KeyboardButtonStyle.PRIMARY),
                action("↩️ Отменить запись", LAST_UNDO, KeyboardButtonStyle.DANGER),
            ],
        )
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
