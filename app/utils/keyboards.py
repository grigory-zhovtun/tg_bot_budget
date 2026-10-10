"""Клавиатуры бота: карты внизу экрана, группы и подгруппы — под сообщением.

Кнопки под сообщением шириной с само сообщение, поэтому подписи над ними
называют карту и выбор: так кнопки шире, а пользователь видит, где он.
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.constants import KeyboardButtonStyle

BACK = "⬅️ Назад"
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


def generate_categories_keyboard(categories: list[str]) -> InlineKeyboardMarkup:
    """Группы по две в ряд: так кнопки шире, а названия не обрезаются."""
    buttons = [
        InlineKeyboardButton(name, callback_data=f"cat_{name}") for name in categories
    ]
    return InlineKeyboardMarkup(_rows(buttons, 2))


def categories_menu(
    categories: list[str], with_actions: bool = False
) -> InlineKeyboardMarkup:
    """Группы; сразу после записи сверху — «Исправить» и «Отменить» для неё."""
    rows = [
        list(row) for row in generate_categories_keyboard(categories).inline_keyboard
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
    sources: list[str], current_source: str | None = None
) -> ReplyKeyboardMarkup:
    """Карты внизу экрана, выбранная — с галочкой."""
    if not sources:
        return ReplyKeyboardMarkup([["Нет доступных источников"]], resize_keyboard=True)
    labels = [f"✅ {s}" if s == current_source else s for s in sources]
    rows = [labels[i : i + 3] for i in range(0, len(labels), 3)]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def generate_subcategories_keyboard(
    subcategories: dict[str, list[str]],
    selected_category: str,
    icons: dict[str, str] | None = None,
) -> InlineKeyboardMarkup:
    """Подгруппы с иконками по две в ряд; в callback — имя без иконки."""
    buttons = [
        InlineKeyboardButton(with_icon(name, icons), callback_data=f"sub_{name}")
        for name in subcategories.get(selected_category, [])
    ]
    back = [InlineKeyboardButton(BACK, callback_data="back_to_categories")]
    return InlineKeyboardMarkup([*_rows(buttons, 2), back])
