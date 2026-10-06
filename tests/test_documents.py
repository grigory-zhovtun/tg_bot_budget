"""Файлы и фото: скачивание во временную папку, извлечение текста, без утечек."""

from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pandas as pd
import PIL.Image
import pytest

from app import config
from app.handlers import messages
from tests.test_messages import FakeAI, make_chat


class FakeFile:
    """Файл Telegram: при скачивании кладёт заданное содержимое и запоминает путь."""

    def __init__(self, writer: Any) -> None:
        self.writer = writer
        self.path: Path | None = None

    async def download_to_drive(self, path: Path) -> Path:
        self.path = Path(path)
        self.writer(self.path)
        return self.path


@pytest.fixture(autouse=True)
def gemini_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "GEMINI_API_KEY", "test")


def attach_document(update: SimpleNamespace, name: str | None, mime: str, file: Any):
    update.message.document = SimpleNamespace(
        file_name=name, mime_type=mime, get_file=AsyncMock(return_value=file)
    )


async def test_document_without_name_is_rejected_politely() -> None:
    update, context, sheets = make_chat("", {})
    attach_document(update, None, "application/octet-stream", FakeFile(lambda p: None))
    await messages.document_handler(update, context)
    update.message.reply_text.assert_awaited_once()
    assert "Поддерживаются только" in update.message.reply_text.await_args.args[0]
    assert sheets.rows == []


@pytest.mark.parametrize("name", ["выписка.csv", "выписка.xlsx"])
async def test_table_document_goes_to_ai_and_temp_file_is_removed(name: str) -> None:
    frame = pd.DataFrame(
        {"Дата": ["05.10.2026"], "Сумма": [48000], "Описание": ["кофе"]}
    )

    def write(path: Path) -> None:
        if path.suffix == ".csv":
            frame.to_csv(path, index=False)
        else:
            frame.to_excel(path, index=False)

    ai = FakeAI(
        [
            {
                "amount": 48000,
                "source": "UZCARD 5837 UZS",
                "category": "🍔 ЕДА",
                "subcategory": "кофе",
            }
        ]
    )
    update, context, sheets = make_chat("", {}, ai)
    file = FakeFile(write)
    attach_document(update, name, "", file)

    await messages.document_handler(update, context)

    assert file.path is not None and not file.path.exists()
    assert file.path.name == f"upload{Path(name).suffix}"
    assert ai.calls == 1
    assert sheets.rows[0][1:4] == ["🍔 ЕДА", "кофе", 48000.0]


async def test_photo_is_closed_after_ai(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[PIL.Image.Image] = []

    class ImageAI(FakeAI):
        async def parse_transaction(self, **kwargs: Any) -> Any:
            seen.append(kwargs["image_part"])
            return []

    update, context, _ = make_chat("", {}, ImageAI([]))
    file = FakeFile(lambda path: PIL.Image.new("RGB", (4, 4), "white").save(path))
    update.message.photo = [SimpleNamespace(get_file=AsyncMock(return_value=file))]
    update.message.caption = None

    await messages.text_handler(update, context)

    [image] = seen
    assert image.size == (4, 4)
    assert file.path is not None and not file.path.exists()
    with pytest.raises(ValueError):
        image.load()  # закрытая картинка больше не читается
