"""Запросы и ответы API Mini App (Pydantic)."""

from datetime import date
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ErrorOut(ApiModel):
    code: str
    message: str
    fields: dict[str, str] | None = None


class ErrorEnvelope(ApiModel):
    error: ErrorOut


class UserOut(ApiModel):
    id: int
    first_name: str


class SourceOut(ApiModel):
    name: str
    currency: str


class SubcategoryOut(ApiModel):
    name: str
    icon: str


class GroupOut(ApiModel):
    name: str
    emoji: str
    title: str
    subcategories: list[SubcategoryOut]


class BootstrapOut(ApiModel):
    user: UserOut
    bot_username: str
    today: date
    default_source: str | None
    sources: list[SourceOut]
    groups: list[GroupOut]


class ExpenseIn(ApiModel):
    """Трата из формы: сумма строкой («12.50»), чтобы центы не терялись во float."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    entry_id: UUID
    source: str = Field(min_length=1, max_length=100)
    category: str = Field(min_length=1, max_length=100)
    subcategory: str = Field(min_length=1, max_length=100)
    amount: Decimal = Field(gt=0, le=Decimal(10) ** 12, decimal_places=2)
    comment: str = Field(default="", max_length=200)
    day: date | None = None  # нет — сегодня по часам бота, а не телефона


class RowsOut(ApiModel):
    first: int
    last: int


class ExpenseOut(ApiModel):
    status: Literal["written", "duplicate"]
    rows: RowsOut
    lines: list[str]


class MessageOut(ApiModel):
    message: str
