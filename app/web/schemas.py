"""Запросы и ответы API Mini App (Pydantic)."""

from datetime import date
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


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


class PlanItemOut(ApiModel):
    name: str
    icon: str
    plan: float
    fact: float


class GroupPlanOut(ApiModel):
    name: str
    plan: float
    fact: float
    items: list[PlanItemOut]


class DayPointOut(ApiModel):
    day: date
    plan: float
    fact: float | None


class DaySpendOut(ApiModel):
    day: date
    spent: float  # без платежей из списка
    limit: float  # лимит, с которым день начался


class ForecastItemOut(ApiModel):
    name: str
    day: int
    amount: float
    currency: str
    uzs: float


class SubscriptionOut(ApiModel):
    name: str
    day: int
    amount: float
    currency: str
    uzs: float
    state: Literal["charged", "twice", "expected", "missed"]


class FrozenOut(ApiModel):
    amount: float
    currency: str
    uzs: float
    change: float


class RunwayOut(ApiModel):
    """Прогноз по сумам: когда кончатся и сколько долларов обменять до поступления."""

    balance: float
    pace: float
    last_day: date | None
    short: float
    until: date | None
    dollars: float | None


class DashboardOut(ApiModel):
    """Сводка: суммы в сумах; у пунктов списка и подписок amount — в их валюте."""

    status: Literal["ok", "no_month_tab", "no_forecast"]
    month: str
    today: date
    limit: float | None = None
    spent_today: float | None = None
    left_today: float | None = None
    plan_per_day: float | None = None
    days_left: int | None = None
    balance: float | None = None
    planned_balance: float | None = None
    frozen: FrozenOut | None = None
    groups: list[GroupPlanOut] = []
    daily: list[DayPointOut] = []  # для открытых до обновления версий приложения
    spending: list[DaySpendOut] = []
    upcoming: list[ForecastItemOut] = []
    subscriptions: list[SubscriptionOut] = []
    runway: RunwayOut | None = None


class PlanHistoryOut(ApiModel):
    average: list[str]  # три полных месяца «YYYY-MM»
    last: str
    current: str
    current_days: int


class PlanArticleOut(ApiModel):
    key: str
    kind: Literal["spend", "income"]
    group: str
    subcategory: str
    icon: str
    currency: str
    plan: float  # положительный, в валюте строки
    average: float
    last: float
    current: float
    editable: bool


class PlanEventOut(ApiModel):
    slot: int
    name: str | None
    day: int | None
    amount: float | None  # со знаком: «+» поступление, «−» платёж
    currency: str | None


class PlanGoalOut(ApiModel):
    amount: float
    currency: str


class PlanAddableOut(ApiModel):
    """Пара справочника, которой нет во вкладке, — для «＋ статья»."""

    kind: Literal["spend", "income"]
    group: str
    subcategory: str
    icon: str
    currency: str
    average: float
    last: float
    current: float


class PlanOut(ApiModel):
    """План месяца для экрана «План»; без вкладки — только статус."""

    month: str
    title: str
    status: Literal["ok", "not_created", "no_month_tab"]
    today: date
    rates: dict[str, float] = {}
    history: PlanHistoryOut | None = None
    rows: list[PlanArticleOut] = []
    events: list[PlanEventOut] | None = None  # None — во вкладке нет блока прогноза
    goal: PlanGoalOut | None = None
    addable: list[PlanAddableOut] = []
    saved: int | None = None  # после сохранения — сколько правок записано


MAX_SUM = Decimal(10) ** 12


class EventValueIn(ApiModel):
    """Строка списка: сумма со знаком — «+» поступление, «−» платёж."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    name: str = Field(min_length=1, max_length=40)
    day: int = Field(ge=1, le=31)
    amount: Decimal = Field(ge=-MAX_SUM, le=MAX_SUM, decimal_places=2)
    currency: str = Field(pattern=r"^[A-Z]{3}$")

    @field_validator("amount")
    @classmethod
    def not_zero(cls, value: Decimal) -> Decimal:
        if value == 0:
            raise ValueError("сумма — не 0")
        return value


class EventCellsIn(ApiModel):
    """Строка списка, какой её видел пользователь (пустые поля — null)."""

    name: str | None
    day: int | None
    amount: float | None
    currency: str | None


class ArticleChangeIn(ApiModel):
    key: str = Field(min_length=3, max_length=200)
    base: float
    plan: Decimal = Field(ge=0, le=MAX_SUM, decimal_places=2)


class EventChangeIn(ApiModel):
    slot: int = Field(ge=0, le=50)
    base: EventCellsIn
    value: EventValueIn | None


class GoalIn(ApiModel):
    amount: Decimal = Field(ge=0, le=MAX_SUM, decimal_places=2)
    currency: str = Field(pattern=r"^[A-Z]{3}$")


class GoalBaseIn(ApiModel):
    amount: float
    currency: str


class GoalChangeIn(ApiModel):
    base: GoalBaseIn
    value: GoalIn


class NewArticleIn(ApiModel):
    """Новая статья: пара (подкатегория, валюта), которой нет во вкладке."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    group: str = Field(min_length=1, max_length=60)
    subcategory: str = Field(min_length=1, max_length=60)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    plan: Decimal = Field(ge=0, le=MAX_SUM, decimal_places=2)


class PlanSaveIn(ApiModel):
    month: str = Field(pattern=r"^\d{4}-\d{2}$")
    rows: list[ArticleChangeIn] = Field(default=[], max_length=300)
    events: list[EventChangeIn] = Field(default=[], max_length=30)
    goal: GoalChangeIn | None = None
    new_rows: list[NewArticleIn] = Field(default=[], max_length=10)  # MAX_NEW_ROWS


class PlanConflictOut(ApiModel):
    """409: план поменяли после открытия — свежий план и что именно поменялось."""

    error: ErrorOut
    plan: PlanOut
    conflicts: list[str]
