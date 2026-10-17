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


class CalendarIn(ApiModel):
    """Запись календаря из окна: сумма строкой, как у траты."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    entry_id: UUID | None = None  # у новой записи: повтор с тем же — без второй строки
    date: date
    repeat: Literal["once", "daily", "weekdays", "monthly"]
    weekdays: list[int] = []
    group: str = Field(min_length=1, max_length=100)
    subcategory: str = Field(min_length=1, max_length=100)
    currency: str = Field(min_length=3, max_length=3)
    amount: Decimal = Field(gt=0, le=Decimal(10) ** 12, decimal_places=2)
    name: str = Field(default="", max_length=60)


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


class SpentItemOut(ApiModel):
    name: str
    icon: str
    amount: float


class SpentGroupOut(ApiModel):
    """Траты группы за месяц по строкам fact, в сумах; подкатегории — от большей."""

    name: str
    amount: float
    items: list[SpentItemOut]


class CategoryLineOut(ApiModel):
    name: str
    icon: str
    plan: float  # вкладка месяца, в сумах
    fact: float  # строки fact месяца, в сумах


class CategoryOut(ApiModel):
    """План-факт группы: план вкладки и траты по строкам fact с 1-го по сегодня."""

    name: str
    plan: float
    fact: float
    items: list[CategoryLineOut]


class UntrackedOut(ApiModel):
    """Траты статьи без строки во вкладке месяца — в план-факт они не попали."""

    group: str
    subcategory: str
    currency: str
    amount: float  # в сумах


class CardOut(ApiModel):
    """Остаток карты по таблице и сверка с банком."""

    name: str  # без валюты — она у суммы: «VISA 1111»
    currency: str
    balance: float | None  # «Текущий баланс»; None — ошибка в ячейке
    uzs: float | None  # None — нет курса или остатка
    bank: float | None  # остаток банка при последней сверке
    checked: date | None  # когда сверяли
    matches: bool | None  # None — сверять не с чем
    stale: bool  # сверке больше недели


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
    spent: list[SpentGroupOut] = []  # для открытых до обновления версий приложения
    untracked: list[UntrackedOut] = []
    cards: list[CardOut] = []
    categories: list[CategoryOut] = []  # план-факт по категориям, от больших трат


class ForecastEventOut(ApiModel):
    day: int
    name: str
    amount: float  # что ещё впереди по строке: в её валюте и со знаком
    late: bool  # день строки прошёл — ждём сегодня


class ForecastDayOut(ApiModel):
    day: int
    plan: float  # остаток на вечер по плану
    pace: float  # остаток на вечер по темпу


class DayArticleOut(ApiModel):
    group: str
    subcategory: str
    icon: str
    amount: float  # в валюте строк; трата — плюсом, возврат — минусом
    uzs: float  # в сумах по курсу таблицы; 0 — курса нет
    income: bool


class ForecastPastOut(ApiModel):
    day: int
    evening: float  # сколько было вечером по факту; сегодня — сейчас
    change: float  # изменение за день: доход плюсом, расход минусом
    spent: float = 0  # траты дня по статьям расходов
    earned: float = 0  # доходы дня
    articles: list[DayArticleOut] = []  # по статьям; у «Всего» — пусто


class ForecastDipOut(ApiModel):
    start: int  # первый день ниже нуля
    until: int | None  # первый день после него не ниже нуля; None — не вернётся


class ForecastDipsOut(ApiModel):
    plan: ForecastDipOut | None
    pace: ForecastDipOut | None


class ForecastRecordOut(ApiModel):
    """Вхождение записи календаря в день: open — что ещё впереди, в валюте записи."""

    id: str
    day: int
    name: str
    group: str
    subcategory: str
    repeat: Literal["once", "daily", "weekdays", "monthly"]
    amount: float
    open: float
    done: bool
    over: bool  # записей статьи больше её остатка


class ForecastCurrencyOut(ApiModel):
    code: str
    now: float
    per_day_plan: float
    per_day_pace: float
    spent_today: float
    in_total: bool  # False — нет курса, во «Всего» не входит
    events: list[ForecastEventOut]
    days: list[ForecastDayOut]
    below_zero: ForecastDipsOut
    records: list[ForecastRecordOut] = []
    past: list[ForecastPastOut] = []  # с 1-го по сегодня: сегодня — что уже было


class ForecastTotalOut(ApiModel):
    uzs: list[ForecastDayOut]
    usd: list[ForecastDayOut]  # пусто — нет курса доллара
    below_zero: ForecastDipsOut
    past_uzs: list[ForecastPastOut] = []
    past_usd: list[ForecastPastOut] = []


class CalendarRecordOut(ApiModel):
    """Запись для списка экрана: повтор или разовая с сегодня."""

    id: str
    date: date
    repeat: Literal["once", "daily", "weekdays", "monthly"]
    weekdays: list[int]  # ISO: пн = 1 … вс = 7
    group: str
    subcategory: str
    currency: str
    amount: float
    name: str
    author: str
    month_total: float  # открыто с сегодня до конца месяца
    booked: float  # её доля в booked статьи — для предупреждения окна
    over: bool


class ForecastArticleOut(ApiModel):
    """Статья трат для окна записи: rest — план минус факт по вчера."""

    group: str
    subcategory: str
    currency: str
    plan: float
    rest: float
    booked: float  # записи статьи с сегодня, на утро


class HintOut(ApiModel):
    typical: float | None
    last: float | None
    per_day: float | None


class ForecastOut(ApiModel):
    """Прогноз до конца месяца по валютам карт и всего; без вкладки — только статус."""

    status: Literal["ok", "no_month_tab"]
    month: str
    today: date
    end: date | None = None
    currencies: list[ForecastCurrencyOut] = []
    total: ForecastTotalOut | None = None
    rates: dict[str, float] = {}  # к суму, без UZS
    calendar: list[CalendarRecordOut] = []
    articles: list[ForecastArticleOut] = []
    hints: dict[str, HintOut] = {}  # «подкатегория|валюта»


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
    received: float | None = None  # пришло по строке: в её валюте и со знаком
    received_on: date | None = None  # дата последней операции
    status: Literal["done", "partial", "waiting", "future"] = "future"


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


class PlanAdviceRowIn(ApiModel):
    key: str = Field(min_length=3, max_length=200)
    plan: Decimal = Field(ge=0, le=MAX_SUM, decimal_places=2)


class PlanAdviceEventIn(ApiModel):
    slot: int = Field(ge=0, le=50)
    value: EventValueIn | None


class PlanAdviceIn(ApiModel):
    """Черновик для советника: значения, как на экране, без того, что было."""

    month: str = Field(pattern=r"^\d{4}-\d{2}$")
    rows: list[PlanAdviceRowIn] = Field(default=[], max_length=300)
    new_rows: list[NewArticleIn] = Field(default=[], max_length=10)
    events: list[PlanAdviceEventIn] = Field(default=[], max_length=30)
    goal: GoalIn | None = None


class AdviceApplyOut(ApiModel):
    key: str
    plan: float


class AdvicePointOut(ApiModel):
    text: str
    apply: AdviceApplyOut | None = None


class PlanAdviceOut(ApiModel):
    points: list[AdvicePointOut]
