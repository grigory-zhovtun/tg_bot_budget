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
    utc_offset: int  # минут к востоку от UTC у бота: «сегодня по боту» в момент нажатия
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


class FrozenPaymentOut(ApiModel):
    """Платёж срока в валюте заморозки: день (в следующем месяце — его день там)."""

    day: date
    name: str
    amount: float  # плюсом


class FrozenOut(ApiModel):
    """«🧊 Заморожено»: доллары на сейчас без платежей срока — подушка и копилки."""

    amount: float
    currency: str
    uzs: float
    change: float = 0.0  # «с 1-го числа» убрана; для открытых до обновления
    total: float = 0.0  # всего на картах в этой валюте
    payments: list[FrozenPaymentOut] = []
    piggies: float | None = None  # копилки на сейчас; None — их не читали
    cushion: float | None = None  # подушка: заморожено без копилок; None — так же
    over: float = 0.0  # копилки больше карты на столько
    lacking: float = 0.0  # копилок нет, а долларов на платежи не хватает
    lacking_on: date | None = None  # день платежа, с которого не хватает
    target: str = ""  # срок платежей, как у «Сумы»: «до зарплаты 30 ноября»


class CushionStepOut(ApiModel):
    """Ступень подушки: 1, 3 месяца или цель — сколько ещё до неё."""

    months: int
    short: float  # в валюте подушки, вверх до целых
    short_uzs: float  # в сумах, целыми


class CushionOut(ApiModel):
    """Подушка безопасности: «Заморожено» без копилок на сейчас в месяцах жизни."""

    amount: float  # в валюте подушки
    currency: str
    uzs: float
    monthly: float  # месяц жизни: средние траты обязательных групп, сумы
    months: float | None  # месяцев подушки, один знак вниз; None — трат ещё нет
    target_months: int
    short_uzs: float  # сколько не хватает до цели, сумы; 0 — цель достигнута
    short: float  # то же в валюте подушки
    frozen: float | None = None  # «Заморожено» на сейчас
    piggies: float = 0.0  # остатки копилок на сейчас, в валюте подушки
    over: float = 0.0  # на сколько копилки больше «Заморожено»
    card: float = 0.0  # всего на картах в валюте подушки
    payments: list[FrozenPaymentOut] = []  # платежи срока в этой валюте
    lacking: float = 0.0  # копилок нет, а долларов на платежи не хватает
    lacking_on: date | None = None  # день платежа, с которого не хватает
    life_months: list[str] = []  # месяцы месяца жизни: «2026-07»
    life_groups: list[str] = []  # обязательные группы справочника
    steps: list[CushionStepOut] = []  # ступени, до которых ещё не дошли


class SavingMonthOut(ApiModel):
    month: str  # «2026-09»
    income: float  # сумы, без «нач остаток»
    spent: float  # сумы, без переводов между своими картами
    rate: float | None  # доля дохода, которая осталась; None — дохода не было


class SavingsOut(ApiModel):
    rate: float | None  # за 3 последних полных месяца
    months: list[SavingMonthOut]  # 6 полных месяцев, от старых
    half_rate: float | None = None  # за 6 полных месяцев; None — дохода не было
    current: SavingMonthOut | None = None  # текущий месяц с 1-го по сегодня
    worst: str | None = None  # «2026-07» — откуда минус за 3 месяца; None — плюс


class RunwayOut(ApiModel):
    """«Сумы» до зарплаты: когда кончатся и сколько долларов обменять до неё."""

    balance: float
    pace: float
    last_day: date | None
    short: float
    until: date | None  # день срока, если не хватает
    dollars: float | None
    left: float | None = None  # останется вечером в день срока
    payday: date | None = None  # день срока; None — конец месяца
    target: str | None = None  # «до зарплаты 30-го», «до зарплаты сегодня»


class TomorrowOut(ApiModel):
    """Лимит с завтра в день прихода зарплаты: сумы сейчас, срок с завтра."""

    limit: float
    days: int
    payday: date | None
    until: str  # «до зарплаты 30 ноября»
    sums: float
    bills: float
    advances: float
    advance_on: date | None


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
    # открытые до обновления версии приложения прячут по null «На картах … по плану»
    planned_balance: float | None = None
    sums: float | None = None  # сумы на вечер вчера: из них лимит до зарплаты
    bills: float | None = None  # платежи в сумах до дня перед зарплатой
    until: str | None = None  # «до зарплаты 30-го», «до аванса 15 ноября»
    payday_on: str | None = None  # «30-го», «15 ноября», «конца месяца»
    payday: date | None = None  # день, по который лимит; None — конец месяца
    advances: float | None = None  # авансы срока: прибавлены к деньгам лимита
    advance_on: date | None = None  # первый из них; None — авансов в сроке нет
    # день прихода зарплаты: лимит с завтра и сколько пришло по строке; иначе None
    tomorrow: TomorrowOut | None = None
    salary_came: float | None = None
    came: str | None = None  # «Пришла ЗП» или «Пришёл аванс» — вместе с tomorrow
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
    # сколько потрачено на самом деле: платёж, закрывший разовую запись, или
    # сегодня — её доля трат статьи; иначе 0
    spent: float = 0


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


class PlanGoalHintOut(ApiModel):
    """«Отложить за месяц» по копилкам для месяца «Плана», сумы."""

    suggested: float  # до тысячи
    piggies: float
    share: float
    spent: float  # траты из копилок в месяце
    percent: float = 0.0  # процент с поступлений, проценты


class PlanPiggyHintOut(ApiModel):
    """Трата из копилки в месяце «Плана»: заложить её в план статьи."""

    piggy: str
    month: str  # «2026-12»
    group: str
    subcategory: str
    currency: str  # валюта копилки — и статьи
    amount: float  # уйдёт из копилки, в её валюте
    need: float  # сколько не хватает плану статьи
    key: str | None  # статья вкладки; None — строки нет, «Применить» добавит её


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
    goal_hint: PlanGoalHintOut | None = None  # None — открытых копилок нет
    piggy_hints: list[PlanPiggyHintOut] = []


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
    # меньше нуля — месяц тратит накопленное (спека 4.7: «Из накоплений»)
    amount: Decimal = Field(ge=-MAX_SUM, le=MAX_SUM, decimal_places=2)
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


class AmountOut(ApiModel):
    id: str  # копилка
    amount: float  # сумы


class MoveOut(ApiModel):
    day: date
    amount: float  # в валюте копилки: плюс — отложили, минус — забрали
    kind: str
    who: str


class SpendOut(ApiModel):
    key: str  # ключ строки fact — для «Не из копилки»
    day: date
    group: str
    subcategory: str
    amount: float  # в валюте копилки


class PiggyOut(ApiModel):
    """Копилка «Накоплений»; суммы — в её валюте."""

    id: str
    row: int  # строка листа «Копилки»
    name: str
    target: float | None
    currency: str
    deadline: date | None
    repeat: Literal["once", "yearly"]
    group: str | None
    subcategory: str | None
    author: str
    saved: float
    spent: float
    balance: float
    collected: float  # к ближайшему сроку: остаток и то, что из него уже потратили
    this_month: float
    left_this_month: float
    status: Literal[
        "on_track", "late", "short", "no_target", "done", "leftover", "goal", "broken"
    ]
    short: float
    leftover: float
    error: str | None
    order: int | None = None  # номер важности; null — не в порядке
    forecast: float | None = None  # будет к ближайшему сроку по плану
    now: float = 0.0  # отложить сейчас
    saved_month: float = 0.0  # отложено в этом месяце
    next_month: float = 0.0  # взнос следующего месяца плана
    history: list[MoveOut]
    spends: list[SpendOut]


class MonthPlanOut(ApiModel):
    month: str  # «2026-10»
    total: float  # взносы копилок со сроком, сумы
    items: list[AmountOut]
    cushion: float  # доля процента подушке
    goals: list[AmountOut]  # доля 10 % долгим целям
    ready: list[str]  # копилки, собранные к сроку в этом месяце


class SavingsGoalOut(ApiModel):
    """«🎯 Отложить за месяц» по копилкам (спека 4.7) и что сейчас во вкладке, сумы."""

    suggested: float  # до тысячи
    in_plan: float | None  # строка цели вкладки; None — строки нет
    piggies: float
    share: float
    spent: float  # траты из копилок в месяце


class SavingsMonthOut(ApiModel):
    """Текущий месяц: процент с поступлений и сколько отложить, сумы."""

    month: str
    percent: float  # проценты: 5.0
    percent_source: Literal["sheet", "default", "bad", "preview"]
    percent_needed: int | None  # целые проценты «успеть всё»; null — копить нечего
    capacity: float  # процент от поступлений плана месяца
    received: float  # пришло с 1-го по сегодня
    due: float  # процент от пришедшего
    left: float  # отложить сейчас — копилкам и долгим целям
    cushion: float  # часть подушке
    saved: float  # отложено копилкам с 1-го числа
    cushion_saved: float = 0.0  # «Отложено подушке»: её настоящий рост с 1-го
    # сколько подушке ещё не отложено из её части, пока действует «пополам» (1.7);
    # 0 — подушке сначала ничего
    cushion_room: float = 0.0


class EnoughOut(ApiModel):
    income: float
    save: float
    life: float
    ordinary: float
    gap: float  # меньше нуля — не хватает


class SavingsScreenOut(ApiModel):
    """«Накопления»: копилки, план на год, подушка за вычетом копилок и норма."""

    status: Literal["ok", "no_month_tab"]
    today: date
    month: SavingsMonthOut | None = None
    goal: SavingsGoalOut | None = None  # None — открытых копилок нет
    cushion: CushionOut | None = None
    piggies: list[PiggyOut] = []
    plan: list[MonthPlanOut] = []
    enough: EnoughOut | None = None
    savings: SavingsOut | None = None
    rates: dict[str, float] = {}


class PiggyIn(ApiModel):
    """Копилка из окна: цель строкой, как сумма траты."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    entry_id: UUID | None = None  # у новой: повтор с тем же — без второй строки
    name: str = Field(min_length=1, max_length=40)
    target: Decimal | None = Field(
        default=None, gt=0, le=Decimal(10) ** 12, decimal_places=2
    )
    currency: str = Field(min_length=3, max_length=3)
    deadline: date | None = None
    repeat: Literal["once", "yearly"] = "once"
    group: str | None = Field(default=None, max_length=100)
    subcategory: str | None = Field(default=None, max_length=100)


class MoveIn(ApiModel):
    entry_id: UUID | None = None
    kind: Literal["saved", "spent", "write_off"]
    amount: Decimal = Field(gt=0, le=Decimal(10) ** 12, decimal_places=2)


class PercentIn(ApiModel):
    """«Откладываем N % с поступлений»."""

    percent: Decimal = Field(ge=0, le=100, decimal_places=1)


class OrderIn(ApiModel):
    """Порядок важности: ID копилок со сроком, самая важная — первой."""

    ids: list[str] = Field(min_length=1, max_length=50)


class LifeIn(ApiModel):
    """Обязательные группы месяца жизни подушки — группы трат справочника."""

    groups: list[str] = Field(min_length=1, max_length=40)


class SaveIn(ApiModel):
    """«Отложил…» одной суммой на все копилки."""

    entry_id: UUID | None = None
    amount: Decimal = Field(gt=0, le=Decimal(10) ** 12, decimal_places=2)
    currency: str = Field(min_length=3, max_length=3)


class ExcludeIn(ApiModel):
    key: str = Field(min_length=1, max_length=500)
