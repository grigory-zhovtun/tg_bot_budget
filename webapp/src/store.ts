/**
 * Ответы API на время работы приложения и их копия в памяти телефона: экран
 * открывается сразу с тем, что уже известно, а свежие данные приходят фоном.
 */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import { ApiError } from "./api";

export const CACHE_KEY = "app-cache";
/** Сколько ответ этого запуска считается свежим: экран его не перезапрашивает. */
export const FRESH_MS = 30_000;

export interface Cached {
  data: unknown; // null — ответа нет
  at: number; // когда пришёл, Date.now(); 0 — устарел после записи
  live: boolean; // пришёл за этот запуск; false — из памяти телефона
  loading: boolean; // запрос в пути
}

const EMPTY: Cached = { data: null, at: 0, live: false, loading: false };

export interface StoreOptions {
  storage?: Storage | null; // нет — без копии в памяти телефона
  build?: string; // сборка приложения: копию другой сборки не берём
  today?: string; // «YYYY-MM-DD»: копию другого дня не берём
  user?: number | null; // пользователь Telegram: копию другого не берём
}

export interface FetchOptions {
  force?: boolean; // не присоединяться к запросу в пути, а отправить новый
  fresh?: boolean; // попросить сервер прочитать таблицу заново
}

type Load<T> = (fresh: boolean) => Promise<T>;

function ofToday(data: unknown, today: string | undefined): boolean {
  return (
    today !== undefined &&
    typeof data === "object" &&
    data !== null &&
    "today" in data &&
    data.today === today
  );
}

export class Store {
  private readonly options: StoreOptions;
  private readonly states = new Map<string, Cached>();
  private readonly listeners = new Map<string, Set<() => void>>();
  private readonly flights = new Map<string, Promise<unknown>>();
  private readonly turns = new Map<string, number>(); // номер последнего запроса ключа
  private readonly doubtful = new Set<string>(); // после записи старые цифры врут
  private opened = false;

  constructor(options: StoreOptions = {}) {
    this.options = options;
    this.restore();
  }

  get(key: string): Cached {
    return this.states.get(key) ?? EMPTY;
  }

  subscribe(key: string, listener: () => void): () => void {
    const set = this.listeners.get(key) ?? new Set();
    set.add(listener);
    this.listeners.set(key, set);
    return () => {
      set.delete(listener);
    };
  }

  /**
   * Загрузить ключ. Запрос того же ключа в пути — общий (если не force).
   * Первый запрос запуска просит таблицу заново: правки руками видны сразу.
   * Опоздавший ответ старого запроса новый не затирает.
   */
  fetch<T>(key: string, load: Load<T>, options: FetchOptions = {}): Promise<T> {
    const flying = this.flights.get(key);
    // «Обновить» к обычному запросу в пути не присоединяется
    if (flying && !options.force && !options.fresh) return flying as Promise<T>;
    const turn = (this.turns.get(key) ?? 0) + 1;
    this.turns.set(key, turn);
    const fresh = options.fresh === true || !this.opened;
    this.opened = true;
    const promise = load(fresh);
    this.flights.set(key, promise);
    this.set(key, { ...this.get(key), loading: true });
    // свои обработчики — первые: к продолжению вызывающего запрос уже приземлился
    const land = () => {
      if (this.flights.get(key) === promise) this.flights.delete(key);
      return this.turns.get(key) === turn;
    };
    promise.then(
      (data) => {
        if (land()) this.accept(key, data);
      },
      () => {
        if (!land()) return;
        // после записи старые цифры врали бы — лучше без них
        const kept = this.doubtful.has(key) ? EMPTY : this.get(key);
        this.doubtful.delete(key);
        this.set(key, { ...kept, loading: false });
      },
    );
    return promise;
  }

  /** Свежее за этот запуск — сразу, запрос в пути — он, иначе — новый запрос. */
  ensure<T>(key: string, load: Load<T>): Promise<T> {
    const state = this.get(key);
    if (state.live && Date.now() - state.at < FRESH_MS) {
      return Promise.resolve(state.data as T);
    }
    return this.fetch(key, load);
  }

  /** Ответ, пришедший с записью: он новее любого запроса в пути. */
  put(key: string, data: unknown): void {
    this.turns.set(key, (this.turns.get(key) ?? 0) + 1);
    this.flights.delete(key);
    this.accept(key, data);
  }

  /**
   * Таблицу поменяли: ключи (или все, что начинаются с «plan:») перезапросить.
   * Цифры видны, пока идёт новый запрос, но если он не удастся — уберутся.
   * Запрос, ушедший до записи, ответ не даст.
   */
  expire(keys: string[]): void {
    const matches = (key: string) =>
      keys.some((item) =>
        item.endsWith(":") ? key.startsWith(item) : key === item,
      );
    for (const [key, state] of this.states) {
      if (!matches(key)) continue;
      this.turns.set(key, (this.turns.get(key) ?? 0) + 1);
      this.flights.delete(key);
      if (state.data !== null) this.doubtful.add(key);
      this.set(key, { ...state, at: 0, loading: false });
    }
  }

  private accept(key: string, data: unknown): void {
    this.doubtful.delete(key);
    this.set(key, { data, at: Date.now(), live: true, loading: false });
    this.save();
  }

  private set(key: string, state: Cached): void {
    this.states.set(key, state);
    for (const listener of this.listeners.get(key) ?? []) listener();
  }

  private save(): void {
    const { storage, build = "", user = null } = this.options;
    if (!storage) return;
    const entries: Record<string, { data: unknown; at: number }> = {};
    for (const [key, state] of this.states) {
      if (ofToday(state.data, this.options.today)) {
        entries[key] = { data: state.data, at: state.at };
      }
    }
    try {
      storage.setItem(CACHE_KEY, JSON.stringify({ build, user, entries }));
    } catch {
      // память закрыта или полна: копия живёт до закрытия приложения
    }
  }

  private restore(): void {
    const { storage, build = "", user = null, today } = this.options;
    if (!storage) return;
    let saved: unknown;
    try {
      saved = JSON.parse(storage.getItem(CACHE_KEY) ?? "null");
    } catch {
      return;
    }
    if (
      typeof saved !== "object" ||
      saved === null ||
      !("build" in saved) ||
      saved.build !== build ||
      !("user" in saved) ||
      (user !== null && saved.user !== null && saved.user !== user) ||
      !("entries" in saved) ||
      typeof saved.entries !== "object" ||
      saved.entries === null
    ) {
      return;
    }
    for (const [key, entry] of Object.entries(saved.entries)) {
      if (
        typeof entry !== "object" ||
        entry === null ||
        !("data" in entry) ||
        !("at" in entry) ||
        typeof entry.at !== "number" ||
        !ofToday(entry.data, today)
      ) {
        continue;
      }
      this.states.set(key, {
        data: entry.data,
        at: entry.at,
        live: false,
        loading: false,
      });
    }
  }
}

/** localStorage, если телефон его даёт: в приватном режиме доступ бросает. */
export function phoneStorage(): Storage | null {
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

const StoreContext = createContext<Store | null>(null);

/** Общий кэш приложения — его создаёт App. */
export const StoreProvider = StoreContext.Provider;

/** Кэш приложения; без App (тесты экрана) — свой у экрана: звать раз на экран. */
export function useStore(): Store {
  const shared = useContext(StoreContext);
  const [own] = useState(() => new Store());
  return shared ?? own;
}

/** Ответ ключа из кэша; экран перерисуется, когда он поменяется. */
export function useCached(store: Store, key: string): Cached {
  const subscribe = useCallback(
    (listener: () => void) => store.subscribe(key, listener),
    [store, key],
  );
  return useSyncExternalStore(subscribe, () => store.get(key));
}

export interface ResourceOptions<T> {
  failure?: string; // текст ошибки, если сервер не объяснил
  always?: boolean; // спрашивать сервер при каждом открытии экрана
  onAnswer?: (data: T) => void; // ответ на запрос этого экрана
}

export interface Resource<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  reload: (fresh?: boolean) => void; // «Обновить» — fresh, «Повторить» — как есть
}

/**
 * Данные экрана: сразу из кэша, а запрос — если их нет, они из памяти
 * телефона или старше полуминуты.
 */
export function useResource<T>(
  store: Store,
  key: string,
  load: Load<T>,
  {
    failure = "Не получилось загрузить — попробуйте ещё раз",
    always = false,
    onAnswer,
  }: ResourceOptions<T>,
): Resource<T> {
  const cached = useCached(store, key);
  const [error, setError] = useState<string | null>(null);
  const [asked, setAsked] = useState<{ fresh: boolean } | null>(null);
  const answer = useRef(onAnswer);

  useEffect(() => {
    answer.current = onAnswer;
  });

  useEffect(() => {
    let alive = true;
    const request =
      asked !== null
        ? store.fetch(key, load, { force: true, fresh: asked.fresh })
        : always
          ? store.fetch(key, load)
          : store.ensure(key, load);
    request.then(
      (data) => {
        if (!alive) return;
        setError(null);
        answer.current?.(data);
      },
      (reason: unknown) => {
        if (alive) {
          setError(reason instanceof ApiError ? reason.message : failure);
        }
      },
    );
    return () => {
      alive = false;
    };
  }, [store, key, load, asked, always, failure]);

  const reload = useCallback((fresh = false) => {
    setError(null);
    setAsked({ fresh });
  }, []);

  return {
    data: cached.data as T | null,
    loading: cached.loading,
    error,
    reload,
  };
}
