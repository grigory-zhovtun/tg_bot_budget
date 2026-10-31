import { useCallback, useEffect, useState } from "react";
import type { CardBalance, Group, Source, Subcategory } from "../api";
import { Table } from "../dashboard/blocks";
import { SPEND_BUTTON } from "../dashboard/controls";
import { moneyText, sheetAmount, withCurrency } from "../format";
import { telegram } from "../telegram";
import {
  amountValue,
  apiAmount,
  displayAmount,
  press,
  type Key,
} from "./amount";
import CardSheet from "./CardSheet";
import { AmountText, CommentField, DayField } from "./EntryFields";
import Keypad from "./Keypad";
import { creditHead, type Outgoing } from "./outgoing";
import { balanceOf, EXCHANGE_SUB, factLines, rateLine } from "./transfer";

type Side = "from" | "to";
type Field = "sent" | "got"; // «Списано» — в валюте «Откуда», «Зачислено» — «Куда»

interface Props {
  group: Group;
  sub: Subcategory;
  sources: Source[];
  current: string; // «Откуда» по умолчанию — текущая карта «Ввода»
  cards: CardBalance[] | null; // остатки «Сводки»; null — не загружены
  today: string;
  entryId: string;
  busy: boolean; // запись уходит
  onSubmit: (out: Outgoing) => void;
}

/** Поле карты: подпись, карта крупно, остаток; тап открывает окно карт. */
function CardField({
  label,
  title,
  note,
  onClick,
}: {
  label: string;
  title: string | null; // null — карта не выбрана
  note: string | null;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      className="flex w-full items-center gap-3 rounded-2xl bg-section p-4 text-left"
      onClick={onClick}
    >
      <span className="flex min-w-0 flex-1 flex-col">
        <span className="text-line font-bold">{label}</span>
        <span
          className={`text-fact font-extrabold break-words ${
            title === null ? "text-link" : ""
          }`}
        >
          {title ?? "выберите карту"}
        </span>
        {note && <span className="text-line">{note}</span>}
      </span>
      <span
        aria-hidden="true"
        className="shrink-0 text-fact font-extrabold text-link"
      >
        ›
      </span>
    </button>
  );
}

/** Поле суммы: активное — толстой рамкой цвета кнопки и «— вводите». */
function SumField({
  label,
  text,
  currency,
  on,
  onClick,
}: {
  label: string;
  text: string; // строка клавиатуры: «1210800», «12,5»
  currency: string;
  on: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      aria-pressed={on}
      className={`flex w-full flex-col items-start gap-1 rounded-2xl bg-section px-4 py-3 text-left ${
        on ? "border-4 border-button" : "border-2 border-outline"
      }`}
      onClick={onClick}
    >
      <span className="text-line font-bold">
        {on ? `${label} — вводите` : label}
      </span>
      <AmountText text={displayAmount(text)} currency={currency} />
    </button>
  );
}

/** «Откуда → Куда»: перевод или обмен между своими картами одной записью — снятие
 * и пополнение; у «Откуда» есть «не моя карта» — деньги от других людей, одна
 * строка «Зачислить» на «Куда». */
export default function TransferScreen({
  group,
  sub,
  sources,
  current,
  cards,
  today,
  entryId,
  busy,
  onSubmit,
}: Props) {
  const [from, setFrom] = useState<string | null>(current); // null — «не моя карта»
  const [to, setTo] = useState<string | null>(null); // null — не выбрана
  const [sent, setSent] = useState("");
  const [got, setGot] = useState("");
  const [active, setActive] = useState<Field>("sent");
  const [comment, setComment] = useState("");
  const [day, setDay] = useState<string | null>(null); // null — сегодня по боту
  const [picking, setPicking] = useState<Side | null>(null); // открыто окно карт
  const tg = telegram();

  const currencyOf = (name: string | null) =>
    sources.find((source) => source.name === name)?.currency ?? null;
  const fromCurrency = currencyOf(from);
  const toCurrency = currencyOf(to);
  // валюты обеих карт известны и разные — две суммы, как в банке
  const two =
    fromCurrency !== null && toCurrency !== null && fromCurrency !== toCurrency;
  const sentCurrency = fromCurrency ?? "UZS";
  const gotCurrency = toCurrency ?? "UZS";
  // одно поле «Сумма» — карты «Откуда», а у «не моя карта» — карты «Куда»
  const typing: Field = two ? active : from === null ? "got" : "sent";
  const decimals = (typing === "sent" ? sentCurrency : gotCurrency) !== "UZS";
  const sentValue = amountValue(sent);
  const gotValue = amountValue(got);
  const ready =
    to !== null &&
    (from === null ? gotValue > 0 : sentValue > 0 && (!two || gotValue > 0));
  const label =
    from === null
      ? "Зачислить"
      : sub.name === EXCHANGE_SUB
        ? "Записать обмен"
        : "Записать перевод";

  const submit = useCallback(() => {
    // окно карт открыто — нажатие записало бы прежнюю «Куда»
    if (!ready || busy || picking !== null || to === null) return;
    const shared = {
      entry_id: entryId,
      category: group.name,
      subcategory: sub.name,
      comment: comment.trim(),
      ...(day ? { day } : {}),
    };
    onSubmit(
      from === null
        ? {
            // деньги от других людей — одна строка «Куда», D минусом
            entry: {
              ...shared,
              source: to,
              incoming: true,
              amount: apiAmount(got),
            },
            title: `${sub.name} ${withCurrency(gotValue, gotCurrency)}`,
            head: creditHead(gotValue, gotCurrency),
            card: to,
          }
        : {
            entry: {
              ...shared,
              source: from,
              to_source: to,
              amount: apiAmount(sent),
              ...(two ? { to_amount: apiAmount(got) } : {}),
            },
            title: `${sub.name} ${withCurrency(sentValue, sentCurrency)} → ${to}`,
            head:
              sub.name === EXCHANGE_SUB
                ? "✓ Записан обмен"
                : "✓ Записан перевод",
            card: to,
          },
    );
  }, [
    busy,
    comment,
    day,
    entryId,
    from,
    got,
    gotCurrency,
    gotValue,
    group.name,
    onSubmit,
    picking,
    ready,
    sent,
    sentCurrency,
    sentValue,
    sub.name,
    to,
    two,
  ]);

  // Кнопка панели Telegram: текст — в том же вызове, что показ
  useEffect(() => {
    if (!tg) return;
    tg.MainButton.setParams({ text: label, is_visible: true });
  }, [tg, label]);

  // ушли с экрана — прячем без крутилки: иначе она останется на кнопке
  useEffect(() => {
    if (!tg) return;
    const main = tg.MainButton;
    return () => {
      main.hideProgress();
      main.hide();
    };
  }, [tg]);

  // сначала прогресс, потом доступность: hideProgress() снова включает кнопку
  const live = ready && picking === null;
  useEffect(() => {
    if (!tg) return;
    if (busy) {
      tg.MainButton.showProgress(false);
      return;
    }
    tg.MainButton.hideProgress();
    if (live) tg.MainButton.enable();
    else tg.MainButton.disable();
  }, [tg, busy, live]);

  useEffect(() => {
    if (!tg) return;
    tg.MainButton.onClick(submit);
    return () => {
      tg.MainButton.offClick(submit);
    };
  }, [tg, submit]);

  function held(name: string): string | null {
    const card = balanceOf(cards, name);
    return card && card.balance !== null
      ? `на карте ${moneyText(card.balance, card.currency)}`
      : null;
  }

  // другая карта «Откуда» или «не моя» стирает свою сумму
  function pickFrom(name: string | null) {
    if (name !== from) {
      setFrom(name);
      setSent("");
      setActive("sent");
    }
    setPicking(null);
  }

  // замена выбранной «Куда» стирает её сумму; первый выбор ничего не стирает
  function pickTo(name: string) {
    if (to !== null && name !== to) setGot("");
    setTo(name);
    setActive("got");
    setPicking(null);
  }

  function type(key: Key) {
    const set = typing === "sent" ? setSent : setGot;
    set((text) => press(text, key, decimals));
  }

  return (
    <div className="flex flex-col gap-3">
      <CardField
        label="Откуда"
        title={from ?? "не моя карта"}
        note={from === null ? "деньги от других людей" : held(from)}
        onClick={() => setPicking("from")}
      />
      <CardField
        label="Куда"
        title={to}
        note={to === null ? null : held(to)}
        onClick={() => setPicking("to")}
      />
      {two ? (
        <>
          <SumField
            label="Списано"
            text={sent}
            currency={sentCurrency}
            on={typing === "sent"}
            onClick={() => setActive("sent")}
          />
          <SumField
            label="Зачислено"
            text={got}
            currency={gotCurrency}
            on={typing === "got"}
            onClick={() => setActive("got")}
          />
        </>
      ) : (
        <SumField
          label="Сумма"
          text={typing === "sent" ? sent : got}
          currency={typing === "sent" ? sentCurrency : gotCurrency}
          on
          onClick={() => setActive(typing)}
        />
      )}
      {two && sentValue > 0 && gotValue > 0 && (
        <p className="text-line font-bold">
          {rateLine(sentValue, sentCurrency, gotValue, gotCurrency)}
        </p>
      )}
      <CommentField value={comment} onChange={setComment} />
      <DayField day={day} today={today} onChange={setDay} />
      <Keypad big decimals={decimals} onKey={type} />
      {ready && to !== null && (
        <Table
          label="В таблицу"
          head={["карта", "в таблицу"]}
          align="lr"
          rows={factLines(from, to, sentValue, gotValue, two).map((line) => [
            line.card,
            sheetAmount(line.amount, currencyOf(line.card) ?? "UZS"),
          ])}
        />
      )}
      {!tg && (
        <button
          type="button"
          className={SPEND_BUTTON}
          disabled={!ready || busy}
          onClick={submit}
        >
          {label}
        </button>
      )}
      {picking && (
        <CardSheet
          side={picking}
          sources={sources}
          chosen={picking === "from" ? from : to}
          exclude={picking === "from" ? to : from}
          cards={cards}
          onPick={(name) => {
            if (picking === "from") pickFrom(name);
            else if (name !== null) pickTo(name);
          }}
          onClose={() => setPicking(null)}
        />
      )}
    </div>
  );
}
