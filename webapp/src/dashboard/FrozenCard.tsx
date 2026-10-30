import type { Frozen } from "../api";
import { glued, moneyText } from "../format";
import { Card, Fact, Line, Table, Warn } from "./blocks";
import { dayMonth } from "./piggyText";

interface WarningsProps {
  currency: string;
  over: number; // копилки больше карты на столько
  lacking: number; // копилок нет, а долларов на платежи не хватает
  lackingOn: string | null; // день платежа, с которого не хватает
}

/** Когда «Заморожено» не сходится — что делать: копилки больше карты или долларов
 * не хватает на платежи срока. Общие для «Заморожено» и «Подушки». */
export function FrozenWarnings({
  currency,
  over,
  lacking,
  lackingOn,
}: WarningsProps) {
  const sum = (value: number) => moneyText(value, currency);
  return (
    <>
      {over > 0 && (
        <>
          <Warn>Копилки больше карты</Warn>
          <Line>{`разница ${sum(over)}`}</Line>
          <Line>отложите разницу</Line>
          <Line>или поправьте копилки</Line>
        </>
      )}
      {lacking > 0 && (
        <>
          <Warn>Долларов не хватает на платежи</Warn>
          <Line>
            {`купите ${sum(lacking)}${lackingOn ? ` до ${dayMonth(lackingOn)}` : ""}`}
          </Line>
        </>
      )}
    </>
  );
}

/** «🧊 Заморожено»: доллары, которые не трогаем, — подушка и копилки; остальные — на
 * платежи до зарплаты; всего на карте — как в «Остатках на картах». */
export default function FrozenCard({ frozen }: { frozen: Frozen }) {
  const sum = (value: number) => moneyText(value, frozen.currency);
  const known = frozen.piggies !== null && frozen.cushion !== null;
  return (
    <Card label="Заморожено">
      <Fact>{`🧊 Заморожено ${sum(frozen.amount)}`}</Fact>
      {known && (
        <>
          <Line>не трогаем:</Line>
          <div className="my-1">
            <Table
              label="Не трогаем"
              align="lr"
              rows={[
                ["подушка", sum(frozen.cushion ?? 0)],
                ["копилки", sum(frozen.piggies ?? 0)],
              ]}
            />
          </div>
        </>
      )}
      <FrozenWarnings
        currency={frozen.currency}
        over={frozen.over}
        lacking={frozen.lacking}
        lackingOn={frozen.lacking_on}
      />
      {frozen.payments.length > 0 && (
        <>
          <Line>{`на платежи ${glued(frozen.target)}:`}</Line>
          {frozen.payments.map((payment, index) => (
            <Line key={index}>
              {`${payment.name} ${dayMonth(payment.day)} — ${sum(payment.amount)}`}
            </Line>
          ))}
        </>
      )}
      <Line>{`всего на карте ${sum(frozen.total)}`}</Line>
    </Card>
  );
}
