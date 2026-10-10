import { useEffect, useState } from "react";
import { canUseHomeScreen, telegram, type HomeScreenStatus } from "./telegram";

/** «📌 На экран телефона»: ярлык всегда ведёт на вход с подписью Telegram.
 *
 * Открыто с initData (профиль бота, ссылка, ярлык) — ярлык ставим сразу. Открыто
 * кнопкой клавиатуры (initData нет, вход по ссылке с токеном) — открываем
 * t.me/<бот>?startapp=home: это то же приложение через Main Mini App, и там
 * offerHomeScreen предложит ярлык.
 */
export default function HomeScreenButton({
  botUsername,
}: {
  botUsername: string;
}) {
  const [status, setStatus] = useState<HomeScreenStatus | null>(null);

  useEffect(() => {
    const app = telegram();
    if (!canUseHomeScreen(app)) return;
    app.checkHomeScreenStatus((value) => setStatus(value));
  }, []);

  if (status !== "missed" && status !== "unknown") return null;

  function add() {
    const app = telegram();
    if (!app) return;
    if (app.initData) app.addToHomeScreen();
    else app.openTelegramLink(`https://t.me/${botUsername}?startapp=home`);
  }

  return (
    <button
      type="button"
      className="rounded-xl bg-section px-4 py-3 text-sm"
      onClick={add}
    >
      📌 На экран телефона
    </button>
  );
}
