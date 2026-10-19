/** Сообщение «заголовок: подробности» — крупная строка и строка под ней, как заметки
 * бота; без «: » — одна строка. */
export function noticeParts(text: string): {
  head: string | null;
  rest: string;
} {
  const at = text.indexOf(": ");
  return at < 0
    ? { head: null, rest: text }
    : { head: text.slice(0, at), rest: text.slice(at + 2) };
}
