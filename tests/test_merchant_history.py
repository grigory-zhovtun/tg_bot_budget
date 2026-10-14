"""История магазинов для SMS: ключ без «>город», агрегаторы, статья большинства."""

import pytest

from app.domain import MerchantBook, merchant_key


@pytest.mark.parametrize(
    ("comment", "expected"),
    [
        ("YANDEX.GO>YUNUSOBOD", "YANDEX.GO"),
        ("GOOGLE *Google One>8; ≈ 19.99 USD по курсу таблицы", "GOOGLE *GOOGLE ONE"),
        ("PAYME OPLATA", None),
        ("AI: Payme Oplata", None),
        ("VSMCUZSKB to QRonlin", None),
        ("PAYME OPLATA MUNIS", "PAYME OPLATA MUNIS"),
        ("входящий перевод: UZCARDKB 2 VISA MC UZS", None),
        ("перевод: списание со счёта", None),
    ],
)
def test_merchant_key_for_sms_names(comment: str, expected: str | None) -> None:
    assert merchant_key(comment) == expected


def test_same_merchant_in_another_city_is_found() -> None:
    book = MerchantBook([("🚌 ТРАНСПОРТ", "такси", "YANDEX.GO>YUNUSOBOD")])
    # без запасного поиска по первому слову — только ключ без «>город»
    found = book.find("YANDEX.GO>CHILONZOR", first_word=False)
    assert found == ("🚌 ТРАНСПОРТ", "такси")


def test_dominant_category_decides_the_merchant() -> None:
    entries = [("🍔 ЕДА", "кофе", "OOO SHAVI CAFE")]
    entries += [("🍔 ЕДА", "кафе", "OOO SHAVI CAFE")] * 4  # 80 % — кафе
    book = MerchantBook(entries)
    assert book.find("OOO SHAVI CAFE>TASHKENT") == ("🍔 ЕДА", "кафе")
    assert "OOO SHAVI CAFE" not in book.mixed


def test_merchant_split_between_categories_stays_open() -> None:
    entries = [("🍔 ЕДА", "кофе", "OOO SHAVI CAFE")] * 2
    entries += [("🍔 ЕДА", "кафе", "OOO SHAVI CAFE")] * 3  # 60 % — мало
    assert MerchantBook(entries).find("OOO SHAVI CAFE") is None


def test_misc_rows_do_not_count_for_the_share() -> None:
    entries = [("🚧 РАЗНОЕ", "неучтенка", "OOO SHAVI CAFE")] * 5
    entries += [("🍔 ЕДА", "кафе", "OOO SHAVI CAFE")]
    assert MerchantBook(entries).find("OOO SHAVI CAFE") == ("🍔 ЕДА", "кафе")
