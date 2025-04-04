import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """
    Функция делает запрос к API Ozon Seller для получения списка товаров магазина.
    Args:
        last_id: Идентификатор последнего полученного товара.
        client_id: Идентификатор клиента Ozon Seller.
        seller_token: Токен продавца Ozon Seller.
    Returns:
        Словарь, содержащий информацию о товарах, полученных от API Ozon.
        Ключ "result" содержит список товаров, если запрос выполнен успешно.
    Raises:
        requests.exceptions.HTTPError: Если HTTP статус ответа не 200 OK.

    Examples:
        >>> get_product_list("", "12345", "abcdefg12345")  # Получение первой страницы
        {'items': [...], 'total': 123, 'last_id': "some_id"}  # Пример успешного ответа
    """

    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """
    Функция итерирует все страницы списка товаров магазина Ozon.
        client_id: Идентификатор клиента Ozon Seller.
        seller_token: Токен продавца Ozon Seller.
    Returns:
        Список строк, представляющих `offer_id` всех товаров магазина.
    Examples:
        >>> get_offer_ids("12345", "abcdefg12345")
        ['123-ABC', '456-DEF', '789-GHI']
    Raises:
        requests.exceptions.HTTPError:  Если `get_product_list` возвращает HTTP ошибку.
        KeyError:  Если структура ответа от `get_product_list` не соответствует ожидаемой.
    """

    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """
    Функция отправляет запрос к API Ozon Seller для обновления цен товаров.
    Args:
        prices: Список словарей, где каждый словарь представляет собой информацию о цене для
                одного товара.
        client_id: Идентификатор клиента Ozon Seller.
        seller_token: Токен продавца Ozon Seller.
    Returns:
        Словарь, представляющий ответ от API Ozon Seller.
    Raises:
        requests.exceptions.HTTPError: Если HTTP статус ответа не 200 OK.
    Examples:
        >>> prices = [{'offer_id': '123-ABC', 'price': 1999.99}, {'offer_id': '456-DEF', 'price': 2499.50}]
        >>> update_price(prices, "12345", "abcdefg12345")
        {'task_id': 123456789, 'status': 'pending'}  # Пример успешного ответа
    """

    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """
    Функция отправляет запрос к API Ozon Seller для обновления данных об остатках товаров.
    Args:
        stocks: Список словарей, где каждый словарь представляет собой информацию об остатках
                для одного товара на определенном складе.
        client_id: Идентификатор клиента Ozon Seller.
        seller_token: Токен продавца Ozon Seller.
    Returns:
        Словарь, представляющий ответ от API Ozon Seller.
    Raises:
        requests.exceptions.HTTPError: Если HTTP статус ответа не 200 OK.
    Examples:
        >>> stocks = [{'offer_id': '123-ABC', 'stock': 10, 'warehouse_id': 1234}, {'offer_id': '456-DEF', 'stock': 5, 'warehouse_id': 5678}]
        >>> update_stocks(stocks, "12345", "abcdefg12345")
        {'task_id': 987654321, 'status': 'success'}  # Пример успешного ответа
    """

    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """
    Функция скачивает ZIP-архив с сайта Timeworld.
    Returns:
        Список словарей, где каждый словарь представляет собой информацию об остатках
        одной модели часов Casio.
    Raises:
        requests.exceptions.HTTPError: Если HTTP статус ответа при скачивании архива не 200 OK.
    """

    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """
    Функция сопоставляет данные об остатках часов с известными offer_id и формирует список словарей.
    Args:
        watch_remnants: Список словарей, представляющих данные об остатках часов.
        offer_ids: Список строк, представляющих offer_id товаров, для которых необходимо обновить остатки.
    Returns:
        Список словарей, где каждый словарь содержит `offer_id` и `stock` (остаток) для отправки в API Ozon.
    Examples:
        >>> watch_remnants = [{"Код": "123-ABC", "Количество": "5"}, {"Код": "456-DEF", "Количество": ">10"}]
        >>> offer_ids = ["123-ABC", "456-DEF", "789-GHI"]
        >>> create_stocks(watch_remnants, offer_ids)
        [{'offer_id': '123-ABC', 'stock': 5}, {'offer_id': '456-DEF', 'stock': 100}, {'offer_id': '789-GHI', 'stock': 0}]
    """

    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Функция сопоставляет данные о товарах с известными offer_id и формирует список словарей,
    пригодный для отправки в API Ozon для обновления информации о ценах. Использует функцию
    `price_conversion` для преобразования цены в нужный формат.

    Args:
        watch_remnants: Список словарей, представляющих данные о товарах.
        offer_ids: Список строк, представляющих offer_id товаров, для которых необходимо обновить цены.

    Returns:
        Список словарей, где каждый словарь содержит данные о цене для товара (offer_id, цена, валюта и т.д.)
        в формате, требуемом API Ozon.

    Examples:
        >>> watch_remnants = [{"Код": "123-ABC", "Цена": "5'990.00 руб."}, {"Код": "456-DEF", "Цена": "10'000.50 руб."}]
        >>> offer_ids = ["123-ABC", "456-DEF"]
        >>> create_prices(watch_remnants, offer_ids)
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '123-ABC', 'old_price': '0', 'price': '5990'}]
    """

    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Эта функция удаляет все символы, кроме цифр, из строки цены и возвращает целое число.
    Args:
        price: Строка, представляющая цену
    Returns:
        Строка, содержащая числовое представление цены.
    Examples:
        >>> price_conversion("5'990.00 руб.")
        '5990'
        >>> price_conversion("1234.56 USD")
        '1234'  # Убираем все кроме цифр перед точкой
    """

    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    Функция разбивает входной список `lst` на подсписки, каждый из которых содержит
    не более `n` элементов.  Функция использует `yield`, что делает её генератором.
    Args:
        lst: Входной список, который нужно разделить.
        n: Максимальный размер каждого подсписка.
    Yields:
        Подсписок из `lst` размером не более `n`.  Последний подсписок может содержать
        меньше `n` элементов, если длина `lst` не кратна `n`.

    Examples:
        >>> list(divide([1, 2, 3, 4, 5, 6, 7], 3))
        [[1, 2, 3], [4, 5, 6], [7]]
    """

    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """
    Функция получает список offer_id товаров, формирует данные о ценах на основе данных о товарах.
    Args:
        watch_remnants: Список словарей, представляющих данные о товарах.
        client_id: Идентификатор клиента Ozon Seller.
        seller_token: Токен продавца Ozon Seller.
    Returns:
        Список словарей, представляющих сформированные данные о ценах, которые были отправлены в API Ozon.
    Raises:
        Exception: Пробрасывает исключения, возникающие в функциях `get_offer_ids`, `create_prices` и `update_price`.
    Examples:
        # Пример асинхронного вызова функции (требует event loop)
        # await upload_prices(watch_remnants, "client_id", "seller_token")
        # (Предполагаемый результат: список словарей с ценами)
    """

    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """
    Функция получает список offer_id товаров, формирует данные об остатках на основе данных о товарах,
    разделяет список остатков на части размером не более 100 элементов, и отправляет запросы к API
    Ozon для обновления информации об остатках.
    Args:
        watch_remnants: Список словарей, представляющих данные о товарах.
        client_id: Идентификатор клиента Ozon Seller.
        seller_token: Токен продавца Ozon Seller.
    Returns:
        Кортеж, содержащий два списка словарей:
        - `not_empty`: Список товаров с ненулевым остатком.
        - `stocks`: Полный список сформированных данных об остатках, которые были отправлены в API Ozon.
    Raises:
        Exception: Пробрасывает исключения, возникающие в функциях `get_offer_ids`, `create_stocks` и `update_stocks`.

    Examples:
        # Пример асинхронного вызова функции (требует event loop)
        # await upload_stocks(watch_remnants, "client_id", "seller_token")
        # (Предполагаемый результат: (список товаров с ненулевым остатком, полный список остатков))
    """

    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Основная функция для скачивания остатков, формирования данных о ценах и остатках и обновления информации на Ozon.

    Функция выполняет следующие шаги:
    1. Загружает переменные окружения SELLER_TOKEN и CLIENT_ID.
    2. Получает список offer_id товаров.
    3. Скачивает данные об остатках товаров.
    4. Формирует данные об остатках и отправляет их в API Ozon.
    5. Формирует данные о ценах и отправляет их в API Ozon.

    Обрабатывает исключения, возникающие при работе с сетью (requests) и другие общие исключения.
    """

    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
