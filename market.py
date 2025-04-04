import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """
    Функция отправляет GET-запрос к API Яндекс Маркета для получения списка товаров.
    Args:
        page: Токен страницы.
        campaign_id: Идентификатор кампании в Яндекс Маркете.
        access_token: Bearer token для аутентификации в API Яндекс Маркета.
    Returns:
        Словарь, содержащий информацию о товарах, полученных от API Яндекс Маркета.
    Raises:
        requests.exceptions.HTTPError: Если HTTP статус ответа не 200 OK.

    Examples:
        >>> get_product_list("", "12345", "abcdefg12345")  # Получение первой страницы
        {'offerMappingEntries': [...], 'paging': {'nextPageToken': 'some_token'}}  # Пример успешного ответа
    """

    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """
    Функция отправляет PUT-запрос к API Яндекс Маркета для обновления данных об остатках товаров.
    Args:
        stocks: Список словарей, где каждый словарь представляет собой информацию об остатках
                для одного товара.
        campaign_id: Идентификатор кампании в Яндекс Маркете.
        access_token: Bearer token для аутентификации в API Яндекс Маркета.
    Returns:
        Словарь, представляющий ответ от API Яндекс Маркета.
    Raises:
        requests.exceptions.HTTPError: Если HTTP статус ответа не 200 OK.
    Examples:
        >>> stocks = [{'offerId': '123-ABC', 'warehouseId': 1234, 'count': 10}, {'offerId': '456-DEF', 'warehouseId': 5678, 'count': 5}]
        >>> update_stocks(stocks, "12345", "abcdefg12345")
        {'result': {'status': 'OK'}}  # Пример успешного ответа
    """

    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """
    Функция отправляет POST-запрос к API Яндекс Маркета для обновления цен товаров в указанной
    кампании.
    Args:
        prices: Список словарей, где каждый словарь представляет собой информацию о цене для
                одного товара.
        campaign_id: Идентификатор кампании в Яндекс Маркете.
        access_token: Bearer token для аутентификации в API Яндекс Маркета.
    Returns:
        Словарь, представляющий ответ от API Яндекс Маркета.
    Raises:
        requests.exceptions.HTTPError: Если HTTP статус ответа не 200 OK.
    Examples:
        >>> prices = [{'offerId': '123-ABC', 'price': 1999.99, 'currencyId': 'RUR'}, {'offerId': '456-DEF', 'price': 2499.50, 'currencyId': 'RUR'}]
        >>> update_price(prices, "12345", "abcdefg12345")
        {'status': 'OK', 'results': [...]}  # Пример успешного ответа
    """

    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """
    Функция итерирует все страницы списка товаров кампании в Яндекс Маркете.
    Args:
        campaign_id: Идентификатор кампании в Яндекс Маркете.
        market_token: Bearer token для аутентификации в API Яндекс Маркета.
    Returns:
        Список строк, представляющих `offer_id` (shopSku) всех товаров кампании.
    Raises:
        requests.exceptions.HTTPError: Если `get_product_list` возвращает HTTP ошибку.
        KeyError: Если структура ответа от `get_product_list` не соответствует ожидаемой.
    Examples:
        >>> get_offer_ids("12345", "abcdefg12345")
        ['123-ABC', '456-DEF', '789-GHI']
    """

    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """
    Функция сопоставляет данные об остатках часов с известными offer_id и формирует список словарей.
    Args:
        watch_remnants: Список словарей, представляющих данные об остатках часов.
        offer_ids: Список строк, представляющих offer_id товаров, для которых необходимо обновить остатки.
        warehouse_id: Идентификатор склада в Яндекс Маркете.
    Returns:
        Список словарей, где каждый словарь содержит данные об остатках для товара (sku, warehouseId, items)
        в формате, требуемом API Яндекс Маркета.
    Examples:
        >>> watch_remnants = [{"Код": "123-ABC", "Количество": "5"}, {"Код": "456-DEF", "Количество": ">10"}]
        >>> offer_ids = ["123-ABC", "456-DEF", "789-GHI"]
        >>> warehouse_id = 12345
        >>> stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
        >>> # Пример частичного результата (значения updatedAt будут отличаться)
        >>> #[{'sku': '123-ABC', 'warehouseId': 12345, 'items': [{'count': 5, 'type': 'FIT', 'updatedAt': '2023-10-27T10:00:00Z'}]
    """

    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Функция сопоставляет данные о товарах с известными offer_id и формирует список словарей.
    Args:
        watch_remnants: Список словарей, представляющих данные о товарах.
        offer_ids: Список строк, представляющих offer_id товаров, для которых необходимо обновить цены.
    Returns:
        Список словарей, где каждый словарь содержит данные о цене для товара (id, price, currencyId)
        в формате, требуемом API Яндекс Маркета.
    Examples:
        >>> watch_remnants = [{"Код": "123-ABC", "Цена": "5'990.00 руб."}, {"Код": "456-DEF", "Цена": "10'000.50 руб."}]
        >>> offer_ids = ["123-ABC", "456-DEF"]
        >>> create_prices(watch_remnants, offer_ids)
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    "currencyId": "RUR",
                },
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """
    Функция получает список offer_id товаров, формирует данные о ценах на основе данных о товарах,
    и разделяет список цен на части размером не более 500 элементов.
    Args:
        watch_remnants: Список словарей, представляющих данные о товарах.
        campaign_id: Идентификатор кампании в Яндекс Маркете.
        market_token: Bearer token для аутентификации в API Яндекс Маркета.
    Returns:
        Список словарей, представляющих сформированные данные о ценах, которые были отправлены в API Яндекс Маркета.
    Raises:
        Exception: Пробрасывает исключения, возникающие в функциях `get_offer_ids`, `create_prices` и `update_price`.
    Examples:
        # Пример асинхронного вызова функции (требует event loop)
        # await upload_prices(watch_remnants, "12345", "abcdefg12345")
        # (Предполагаемый результат: список словарей с ценами)
    """

    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """
    Функция получает список offer_id товаров, формирует данные об остатках на основе данных о товарах,
    разделяет список остатков на части размером не более 2000 элементов.
    Args:
        watch_remnants: Список словарей, представляющих данные о товарах.
        campaign_id: Идентификатор кампании в Яндекс Маркете.
        market_token: Bearer token для аутентификации в API Яндекс Маркета.
        warehouse_id: Идентификатор склада в Яндекс Маркете.
    Returns:
        Кортеж, содержащий два списка словарей:
        - `not_empty`: Список товаров с ненулевым остатком.
        - `stocks`: Полный список сформированных данных об остатках, которые были отправлены в API Яндекс Маркета.
    Raises:
        Exception: Пробрасывает исключения, возникающие в функциях `get_offer_ids`, `create_stocks` и `update_stocks`.
    Examples:
        # Пример асинхронного вызова функции (требует event loop)
        # await upload_stocks(watch_remnants, "12345", "abcdefg12345", 12345)
        # (Предполагаемый результат: (список товаров с ненулевым остатком, полный список остатков))
    """

    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """Основная функция для скачивания остатков, формирования данных о ценах и остатках и обновления информации на Яндекс Маркете для FBS и DBS кампаний.

    Функция выполняет следующие шаги:
    1. Загружает переменные окружения MARKET_TOKEN, FBS_ID, DBS_ID, WAREHOUSE_FBS_ID и WAREHOUSE_DBS_ID.
    2. Скачивает данные об остатках товаров.
    3. Получает список offer_id товаров для FBS кампании.
    4. Формирует данные об остатках и отправляет их в API Яндекс Маркета для FBS кампании.
    5. Формирует данные о ценах и отправляет их в API Яндекс Маркета для FBS кампании.
    6. Повторяет шаги 3-5 для DBS кампании.

    Обрабатывает исключения, возникающие при работе с сетью (requests) и другие общие исключения.
    """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        upload_prices(watch_remnants, campaign_fbs_id, market_token)
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
