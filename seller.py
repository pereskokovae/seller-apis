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
    """Получает список товаров магазина озон.

    Аргументы:
        last_id(str): Идентификатор последнего полученного товара.
        client_id(str): Идентификатор клиента.
        seller_token(str): Секретный пароль продавца.

    Возвращает:
        (dict): Данные о товарах.

    Пример корректного использования функции:
    {
        "items": [
            {
                "archived": True,
                "has_fbo_stocks": True,
                "has_fbs_stocks": True,
                "is_discounted": True,
                "offer_id": "136748",
                "product_id": 223681945,
                "quants": [
                    {
                        "quant_code": "string",
                        "quant_size": 0
                    }
                ]
            }
        ],
        "total": 1,
        "last_id": "bnVсbA=="
    }

    Пример некорректного исполнения функции:
    {
      "code": 0,
      "details": [
        {
          "typeUrl": "string",
          "value": "string"
        }
      ],
      "message": "string"
    }

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
    """Получает артикулы товаров магазина озон.

    Аргументы:
        client_id(str): Идентификатор клиента.
        seller_token(str): Секретный пароль продавца.

    Возвращает:
        (list): Артикулы на товары.

    Пример корректного использования функции:
        ["136748", "563421", "036492"]

    Пример некорректного исполнения функции:
        None

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
    """Обновляет цены товаров.

    Аргументы:
        prices(list): Список с ценами товаров.
        client_id(str): Идентификатор клиента.
        seller_token(str): Секретный пароль продавца.

    Возвращает:
        (dict): Данные с товами и их ценами.

    Пример корректного использования функции:
    {
      "result": [
        {
          "product_id": 1386,
          "offer_id": "PH8865",
          "updated": True,
          "errors": []
        }
      ]
    }

    Пример некорректного исполнения функции:
    {
      "code": 0,
      "details": [
        {
          "typeUrl": "string",
          "value": "string"
        }
      ],
      "message": "string"
    }

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
    """Обновляет остатки.

    Аргументы:
        stocks(list): Список с остатками товара.
        client_id(str): Идентификатор клиента.
        seller_token(str): Секретный пароль продавца.

    Возвращает:
        (dict): Словарь с данными ответа API о статусе обновления и остатках.

    Пример корректного использования функции:
    {
      "result": [
        {
          "product_id": 55946,
          "offer_id": "PG-2404С1",
          "updated": True,
          "errors": []
        }
      ]
    }

    Пример некорректного исполнения функции:
    {
      "code": 0,
      "details": [
        {
          "typeUrl": "string",
          "value": "string"
        }
      ],
      "message": "string"
    }

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
    """Скачивает архив ostatki с сайта casio, извлекает excel-файл 
    считывает информацию и возвращает данные об остатках товара.

    Возвращает:
        (list): Список словарей с данными об остатках товара.

    Исключения:
        zipfile.BadZipfile: если архив поврежден.
        FileNotFoundError: если excel-файл не найден.
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создает список с отстатками товара.

    Аргументы:
        watch_remnants(list): Список словарей с данными об остатках товара.
        offer_ids(list): Артикулы на товары.
        warehouse_id(int): Идентификатор склада.
    
    Возвращает:
        (list): Количество остатков товара на складе.
    """
    # Уберем то, что не загружено в seller
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
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Устанавливает цены.

    Аргументы:
        watch_remnants(list): Список словарей с данными об остатках товара.
        offer_ids(list): Артикулы на товары.

    Возвращает:
        (list): Список с ценами товаров.
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
    """Преобразовывает цену в строку без остатков и лишних знаков.

    Аргументы:
        price(str): Строка с ценой.

    Возвращает:
        (str): Строка с ценой без лишних знаков.

    Пример:
        5'990.00 руб. -> 5990
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделяет список lst на части по n элементов

    Аргументы:
        lst(list): Список.
        n(int): Число.
  
    Пример:
        lst = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        n = 3

    Возвращает:
        [1, 2, 3]
        [4, 5, 6]
        [7, 8, 9]
        [10]
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Асинхронная функция, которая загружает цены товаров.

    Аргументы:
        watch_remnants(list): Список словарей с данными об остатках товара.
        client_id(str): Идентификатор клиента.
        seller_token(str): Секретный пароль продавца.
    
    Возвращает:
        (list): Список с ценами товаров.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Асинхронная функция, загружает количество остатков товаров.

    Аргументы:
        watch_remnants(list): Список словарей с данными об остатках товара.
        client_id(str): Идентификатор клиента.
        seller_token(str): Секретный пароль продавца.
    
    Возвращает:
        not_empty(list): Остатки товаров, количество котрых больше 0.
        stocks(list): Остатки товара на складе.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
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
