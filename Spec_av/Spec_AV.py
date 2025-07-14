import requests
from urllib.parse import urlencode
import re
import pandas as pd
from bs4 import BeautifulSoup
import datetime
from datetime import datetime
import json
import os
from tqdm import tqdm
from requests.exceptions import SSLError
import tkinter as tk
from tkinter import messagebox
#cd C:\ 
# \av_parcer\для работы\Spec_av
#pyinstaller --onefile --console spec_av.py

headers = {
    'authority': 'moto.av.by',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'ru,en;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "YaBrowser";v="24.1", "Yowser";v="2.5"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 YaBrowser/24.1.0.0 Safari/537.36',
}

url_page = "https://spec.av.by/filter?price_currency=2"
page_counter = 1

current_time_str = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
print(current_time_str)

categories = {
    "ДСТ": "&categories[0][category]=6",
    "Коммуналка": "&categories[0][category]=7",
    "Лес": "&categories[0][category]=8",
    "Погрузчики": "&categories[0][category]=4",
    "Экскаваторы": "&categories[0][category]=1",
    "Краны": "&categories[0][category]=2",
    "Подъемники и автовышки": "&categories[0][category]=3"
}
# пример корректной ссылки - https://spec.av.by/filter?price_currency=2&categories[0][category]=7&page=9

df = pd.DataFrame(columns=['Main', 'ID', 'Price', 'PriceBYN', 'Pub Date', 'Seller', 'URL', 'Location', 'Condition', 'Name', 'Type', 'Year'])

# messagebox для завершения программы
def show_completion_message(page_counter, ad_counter):
    # Создаём корневое окно
    root = tk.Tk()
    root.withdraw()  # Скрываем главное окно, чтобы не было видно

    # Устанавливаем окно поверх всех
    root.attributes('-topmost', True)

    # Отображаем сообщение о завершении
    messagebox.showinfo(
        title="Выполнение завершено",
        message=f"Выполнение завершено, пройдено {page_counter} страниц, собрано {ad_counter} объявлений"
    )

    # Закрываем корневое окно
    root.destroy()

def fetch_data (category_name, category_query):
    global df
    global ad_counter
    page_counter = 1
    ad_counter = 0

    while True: #цикл перебора страниц cat_dst
        url_cycle = url_page + category_query + "&page=" + str(page_counter)

        #Функция
        response = requests.get(url_cycle, headers=headers)

        src = response.text
        soup = BeautifulSoup(src, 'lxml')
        #Проверка на доступность страницы
        if response.status_code != 200:
            print(f"Ошибка загрузки страницы: {url_cycle} код:", response.status_code)
            break

        script_element = soup.find("script", id="__NEXT_DATA__") #Достаем жсон
        empty_page_element = soup.find("div", class_="listing__empty") #проверка надписи "не нашли ничего"
        #Проверка имеется ли табличка на странице
        if empty_page_element:
            print(f"Не нашли объяв в {category_name}, , {url_cycle}")
            break

        #Проверка имеется ли жсон на странице
        if script_element is None:
            print("Элемент script не найден")
            break

        json_string = script_element.string #Конвертируем жсон в стринг
        data = json.loads(json_string) #Пакуем в data

        #Пустые хранилища
        ids = []
        prices = []
        pricesbyn = []
        publisheds = []
        sellers = []
        urlss = []
        locations = []
        conditions = []
        names = []
        types = []
        years = []

        for item in data['props']['initialState']['filter']['main']['adverts']:
            advert_id = item['id']
            price = item['price']['usd']['amount']
            pricebyn = item['price']['byn']['amount']
            published_at = item['publishedAt']
            seller = item['sellerName']
            properties = item['properties']
            public_url = item['publicUrl']
            location = item['locationName']
            if isinstance(item['metadata'], dict):
                condition = item['metadata']['condition']['label']
            else:
                condition = 'н.и.'

            # Искать нужные свойства по имени
            name = next((prop['value'] for prop in properties if prop['name'] == 'name'), None)
            type = next((prop['value'] for prop in properties if prop['name'] == 'sub_category'), None)
            year = next((prop['value'] for prop in properties if prop['name'] == 'year'), None)

            # Добавить значения в списки
            ids.append(advert_id)
            prices.append(price)
            pricesbyn.append(pricebyn)
            publisheds.append(published_at)
            sellers.append(seller)
            urlss.append(public_url)
            locations.append(location)
            conditions.append(condition)
            names.append(name)
            types.append(type)
            years.append(year)

        for id, price, pricebyn, published, seller, url, location, condition, name, type, year in zip(ids, prices, pricesbyn, publisheds, sellers, urlss, locations, conditions, names, types, years):
            datetime_obj = datetime.strptime(published, '%Y-%m-%dT%H:%M:%S%z')
            date_normal = datetime_obj.strftime('%Y-%m-%d %H:%M:%S')
            print(f"Price - {price}, Date - {date_normal}, Brand - {name} {type}, Year - {year}, URL - {url}")
            df.loc[len(df)] = (category_name, id, price, pricebyn, published, seller, url, location, condition, name, type, year)
            ad_counter +=1
        page_counter +=1
        print(f"Страница {category_name} - {page_counter}")

# По spec.av
for category_name, category_query in categories.items():
    fetch_data(category_name, category_query)

# По agro.av
url_page = "https://agro.av.by/filter?price_currency=2"
page_counter = 1

categories = {
    "Трактор": "&categories[0][category]=1",
    "Обработка и подготовка почвы": "&categories[0][category]=2",
    "Посев и посадка": "&categories[0][category]=3",
    "Уход за культурами": "&categories[0][category]=4",
    "Сбор зерновых культур": "&categories[0][category]=5",
    "Сбор кормов": "&categories[0][category]=6",
    "Сбор овощей": "&categories[0][category]=7",
    "Сбор других культур": "&categories[0][category]=8",
    "Послеуборочная обработка": "&categories[0][category]=9",
    "Животноводство": "&categories[0][category]=10",
    "Минитехника": "&categories[0][category]=11"
}

for category_name, category_query in categories.items():
    fetch_data(category_name, category_query)

path1 = f"D:/parsing/spec.av.by/spec_av_{current_time_str}.xlsx"
path2 = f"//192.168.11.194/омиип рабочая/00_Базы/02_Базы по ценам/11_парсинг/spec.av.by/spec_av_{current_time_str}.xlsx"

df.to_excel(path1, index=False)
df.to_excel(path2, index=False)

print(f"Данные сохранены в файл, обработано {page_counter} страниц")

show_completion_message(page_counter, ad_counter)