import requests
import json
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import re
import datetime
from datetime import datetime
import dateparser
import psycopg2
import smtplib
import pandas as pd
import time
from requests.exceptions import RequestException, ConnectionError, ConnectTimeout
from itertools import zip_longest
import tkinter as tk
from tkinter import messagebox
import certifi

#cd C:\  \для работы\exc_ru
#pyinstaller --onefile --console excavator_ru_parser.py

# Чтение текущей даты
current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"Привет! Текущая дата - {current_time_str}")

# Счетчики
number = 1
page_counter = 1
page_generation = ''
stop_flag = False
df = pd.DataFrame(columns=['ID', 'Name', 'Date', 'Price RUB', 'Price USD', 'Type', 'Seller', 'Description', 'Prim. Location', 'URL'])
df_date = datetime.now().strftime('%Y-%m-%d')

# messagebox для завершения программы
def show_completion_message(page_counter, number):
    # Создаём корневое окно
    root = tk.Tk()
    root.withdraw()  # Скрываем главное окно, чтобы не было видно

    # Устанавливаем окно поверх всех
    root.attributes('-topmost', True)

    # Отображаем сообщение о завершении
    messagebox.showinfo(
        title="Выполнение завершено",
        message=f"Выполнение завершено, пройдено {page_counter} страниц, собрано {number} объявлений"
    )

    # Закрываем корневое окно
    root.destroy()

cookies = {
    '_ym_uid': '1545027567707528589',
    '_ga': 'GA1.1.255704567.1719303719',
    'clbvid': '66c6c80a7e24badc9e6e8f8c',
    'real-sklad': '1',
    'sbjs_current': 'typ%3Dutm%7C%7C%7Csrc%3Dmain_page%7C%7C%7Cmdm%3Dmenu%7C%7C%7Ccmp%3Dnews%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29',
    '_ym_d': '1736768379',
    'sbjs_udata': 'vst%3D82%7C%7C%7Cuip%3D%28none%29%7C%7C%7Cuag%3DMozilla%2F5.0%20%28Windows%20NT%2010.0%3B%20Win64%3B%20x64%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F131.0.0.0%20Safari%2F537.36',
    'cc_cookie': '{"categories":["necessary","analytics","targeting"],"level":["necessary","analytics","targeting"],"revision":0,"data":null,"rfc_cookie":false,"consent_date":"2025-04-09T11:58:17.994Z","consent_uuid":"cf03ed80-d5c8-4e29-b443-183257c07b1c","last_consent_update":"2025-04-09T11:58:17.994Z"}',
    'sbscrformoff': '1',
    'online-conf1': '1',
    '__ddg1_': 'ziq14E1dB50LpnRwlji4',
    '_gcl_au': '1.1.699599966.1750146208',
    'online-conf3': '1',
    'cf_clearance': 'r3KU_9.hB667lzBjzCSBFRAiMzfArBniaKfWE92bsQY-1751017248-1.2.1.1-HBbivou3B7GjyMRnPLN_K6eD4xwW60DSnL1mytTIkTyYzbcJZRLD4HDSiO84Tj1PHciigG8s.s.Czpd9.9P_.vI3HNyZd4esHjGZPZID.iZluFp1N9iK5aiuB0x8riAtNLzXxfDYe10YoQyfb._RUhkGl4dOyS278CP1y6Tlph7vGjPD0LA6rKAP3dK_LTt1gr8BWqkauVe3ZaXDi_b7UNR_aLJlF97CyGL2_JBEKdTvQUghhURNtzLcNw0Vqj8Dvqp3_kPtef9efTt8.ASTwS35rH4HQDuK9n_ZMXaAedMlDdfsPilC_3h6L4olbQC8Z2l4OYsY.EBREJhaZbMCsX990svNnDDGASSMU_.zYo057UUq1eNvxSuleXUSMsg7',
    'tb_favourites_hash': '175101725057965090',
    'sl-session': 'qzU+cb3KbGgFNjUkW0l7Vw==',
    '_ym_isad': '1',
    '_ym_visorc': 'w',
    'exkavator_session': 'a%3A5%3A%7Bs%3A10%3A%22session_id%22%3Bs%3A32%3A%2246daa8cf0e088c699f25d6c957ab5e7b%22%3Bs%3A10%3A%22ip_address%22%3Bs%3A13%3A%2237.45.252.163%22%3Bs%3A10%3A%22user_agent%22%3Bs%3A111%3A%22Mozilla%2F5.0+%28Windows+NT+10.0%3B+Win64%3B+x64%29+AppleWebKit%2F537.36+%28KHTML%2C+like+Gecko%29+Chrome%2F137.0.0.0+Safari%2F537.36%22%3Bs%3A13%3A%22last_activity%22%3Bi%3A1751873853%3Bs%3A9%3A%22user_data%22%3Bs%3A0%3A%22%22%3B%7D41d21f0e08d1720338dde81ed275ddaf',
    '_ga_16VD85X6Z6': 'GS2.1.s1751873879$o70$g1$t1751874015$j60$l0$h0',
}

headers = {
    'accept': 'text/css,*/*;q=0.1',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,be-BY;q=0.6,be;q=0.5',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0',
    'referer': 'https://exkavator.ru/',
    'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'style',
    'sec-fetch-mode': 'no-cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
    # 'cookie': '_ym_uid=1545027567707528589; _ga=GA1.1.255704567.1719303719; clbvid=66c6c80a7e24badc9e6e8f8c; real-sklad=1; sbjs_current=typ%3Dutm%7C%7C%7Csrc%3Dmain_page%7C%7C%7Cmdm%3Dmenu%7C%7C%7Ccmp%3Dnews%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29; _ym_d=1736768379; sbjs_udata=vst%3D82%7C%7C%7Cuip%3D%28none%29%7C%7C%7Cuag%3DMozilla%2F5.0%20%28Windows%20NT%2010.0%3B%20Win64%3B%20x64%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F131.0.0.0%20Safari%2F537.36; cc_cookie={"categories":["necessary","analytics","targeting"],"level":["necessary","analytics","targeting"],"revision":0,"data":null,"rfc_cookie":false,"consent_date":"2025-04-09T11:58:17.994Z","consent_uuid":"cf03ed80-d5c8-4e29-b443-183257c07b1c","last_consent_update":"2025-04-09T11:58:17.994Z"}; sbscrformoff=1; online-conf1=1; __ddg1_=ziq14E1dB50LpnRwlji4; _gcl_au=1.1.699599966.1750146208; online-conf3=1; cf_clearance=r3KU_9.hB667lzBjzCSBFRAiMzfArBniaKfWE92bsQY-1751017248-1.2.1.1-HBbivou3B7GjyMRnPLN_K6eD4xwW60DSnL1mytTIkTyYzbcJZRLD4HDSiO84Tj1PHciigG8s.s.Czpd9.9P_.vI3HNyZd4esHjGZPZID.iZluFp1N9iK5aiuB0x8riAtNLzXxfDYe10YoQyfb._RUhkGl4dOyS278CP1y6Tlph7vGjPD0LA6rKAP3dK_LTt1gr8BWqkauVe3ZaXDi_b7UNR_aLJlF97CyGL2_JBEKdTvQUghhURNtzLcNw0Vqj8Dvqp3_kPtef9efTt8.ASTwS35rH4HQDuK9n_ZMXaAedMlDdfsPilC_3h6L4olbQC8Z2l4OYsY.EBREJhaZbMCsX990svNnDDGASSMU_.zYo057UUq1eNvxSuleXUSMsg7; tb_favourites_hash=175101725057965090; sl-session=qzU+cb3KbGgFNjUkW0l7Vw==; _ym_isad=1; _ym_visorc=w; exkavator_session=a%3A5%3A%7Bs%3A10%3A%22session_id%22%3Bs%3A32%3A%2246daa8cf0e088c699f25d6c957ab5e7b%22%3Bs%3A10%3A%22ip_address%22%3Bs%3A13%3A%2237.45.252.163%22%3Bs%3A10%3A%22user_agent%22%3Bs%3A111%3A%22Mozilla%2F5.0+%28Windows+NT+10.0%3B+Win64%3B+x64%29+AppleWebKit%2F537.36+%28KHTML%2C+like+Gecko%29+Chrome%2F137.0.0.0+Safari%2F537.36%22%3Bs%3A13%3A%22last_activity%22%3Bi%3A1751873853%3Bs%3A9%3A%22user_data%22%3Bs%3A0%3A%22%22%3B%7D41d21f0e08d1720338dde81ed275ddaf; _ga_16VD85X6Z6=GS2.1.s1751873879$o70$g1$t1751874015$j60$l0$h0',
}

# сбор кукисов 1
session = requests.Session()

# курс usd-rub по api
curr_api = "https://www.cbr-xml-daily.ru/daily_json.js"
response = requests.get(curr_api)
curr_resp = response.json()
usd_rub = curr_resp['Valute']['USD']['Value']
usd_rub_date = datetime.strptime(curr_resp['Date'], '%Y-%m-%dT%H:%M:%S%z')
usd_rub_date = usd_rub_date.strftime('%Y-%m-%d')
print(f'Курс USD-RUB на дату {usd_rub_date} составляет {usd_rub}')

while stop_flag == False:
    try:
        url_page = f'https://exkavator.ru/trade/search/TradeFlag/1/NoPrice/1/pages/{page_generation}'
        
        # сбор кукисов 2
        session_с = session.get(url_page, headers=headers, cookies=session.cookies, verify=certifi.where())
    
        response_page = requests.get(url_page, headers=headers, cookies=session.cookies, verify=certifi.where())
        page = response_page.text

        if response_page.status_code == 200:
            soup = BeautifulSoup(page, 'lxml')
            print(response_page, url_page)

            print (f'Страница {page_counter}')
            names = soup.find_all("span", class_="title-tech")
            prices = soup.select("a > span.info > span.price > div:nth-child(1)")
            types = soup.find_all("span", class_="nw", style="color:#333;") 
            sellers = soup.select("a > span.info > span.company-garant-years > span.company-name")
            descriptions = soup.find_all("span", class_="desc")
            dates = soup.find_all("span", class_="update-line", style="bottom: 8px;")
            #infos = soup.find_all("div", class_="tech-info lot-click")
            urls = soup.find_all("a", class_="link", target="_blank")
            #locs = soup.select ('a > span.info > span.city-year-mile > span > span > span')
            #locs += soup.select ('a > span.info > span.city-year-mile > span.val.line-val-bubble > span > span')
            locs = soup.select ('a > span.info > span.city-year-mile > span.val.line-val-bubble > span')
            #if len(locs) == 0:
                #locs = soup.select ('a > span.info > span.city-year-mile > span.val.line-val-bubble > span > span')
                #if len(locs) == 0:
                    #locs = soup.select ('a > span.info > span.city-year-mile > span.val.line-val-bubble > span')

            for name, price, type, seller, description, date, loc, url in zip(names, prices, types, sellers, descriptions, dates, locs, urls):
                # локация
                loc = (re.sub(r' и еще.*', '', loc.text).strip().replace(' ', ''))

                # дата
                date = date.text.strip().replace(' ', '')
                if date == 'Обновленосегодня':
                    date = current_time_str
                elif date == 'Давнонеобновлялось':
                    stop_flag = True
                else:
                    date = dateparser.parse(date)
                    #date_str = date.strftime('%Y-%m-%d %H:%M:%S')
                    
                # подгонка и подрезка
                if price.text == 'Цена по запросу':
                    price = 0
                else:
                    price = re.sub(r'\sруб.', '', price.text).strip().replace(' ', '')
                #category = category.strip()
                
                name = name.text.strip()
                url = url.get("href")
                id = re.search(r'lot/(\d+)/', url)
                id = int(id.group(1))
                url = 'https://exkavator.ru' + url
                price = int(price)
                price_usd = price/usd_rub

                df.loc[len(df)] = (id, name, date, price, price_usd, type.text, seller.text, description.text, loc, url)
        #        print(f""" {number}
        #    id - {id}
        #    имя - {name}
        #    цена - {price}
        #    тип - {type.text}
        #    продавец - {seller.text}
        #    описание - {description.text}
        #    дата - {date}
        #    место - {loc}
        #    url - {url}
        #    ------------
        #    """)
                number +=1
            page_counter +=1
            print (f'Объяв на странице {number}')
            page_generation = str(40*(page_counter-1))
            time.sleep(10)
        else:
            print (f'Ошибка чтения страницы - {response_page.status_code}')
            break
    except RequestException or ConnectionError or ConnectTimeout as e:
        print(f"Произошла ошибка при запросе страницы {page_counter}: {e}")
        time.sleep(60)
        continue
    except:
        print('Произошла непредвиденная ошибка')
        break

path1 = f"D:/parsing/excavator.ru/exc_ru_{df_date}.xlsx"
path2 = f"//192.168.11.194/омиип рабочая/00_Базы/02_Базы по ценам/11_парсинг/excavator.ru/exc_ru_{df_date}.xlsx"

df.to_excel(path1, index=False)
df.to_excel(path2, index=False)

print ('Файлы записаны')

show_completion_message(page_counter, number)
