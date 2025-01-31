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
from requests.exceptions import RequestException, ConnectionError
from itertools import zip_longest

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

cookies = {
    '_ym_uid': '1545027567707528589',
    '_ym_d': '1719303718',
    '_gcl_au': '1.1.1793393708.1719303719',
    '_ga': 'GA1.1.255704567.1719303719',
    'tb_favourites_hash': '171930371832733292',
    'cc_cookie': '{"categories":["necessary","analytics","targeting"],"level":["necessary","analytics","targeting"],"revision":0,"data":null,"rfc_cookie":false,"consent_date":"2024-06-25T08:22:22.588Z","consent_uuid":"0b05ba09-2875-4dc8-b667-50ab3b395a27","last_consent_update":"2024-06-25T08:22:22.588Z"}',
    'sbjs_migrations': '1418474375998%3D1',
    'sbjs_first_add': 'fd%3D2024-08-09%2010%3A24%3A14%7C%7C%7Cep%3Dhttps%3A%2F%2Fexkavator.ru%2Fmain%2Fnews%3Futm_source%3Dmain_page%26utm_medium%3Dmenu%26utm_campaign%3Dnews%7C%7C%7Crf%3Dhttps%3A%2F%2Fexkavator.ru%2Fmain%2Fnews%3Futm_source%3Dmain_page%26utm_medium%3Dmenu%26utm_campaign%3Dnews%26__cf_chl_tk%3DOlmPOphRNJNTsQM5UmeUiodI7gf3gTQsIIY7yGge1yw-1723188245-0.0.1.1-4884',
    'sbjs_first': 'typ%3Dutm%7C%7C%7Csrc%3Dmain_page%7C%7C%7Cmdm%3Dmenu%7C%7C%7Ccmp%3Dnews%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29',
    'sbjs_current': 'typ%3Dorganic%7C%7C%7Csrc%3Dgoogle%7C%7C%7Cmdm%3Dorganic%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29',
    'sbjs_current_add': 'fd%3D2024-08-15%2011%3A43%3A41%7C%7C%7Cep%3Dhttps%3A%2F%2Fexkavator.ru%2Fexcapedia%2Ftechnic%2Frm-terextl155%7C%7C%7Crf%3Dhttps%3A%2F%2Fwww.google.com%2F',
    'v1_sessions_callibri': '920237060',
    'clbvid': '66c6c80a7e24badc9e6e8f8c',
    'callibri_start_date': '1724303371064',
    'callibri_current_page': 'https%3A//exkavator.ru/trade/search/TradeFlag/1/NoPrice/1/pages/',
    'callibri_page_counter': '29',
    'cf_clearance': 'oPDvGHacaaxOBptR7bZZjCmyYhPrve1moKNzeAbhG24-1725945431-1.2.1.1-R_jWEYuYQW9l8bpiANk_CV7XOAYEx.IrvPvcHYpS9q0us5.vP7tVAEGBW5OFqSP99tkz5f0gSUh8OzyDHjHXo0wdS8C2P6eqxmREGqGHz4nOIsubMQBe7cCb5AWXO0avJhX7rsxSHgNXGZMS.ykFVMpnX67WMP_vy1Ay6WfGSRZcio4kLzt20mHy6IX38VgKKu.HqImtGzE9jliJf2B0mPVQeMA7ndygm2beXrCbwv6BFEbuigK7Hw0_Z.E9_41PV01DE2rbi55wu.C0DKuX_JyZcsGR_AhOJofGwvhMuZtfhEi7UswjMhMf1jh400JtLtZJ0dEN_XEY1COR_bjgwBjNKowRsF2t6auf4R2s6S3GHLthf9Ns33PZMdWTo2ynSS5gERlPSfbHielzLhbN8wkLN6cuesaeAZmvvozMM7AMkbDm8s1CL2lKwxV5kzah',
    '_ym_isad': '1',
    'sbjs_udata': 'vst%3D51%7C%7C%7Cuip%3D%28none%29%7C%7C%7Cuag%3DMozilla%2F5.0%20%28Windows%20NT%2010.0%3B%20Win64%3B%20x64%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F128.0.0.0%20Safari%2F537.36',
    'v1_referrer_callibri': 'https%3A//exkavator.ru/trade/search/TradeFlag/1/NoPrice/1/pages/%3F__cf_chl_tk%3DbFXOjJU_QMJEBJJrPXhb9P7HwXgOucbvjPyHq3iCwvs-1725945431-0.0.1.1-9556',
    'exkavator_session': 'a%3A5%3A%7Bs%3A10%3A%22session_id%22%3Bs%3A32%3A%22b7cb5fca0cd619acff16dc8e163481ae%22%3Bs%3A10%3A%22ip_address%22%3Bs%3A12%3A%2293.85.160.97%22%3Bs%3A10%3A%22user_agent%22%3Bs%3A111%3A%22Mozilla%2F5.0+%28Windows+NT+10.0%3B+Win64%3B+x64%29+AppleWebKit%2F537.36+%28KHTML%2C+like+Gecko%29+Chrome%2F128.0.0.0+Safari%2F537.36%22%3Bs%3A13%3A%22last_activity%22%3Bi%3A1725952796%3Bs%3A9%3A%22user_data%22%3Bs%3A0%3A%22%22%3B%7Dcea228a96d0fac6efb511a450b95c0fa',
    '_ga_16VD85X6Z6': 'GS1.1.1725952793.35.0.1725952798.55.0.0',
}

headers = {
    'accept': 'text/css,*/*;q=0.1',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,be-BY;q=0.6,be;q=0.5',
    'cache-control': 'no-cache',
    # 'cookie': '_ym_uid=1545027567707528589; _ym_d=1719303718; _gcl_au=1.1.1793393708.1719303719; _ga=GA1.1.255704567.1719303719; tb_favourites_hash=171930371832733292; cc_cookie={"categories":["necessary","analytics","targeting"],"level":["necessary","analytics","targeting"],"revision":0,"data":null,"rfc_cookie":false,"consent_date":"2024-06-25T08:22:22.588Z","consent_uuid":"0b05ba09-2875-4dc8-b667-50ab3b395a27","last_consent_update":"2024-06-25T08:22:22.588Z"}; sbjs_migrations=1418474375998%3D1; sbjs_first_add=fd%3D2024-08-09%2010%3A24%3A14%7C%7C%7Cep%3Dhttps%3A%2F%2Fexkavator.ru%2Fmain%2Fnews%3Futm_source%3Dmain_page%26utm_medium%3Dmenu%26utm_campaign%3Dnews%7C%7C%7Crf%3Dhttps%3A%2F%2Fexkavator.ru%2Fmain%2Fnews%3Futm_source%3Dmain_page%26utm_medium%3Dmenu%26utm_campaign%3Dnews%26__cf_chl_tk%3DOlmPOphRNJNTsQM5UmeUiodI7gf3gTQsIIY7yGge1yw-1723188245-0.0.1.1-4884; sbjs_first=typ%3Dutm%7C%7C%7Csrc%3Dmain_page%7C%7C%7Cmdm%3Dmenu%7C%7C%7Ccmp%3Dnews%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29; sbjs_current=typ%3Dorganic%7C%7C%7Csrc%3Dgoogle%7C%7C%7Cmdm%3Dorganic%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29; sbjs_current_add=fd%3D2024-08-15%2011%3A43%3A41%7C%7C%7Cep%3Dhttps%3A%2F%2Fexkavator.ru%2Fexcapedia%2Ftechnic%2Frm-terextl155%7C%7C%7Crf%3Dhttps%3A%2F%2Fwww.google.com%2F; v1_sessions_callibri=920237060; clbvid=66c6c80a7e24badc9e6e8f8c; callibri_start_date=1724303371064; callibri_current_page=https%3A//exkavator.ru/trade/search/TradeFlag/1/NoPrice/1/pages/; callibri_page_counter=29; cf_clearance=oPDvGHacaaxOBptR7bZZjCmyYhPrve1moKNzeAbhG24-1725945431-1.2.1.1-R_jWEYuYQW9l8bpiANk_CV7XOAYEx.IrvPvcHYpS9q0us5.vP7tVAEGBW5OFqSP99tkz5f0gSUh8OzyDHjHXo0wdS8C2P6eqxmREGqGHz4nOIsubMQBe7cCb5AWXO0avJhX7rsxSHgNXGZMS.ykFVMpnX67WMP_vy1Ay6WfGSRZcio4kLzt20mHy6IX38VgKKu.HqImtGzE9jliJf2B0mPVQeMA7ndygm2beXrCbwv6BFEbuigK7Hw0_Z.E9_41PV01DE2rbi55wu.C0DKuX_JyZcsGR_AhOJofGwvhMuZtfhEi7UswjMhMf1jh400JtLtZJ0dEN_XEY1COR_bjgwBjNKowRsF2t6auf4R2s6S3GHLthf9Ns33PZMdWTo2ynSS5gERlPSfbHielzLhbN8wkLN6cuesaeAZmvvozMM7AMkbDm8s1CL2lKwxV5kzah; _ym_isad=1; sbjs_udata=vst%3D51%7C%7C%7Cuip%3D%28none%29%7C%7C%7Cuag%3DMozilla%2F5.0%20%28Windows%20NT%2010.0%3B%20Win64%3B%20x64%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F128.0.0.0%20Safari%2F537.36; v1_referrer_callibri=https%3A//exkavator.ru/trade/search/TradeFlag/1/NoPrice/1/pages/%3F__cf_chl_tk%3DbFXOjJU_QMJEBJJrPXhb9P7HwXgOucbvjPyHq3iCwvs-1725945431-0.0.1.1-9556; exkavator_session=a%3A5%3A%7Bs%3A10%3A%22session_id%22%3Bs%3A32%3A%22b7cb5fca0cd619acff16dc8e163481ae%22%3Bs%3A10%3A%22ip_address%22%3Bs%3A12%3A%2293.85.160.97%22%3Bs%3A10%3A%22user_agent%22%3Bs%3A111%3A%22Mozilla%2F5.0+%28Windows+NT+10.0%3B+Win64%3B+x64%29+AppleWebKit%2F537.36+%28KHTML%2C+like+Gecko%29+Chrome%2F128.0.0.0+Safari%2F537.36%22%3Bs%3A13%3A%22last_activity%22%3Bi%3A1725952796%3Bs%3A9%3A%22user_data%22%3Bs%3A0%3A%22%22%3B%7Dcea228a96d0fac6efb511a450b95c0fa; _ga_16VD85X6Z6=GS1.1.1725952793.35.0.1725952798.55.0.0',
    'pragma': 'no-cache',
    'priority': 'u=0',
    'referer': 'https://exkavator.ru/',
    'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'style',
    'sec-fetch-mode': 'no-cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
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
    url_page = f'https://exkavator.ru/trade/search/TradeFlag/1/NoPrice/1/pages/{page_generation}'
    
    # сбор кукисов 2
    session_с = session.get(url_page)

    try:
        response_page = requests.get(url_page, headers=headers, cookies=session.cookies)
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
    except RequestException or ConnectionError as e:
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
input('Нажмите Enter для выхода')