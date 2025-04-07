# Данный код предназначен для парсинга данных с сайта autodoc.ru через внутренний api сайта
# Собираются номера(OEM, артикулы, sku) запчастей для ТО. Каждому номеру соответствует марка, модель, и т.д.
# Данные собираются в MySQL базу функцией insert_to_sql
# Конфиденциальные данные хранятся в файле local_settings
# Для предотвражения срабатывания блокировки используются случайные User_Agent и время меджу запросами sleep(randint(a, b))
# Логирование обеспечивается функцией log_to_file, данные записываются в txt файл (разницы с csv не замечено)
# Есть функция send_telegram для отправки сообщения об ошибки в мессенджер, но исключения не проработаны


import requests
from fake_useragent import UserAgent
from mysql.connector import connect, Error # mysql
import local_settings as settings
#import pandas as pd
from time import sleep
import datetime
from random import randint
#from bs4 import BeautifulSoup


# test_data = [{'id': 11351, 'name': 'ABARTH'}, {'id': 11156, 'name': 'DONGFENG'}]
# 169002 max index

# добавление данных в mysql таблицу
def insert_to_sql(table):
    insert_reviewers_query = """
    INSERT parts_to
    (id_car, name_brand, id_model, name_model, yearFrom, yearTo, id_modification,
    name_modification, engineCode, constructionType, fuel, horsePower, startDate, engineCapacity, numberOfCylinders,
    valves, valvesTotal, motorType, fullName, comment_, categoryId, itemId, itemName, partNumber, id_manufacturer,
    name_manufacturer, quantity)
    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s, %s, %s )
    """

    try:
        with connect(
            host=settings.HOST,
            user=settings.MYSQL_USER,
            password=settings.MYSQL_PASSWORD,
            database="autodoc_ru",
        ) as connection:
            connection.cursor().executemany(insert_reviewers_query, table)
            connection.commit()
            
    except Error as e:
        print(e)
        send_telegram(e)


# send message to telegram
def send_telegram(msg):
    TOKEN = settings.TELEGRAM_TOKEN
    chat_id = settings.TELEGRAM_CHAT_ID

    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={msg}"

    requests.get(url)

# обновление заголовка
def hdrs():
    ua = UserAgent()
    headers = {
        'authority': 'webapi.autodoc.ru',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'authorization': 'Bearer',
        'origin': 'https://www.autodoc.ru',
        'referer': 'https://www.autodoc.ru/',
        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': ua.random,
    }
    return headers

def log_to_file(txt):
    with open("log_all_auto_ru.txt", "a") as file:
        file.write(txt)
    print(txt.strip())

    
if __name__ == "__main__":
    date_now = datetime.datetime.now()
    
    # список марок
    list_brands = requests.get('https://webapi.autodoc.ru/api/cars/brands', headers=hdrs())

    for brand in list_brands.json(): # после HYUNDAI [53:]
    # for brand in test_data:
        # список моделей
        brand_id = brand['id']
        list_models = requests.get(f'https://webapi.autodoc.ru/api/cars/brands/{brand_id}/models', headers=hdrs())
            
        if list_models.status_code != 200:
                date_new = datetime.datetime.now()
                time = str(date_new - date_now)
                log_to_file('{}; ERROR; {}\n'.format(time, brand.get('name')))
                send_telegram('{}; ERROR; {}\n'.format(time, brand.get('name')))
                continue
                
        for model in list_models.json():
            # список модификаций
            model_id = model['id']
            list_modification = requests.get(f'https://webapi.autodoc.ru/api/cars/models/{model_id}/modifications', headers=hdrs())
            
            if list_modification.status_code != 200:
                date_new = datetime.datetime.now()
                time = str(date_new - date_now)
                log_to_file('{}; ERROR; {}; {}\n'.format(time, brand.get('name'), model.get('name')))
                send_telegram('{}; ERROR; {}; {}\n'.format(time, brand.get('name'), model.get('name')))
                continue
                
            for modification in list_modification.json()['modifications']:
                # print(brand['name'], model['name'], modification['name'])
                # список запчастей
                modification_id = modification['id']
                list_items = requests.get(f'https://webapi.autodoc.ru/api/maintenance/modifications/{modification_id}/maintenanceitems', headers=hdrs())
                
                if list_items.status_code != 200:
                    date_new = datetime.datetime.now()
                    time = str(date_new - date_now)
                    log_to_file('{}; ERROR; {}; {}; {}\n'.format(time,
                                                               brand.get('name'),
                                                               model.get('name'),
                                                               modification.get('name')
                                                              ))
                    send_telegram('ERROR')
                    continue
                
                list_for_DB = []
                
                if len(list_items.json()) == 0:
                    # save to DB without item
                    list_for_DB.append((brand.get('id'),
                                        brand.get('name'),
                                        model.get('id'),
                                        model.get('name'),
                                        model.get('yearFrom'),
                                        model.get('yearTo'),
                                        modification.get('id'),
                                        modification.get('name'),
                                        modification.get('engineCode'),
                                        modification.get('constructionType'),
                                        modification.get('fuel'),
                                        modification.get('horsePower'),
                                        modification.get('startDate'),
                                        modification.get('engineCapacity'),
                                        modification.get('numberOfCylinders'),
                                        modification.get('valves'),
                                        modification.get('valvesTotal'),
                                        modification.get('motorType'),
                                        modification.get('fullName'), #19
                                        None, None, None, None, None, None, None, None
                                       ))
                    
                for item in list_items.json()['items']:
                    list_for_DB.append((brand.get('id'),
                                        brand.get('name'),
                                        model.get('id'),
                                        model.get('name'),
                                        model.get('yearFrom'),
                                        model.get('yearTo'),
                                        modification.get('id'),
                                        modification.get('name'),
                                        modification.get('engineCode'),
                                        modification.get('constructionType'),
                                        modification.get('fuel'),
                                        modification.get('horsePower'),
                                        modification.get('startDate'),
                                        modification.get('engineCapacity'),
                                        modification.get('numberOfCylinders'),
                                        modification.get('valves'),
                                        modification.get('valvesTotal'),
                                        modification.get('motorType'),
                                        modification.get('fullName'),
                                        item.get('comment'),
                                        item.get('categoryId'),
                                        item.get('itemId'),
                                        item.get('itemName'),
                                        item.get('partNumber'),
                                        item.get('id_manufacturer'),
                                        item.get('name_manufacturer'),
                                        item.get('quantity')
                                       ))
                    
                insert_to_sql(list_for_DB)
                
                #sleep(randint(1, 5)) # avg 3 sec
                date_new = datetime.datetime.now()
                time = str(date_new - date_now)
                log_to_file('{}; OK; {}; {}; {}\n'.format(time,
                                                        brand.get('name'),
                                                        model.get('name'),
                                                        modification.get('name')
                                                       ))
    print('script_finished')
                
