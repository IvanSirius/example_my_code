import requests
import pandas as pd

from fake_useragent import UserAgent
from mysql.connector import connect, Error # mysql
import local_settings as settings
from time import sleep
import datetime
from random import randint


cookies = settings.COOKIES

def hdrs():
    headers = settings.HEADERS
    headers['user-agent'] = UserAgent().random
    return headers

def get_models(mark):
    json_data = {
        'catalog_filter': [
            {
                'mark': mark, # 'GEELY'
            },
        ],
        'section': 'all',
        'category': 'cars',
        'geo_radius': 200,
        'geo_id': [
            213,
        ],
    }

    response = requests.post(
        'https://auto.ru/-/ajax/desktop-search/getBreadcrumbsWithFilters/',
        cookies=cookies,
        headers=hdrs(),
        json=json_data,
    )

    if response.status_code == 200:
        sleep(randint(a, b))

        df = pd.DataFrame()

        for item in response.json()[0]['entities']:
            input = dict()
            input['count'] = item.get('count')
            input['reviews_count'] = item.get('reviews_count')
            input['logbook_count'] = item.get('logbook_count')
            input['cyrillic_name'] = item.get('cyrillic_name')
            input['id'] = item.get('id')
            input['itemFilterParams_model'] = item['itemFilterParams'].get('model')
            input['name'] = item.get('name')
            input['nameplates'] = str(item.get('nameplates'))
            input['popular'] = item.get('popular')
            input['year_from'] = item.get('year_from')
            input['year_to'] = item.get('year_to')
            input['section'] = item.get('section')
            if type(item.get('twin_model_info')) == dict:
                input['twin_model'] = str(item.get('twin_model_info').get('twin_model'))
                input['photo'] = item.get('twin_model_info').get('photo').get('main')
            
            df = pd.concat([df, pd.DataFrame([input])], ignore_index=True)

        df['catalog_filter_mark'] = mark

        return df
    else:
        print(f'{str(response.status_code)} {mark}')


def get_generation(mark, model):
    json_data = {
        'catalog_filter': [
            {
                'mark': mark, # 'GEELY'
                'model': model, # 'ATLAS'
            },
        ],
        'section': 'all',
        'category': 'cars',
        'geo_radius': 200,
        'geo_id': [
            213,
        ],
    }

    response = requests.post(
        'https://auto.ru/-/ajax/desktop-search/getBreadcrumbsWithFilters/',
        cookies=cookies,
        headers=hdrs(),
        json=json_data,
    )
    if response.status_code == 200:
        sleep(randint(a, b))

        df = pd.DataFrame()

        for item in response.json()[0]['entities']:
            input = dict()
            input['id'] = item.get('id')
            input['count'] = item.get('count')
            input['logbook_count'] = item.get('logbook_count')
            input['reviews_count'] = item.get('reviews_count')
            input['name'] = item.get('name')
            input['photo'] = item.get('photo')
            input['mobilePhoto'] = item.get('mobilePhoto')
            input['yearFrom'] = item.get('yearFrom')
            input['yearTo'] = item.get('yearTo')
            input['super_gen'] = str(item.get('super_gen'))
            input['itemFilterParams'] = str(item.get('itemFilterParams'))

            df = pd.concat([df, pd.DataFrame([input])], ignore_index=True)
            
        df['catalog_filter_mark'] = mark
        df['catalog_filter_model'] = model

        return df
    else:
        print(f'{str(response.status_code)} {mark} {model}')


def get_filters(mark, model, generation):
    json_data = {
        'category': 'cars',
        'catalog_filter': [
            {
                'mark': mark, # 'GEELY'
                'model': model, # 'ATLAS'
                'generation': generation, # '23774248'
            },
        ],
        'geo_radius': 200,
        'geo_id': [
            213,
        ],
    }

    response = requests.post(
        'https://auto.ru/-/ajax/desktop-search/availableVariantsForFilters/',
        cookies=cookies,
        headers=hdrs(),
        json=json_data,
    )
    if response.status_code == 200:
        sleep(randint(a, b))

        return response.json() # 'gear_type': ['FORWARD_CONTROL', 'ALL_WHEEL_DRIVE']
    else:
        print(f'{str(response.status_code)} {mark} {model} {generation}')


def get_count(mark, model, generation, gear_type):
    json_data = {
        'catalog_filter': [
            {
                'mark': mark, # 'GEELY'
                'model': model, # 'ATLAS'
                'generation': generation, # '23774248'
            },
        ],
        'section': 'all',
        'category': 'cars',
        'gear_type': [
            gear_type, # 'ALL_WHEEL_DRIVE'
        ],
        'output_type': 'list',
        'geo_radius': 200,
        'geo_id': [
            213,
        ],
    }

    response = requests.post('https://auto.ru/-/ajax/desktop-search/listing/', cookies=cookies, headers=hdrs(), json=json_data)
    if response.status_code == 200:
        sleep(randint(a, b))

        return response.json() # ['pagination']['total_offers_count']
    else:
        print(f'{str(response.status_code)} {mark} {model} {generation} count')


def insert_to_sql(table_name, df: pd.DataFrame):

    df.fillna('', inplace=True)

    insert_reviewers_query = """
    INSERT INTO {}
    ( {} )
    VALUES ( {}%s )
    """.format(table_name, ', '.join(list(df.columns)), '%s, '*(df.shape[1]-1))

    list_items = list(df.itertuples(index=False, name=None))

    try:
        with connect(
            host=settings.HOST,
            user=settings.MYSQL_USER,
            password=settings.MYSQL_PASSWORD,
            database="auto_ru",
        ) as connection:
            connection.cursor().executemany(insert_reviewers_query, list_items) # table list of tuple
            connection.commit()
            
    except Error as e:
        print(e)



if __name__ == "__main__":

    date_now = datetime.datetime.now()

    try:
        df = pd.read_excel(r'marks_input.xlsx')
    except:
        print('не открылся исходный файл marks_input.xlsx')

    a, b = 1, 1

    for index1, mark in df.iterrows():
        # if index1 > 1: ####################################
        #     break
        
        try:
            table_models = get_models(mark['itemFilterParams_mark'])

            insert_to_sql('models', table_models)

            for index2, model in table_models.iterrows():
                # if index2 > 1: ####################################
                #     break
                
                try:
                    table_gen = get_generation(mark['itemFilterParams_mark'], model['itemFilterParams_model'])

                    insert_to_sql('generations', table_gen)
                    
                    for index3, gen in table_gen.iterrows():
                        # if index2 > 1: ####################################
                        #     break
                        
                        try:
                            table_filters = get_filters(mark['itemFilterParams_mark'], model['itemFilterParams_model'], gen['id']) # json

                            dict_filters: dict = {}
                            dict_filters['mark'] = mark['itemFilterParams_mark']
                            dict_filters['model'] = model['itemFilterParams_model']
                            dict_filters['generation'] = gen['id']
                            dict_filters['filters'] = str(table_filters)
                            dict_filters['gear_type'] = str(table_filters['gear_type'])

                            insert_to_sql('filters', pd.DataFrame([dict_filters]))

                            date_new = datetime.datetime.now()
                            time = str(date_new - date_now)
                            print('ok {} --- {} --- {} --- {}'.format(mark['name'], model['name'], gen['name'], time))

                            for gear_type in table_filters['gear_type']:
                                
                                try:
                                    table_count = get_count(mark['itemFilterParams_mark'], model['itemFilterParams_model'], gen['id'], gear_type) # json

                                    dict_count: dict = {}
                                    dict_count['mark'] = mark['itemFilterParams_mark']
                                    dict_count['model'] = model['itemFilterParams_model']
                                    dict_count['generation'] = gen['id']
                                    dict_count['gear_type'] = gear_type
                                    # dict_count['all'] = str(table_count)
                                    dict_count['count'] = table_count['pagination']['total_offers_count']

                                    insert_to_sql('count', pd.DataFrame([dict_count]))

                                    dict_final: dict = {}
                                    dict_final['mark'] = mark['name']
                                    dict_final['model'] = model['name']
                                    dict_final['year_from'] = gen['yearFrom']
                                    dict_final['year_to'] = gen['yearTo']
                                    dict_final['name_generation'] = gen['name']
                                    dict_final['gear_type'] = gear_type
                                    dict_final['count'] = table_count['pagination']['total_offers_count']

                                    insert_to_sql('final', pd.DataFrame([dict_final]))

                                except:
                                    print('error {} {} {} {}'.format(mark['itemFilterParams_mark'], model['itemFilterParams_model'], gen['id'], gear_type))
                                    continue
                        except:
                            print('error {} {} {}'.format(mark['itemFilterParams_mark'], model['itemFilterParams_model'], gen['id']))
                            continue
                except:
                    print('error {} {}'.format(mark['itemFilterParams_mark'], model['itemFilterParams_model']))
                    continue
        except:
            print('error {}'.format(mark['itemFilterParams_mark']))
            continue
