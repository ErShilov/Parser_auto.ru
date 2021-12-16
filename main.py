import requests
import pandas as pd
import datetime as dt
import os
import glob


# Подготовка заголовка запроса записанного в файл
with open('header.txt', 'r') as file:
    headers = {}
    for line in file:
        key, value = line.strip().split(': ')
        headers[key] = value

# Подготовка JSON запроса и URL
url = 'https://auto.ru/-/ajax/desktop/listing/'

# Вычисляем ценовой диапазон на рынке
# Запрос по всем машинам во всей стране (category: cars, section: all, geo_id: 255)
request_parameter = {
    "category": "cars",
    "section": "all",
    "page": 1,
    "geo_id": 225
}
response = requests.post(url=url, json=request_parameter, headers=headers)
dump = response.json()
min_price = dump['price_range']['min']['price']
max_price = dump['price_range']['max']['price']
total_cars = dump['pagination']['total_offers_count']

# Выводим справочную информацию
print('Количество объявлений:\t\t{0:,}'.format(total_cars).replace(',', ' '))
print('Минимальная цена на рынке:\t{0:,} руб.'.format(min_price).replace(',', ' '))
print('Максимальная цена на рынке:\t{0:,} руб.'.format(max_price).replace(',', ' '))

# Подготавливаем переменные к основному циклу запросов
start_price = min_price
# Начальный шаг поиска
search_step = 10000
# Количество пройденных машин
found_cars = 0

# Подготавливаем директорию для файлов если она отсутствует
dir_name = 'OUT'
if not os.path.exists(dir_name):
    dir_path = os.path.join(dir_name)
    os.mkdir(dir_path)

# Подготавливаем дату для имени файла
now = dt.datetime.now()
date_for_name = '{}-{}-{}'.format(now.year, now.month, now.day)

# Основной цикл
while start_price < max_price:
    # Подготавливаем ценовой диапазон
    stop_price = start_price + search_step - 1

    # Подготовительный запрос
    request_parameter = {
        "category": "cars",
        "section": "all",
        "price_to": start_price,
        "price_from": stop_price,
        "page": 1,
        "geo_id": [225]
    }

    response = requests.post(url=url, json=request_parameter, headers=headers)
    dump = response.json()

    # Вытаскиваем количество страниц результата поиска и количество найденных машин из ответа
    total_pages = dump['pagination']['total_page_count']
    cars = dump['pagination']['total_offers_count']
    found_cars += cars

    # Корректировка шага поиска
    # На основании количества страниц ответа корректируем шаг поиска,
    # так как auto.ru не может выдавать больше 99 страниц ответа
    # Оставляем вариант пропуска условия для краев полного диапазона когда total_page = 1
    if 1 < total_pages < 10:
        search_step = int(search_step * 1.5)
        continue
    elif total_pages > 90:
        search_step = int(search_step / 1.2)
        continue
    print('\nВ ценовом диапазоне {}-{} рублей найдено {} объявлений на {} страницах' \
          .format(start_price, stop_price, cars, total_pages))

    # Вспомогательным циклом проходим по всем страницам
    all_cars = []
    page = 1

    while page < total_pages + 1:
        # Запрос по всем машинам во всей стране (category: cars, section: all, geo_id: 255)
        request_parameter = {
            "category": "cars",
            "section": "all",
            "price_to": start_price,
            "price_from": stop_price,
            "page": page,
            "geo_id": [225]
        }

        # Выполняем запрос
        # Если все в порядке идем дальше, если нет пропускаем страницу
        # Ловим ошибки и не даем отвалиться программе
        try:
            response = requests.post(url=url, json=request_parameter, headers=headers)
            if response.status_code == 200:
                dump = response.json()
                # Из JSON вытаскиваем все что нас интересует, можно вообще все вытащить, но это кому как надо
                # TODO: Тут можно расширить список выводимых параметров. Все параметры есть в файле output.txt
                for car in dump['offers']:
                    car_dict = {}
                    car_dict.update(car['price_info'])
                    car_dict.update(car['documents'])
                    car_dict['ID'] = car['id']
                    car_dict['Condition'] = car.get('section', 'None')
                    car_dict['Color'] = car.get('color_hex', 'None')
                    car_dict['About'] = car.get('lk_summary', 'None')
                    car_dict['Description'] = car.get('description', 'None')
                    car_dict['Seller'] = car['seller_type']
                    car_dict['Mark'] = car['vehicle_info']['mark_info']['name']
                    car_dict['Model'] = car['vehicle_info']['model_info']['name']
                    car_dict['Engine'] = car['vehicle_info']['tech_param']['engine_type']
                    car_dict['Power_hp'] = car['vehicle_info']['tech_param']['power']
                    car_dict['Gear'] = car['vehicle_info']['tech_param']['gear_type']
                    car_dict['Transmission'] = car['vehicle_info']['tech_param']['transmission']
                    car_dict['Mileage'] = car['state']['mileage']
                    car_dict['Location'] = car['seller']['location']['region_info']['name']
                    car_dict['Days_on_sale'] = car['additional_info']['days_on_sale']
                    all_cars.append(car_dict)
            else:
                print('Ошибка {} на странице: {} Повтор запроса...'.format(response.status_code, page))
                response.close()
                continue
        except KeyError:
            print('Ошибка получения данных на странице: ', page)
        # Закрываем соединение, переходим к следующей страницы
        response.close()
        page += 1

    # Формируем датафрейм
    df = pd.DataFrame(all_cars)
    df.drop_duplicates(subset=['ID'], inplace=True)

    # Сохраняем все в файл
    file_name = '{}-{}_cars_auto.ru_{}.xlsx'.format(start_price, stop_price, date_for_name)
    file_path = os.path.join(dir_name, file_name)
    df.to_excel(file_path, index=False)

    # Выводим информацию о пройденном диапазоне и статусе прогресса
    print('Файл {} успешно сохранен'.format(file_name))
    print('Выполнено {:.2%} поиска'.format(found_cars / total_cars))

    # Переходим к следующему ценовому диапазону
    start_price = start_price + search_step

# Если все ок выводим сообщение:
print('Парсинг данных успешно завершен')

# Объединяем все созданные файлы в один через общий датафрейм
print('\nОбъединяю все в общий файл, это может потребовать несколько минут...')
files_mask = '*.xlsx'
files_path = os.path.join(dir_name, files_mask)
files = glob.glob(files_path)

df = pd.DataFrame()
for file in files:
    dump = pd.read_excel(file)
    df = pd.concat([df, dump])

# Сохраняем результат объединения в один файл
file_name = 'All_cars_{}.xlsx'.format(date_for_name)
file_path = os.path.join(dir_name, file_name)
df.to_excel(file_path, index=False)

# Если файл сохранен выводим сообщение об этом и удалям ненужные временные файлы
if os.path.exists(file_path):
    print('Файл "{}" успешно сохранен'.format(file_name))
    for file in files:
        os.remove(file)
