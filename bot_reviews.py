import requests
import json
from datetime import date, datetime, timedelta
import pytz
import gspread
import os


# Загрузка конфигурации из файла
with open("config.json", "r", encoding="utf-8") as config_file:
    config = json.load(config_file)


TELEGRAM_BOT_TOKEN = config["telegram"]["bot_token"]
FORUM_CHAT_ID = config["telegram"]["forum_chat_id"]
FORUM_TOPIC_ID = config["telegram"]["forum_topic_id"]
ADMIN_CHAT_ID = config["telegram"]["admin_chat_id"]


def send_message_to_forum_topic(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            'chat_id': FORUM_CHAT_ID,
            'text': message,
            'message_thread_id': FORUM_TOPIC_ID
        }
        
        response = requests.post(url, data=payload)

        if response.status_code == 200:
            print("Сообщение успешно отправлено!")
        else:
            print(f"Ошибка отправки сообщения: {response.status_code}, {response.text}")
    
    except Exception as e:
        print(f"Ошибка отправки сообщения в Telegram: {e}")


def send_telegram_notification_error(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            'chat_id': ADMIN_CHAT_ID,
            'text': str(message)
        }
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            print("Админу отправлено успешно!")
        else:
            print(f"Ошибка отправки сообщения: {response.status_code}")
    except Exception as e:
        print(f"Ошибка отправки сообщения в Telegram: {e}")


def get_rewievs(head, project):
    '''Функция получения массива данных с отзывами через ozon api'''

    method = "https://api-seller.ozon.ru/v1/review/list"
    body = {
        "limit": 100,
        "sort_dir": "DESC",
        "status": "ALL"
    }

    body = json.dumps(body)
    try:
        response = requests.post(method, headers=head, data=body).json()
        result = response.get("reviews", None)
        if result is None:
            print("Ошибка обработки запроса")
            send_telegram_notification_error(f"Возникла ошибка обработки запроса в боте Rewiews_{project}")
        else:
            return result
    except requests.exceptions.RequestException as e:
        print("Response text:", e)


def rewievs(head, project_name, id_dict, remains):
    '''Функция обработки полученных данных с озона, возвращает массив основных данных об отзыве'''
    try:
        moscow_tz = pytz.timezone('Europe/Moscow')
        current_time = datetime.now(moscow_tz)
        current_date = current_time.date()

        try:
            rew = get_rewievs(head=head, project=project_name)
        except Exception as e:
            print(f"Ошибка при получении отзывов с API {project_name}: {e}")
            return

        lst = []
        for i in rew:
            try:
                published_at = i.get('published_at', '')
                if not published_at:
                    print(f"{project_name}: Отсутствует время публикации для отзыва SKU: {i.get('sku')}")
                    continue

                published_datetime = datetime.fromisoformat(published_at.replace('Z', '+00:00')).astimezone(moscow_tz)

                if published_datetime.date() == current_date and i.get('rating') < 5 and i.get('sku') in remains.keys():
                    lst.append({
                        "SKU": i.get('sku'),
                        "Наименование": id_dict.get(i.get('sku')),
                        "Комментарий": i.get('text'),
                        "Rating": i.get('rating'),
                        "Time": published_datetime.strftime('%Y-%m-%d %H:%M')
                    })
            except ValueError as e:
                print(f"Ошибка при обработке временной метки для отзыва SKU: {i.get('sku')}. Ошибка: {e}")
            except Exception as e:
                print(f"Неизвестная ошибка при обработке отзыва SKU: {i.get('sku')}. Ошибка: {e}")

        return lst
        
    except Exception as e:
        print(f"Произошла ошибка в процессе выполнения функции: {e}")


def get_sku_unit_name(google_account, ik, worksheet_name) -> list | dict:
    '''Функция получения из учетной гугл таблицы словаря соответствующих артикулов для товаров'''

    gc = gspread.service_account(filename=google_account)
    sh_ik = gc.open_by_key(ik)
    worksheet_unit = sh_ik.worksheet(worksheet_name)
    
    try:
        ranges = ['C:C', 'D:D']
        data = worksheet_unit.batch_get(ranges)

        sku_list = [int(item[0]) for item in data[0][2:] if item[0].isdigit()]
        unit_name_list = [item[0] for item in data[1][2:]]

        id_dict = dict(zip(sku_list, unit_name_list))

        return id_dict
    except Exception as e:
        print(f" Ошибка получения даннвх из гугл таблицы: {e}")


def get_remains(google_account, shipments) -> list | dict:
    '''Функция получения из учетной гугл таблицы данных остатков на товары
    уведомления отправляются только по товарам которые активно продаются'''

    gc = gspread.service_account(filename=google_account)
    sh_ik = gc.open_by_key(shipments)
    worksheet_ik = sh_ik.worksheet("Продажи")
    
    ranges = ['B:B', 'H:H']
    data = worksheet_ik.batch_get(ranges)

    sku_list = [int(item[0]) for item in data[0][2:]]
    remains_list = [int(item[0]) if "\xa0" not in item[0] else int(item[0].replace('\xa0', '')) for item in data[1][2:]]
    
    remains_dict = {key: value for key, value in zip(sku_list, remains_list) if value > 12}

    return remains_dict


def process_project(config_dict, id_dict, remains):
    '''Основня функция учета отзывов. После каждого запроса отзывы сохраняются в файл.
    При следующем запросе новые отзывы стравниваются со старыми и если есть новые, по ним отправляются уведомления
    На каждлый день на каждый кабинет один файл.'''
    
    head = config_dict["head"]
    project_name = config_dict["project"]

    date_str = date.today().strftime('%d.%m.%Y')
    print(f"Дата отзывов {project_name}: {date_str}")

    rewievs_list = rewievs(head, project_name, id_dict, remains)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, f'D:\\bot_rewievs\\files\\reviews_{project_name}_{date_str}.json')

    if os.path.exists(file_path):
        with open(file_path, 'r', encoding="utf-8") as file:
            data = json.load(file)
            rewievs_list_load = data["rewievs"]
        
        for i in rewievs_list:
            if i not in rewievs_list_load:
                send_message_to_forum_topic(f"Обнаружен отзыв с оценкой меньше 5:\n"
                                            f"Кабинет: {project_name}\n"
                                            f"SKU: {i.get('SKU')}\n" 
                                            f"Наименование: {i.get('Наименование')}\n" 
                                            f"Комментарий: {i.get('Комментарий')}\n" 
                                            f"Рейтинг: {i.get('Rating')}\n"
                                            f"Дата и время: {i.get('Time')}\n")
        
    else:
        for i in rewievs_list:
            send_message_to_forum_topic(f"Обнаружен отзыв с оценкой меньше 5:\n"
                                        f"Кабинет: {project_name}\n"
                                        f"SKU: {i.get('SKU')}\n" 
                                        f"Наименование: {i.get('Наименование')}\n" 
                                        f"Комментарий: {i.get('Комментарий')}\n" 
                                        f"Рейтинг: {i.get('Rating')}\n"
                                        f"Дата и время: {i.get('Time')}\n"
                                        )
    with open(file_path, 'w', encoding='utf-8') as json_file:
        data = {"rewievs": rewievs_list}
        json.dump(data, json_file, ensure_ascii=False, indent=4)

    print(f"Данные записаны в файл {file_path}")


def projects1_3(ik_1_3, p_1_3):
    google_account = config["google"]["service_account_file"]
    ik = ik_1_3["ik"]
    worksheet_name = ik_1_3["worksheet"]
    shipments = ik_1_3["shipments"]
    config1 = p_1_3["project1"]
    config3 = p_1_3["project3"]

    try:
        id_dict = get_sku_unit_name(google_account, ik, worksheet_name)
        remains = get_remains(google_account, shipments)
        process_project(config1, id_dict, remains)
        process_project(config3, id_dict, remains)

    except Exception as e:
        print(f'Ошибка проверки или отправки отзывов в ТГ project1-3: {e}')
        send_telegram_notification_error(f'Ошибка проверки или отправки отзывов в ТГ project1-3: {e}')


def projects2_4(ik_2_4, p_2_4):
    google_account = config["google"]["service_account_file"]
    ik = ik_2_4["ik"]
    worksheet_name = ik_2_4["worksheet"]
    shipments = ik_2_4["shipments"]
    config2 = p_2_4["project2"]
    config4 = p_2_4["project4"]

    try:
        id_dict = get_sku_unit_name(google_account, ik, worksheet_name)
        remains = get_remains(google_account, shipments)
        process_project(config2, id_dict, remains)
        process_project(config4, id_dict, remains)
        
    except Exception as e:
        print(f'Ошибка проверки или отправки отзывов в ТГ project2-4: {e}')
        send_telegram_notification_error(f'Ошибка проверки или отправки отзывов в ТГ project2-4: {e}')


def main():
    with open("config.json", "r", encoding="utf-8") as file:
        data = json.load(file)
        ik_1_3 = data['google']['ik_1_3']
        ik_2_4 = data['google']['ik_2_4']
        p_1_3 = data["projects"]["p_1_3"]
        p_2_4 = data["projects"]["p_2_4"]

    projects1_3(ik_1_3, p_1_3)
    projects2_4(ik_2_4, p_2_4)


if __name__ == "__main__":
    main()