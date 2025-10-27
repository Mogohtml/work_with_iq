import os
import vk_api
from dotenv import load_dotenv
import csv
from typing import List, Union, Dict, Any

from twocaptcha import TwoCaptcha

POST_RECORDS = 100
COMMENT_RECORDS = 100






# Авторизация VK Api
load_dotenv()
session = vk_api.VkApi(login=os.getenv("LOGIN"), password=os.getenv("PASSWORD"))
session.auth()
print("Авторизация прошла успешно!")

vk = session.get_api()


# Выгрузка данных в таблицу
def data_upload(file_name: str, data: List[Dict[str, Any]]) -> int:
    old_data = []
    with open(file_name, "r", newline="", encoding="utf-8") as csv_file:
        csv_file.readline()
        csv_reader = csv.reader(csv_file, delimiter=",")
        for row in csv_reader:
            old_data.append(int(row[1]))

    with open(file_name, "a", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=",")
        cnt = 0
        for post in data:
            for key, value in post.items():
                for comment in value:
                    if comment["id"] in old_data:
                        continue
                    csv_writer.writerow([key, comment["id"], comment["text"]])
                    cnt += 1
    return cnt


# Получение информации постов по айди группы или короткому имени
def get_posts(group: Union[int, str], offset: int) -> Dict[str, Any]:
    if type(group) is str:
        response = vk.wall.get(domain=group, count=POST_RECORDS, offset=offset)
    else:
        response = vk.wall.get(owner_id=group, count=POST_RECORDS, offset=offset)
    return {x["id"]: {"group_id": x["owner_id"]} for x in response["items"]}


# Получение информации по комментариям
def get_comments(posts: Dict[str, Any]) -> List[Dict[str, Any]]:
    comments = []
    for key, value in posts.items():
        response = vk.wall.getComments(owner_id=value["group_id"], post_id=key, count=COMMENT_RECORDS)["items"]
        response = {key: list(
            filter(
                lambda verifiable: verifiable["text"].strip() != "",
                map(
                    lambda comment: {
                        "id": comment["id"],
                        "text": comment["text"].replace("\n", " ").strip()
                    }, response
                ))
        )}
        comments.append(response)
    return comments


def main():
    """
    Пример использования парсера
    """

    # ID или короткое имя группы для парсинга
    # Примеры: "fitness", "workout", или просто числовой ID
    GROUP_ID = "MoreGorewear"  # Замените на нужную группу

    # Фильтры для отбора пользователей
    FILTERS = {
        'city_ids': [],  # 1-Москва, 2-СПб (можно убрать для всех городов)
        'age_from': 18,
        'age_to': 35,
        'sex': 0,  # 0-любой, 1-женский, 2-мужской
        'only_can_message': True,  # Только с открытыми ЛС
        'only_active': True,  # Были онлайн за последние 30 дней
        'has_mobile': False,  # Необязательно
    }

    # Максимум пользователей для парсинга
    MAX_USERS = 50

    # Шаблон сообщения для рассылки
    MESSAGE_TEMPLATE = """Привет, {first_name}!"""

    # Настройки рассылки
    SEND_SETTINGS = {
        'delay_range': (120, 300),  # Задержка 2-5 минут между сообщениями
        'max_per_day': 30,  # Максимум 30 сообщений в день (безопасно)
        'dry_run': True  # True = тестовый режим (не отправляет реально)
    }

    try:
        # Инициализация
        parser = VKGroupParser(token=TOKEN)

        # парсинг группы
        logger.info("=" * 50)
        logger.info("Шаг 1: парсинг участников группы")
        logger.info("=" * 50)

        users = parser.parse_group_members(
            group_id=GROUP_ID,
            max_users=MAX_USERS,
            filters=FILTERS
        )

        # Сохраняем результаты
        json_file, csv_file = parser.save_users(users, filename='parsed_users')

        # Статистика
        stats = parser.get_user_stats(users)
        logger.info(f"""
        Стастистика парсера:
        Всего пользователей: {stats['total']}
        С открытыми ЛС: {stats['can_message']}
        Сейчас онлайн: {stats['online_now']}
        Используют мобильное приложение: {stats['has_mobile']}

        Пол:
        - Мужчины: {stats['sex']['male']}
        - Женщины: {stats['sex']['female']}
        - Не указан: {stats['sex']['unknown']}

        Активность:
        - Сегодня: {stats['activity']['today']}
        - За неделю: {stats['activity']['week']}
        - За месяц: {stats['activity']['month']}

        """)

        '''# ШАГ 2: Рассылка (опционально)
        if stats['can_message'] > 0:
            logger.info("=" * 50)
            logger.info("Шаг 2: рассылка сообщений")
            logger.info("=" * 50)

            choice = input(f"\nНайдено {stats['can_message']} пользователей с открытыми ЛС.\nНачать рассылку? (yes/no): ")

            if choice.lower() in ['yes', 'y', 'да', 'д']:
                # Фильтруем только тех, кому можно писать
                users_to_message = [u for u in users if u.get('can_write_private_message')]

                # Запускаем рассылку
                send_stats = parser.send_messages(
                    users=users_to_message,
                    message_template=MESSAGE_TEMPLATE,
                    **SEND_SETTINGS
                )

                # Сохраняем статистику рассылки
                with open('send_stats.json', 'w', encoding='utf-8') as f:
                    json.dump(send_stats, f, ensure_ascii=False, indent=2)
            else:
                logger.info("Рассылка отменена пользователем")
        else:
            logger.warning("Нет пользователей с открытыми ЛС для рассылки")'''

    except vk_api.exceptions.ApiError as e:
        logger.error(f"Ошибка VK API: {e}")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)

if __name__ == "__main__":
    print("[INFO] СКРИПТ ЗАПУЩЕН ...")
    while True:
        input_text = input("Введите айди группы или его короткое имя: ")
        if not input_text.strip():
            print("[INFO] ВЫКЛЮЧЕНИЕ ...")
            break
        idd = input_text
        if input_text.isdecimal():
            idd = int(input_text)

        input_pages = int(input("Введите количество страниц: "))

        comments = []
        posts_cnt = 0
        for i in range(input_pages):
            posts = get_posts(idd, i * POST_RECORDS)
            posts_cnt += len(posts)
            comments += get_comments(posts)
            print(f"[INFO] ЗАГРУЖЕНА СТРАНИЦА: {i+1}/{input_pages}")

        print(f"[INFO] ПОЛУЧЕНО ПОСТОВ: {posts_cnt}")
        cnt = data_upload("data.csv", comments)
        print(f"[INFO] ЗАНЕСЕНО В ТАБЛИЦУ {cnt} КОММЕНТАРИЕВ")

