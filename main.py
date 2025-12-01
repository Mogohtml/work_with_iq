import os

import keyboard  # Для горячих клавиш
import threading  # Для фоновой проверки клавиш
import vk_api
from fuzzywuzzy import process
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
import time
import json
import csv
import random
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional
import logging
from pathlib import Path
from os import getenv
from dotenv import *
import pandas as pd

import nltk
from nltk.corpus import wordnet
from ruwordnet import RuWordNet
from typing import List, Optional

# настройка переменных среды
load_dotenv()

TOKEN = getenv("ACCESS_TOKEN", "YOUR_TOKEN")

# настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('vk_parser.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Скачивание WordNet, если ещё не скачали
try:
    nltk.data.find('corpora/wordnet')
except LookupError:
    nltk.download('wordnet')




class VKGroupParser:
    """ класс парсера участников групп ВКонтакте с возможностью рассылки """

    def __init__(self, token: str):
        """
        аргументы:
            token: Пользовательский токен VK с правами messages, groups, friends, offline
        """
        self.token = token  # работает также токен для Kate Mobile
        self.session = vk_api.VkApi(token=token)
        self.vk = self.session.get_api()
        self.user_id = None

        # статистика запросов
        self.requests_count = 0
        self.last_request_time = 0

        # Создание экземпляра RuWordNet
        self.ruwordnet = RuWordNet()
        # Флаг для пропуска группы
        self.skip_group = False

        # инициализация
        self._init_user()

        self.keywords = [
            # Графический дизайн
            "дизайн", "графический дизайн", "логотип", "баннер", "аватарка", "фирменный стиль", "брендинг",
            "ui/ux", "веб-дизайн", "дизайнер", "photoshop", "illustrator", "figma", "sketch", "графика",

            # Веб-разработка
            "разработка", "программирование", "it", "айти", "веб-разработка", "сайт", "программист", "разработчик",
            "python", "django", "flask", "frontend", "backend", "fullstack", "devops", "software",

            # Интернет-магазины
            "интернет-магазин", "онлайн-магазин", "электронная коммерция", "e-commerce", "продажи онлайн", "магазин",
            "товар", "корзина", "оплата онлайн", "доставка", "интернет торговля", "онлайн покупки", "shopify",
            "woocommerce",

            # Боты
            "бот", "чат-бот", "телеграм бот", "telegram bot", "viber бот", "whatsapp бот", "автоматизация", "рассылка",

            # Мобильные приложения
            "мобильное приложение", "flutter", "android", "ios", "кроссплатформенное приложение", "ui/ux дизайн",
            "приложение", "app store", "google play", "пуш-уведомления", "геолокация"
        ]

        logger.info(f"Парсер инициализирован для пользователя {self.user_id}")

    def _init_user(self):
        """получение информации о текущем пользователе, чей токен используется"""
        try:
            user_info = self.vk.users.get()[0]
            self.user_id = user_info['id']
            logger.info(f"Авторизован как: {user_info['first_name']} {user_info['last_name']}")
        except Exception as e:
            logger.error(f"Ошибка авторизации: {e}")
            raise

    def _smart_delay(self):
        """умная задержка для избежания бана и капчи"""
        self.requests_count += 1

        # каждые 3 запроса - большая пауза
        if self.requests_count % 3 == 0:
            delay = random.uniform(2.0, 4.0)
        else:
            delay = random.uniform(0.5, 1.5)

        # каждые 20 запросов делаем длинную паузу
        if self.requests_count % 20 == 0:
            logger.info("Делаем паузу 30 секунд для избежания ограничений")
            time.sleep(30)
        else:
            time.sleep(delay)

        self.last_request_time = time.time()

    def _listen_for_skip(self):
        """Фоновая функция для прослушки Ctrl + N"""
        keyboard.add_hotkey('ctrl+n', lambda: setattr(self, 'skip_group', True))
        keyboard.wait()  # Бесконечный цикл для прослушки

    def parse_group_members(
            self,
            group_id: str,
            max_users: int = 10,
            filters: Dict = None
    ) -> List[Dict]:
        """
        парсинг участников группы
        аргументы:
            group_id: ID или короткое имя группы (например: "fitness" или 123456)
            max_users: Максимальное количество пользователей для парсинга
            filters: Фильтры для отбора пользователей
                {
                    'city_ids': [1, 2],  | ID городов, можно оставить пустым, если без разницы
                    'age_from': 18, | минимальный возраст
                    'age_to': 35, | максимальный возраст
                    'sex': 2,  | 1-жен, 2-муж
                    'only_can_message': True,  | Только с открытыми ЛС
                    'only_active': True,  | Только активные (были онлайн за 30 дней)
                    'has_mobile': True,  | Есть мобильное приложение
                }

        Returns:
            Список пользователей с данными
        """
        logger.info(f"Начинаем парсинг группы: {group_id}")

        # Запускаем фоновую прослушку клавиш
        listener_thread = threading.Thread(target=self._listen_for_skip, daemon=True)
        listener_thread.start()

        # получаем информацию о группе
        group_info = self._get_group_info(group_id)
        logger.info(f"Группа: {group_info['name']}, участников: {group_info['members_count']}")

        users = []
        offset = 0
        count = 4  # максимум за один запрос

        # дефолтные фильтры
        if filters is None:
            filters = {}

        while len(users) < max_users:
            if self.skip_group:
                logger.info(f"Пропускаем группу {group_id} по Ctrl + N")
                self.skip_group = False
                break
            try:
                self._smart_delay()

                # запрос участников и данных о них
                response = self.vk.groups.getMembers(
                    group_id=group_id,
                    offset=offset,
                    count=count,
                    fields='sex,bdate,city,country,photo_200,education,' \
                           'last_seen,online,can_write_private_message,' \
                           'mobile_phone,home_phone,contacts,site,connections,' \
                           'status,interests,occupation,relation,personal,' \
                           'universities,schools,followers_count,counters,' \
                           'has_mobile,career'
                )

                items = response.get('items', [])

                if not items:
                    logger.info("Достигнут конец списка участников")
                    break

                # фильтруем пользователей
                for user in items:
                    if self._filter_user(user, filters, group_id):
                        users.append(user)

                        # Прерываем если достигли лимита
                        if len(users) >= max_users:
                            break

                logger.info(f"обработано: {offset + len(items)}, отфильтровано: {len(users)}")

                offset += count

                # Если получили меньше чем запросили - это конец
                if len(items) < count:
                    break

            except vk_api.exceptions.ApiError as e:
                logger.error(f"Ошибка API: {e}")
                if 'Access denied' in str(e):
                    logger.error("Нет доступа к участникам группы (закрытая группа)")
                    break
                time.sleep(5)
            except Exception as e:
                logger.error(f"Неожиданная ошибка: {e}")
                break

        logger.info(f"Парсинг завершен. Собрано {len(users)} пользователей")
        return users


    def get_synonyms_for_language(self, word: str, lang: str) -> List[str]:
        """
        Получает синонимы для слова на указанном языке.

        Args:
            word: Исходное слово.
            lang: Язык ('en' для английского, 'ru' для русского).

        Returns:
            Список синонимов.
        """
        synonyms = set()
        if lang == 'en':
            for syn in wordnet.synsets(word):
                for lemma in syn.lemmas():
                    synonym = lemma.name().replace('_', ' ')
                    if synonym.lower() != word.lower():
                        synonyms.add(synonym)
        elif lang == 'ru':
            # Временно отключаем RuWordNet
            synonyms.update(["спортзал", "тренажерный зал", "кроссфит"])  # Пример для "фитнес"
        else:
            print(f"Поддержка языка '{lang}' не реализована.")
        return list(synonyms)


    def _expand_group_by_niche(self, niche: str, lang: Optional[str] = None) -> List[str]:
        """ Расширение для обработки синонимов и опечаток в названии ниши для групп
        Args:
            niche: Исходный запрос (например, "фитнес").
        Returns:
            Список расширенных запросов.
        """
        niche = niche.lower().strip()
        if lang is None:
            lang = 'ru' if any('а' <= c <= 'я' for c in niche) else 'en'

        synonym_map = {
            'ru': {
                "фитнес": ["fitness", "спортзал", "тренажерный зал", "бодибилдинг", "кроссфит"],
            },
            'en': {
                "fitness": ["gym", "workout", "тренажерный зал", "бодибилдинг"],
            }
        }

        expanded_queries = set([niche])

        if lang in synonym_map:
            if niche in synonym_map[lang]:
                expanded_queries.update(synonym_map[lang][niche])
            else:
                all_known_niches = list(synonym_map[lang].keys())
                best_match, score = process.extractOne(niche, all_known_niches)
                if score > 80:
                    expanded_queries.update(synonym_map[lang][best_match])
                else:
                    expanded_queries.add(niche + "*")
        else:
            expanded_queries.add(niche + "*")

        return list(expanded_queries)


    def find_groups_by_niche(self, niche: str, count: int) -> List[str]:
        """
        Поиск групп по нише с учетом синонимов и нечеткого поиска.
        """
        expanded_queries = self._expand_group_by_niche(niche)
        all_groups = set()
        for query in expanded_queries:
            try:
                response = self.vk.groups.search(q=query, count=count, type="group")
                groups = response.get('items', [])
                for group in groups:
                    all_groups.add(str(group['id']))
            except Exception as e:
                logger.error(f"Ошибка поиска групп для запроса: {query}: {e}")
        logger.info(f"Найдено групп по нише {niche}: {len(all_groups)}")
        return list(all_groups)


    def _is_group_relevant(self, group: Dict, niche_keywords: List[str]) -> bool:
        description = group.get("description", "").lower()
        return any(keyword in description for keyword in niche_keywords)

    def _is_group_active(self, group_id: str) -> bool:
        """
        Проверяет, был ли последний пост в группе опубликован в течение последних 6 месяцев.
        Args:
            group_id: ID группы.
        Returns:
            True, если группа активна, иначе False.
        """
        try:
            posts_response = self.vk.wall.get(owner_id=f"-{group_id}", count=1)
            posts = posts_response.get('items', [])

            if posts:
                last_post = posts[0]
                last_post_timestamp = last_post.get('date')

                if last_post_timestamp:
                    last_post_date = datetime.fromtimestamp(last_post_timestamp)
                    six_months_ago = datetime.now() - timedelta(days=6 * 30)

                    return last_post_date > six_months_ago
                else:
                    # Если у поста нет даты, считаем группу неактивной или пропускаем
                    return False
            else:
                # Если постов нет, считаем группу неактивной
                return False
        except Exception as e:
            logger.error(f"Ошибка проверки активности группы {group_id}: {e}")
            return False

    def _get_user_comments_in_group(self, user_id: int, group_id: str, months: int = 2) -> List[Dict]:
        comments = []
        try:
            current_time = int(time.time())
            start_time = current_time - months * 30 * 24 * 60 * 60
            posts_response = self.vk.wall.get(owner_id=f"-{group_id}", count=5)  # Получаем последние 5 постов
            posts = posts_response.get('items', [])
            for post in posts:
                post_id = post['id']
                comments_response = self.vk.wall.getComments(
                    owner_id=f"-{group_id}",
                    post_id=post_id,
                    count=100,
                    sort='asc'
                )
                post_comments = comments_response.get('items', [])
                for comment in post_comments:
                    if comment['from_id'] == user_id and comment['date'] >= start_time:
                        comments.append(comment)
        except Exception as e:
            logger.error(f"Ошибка получения комментариев пользователя {user_id} в группе {group_id}: {e}")
        return comments

    def _are_comments_relevant(self, comments: List[Dict], keywords: List[str]) -> bool:
        logger.info(f"Проверка релевантности {len(comments)} комментариев")
        for comment in comments:
            comment_text = comment.get('text', '').lower()
            logger.info(f"Текст комментария: {comment_text}")
            if any(keyword in comment_text for keyword in keywords):
                logger.info("Найден релевантный комментарий")
                return True
        logger.info("Релевантные комментарии не найдены")
        return False

    def parse_leads_by_niche(self, niche: str, max_users: int = 10, filters: Dict = None, group_count: int = 20,
                             max_active_groups: int = 10) -> List[Dict]:
        """
        Парсинг лидов по нише с расширенным поиском групп и сохранением по группам.
        """
        group_ids = self.find_groups_by_niche(niche, group_count)
        if not group_ids:
            logger.warning(f"Не найдено групп по нише: {niche}")
            return []

        all_leads = []
        for group_id in group_ids[:max_active_groups]:  # Ограничиваем количеством активных групп
            if self._is_group_active(group_id):
                logger.info(f"Парсинг группы: {group_id}...")
                remaining_users = max_users - len(all_leads)
                if remaining_users <= 0:
                    break
                leads = self.parse_group_members(group_id=group_id, max_users=remaining_users, filters=filters)
                if leads:
                    # Сохраняем для каждой группы отдельно в cash
                    self.save_users(leads, filename=f"leads_{niche}_{group_id}")
                    all_leads.extend(leads)
                    if len(all_leads) >= max_users:
                        break
            else:
                logger.info(f"Группа {group_id} неактивна, пропускаем")

        # Сохраняем все уникальные лиды в общий user_ids.xlsx
        if all_leads:
            unique_leads = self._remove_duplicates(all_leads)
            self.save_users(unique_leads, filename="user_ids")  # Общий файл

        logger.info(f"Собрано {len(all_leads)} лидов по нише: {niche}")
        return all_leads

    def _get_group_info(self, group_id: str) -> Dict:
        """Получение информации о группе"""
        try:
            response = self.vk.groups.getById(
                group_id=group_id,
                fields='members_count,description,status,activity'
            )[0]
            return response
        except Exception as e:
            logger.error(f"Ошибка получения информации о группе: {e}")
            raise

    def _filter_user(self, user: Dict, filters: Dict, group_id: str = None) -> bool:
        """Фильтрация пользователя по критериям"""

        # Пропускаем удаленные аккаунты
        if 'deactivated' in user:
            return False

        # Фильтр по возможности писать в ЛС
        if filters.get('only_can_message', False):
            if not user.get('can_write_private_message'):
                return False

        # Фильтр по активности
        if filters.get('only_active', True):
            if not self._is_user_active(user):
                return False

        # Фильтр по городу
        if filters.get('city_ids'):
            user_city = user.get('city', {}).get('id')
            if user_city not in filters['city_ids']:
                return False

        # Фильтр по полу
        if filters.get('sex'):
            if user.get('sex') != filters['sex']:
                return False

        # Фильтр по возрасту
        if filters.get('age_from') or filters.get('age_to'):
            age = self._get_user_age(user)
            if age:
                if filters.get('age_from') and age < filters['age_from']:
                    return False
                if filters.get('age_to') and age > filters['age_to']:
                    return False

        # Фильтр по наличию мобильного приложения
        if filters.get('has_mobile', False):
            if not user.get('has_mobile'):
                return False

        # Проверка релевантности комментариев пользователя в группе
            # Проверка релевантности комментариев пользователя в группе
            if group_id:
                keywords = self.keywords
                comments = self._get_user_comments_in_group(user['id'], group_id, months=6)
                if comments:
                    if not self._are_comments_relevant(comments, keywords):
                        logger.info("Пропущен: комментарии пользователя не релевантны")
                        return False
                else:
                    logger.info("Пропущен: у пользователя нет комментариев в группе")
                    return False

        logger.info("Пользователь прошел фильтрацию")
        return True

    def _is_user_active(self, user: Dict, days: int = 30) -> bool:
        """Проверка активности пользователя"""
        if user.get('online'):
            return True

        if 'last_seen' in user:
            last_seen = user['last_seen'].get('time', 0)
            days_inactive = (time.time() - last_seen) / 86400
            return days_inactive <= days

        return False

    def _get_user_age(self, user: Dict) -> Optional[int]:
        """Вычисление возраста пользователя"""
        bdate = user.get('bdate')
        if not bdate:
            return None

        parts = bdate.split('.')
        if len(parts) != 3:
            return None

        try:
            birth_year = int(parts[2])
            current_year = datetime.now().year
            return current_year - birth_year
        except:
            return None

    def check_message_availability(self, user_ids: List[int]) -> Dict[int, bool]:
        """
        Проверка возможности отправки сообщений пользователям

        Args:
            user_ids: Список ID пользователей

        Returns:
            Словарь {user_id: можно_писать}
        """
        logger.info(f"Проверяем возможность отправки сообщений для {len(user_ids)} пользователей")

        availability = {}

        # Проверяем батчами по 100
        for i in range(0, len(user_ids), 100):
            batch = user_ids[i:i + 100]

            try:
                self._smart_delay()

                # Используем метод messages.isMessagesFromGroupAllowed для проверки
                # Но для пользователя используем users.get с полем can_write_private_message
                users_info = self.vk.users.get(
                    user_ids=','.join(map(str, batch)),
                    fields='can_write_private_message'
                )

                for user in users_info:
                    availability[user['id']] = user.get('can_write_private_message', False)

            except Exception as e:
                logger.error(f"Ошибка проверки доступности: {e}")
                # В случае ошибки считаем что нельзя
                for user_id in batch:
                    availability[user_id] = False

        can_message = sum(1 for v in availability.values() if v)
        logger.info(f"Можно написать {can_message} из {len(user_ids)} пользователей")

        return availability

    def send_messages(
            self,
            users: List[Dict],
            message_template: str,
            delay_range: tuple = (60, 120),
            max_per_day: int = 50,
            dry_run: bool = False
    ) -> Dict:
        """
        Рассылка сообщений пользователям

        Args:
            users: Список пользователей для рассылки
            message_template: Шаблон сообщения. Поддерживает переменные:
                {first_name}, {last_name}, {city}
            delay_range: Диапазон задержки между сообщениями в секундах
            max_per_day: Максимум сообщений в день
            dry_run: Тестовый режим (не отправляет реально)

        Returns:
            Статистика рассылки
        """
        logger.info(f"Начинаем рассылку для {len(users)} пользователей")

        stats = {
            'total': len(users),
            'sent': 0,
            'failed': 0,
            'skipped': 0,
            'errors': []
        }

        sent_today = 0

        for user in users:
            # Лимит в день
            if sent_today >= max_per_day:
                logger.warning(f"Достигнут дневной лимит: {max_per_day}")
                stats['skipped'] = len(users) - stats['sent'] - stats['failed']
                break

            user_id = user['id']

            # Проверяем возможность отправки
            if not user.get('can_write_private_message'):
                logger.debug(f"Пропускаем {user_id} - закрытые ЛС")
                stats['skipped'] += 1
                continue

            # Формируем сообщение
            try:
                message = self._format_message(message_template, user)
            except Exception as e:
                logger.error(f"Ошибка форматирования сообщения для {user_id}: {e}")
                stats['failed'] += 1
                continue

            # Отправляем
            try:
                if not dry_run:
                    self._smart_delay()

                    self.vk.messages.send(
                        user_id=user_id,
                        message=message,
                        random_id=random.randint(1, 2 ** 31)
                    )

                stats['sent'] += 1
                sent_today += 1

                logger.info(f"✓ Отправлено {user_id}: {user.get('first_name')} {user.get('last_name')}")

                # Случайная задержка
                delay = random.uniform(*delay_range)
                if not dry_run:
                    logger.debug(f"Задержка {delay:.1f} сек...")
                    time.sleep(delay)

            except vk_api.exceptions.ApiError as e:
                error_msg = str(e)
                stats['failed'] += 1
                stats['errors'].append({
                    'user_id': user_id,
                    'error': error_msg
                })

                logger.error(f"✗ Ошибка отправки {user_id}: {error_msg}")

                # Если бан - останавливаемся
                if 'flood control' in error_msg.lower():
                    logger.error("FLOOD CONTROL! Слишком много запросов. Остановка на 1 час.")
                    time.sleep(3600)
                elif 'user is blocked' in error_msg.lower():
                    logger.error("Аккаунт заблокирован! Останавливаем рассылку.")
                    break

            except Exception as e:
                stats['failed'] += 1
                logger.error(f"Неожиданная ошибка для {user_id}: {e}")

        # Итоговая статистика
        logger.info(f"""
        Всего пользователей: {stats['total']}
        Отправлено: {stats['sent']}
        Ошибок: {stats['failed']}
        Пропущено: {stats['skipped']}
        """)

        return stats

    def _format_message(self, template: str, user: Dict) -> str:
        """Форматирование сообщения с подстановкой данных"""
        return template.format(
            first_name=user.get('first_name', ''),
            last_name=user.get('last_name', ''),
            city=user.get('city', {}).get('title', 'вашего города'),
            age=self._get_user_age(user) or ''
        )

    def save_users(self, users: List[Dict], filename: str = 'user_ids'):
        """Сохранение пользователей в Excel файл."""
        # Создаем список строк в нужном формате
        user_data = []
        for user in users:
            first_name = user.get('first_name', '')
            last_name = user.get('last_name', '')
            user_id = user.get('id', '')
            user_url = f"https://vk.com/id{user_id}"
            user_data.append(f"{first_name} {last_name}\t{user_id}\t{user_url}")
        # Создаем DataFrame
        df = pd.DataFrame(user_data, columns=['UserInfo'])
        df[['Name', 'ID', 'URL']] = df['UserInfo'].str.split('\t', expand=True, n=2)
        df = df.drop(columns=['UserInfo'])

        # Убедимся, что директория существует
        script_dir = os.path.dirname(os.path.abspath(__file__))
        save_path = os.path.join(script_dir, 'vk_spam_bot-main')
        os.makedirs(save_path, exist_ok=True)

        if filename == 'user_ids':
            # Общий файл: только в vk_spam_bot-main/user_ids.xlsx
            excel_filename = os.path.join(save_path, "user_ids.xlsx")
            df.to_excel(excel_filename, index=False)
            logger.info(f"Лидов сохранено в {excel_filename}")
        else:
            # Файлы по группам: только в cash/
            cash_path = os.path.join(save_path, 'cash')
            os.makedirs(cash_path, exist_ok=True)
            cash_filename = os.path.join(cash_path, f"{filename}.xlsx")
            df.to_excel(cash_filename, index=False)
            logger.info(f"Лидов сохранено в {cash_filename}")

        return excel_filename if filename == 'user_ids' else cash_filename

    def _remove_duplicates(self, users: List[Dict]) -> List[Dict]:
        """Удаляет дубликаты пользователей по ID."""
        seen_ids = set()
        unique_users = []
        for user in users:
            user_id = user.get('id')
            if user_id and user_id not in seen_ids:
                seen_ids.add(user_id)
                unique_users.append(user)
        return unique_users

    def load_users(self, filename: str) -> List[Dict]:
        """Загрузка пользователей из JSON файла"""
        with open(filename, 'r', encoding='utf-8') as f:
            users = json.load(f)
        logger.info(f"Загружено {len(users)} пользователей из {filename}")
        return users

    def get_user_stats(self, users: List[Dict], all_groups, actual_groups) -> Dict:
        """Статистика по собранным пользователям"""
        stats = {
            'groups': all_groups,
            'actual_groups': actual_groups,
            'total': len(users),
            'can_message': sum(1 for u in users if u.get('can_write_private_message')),
            'online_now': sum(1 for u in users if u.get('online')),
            'has_mobile': sum(1 for u in users if u.get('has_mobile')),
            'sex': {'male': 0, 'female': 0, 'unknown': 0},
            'cities': {},
            'activity': {
                'today': 0,
                'week': 0,
                'month': 0,
                'older': 0
            }
        }

        for user in users:
            # Пол
            sex = user.get('sex', 0)
            if sex == 2:
                stats['sex']['male'] += 1
            elif sex == 1:
                stats['sex']['female'] += 1
            else:
                stats['sex']['unknown'] += 1

            # Города
            city = user.get('city', {}).get('title', 'Не указан')
            stats['cities'][city] = stats['cities'].get(city, 0) + 1

            # Активность
            if user.get('online'):
                stats['activity']['today'] += 1
            elif 'last_seen' in user:
                days = (time.time() - user['last_seen']['time']) / 86400
                if days < 1:
                    stats['activity']['today'] += 1
                elif days < 7:
                    stats['activity']['week'] += 1
                elif days < 30:
                    stats['activity']['month'] += 1
                else:
                    stats['activity']['older'] += 1

        # Сортируем города по популярности
        stats['cities'] = dict(sorted(stats['cities'].items(), key=lambda x: x[1], reverse=True)[:10])

        return stats


def main():
    NICHE = "стартап"  # Попробуй также "fitness", "фитнесс", "спортзал"
    FILTERS = {
        'city_ids': [1, 2],  # Москва и СПб
        'age_from': 18,
        'age_to': 35,
        'sex': 0,
        'only_can_message': True,
        'only_active': True,
    }
    MAX_LEADS = 50
    try:
        parser = VKGroupParser(token=TOKEN)
        leads = parser.parse_leads_by_niche(
            niche=NICHE,
            max_users=MAX_LEADS,
            filters=FILTERS
        )
        if leads:
            # Определяем group_count (из параметров parse_leads_by_niche)
            group_count = 20  # Или любое значение, как в вызове parse_leads_by_niche

            # Вычисляем all_groups и actual_groups
            all_groups = parser.find_groups_by_niche(NICHE, group_count)
            actual_groups = [g for g in all_groups if parser._is_group_active(g)]

            stats = parser.get_user_stats(leads, all_groups, actual_groups)
            logger.info(f"""
                        Стастистика парсера:
                        Всего групп: {len(all_groups)}
                        Активных групп: {len(actual_groups)}
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

            logger.info(f"Статистика по лидам из ниши '{NICHE}': {stats}")
        else:
            logger.warning(f"Не удалось собрать лидов по нише '{NICHE}'")
    except Exception as e:
        logger.error(f"Ошибка: {e}")


if __name__ == "__main__":
    main()