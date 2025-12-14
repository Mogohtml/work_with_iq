import os
import json
import sys
import threading
import vk_api
import time
import random
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pandas as pd
import requests
from database import VKUserDatabase


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('vk_parser.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class VKGroupParser:
    def __init__(self, token: str):
        self.token = token
        self.session = vk_api.VkApi(token=token)
        self.vk = self.session.get_api()
        self.user_id = None
        self.requests_count = 0
        self.last_request_time = 0
        self.skip_group = False
        self._init_user()

        # Ключевые слова для фильтрации пользователей
        self.keywords = [
            # Разработка интернет-магазина
            ["нужен", "разработчик", "интернет-магазина"],
            ["заказать", "интернет-магазин", "срочно"],
            ["создать", "интернет-магазин", "под ключ"],
            ["разработка", "интернет-магазина", "недорого"],
            ["фрилансер", "интернет-магазин", "python"],
            ["интернет-магазин", "на", "fastapi"],
            ["интернет-магазин", "на", "django"],
            ["интернет-магазин", "на", "flask"],
            ["интернет-магазин", "с", "админ-панелью"],
            ["интернет-магазин", "с", "личным кабинетом"],
            ["интернет-магазин", "с", "корзиной"],
            ["интернет-магазин", "с", "фильтрами"],
            ["интернет-магазин", "с", "поиском"],
            ["интернет-магазин", "с", "отзывами"],
            ["интернет-магазин", "с", "блогом"],
            ["интернет-магазин", "с", "мультиязычностью"],
            ["интернет-магазин", "с", "мобильной версией"],
            ["интернет-магазин", "с", "сео оптимизацией"],
            ["интернет-магазин", "с", "аналитикой"],
            ["интернет-магазин", "с", "интеграцией 1с"],
            ["интернет-магазин", "с", "интеграцией crm"],
            ["интернет-магазин", "с", "интеграцией телеграм"],
            ["интернет-магазин", "с", "интеграцией ватсап"],
            ["интернет-магазин", "с", "интеграцией оплаты"],
            ["интернет-магазин", "с", "интеграцией доставки"],
            ["интернет-магазин", "с", "интеграцией склад"],
            ["интернет-магазин", "с", "интеграцией маркетплейсов"],
            ["интернет-магазин", "с", "интеграцией соцсетей"],
            ["интернет-магазин", "с", "интеграцией email рассылки"],
            ["интернет-магазин", "с", "интеграцией чат-бота"],
            ["интернет-магазин", "с", "интеграцией аналитики"],
            ["интернет-магазин", "с", "интеграцией smm"],
            ["интернет-магазин", "с", "интеграцией рекламы"],
            ["интернет-магазин", "с", "интеграцией лк"],
            ["интернет-магазин", "с", "интеграцией api"],
            ["интернет-магазин", "с", "интеграцией платежных систем"],
            ["интернет-магазин", "с", "интеграцией логистики"],
            ["интернет-магазин", "с", "интеграцией скриптов"],
            ["интернет-магазин", "с", "интеграцией модулей"],
            ["интернет-магазин", "с", "интеграцией плагинов"],
            ["интернет-магазин", "с", "интеграцией сервисов"],
            ["интернет-магазин", "с", "интеграцией баз данных"],
            ["интернет-магазин", "с", "интеграцией облака"],
            ["интернет-магазин", "с", "интеграцией хостинга"],
            ["интернет-магазин", "с", "интеграцией домена"],
            ["интернет-магазин", "с", "интеграцией ssl"],
            ["интернет-магазин", "с", "интеграцией безопасности"],
            ["интернет-магазин", "с", "интеграцией резервного копирования"],
            ["интернет-магазин", "с", "интеграцией поддержки"],
            ["интернет-магазин", "с", "интеграцией обратной связи"],

            # Дизайн интернет-магазина
            ["дизайн", "интернет-магазина", "уникальный"],
            ["дизайн", "интернет-магазина", "минималистичный"],
            ["дизайн", "интернет-магазина", "современный"],
            ["дизайн", "интернет-магазина", "креативный"],
            ["дизайн", "интернет-магазина", "адаптивный"],
            ["дизайн", "интернет-магазина", "мобильный"],
            ["дизайн", "интернет-магазина", "корпоративный"],
            ["дизайн", "интернет-магазина", "фирменный"],
            ["дизайн", "интернет-магазина", "с логотипом"],
            ["дизайн", "интернет-магазина", "с анимацией"],
            ["дизайн", "интернет-магазина", "с иллюстрациями"],
            ["дизайн", "интернет-магазина", "с баннерами"],
            ["дизайн", "интернет-магазина", "с иконками"],
            ["дизайн", "интернет-магазина", "с шрифтами"],
            ["дизайн", "интернет-магазина", "с цветами"],
            ["дизайн", "интернет-магазина", "с ui/ux"],

            # Техническая поддержка
            ["поддержка", "интернет-магазина", "техническая"],
            ["поддержка", "интернет-магазина", "круглосуточная"],
            ["поддержка", "интернет-магазина", "онлайн"],
            ["поддержка", "интернет-магазина", "обновление"],
            ["поддержка", "интернет-магазина", "бэкап"],
            ["поддержка", "интернет-магазина", "безопасность"],
            ["поддержка", "интернет-магазина", "хостинг"],
            ["поддержка", "интернет-магазина", "домен"],
            ["поддержка", "интернет-магазина", "ssl сертификат"],
            ["поддержка", "интернет-магазина", "оптимизация"],
            ["поддержка", "интернет-магазина", "скорость"],
            ["поддержка", "интернет-магазина", "ошибки"],
            ["поддержка", "интернет-магазина", "интеграции"],
            ["поддержка", "интернет-магазина", "модули"],
            ["поддержка", "интернет-магазина", "плагины"],

            # Интеграции с внешними сервисами
            ["интеграция", "интернет-магазина", "1с"],
            ["интеграция", "интернет-магазина", "crm"],
            ["интеграция", "интернет-магазина", "платежные системы"],
            ["интеграция", "интернет-магазина", "доставка"],
            ["интеграция", "интернет-магазина", "маркетплейсы"],
            ["интеграция", "интернет-магазина", "соцсети"],
            ["интеграция", "интернет-магазина", "email рассылка"],
            ["интеграция", "интернет-магазина", "чат-бот"],
            ["интеграция", "интернет-магазина", "аналитика"],
            ["интеграция", "интернет-магазина", "smm"],
            ["интеграция", "интернет-магазина", "реклама"],
            ["интеграция", "интернет-магазина", "личный кабинет"],
            ["интеграция", "интернет-магазина", "api"],
            ["интеграция", "интернет-магазина", "склад"],
            ["интеграция", "интернет-магазина", "логистика"],
            ["интеграция", "интернет-магазина", "скрипты"],
            ["интеграция", "интернет-магазина", "модули"],
            ["интеграция", "интернет-магазина", "плагины"],
            ["интеграция", "интернет-магазина", "сервисы"],
            ["интеграция", "интернет-магазина", "базы данных"],
            ["интеграция", "интернет-магазина", "облако"],
            ["интеграция", "интернет-магазина", "хостинг"],
            ["интеграция", "интернет-магазина", "домен"],
            ["интеграция", "интернет-магазина", "ssl"],
            ["интеграция", "интернет-магазина", "безопасность"],
            ["интеграция", "интернет-магазина", "резервное копирование"],
            ["интеграция", "интернет-магазина", "поддержка"],
            ["интеграция", "интернет-магазина", "обратная связь"],

            # Оптимизация и аналитика
            ["оптимизация", "интернет-магазина", "скорость"],
            ["оптимизация", "интернет-магазина", "сео"],
            ["оптимизация", "интернет-магазина", "конверсия"],
            ["оптимизация", "интернет-магазина", "юзабилити"],
            ["оптимизация", "интернет-магазина", "трафик"],
            ["оптимизация", "интернет-магазина", "аналитика"],
            ["оптимизация", "интернет-магазина", "реклама"],
            ["оптимизация", "интернет-магазина", "лидогенерация"],
            ["оптимизация", "интернет-магазина", "брендинг"],
            ["оптимизация", "интернет-магазина", "контент"],
            ["оптимизация", "интернет-магазина", "дизайн"],
            ["оптимизация", "интернет-магазина", "мобильная версия"],
            ["оптимизация", "интернет-магазина", "безопасность"],
            ["оптимизация", "интернет-магазина", "хостинг"],
            ["оптимизация", "интернет-магазина", "домен"],
            ["оптимизация", "интернет-магазина", "ssl"],
            ["оптимизация", "интернет-магазина", "интеграции"],
            ["оптимизация", "интернет-магазина", "модули"],
            ["оптимизация", "интернет-магазина", "плагины"],

            # Запуск и развитие
            ["запуск", "интернет-магазина", "с нуля"],
            ["запуск", "интернет-магазина", "под ключ"],
            ["запуск", "интернет-магазина", "быстро"],
            ["запуск", "интернет-магазина", "недорого"],
            ["запуск", "интернет-магазина", "с поддержкой"],
            ["развитие", "интернет-магазина", "стратегия"],
            ["развитие", "интернет-магазина", "маркетинг"],
            ["развитие", "интернет-магазина", "продвижение"],
            ["развитие", "интернет-магазина", "аналитика"],
            ["развитие", "интернет-магазина", "конверсия"],
            ["развитие", "интернет-магазина", "трафик"],
            ["развитие", "интернет-магазина", "лидогенерация"],
            ["развитие", "интернет-магазина", "брендинг"],
            ["развитие", "интернет-магазина", "контент"],
            ["развитие", "интернет-магазина", "дизайн"],
            ["развитие", "интернет-магазина", "интеграции"],
            ["развитие", "интернет-магазина", "модули"],
            ["развитие", "интернет-магазина", "плагины"],
        ]

    def _init_user(self):
        try:
            user_info = self.vk.users.get()[0]
            self.user_id = user_info['id']
            logger.info(f"Авторизован как: {user_info['first_name']} {user_info['last_name']}")
        except Exception as e:
            logger.error(f"Ошибка авторизации: {e}")
            raise

    def _smart_delay(self):
        self.requests_count += 1
        if self.requests_count % 3 == 0:
            delay = random.uniform(5.0, 10.0)  # Увеличьте задержки
        else:
            delay = random.uniform(2.0, 5.0)
        if self.requests_count % 10 == 0:  # Уменьшите частоту длинных пауз, но увеличьте их продолжительность
            logger.info("Делаем паузу 90 секунд для избежания ограничений")
            time.sleep(90)
        else:
            time.sleep(delay)
        self.last_request_time = time.time()

    def parse_group_members(self, group_id: str, max_users: int = 500, filters: Dict = None) -> List[Dict]:
        logger.info(f"Начинаем парсинг группы: {group_id}")

        group_info = self._get_group_info(group_id)
        logger.info(f"Группа: {group_info['name']}, участников: {group_info['members_count']}")

        users = []
        offset = 0
        count = 200

        if filters is None:
            filters = {}

        while len(users) < max_users:
            if self.skip_group:
                logger.info(f"Пропускаем группу {group_id}")
                self.skip_group = False
                break

            try:
                self._smart_delay()
                response = self.vk.groups.getMembers(
                    group_id=group_id,
                    offset=offset,
                    count=count,
                    fields='sex,bdate,city,can_write_private_message,last_seen,online'
                )
                items = response.get('items', [])
                if not items:
                    logger.info("Достигнут конец списка участников")
                    break

                for user in items:
                    if self._filter_user(user, filters):
                        users.append(user)
                        if len(users) >= max_users:
                            break

                logger.info(f"Обработано: {offset + len(items)}, отфильтровано: {len(users)}")
                offset += count

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

    def _filter_user(self, user: Dict, filters: Dict) -> bool:
        if 'deactivated' in user:
            return False

        if filters.get('only_can_message', False) and not user.get('can_write_private_message'):
            return False

        if filters.get('only_active', True) and not self._is_user_active(user):
            return False

        if filters.get('city_ids') and user.get('city', {}).get('id') not in filters['city_ids']:
            return False

        if filters.get('sex') and user.get('sex') != filters['sex']:
            return False

        if filters.get('age_from') or filters.get('age_to'):
            age = self._get_user_age(user)
            if age and ((filters.get('age_from') and age < filters['age_from']) or
                        (filters.get('age_to') and age > filters['age_to'])):
                return False

        return True

    def _is_user_active(self, user: Dict, days: int = 30) -> bool:
        if user.get('online'):
            return True
        if 'last_seen' in user:
            last_seen = user['last_seen'].get('time', 0)
            days_inactive = (time.time() - last_seen) / 86400
            return days_inactive <= days
        return False

    def _get_user_age(self, user: Dict) -> Optional[int]:
        bdate = user.get('bdate')
        if not bdate or len(bdate.split('.')) != 3:
            return None
        try:
            birth_year = int(bdate.split('.')[2])
            current_year = datetime.now().year
            return current_year - birth_year
        except:
            return None

    def _get_group_info(self, group_id: str) -> Dict:
        try:
            response = self.vk.groups.getById(
                group_id=group_id,
                fields='members_count,description,status,activity'
            )[0]
            return response
        except Exception as e:
            logger.error(f"Ошибка получения информации о группе: {e}")
            raise

    def find_groups_by_niche(self, niche: str, count: int = 1000) -> List[str]:
        all_groups = set()
        try:
            response = self.vk.groups.search(q=niche, count=count, type="group")
            groups = response.get('items', [])
            for group in groups:
                all_groups.add(str(group['id']))
        except Exception as e:
            logger.error(f"Ошибка поиска групп для запроса: {niche}: {e}")
        logger.info(f"Найдено групп по нише {niche}: {len(all_groups)}")
        return list(all_groups)

    def _is_group_active(self, group_id: str) -> bool:
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
            return False
        except Exception as e:
            logger.error(f"Ошибка проверки активности группы {group_id}: {e}")
            return False

    def save_parsed_groups(self, groups: List[str], niche: str):
        """Сохранение информации о спарсенных группах в базе данных."""
        try:
            db = VKUserDatabase()
            for group_id in groups:
                db.add_group(group_id, niche)
        except Exception as e:
            logger.error(f"Ошибка при сохранении информации о группах: {e}")

    def parse_leads_by_niche(self, niche: str, max_users: int = 500, filters: Dict = None, group_count: int = 20) -> \
    List[Dict]:
        group_ids = self.find_groups_by_niche(niche, 1000)  # Ищем 1000 групп, но парсим только 20
        if not group_ids:
            logger.warning(f"Не найдено групп по нише: {niche}")
            return []

        db = VKUserDatabase()
        parsed_groups = db.get_parsed_groups(niche)
        new_groups = [group_id for group_id in group_ids if group_id not in parsed_groups]

        if not new_groups:
            logger.warning(f"Все группы по нише {niche} уже спарсены.")
            return []

        # Ограничиваем количество групп для парсинга за один запуск
        groups_to_parse = new_groups[:group_count]

        all_leads = []
        for group_id in groups_to_parse:
            if self._is_group_active(group_id):
                logger.info(f"Парсинг группы: {group_id}...")
                remaining_users = max_users - len(all_leads)
                if remaining_users <= 0:
                    break
                leads = self.parse_group_members(group_id=group_id, max_users=remaining_users, filters=filters)
                if leads:
                    self.save_users(leads, filename=f"leads_{niche}_{group_id}")
                    all_leads.extend(leads)
                    if len(all_leads) >= max_users:
                        break
                time.sleep(random.uniform(10.0, 20.0))
            else:
                logger.info(f"Группа {group_id} неактивна, пропускаем")

        if all_leads:
            unique_leads = self._remove_duplicates(all_leads)
            self.save_users(unique_leads, filename="user_ids")
            self.save_parsed_groups(groups_to_parse, niche)

        logger.info(f"Собрано {len(all_leads)} лидов по нише: {niche}")
        return all_leads

    def is_group_parsed(self, group_id: str, niche: str) -> bool:
        """Проверка, была ли группа уже спарсена."""
        try:
            db = VKUserDatabase()
            return db.is_group_parsed(group_id, niche)
        except Exception as e:
            logger.error(f"Ошибка проверки группы {group_id}: {e}")
            return False

    def save_users(self, users: List[Dict], filename: str = 'user_ids'):
        if not users:
            logger.info("Нет пользователей для сохранения.")
            return

        # Преобразуем данные для SQLite
        users_for_db = []
        for user in users:
            user_id = user.get('id') or user.get('ID')
            if not user_id:
                continue
            users_for_db.append({
                "id": user_id,
                "first_name": user.get('first_name', ''),
                "last_name": user.get('last_name', ''),
            })

        # Сохраняем в SQLite
        try:
            db = VKUserDatabase()
            db.backup_db()
            db.add_users(users_for_db)
        except Exception as e:
            logger.error(f"Ошибка при сохранении в базу: {e}")

        # Сохраняем в кэш (Excel) только если это не основной файл
        if filename != 'user_ids':
            users_for_excel = []
            for user in users:
                user_id = user.get('id') or user.get('ID')
                users_for_excel.append({
                    "Name": f"{user.get('first_name', '')} {user.get('last_name', '')}",
                    "ID": user_id,
                    "URL": f"https://vk.com/id{user_id}",
                    "sent": False,
                })
            df = pd.DataFrame(users_for_excel)
            cash_path = os.path.join("vk_spam_bot-main", "cash")
            os.makedirs(cash_path, exist_ok=True)
            df.to_excel(os.path.join(cash_path, f"{filename}.xlsx"), index=False)
            logger.info(f"Сохранено {len(users_for_excel)} пользователей в кэш.")

    def is_group_parsed(self, group_id: str, niche: str) -> bool:
        """Проверка, была ли группа уже спарсена."""
        try:
            db = VKUserDatabase()
            return db.is_group_parsed(group_id, niche)
        except Exception as e:
            logger.error(f"Ошибка проверки группы {group_id}: {e}")
            return False

    def _remove_duplicates(self, users: List[Dict]) -> List[Dict]:
        seen_ids = set()
        unique_users = []
        for user in users:
            user_id = user.get('id')
            if user_id and user_id not in seen_ids:
                seen_ids.add(user_id)
                unique_users.append(user)
        return unique_users

    def check_token_validity(self):
        try:
            self.vk.users.get()
            return True
        except Exception as e:
            logger.error(f"Токен недействителен: {e}")
            return False

    def upload_photo(self, peer_id: int, photo_path: str) -> str:
        if not os.path.exists(photo_path):
            logger.error(f"Файл не найден: {photo_path}")
            return ""
        try:
            upload_url = self.vk.photos.getMessagesUploadServer(peer_id=peer_id)['upload_url']
            with open(photo_path, 'rb') as photo_file:
                response = requests.post(upload_url, files={'photo': photo_file}).json()
            if 'error' in response:
                logger.error(f"Ошибка загрузки фото: {response['error']}")
                return ""
            photo_data = self.vk.photos.saveMessagesPhoto(**response)
            if not photo_data:
                logger.error("Ошибка сохранения фото")
                return ""
            return f"photo{photo_data[0]['owner_id']}_{photo_data[0]['id']}"
        except Exception as e:
            logger.error(f"Ошибка загрузки фото {photo_path}: {e}")
            return ""

    def send_messages(
            self,
            users: List[Dict],
            message_template: str,
            photo_paths: List[str],
            max_per_day: int = 20
    ) -> Dict:
        logger.info(f"Начинаем рассылку для {len(users)} пользователей")
        stats = {
            'total': len(users),
            'sent': 0,
            'failed': 0,
            'skipped': 0,
            'errors': []
        }
        sent_today = 0

        if not isinstance(photo_paths, list):
            logger.error(f"photo_paths должен быть списком строк, получен: {type(photo_paths)}")
            return stats

        for user in users:
            if sent_today >= max_per_day:
                logger.warning(f"Достигнут дневной лимит: {max_per_day}")
                break

            user_id = user.get('ID')
            if not user_id:
                logger.debug(f"Пропускаем пользователя — отсутствует ID")
                stats['skipped'] += 1
                continue

            try:
                message = message_template.format(first_name=user.get('first_name', ''))
            except Exception as e:
                logger.error(f"Ошибка форматирования сообщения для {user_id}: {e}")
                stats['failed'] += 1
                continue

            try:
                self._smart_delay()
                attachments = []
                for photo_path in photo_paths:
                    if not isinstance(photo_path, str):
                        logger.error(f"Путь к фотографии должен быть строкой, получен: {type(photo_path)}")
                        continue
                    if not os.path.exists(photo_path):
                        logger.debug(f"Файл не найден: {photo_path}")
                        continue
                    attachment = self.upload_photo(user_id, photo_path)
                    if attachment:
                        attachments.append(attachment)

                self.vk.messages.send(
                    user_id=user_id,
                    message=message,
                    attachment=",".join(attachments) if attachments else None,
                    random_id=random.randint(1, 2 ** 31)
                )

                stats['sent'] += 1
                sent_today += 1
                logger.info(f"✓ Отправлено {user_id}: {user.get('first_name')} {user.get('last_name', '')}")

                try:
                    db = VKUserDatabase()
                    db.update_sent_status(user_id, sent=True)
                except Exception as e:
                    logger.error(f"Ошибка обновления статуса для пользователя {user_id}: {e}")

                time.sleep(random.uniform(150, 220))

            except vk_api.exceptions.ApiError as e:
                error_msg = str(e)
                stats['failed'] += 1
                stats['errors'].append({'user_id': user_id, 'error': error_msg})
                logger.error(f"✗ Ошибка отправки {user_id}: {error_msg}")
                if 'flood control' in error_msg.lower():
                    logger.error("FLOOD CONTROL! Останавливаемся на 1 час.")
                    time.sleep(3600)
                elif 'user is blocked' in error_msg.lower():
                    logger.error("Аккаунт заблокирован! Останавливаем рассылку.")
                    break
            except Exception as e:
                stats['failed'] += 1
                logger.error(f"Неожиданная ошибка для {user_id}: {e}")

        logger.info(f"Отправлено: {stats['sent']}, Ошибок: {stats['failed']}, Пропущено: {stats['skipped']}")
        return stats


def main():
    time.sleep(10)

    FILTERS = {
        'city_ids': [1, 2],
        'age_from': 18,
        'age_to': 35,
        'sex': 0,
        'only_can_message': True,
        'only_active': True,
    }

    # Ниши для поиска групп
    NICHES = [
        # Общие
        "бизнес", "предпринимательство", "дело", "стартап", "проект", "фирма", "компания",
        "старт", "начало", "идея", "рост", "scale", "biz", "startup", "company", "project",
        "growth", "business", "entreprenuership",

        # Дизайн и Визуал
        "дизайн", "арт", "стиль", "визуал", "графика", "проектирование", "моделирование",
        "верстка", "айдентика", "брендбук", "креатив", "логотип", "лого", "знак", "баннер",
        "афиша", "образец", "шаблон", "макет", "ui/ux", "ui", "ux", "интерфейс", "юзабилити",
        "web", "веб", "дизайнер", "brand", "design", "style", "logo", "banner", "creative",
        "guide", "guideline", "identity", "art", "graphic", "layout", "template", "mockup",
        "wireframe", "prototype", "frontend", "uiux", "uxui",

        # Маркетинг и Реклама
        "маркетинг", "реклама", "продвижение", "промо", "промоушен", "пиар", "pr", "продажи",
        "контент", "кампания", "объявления", "трафик", "лиды", "конверсия", "метрики",
        "аналитика", "сео", "seo", "sem", "контекст", "таргет", "email", "аудитория",
        "бренд", "продукт", "рынок", "клиент", "потребитель", "анонс", "propaganda", "ad",
        "ads", "ppc", "smm", "marketing", "promotion", "sales", "leads", "traffic",
        "conversion", "metrics", "analytics", "audience", "brand", "product", "market",
        "customer", "user", "campaign", "content", "target", "social",

        # Прочее
        "рынок", "ниша", "анализ", "конкуренты", "тренд", "план", "стратегия", "MVP", "A/B",
        "market", "niche", "analysis", "competitors", "trend", "plan", "strategy", "test",
    ]
    # Добавьте задержку перед началом работы
    time.sleep(10)

    # Загружаем текущую нишу из файла
    current_niche_file = "current_niche.txt"
    if os.path.exists(current_niche_file):
        with open(current_niche_file, "r") as f:
            current_niche_index = int(f.read().strip())
    else:
        current_niche_index = 0

    # Парсим только одну нишу за запуск
    if current_niche_index < len(NICHES):
        niche = NICHES[current_niche_index]
        logger.info(f"Парсинг по нише: {niche}")

        parser = VKGroupParser(token=os.environ.get("ACCESS_TOKEN_1"))
        leads = parser.parse_leads_by_niche(niche=niche, max_users=500, filters=FILTERS)
        if leads:
            logger.info(f"Собрано {len(leads)} лидов по нише: {niche}")
        else:
            logger.warning(f"Не удалось собрать лидов по нише: {niche}")

        # Проверка токена на актуальность
        if not parser.check_token_validity():
            logger.error("Токен недействителен. Обновите токен и перезапустите скрипт.")
            sys.exit(1)

        # Отправляем сообщения через все доступные токены
        for token in [os.environ.get(f"ACCESS_TOKEN_{i}") for i in range(1, 2) if os.environ.get(f"ACCESS_TOKEN_{i}")]:
            try:
                sender = VKGroupParser(token=token)
                db = VKUserDatabase()
                users_to_send = db.get_unsent_users()

                if not users_to_send:
                    logger.info(f"Нет пользователей для отправки сообщений с токена {token[:5]}...")
                    continue

                message_template = """👋 Привет, {first_name}!

                Я Магомед-Басир, разработчик интернет-решений. Нашел тебя в группе по теме "{niche}" и решил предложить свои услуги, так как вижу, что ты интересуешься этой областью.

                🔹 Чем могу помочь:
                ✔ Разработка интернет-магазинов и лендингов под ключ
                ✔ Создание ботов и мини-приложений
                ✔ Интеграции с платежками, CRM, 1С
                ✔ Адаптивный дизайн и техническая поддержка

                🔹 Мои работы:
                🌐 Интернет-магазины
                🤖 Боты для бизнеса
                📱 Мини-приложения
                🎨 Уникальный дизайн

                📌 Портфолио и отзывы:
                🔸 [profi.ru/profile/DzhabagiyevMM](https://profi.ru/profile/DzhabagiyevMM)
                🔸 [Документ с кейсами](https://docs.google.com/document/d/17Uoh5Pw6aU20O719HH0AIwlFDlRftgjy1YlSqapNPjY/edit?usp=sharing)

                Если заинтересовало, напиши мне "МАГАЗИН" - отвечу на вопросы и помогу с проектом!

                📞 Связаться:
                💬 Telegram: @Basmansky
                📱 Телефон: +7 (964) 026-72-30

                Удачи в деле! 🌟
                """

                photo_paths = [
                    os.path.join(os.path.dirname(os.path.abspath(__file__)), "images/works_site_1.jpg"),
                    os.path.join(os.path.dirname(os.path.abspath(__file__)), "images/works_site_2.jpg"),
                    os.path.join(os.path.dirname(os.path.abspath(__file__)), "images/works_site_3.jpg"),
                    os.path.join(os.path.dirname(os.path.abspath(__file__)), "images/works_site_4.jpg"),
                    os.path.join(os.path.dirname(os.path.abspath(__file__)), "images/works_site_5.jpg"),
                    os.path.join(os.path.dirname(os.path.abspath(__file__)), "images/works_shop_1.jpg"),
                    os.path.join(os.path.dirname(os.path.abspath(__file__)), "images/works_shop_4.jpg"),
                    os.path.join(os.path.dirname(os.path.abspath(__file__)), "images/works_shop_3.jpg"),
                    os.path.join(os.path.dirname(os.path.abspath(__file__)), "images/works_shop_5.jpg"),
                    os.path.join(os.path.dirname(os.path.abspath(__file__)), "images/works_shop_6.jpg"),
                ]

                stats = sender.send_messages(users_to_send, message_template, photo_paths, max_per_day=20)
                logger.info(f"Отправка на токене {token[:5]}...: {stats}")

            except Exception as e:
                logger.error(f"Ошибка при отправке сообщений с токена {token[:5]}: {e}")

        # Сохраняем индекс следующей ниши
        with open(current_niche_file, "w") as f:
            f.write(str(current_niche_index + 1))

    else:
        logger.info("Все ниши обработаны! Начните заново, удалив файл current_niche.txt")


if __name__ == "__main__":
    main()