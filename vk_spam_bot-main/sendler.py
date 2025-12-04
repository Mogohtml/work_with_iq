import os
import keyboard
import threading
import vk_api
import time
import random
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pandas as pd
import requests

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
            delay = random.uniform(2.0, 4.0)
        else:
            delay = random.uniform(0.5, 1.5)
        if self.requests_count % 20 == 0:
            logger.info("Делаем паузу 30 секунд для избежания ограничений")
            time.sleep(30)
        else:
            time.sleep(delay)
        self.last_request_time = time.time()

    def _listen_for_skip(self):
        keyboard.add_hotkey('ctrl+n', lambda: setattr(self, 'skip_group', True))
        keyboard.wait()

    def parse_group_members(self, group_id: str, max_users: int = 500, filters: Dict = None) -> List[Dict]:
        logger.info(f"Начинаем парсинг группы: {group_id}")
        listener_thread = threading.Thread(target=self._listen_for_skip, daemon=True)
        listener_thread.start()

        group_info = self._get_group_info(group_id)
        logger.info(f"Группа: {group_info['name']}, участников: {group_info['members_count']}")

        users = []
        offset = 0
        count = 1000

        if filters is None:
            filters = {}

        while len(users) < max_users:
            if self.skip_group:
                logger.info(f"Пропускаем группу {group_id} по Ctrl + N")
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

    def parse_leads_by_niche(self, niche: str, max_users: int = 500, filters: Dict = None, group_count: int = 1000) -> List[Dict]:
        group_ids = self.find_groups_by_niche(niche, group_count)
        if not group_ids:
            logger.warning(f"Не найдено групп по нише: {niche}")
            return []

        all_leads = []
        for group_id in group_ids:
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
            else:
                logger.info(f"Группа {group_id} неактивна, пропускаем")

        if all_leads:
            unique_leads = self._remove_duplicates(all_leads)
            self.save_users(unique_leads, filename="user_ids")

        logger.info(f"Собрано {len(all_leads)} лидов по нише: {niche}")
        return all_leads

    def save_users(self, users: List[Dict], filename: str = 'user_ids'):
        user_data = []
        for user in users:
            first_name = user.get('first_name', '')
            last_name = user.get('last_name', '')
            user_id = user.get('id', '')
            user_url = f"https://vk.com/id{user_id}"
            user_data.append(f"{first_name} {last_name}\t{user_id}\t{user_url}")

        df = pd.DataFrame(user_data, columns=['UserInfo'])
        df[['Name', 'ID', 'URL']] = df['UserInfo'].str.split('\t', expand=True, n=2)
        df = df.drop(columns=['UserInfo'])

        script_dir = os.path.dirname(os.path.abspath(__file__))
        save_path = os.path.join(script_dir, 'vk_spam_bot-main')
        os.makedirs(save_path, exist_ok=True)

        if filename == 'user_ids':
            excel_filename = os.path.join(save_path, "user_ids.xlsx")
            if os.path.exists(excel_filename):
                existing_df = pd.read_excel(excel_filename)
                existing_ids = set(existing_df['ID'].dropna().astype(int).tolist())
                df_filtered = df[~df['ID'].isin(existing_ids)]
                if not df_filtered.empty:
                    combined_df = pd.concat([existing_df, df_filtered], ignore_index=True)
                    combined_df.to_excel(excel_filename, index=False)
            else:
                df.to_excel(excel_filename, index=False)
        else:
            cash_path = os.path.join(save_path, 'cash')
            os.makedirs(cash_path, exist_ok=True)
            cash_filename = os.path.join(cash_path, f"{filename}.xlsx")
            df.to_excel(cash_filename, index=False)

        logger.info(f"Лиды сохранены в {filename}.xlsx")

    def _remove_duplicates(self, users: List[Dict]) -> List[Dict]:
        seen_ids = set()
        unique_users = []
        for user in users:
            user_id = user.get('id')
            if user_id and user_id not in seen_ids:
                seen_ids.add(user_id)
                unique_users.append(user)
        return unique_users

    def upload_photo(self, peer_id: int, photo_path: str) -> str:
        if not os.path.exists(photo_path):
            return ""
        try:
            upload_url = self.vk.photos.getMessagesUploadServer(peer_id=peer_id)['upload_url']
            response = requests.post(upload_url, files={'photo': open(photo_path, 'rb')}).json()
            if 'error' in response:
                return ""
            photo_data = self.vk.photos.saveMessagesPhoto(**response)
            if not photo_data:
                return ""
            owner_id = photo_data[0]['owner_id']
            photo_id = photo_data[0]['id']
            return f"photo{owner_id}_{photo_id}"
        except Exception as e:
            logger.error(f"Ошибка загрузки фото: {e}")
            return ""

    def send_messages(self, users: List[Dict], message_template: str, photo_paths: List[str], max_per_day: int = 60) -> Dict:
        logger.info(f"Начинаем рассылку для {len(users)} пользователей")
        stats = {'total': len(users), 'sent': 0, 'failed': 0, 'skipped': 0, 'errors': []}
        sent_today = 0

        for user in users:
            if sent_today >= max_per_day:
                logger.warning(f"Достигнут дневной лимит: {max_per_day}")
                stats['skipped'] = len(users) - stats['sent'] - stats['failed']
                break

            user_id = user['id']
            if not user.get('can_write_private_message'):
                logger.debug(f"Пропускаем {user_id} - закрытые ЛС")
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
                attachments = [self.upload_photo(user_id, p) for p in photo_paths if p]
                attachments = [a for a in attachments if a]

                if attachments:
                    self.vk.messages.send(
                        user_id=user_id,
                        message=message,
                        attachment=",".join(attachments),
                        random_id=random.randint(1, 2 ** 31)
                    )
                else:
                    self.vk.messages.send(
                        user_id=user_id,
                        message=message,
                        random_id=random.randint(1, 2 ** 31)
                    )

                stats['sent'] += 1
                sent_today += 1
                logger.info(f"✓ Отправлено {user_id}: {user.get('first_name')} {user.get('last_name')}")
                delay = random.uniform(80, 140)
                logger.debug(f"Задержка {delay:.1f} сек...")
                time.sleep(delay)
            except vk_api.exceptions.ApiError as e:
                error_msg = str(e)
                stats['failed'] += 1
                stats['errors'].append({'user_id': user_id, 'error': error_msg})
                logger.error(f"✗ Ошибка отправки {user_id}: {error_msg}")
                if 'flood control' in error_msg.lower():
                    logger.error("FLOOD CONTROL! Слишком много запросов. Остановка на 1 час.")
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
    FILTERS = {
        'city_ids': [1, 2],
        'age_from': 18,
        'age_to': 35,
        'sex': 0,
        'only_can_message': True,
        'only_active': True,
    }

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

    TOKENS = [os.environ.get(f"ACCESS_TOKEN_{i}") for i in range(1, 4) if os.environ.get(f"ACCESS_TOKEN_{i}")]

    if not TOKENS:
        logger.error("Нет доступных токенов!")
        return

    parser = VKGroupParser(token=TOKENS[0])

    for niche in NICHES:
        logger.info(f"Парсинг по нише: {niche}")
        leads = parser.parse_leads_by_niche(niche=niche, max_users=500, filters=FILTERS)
        if leads:
            logger.info(f"Собрано {len(leads)} лидов по нише: {niche}")
        else:
            logger.warning(f"Не удалось собрать лидов по нише: {niche}")

    for token in TOKENS:
        sender = VKGroupParser(token=token)
        try:
            df = pd.read_excel("vk_spam_bot-main/user_ids.xlsx")
            users = df.to_dict('records')
            message_template = """
Привет, {first_name}! 👋
🎨 **Я специализируюсь на создании визуальных решений**, которые помогают вашему бренду выделяться и запоминаться. Моя задача — превратить ваши идеи в стильный, функциональный и эффективный дизайн, чтобы ваш бизнес сиял! ✨
Что я предлагаю:
- 🏗️ **Дизайн выставочных стендов** — яркие и запоминающиеся конструкции для презентаций.
- 🎯 **Разработка фирменного стиля** — логотип, цветовая палитра, шрифты, брендбук для полного брендинга.
- 📄 **Полиграфическая продукция** — буклеты, плакаты, визитки, упаковка с индивидуальным подходом.
- 💻 **Цифровой дизайн** — креативные решения для веб и мобильных приложений.
- 🔗 **QR-коды и интерактивные элементы** — современные инструменты для взаимодействия с аудиторией.
💬 {first_name}, давайте обсудим ваш проект и создадим что-то уникальное! 🚀
📌 **Портфолио и отзывы:** [profi.ru/profile/DzhabagiyevMM](https://profi.ru/profile/DzhabagiyevMM)
"""
            photo_paths = [
                "images/works_design_5.jpg",
                "images/works_design_8.jpg",
                "images/works_shop_1.jpg",
                "images/works_shop_3.jpg",
                "images/works_shop_4.jpg",
                "images/works_site_1.jpg",
                "images/works_site_2.jpg",
                "images/works_site_5.jpg",
            ]
            stats = sender.send_messages(users, message_template, photo_paths, max_per_day=60)
            logger.info(f"Отправка на токене: {stats}")
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщений: {e}")

if __name__ == "__main__":
    main()
