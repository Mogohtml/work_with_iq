# sender.py
import os
import vk_api
import time
import random
import logging
import pandas as pd
import requests
from typing import List, Tuple, Dict
from dotenv import load_dotenv

# Настройка переменных среды
load_dotenv()
TOKEN = os.getenv("ACCESS_TOKEN_1", "YOUR_TOKEN")  # Пользовательский токен VK с правами messages

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('vk_sender.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class VKPersonalMessageSender:
    """Класс для отправки личных сообщений пользователям ВКонтакте через пользовательский токен (без ID сообществ)."""

    def __init__(self, token: str):
        """
        Инициализация отправителя.
        Args:
            token: Пользовательский токен VK с правами messages.
        """
        self.token = token
        self.session = vk_api.VkApi(token=token)
        self.vk = self.session.get_api()
        self.requests_count = 0
        self.last_request_time = 0
        logger.info("VKPersonalMessageSender инициализирован")

    def _smart_delay(self):
        """Умная задержка для избежания бана и капчи."""
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

    def upload_photo(self, peer_id: int, photo_path: str) -> str:
        """
        Загружает фотографию на сервер ВКонтакте и возвращает строку вложения.
        Args:
            peer_id: ID пользователя (для личных сообщений).
            photo_path: Путь к файлу фотографии.
        Returns:
            Строка вложения (например, 'photo123456_789') или пустая строка при ошибке.
        """
        if not os.path.exists(photo_path):
            logger.warning(f"Файл {photo_path} не найден, пропускаем загрузку")
            return ""

        try:
            upload_url = self.vk.photos.getMessagesUploadServer(peer_id=peer_id)['upload_url']
            response = requests.post(upload_url, files={'photo': open(photo_path, 'rb')}).json()

            # Проверяем ответ на ошибки
            if 'error' in response:
                logger.error(f"Ошибка API при загрузке {photo_path}: {response['error']}")
                return ""

            photo_data = self.vk.photos.saveMessagesPhoto(**response)
            if not photo_data:
                logger.error(f"Не удалось сохранить фото {photo_path}")
                return ""

            owner_id = photo_data[0]['owner_id']
            photo_id = photo_data[0]['id']
            return f"photo{owner_id}_{photo_id}"
        except requests.exceptions.RequestException as e:
            logger.error(f"Сетевая ошибка при загрузке {photo_path}: {e}")
            return ""
        except vk_api.exceptions.ApiError as e:
            logger.error(f"Ошибка VK API при загрузке {photo_path}: {e}")
            return ""
        except Exception as e:
            logger.error(f"Неожиданная ошибка при загрузке {photo_path}: {e}")
            return ""

    def send_messages_from_excel(
            self,
            excel_file_path: str,
            message_template: str,
            photo_paths: List[str],
            delay_range: Tuple[float, float] = (60, 120),
            max_per_day: int = 50,
            dry_run: bool = False
    ) -> Dict:
        """
        Отправляет персонализированные сообщения пользователям из Excel файла с фотографиями.
        Args:
            excel_file_path: Путь к Excel файлу с колонками 'Name', 'ID', 'URL'.
            message_template: Шаблон сообщения с переменными {first_name}.
            photo_paths: Список путей к фотографиям.
            delay_range: Диапазон задержки между сообщениями в секундах.
            max_per_day: Максимум сообщений в день.
            dry_run: Тестовый режим (не отправляет реально).
        Returns:
            Статистика рассылки.
        """
        logger.info(f"Начинаем рассылку из {excel_file_path}")

        # Загружаем данные из Excel
        try:
            df = pd.read_excel(excel_file_path)
        except Exception as e:
            logger.error(f"Ошибка чтения Excel файла: {e}")
            return {'error': str(e)}

        stats = {
            'total': len(df),
            'sent': 0,
            'failed': 0,
            'skipped': 0,
            'errors': []
        }
        sent_today = 0

        # Проверяем фотографии заранее
        valid_photo_paths = [p for p in photo_paths if os.path.exists(p)]
        if len(valid_photo_paths) != len(photo_paths):
            logger.warning(f"Некоторые фото не найдены: {set(photo_paths) - set(valid_photo_paths)}")

        for index, row in df.iterrows():
            # Лимит в день
            if sent_today >= max_per_day:
                logger.warning(f"Достигнут дневной лимит: {max_per_day}")
                stats['skipped'] = len(df) - stats['sent'] - stats['failed']
                break

            user_id = row.get('ID')
            name = row.get('Name', '')
            first_name = name.split()[0] if name else ''

            if not user_id or pd.isna(user_id):
                logger.warning(f"Пропускаем строку {index}: нет ID")
                stats['skipped'] += 1
                continue

            try:
                # Формируем сообщение
                message = message_template.format(first_name=first_name)

                # Загружаем фотографии (только если они существуют)
                attachments = []
                for photo_path in valid_photo_paths:
                    attachment = self.upload_photo(user_id, photo_path)
                    if attachment:
                        attachments.append(attachment)
                    # Не логируем каждую ошибку здесь, чтобы не засорять лог

                # Отправляем
                if not dry_run:
                    self._smart_delay()
                    if attachments:
                        attachments_str = ",".join(attachments)
                        self.vk.messages.send(
                            user_id=user_id,
                            message=message,
                            attachment=attachments_str,
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
                logger.info(f"✓ Отправлено {name} (ID: {user_id})")

                # Задержка
                delay = random.uniform(*delay_range)
                if not dry_run:
                    time.sleep(delay)

            except vk_api.exceptions.ApiError as e:
                error_msg = str(e)
                stats['failed'] += 1
                stats['errors'].append({
                    'user_id': user_id,
                    'error': error_msg
                })
                logger.error(f"✗ Ошибка отправки {name} (ID: {user_id}): {error_msg}")
                if 'flood control' in error_msg.lower():
                    logger.error("FLOOD CONTROL! Остановка на 1 час.")
                    time.sleep(3600)
                elif 'user is blocked' in error_msg.lower():
                    logger.error("Аккаунт заблокирован! Останавливаем рассылку.")
                    break
            except Exception as e:
                stats['failed'] += 1
                logger.error(f"Неожиданная ошибка для {name} (ID: {user_id}): {e}")

        # Итоговая статистика
        logger.info(f"""
        Всего пользователей: {stats['total']}
        Отправлено: {stats['sent']}
        Ошибок: {stats['failed']}
        Пропущено: {stats['skipped']}
        """)
        return stats


# Пример использования (можно добавить в конец файла или вызвать отдельно)
if __name__ == "__main__":
    sender = VKPersonalMessageSender(token=TOKEN)

    excel_file_path = 'user_ids.xlsx'
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

    # Обновленные пути к фото (с поддиректорией "images/")
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

    # Запуск рассылки
    stats = sender.send_messages_from_excel(
        excel_file_path=excel_file_path,
        message_template=message_template,
        photo_paths=photo_paths,
        delay_range=(20, 40),  # Можно ускорить до (20, 40) для 3x скорости, но с риском бана
        max_per_day=50,
        dry_run=False  # Установите True для теста
    )
    print("Статистика рассылки:", stats)