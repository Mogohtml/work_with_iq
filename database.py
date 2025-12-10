# database.py
import sqlite3
import os
import shutil
from datetime import datetime
import logging

# Настройка логирования для базы
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("database")

class VKUserDatabase:
    def __init__(self, db_path="users.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Инициализация базы данных и таблиц."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id INTEGER PRIMARY KEY,
                        first_name TEXT,
                        last_name TEXT,
                        url TEXT,
                        sent BOOLEAN DEFAULT 0,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.commit()
                logger.info("База данных инициализирована.")
        except Exception as e:
            logger.error(f"Ошибка инициализации базы: {e}")
            raise

    def backup_db(self, backup_dir="backups"):
        """Создание резервной копии базы данных."""
        try:
            os.makedirs(backup_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = os.path.join(backup_dir, f"users_backup_{timestamp}.db")
            shutil.copy2(self.db_path, backup_path)
            logger.info(f"Резервная копия базы создана: {backup_path}")
            return backup_path
        except Exception as e:
            logger.error(f"Ошибка создания резервной копии: {e}")
            raise

    def add_users(self, users):
        """Добавление пользователей в базу данных (игнорируются дубликаты)."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                for user in users:
                    cursor.execute("""
                        INSERT OR IGNORE INTO users (id, first_name, last_name, url)
                        VALUES (?, ?, ?, ?)
                    """, (user["id"], user.get("first_name", ""), user.get("last_name", ""), f"https://vk.com/id{user['id']}"))
                conn.commit()
                logger.info(f"Добавлено {len(users)} пользователей в базу.")
        except Exception as e:
            logger.error(f"Ошибка добавления пользователей: {e}")
            raise

    def update_sent_status(self, user_id, sent=True):
        """Обновление статуса отправки сообщения."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE users SET sent = ? WHERE id = ?
                """, (sent, user_id))
                conn.commit()
                logger.info(f"Обновлён статус отправки для пользователя {user_id}.")
        except Exception as e:
            logger.error(f"Ошибка обновления статуса: {e}")
            raise

    def get_unsent_users(self):
        """Получение пользователей, которым ещё не отправлено сообщение."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id, first_name, last_name FROM users WHERE sent = 0")
                rows = cursor.fetchall()
                users = []
                for row in rows:
                    users.append({
                        "id": row[0],
                        "first_name": row[1],
                        "last_name": row[2]
                    })
                return users
        except Exception as e:
            logger.error(f"Ошибка получения пользователей: {e}")
            raise

