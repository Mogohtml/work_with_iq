import sqlite3
import logging
from typing import List, Dict, Optional
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class VKUserDatabase:
    def __init__(self, db_path="../users.db"):
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
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS groups (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        group_id INTEGER,
                        niche TEXT,
                        parsed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(group_id, niche)
                    )
                """)
                conn.commit()
                logger.info("База данных инициализирована.")
        except Exception as e:
            logger.error(f"Ошибка инициализации базы: {e}")
            raise

    def backup_db(self):
        """Создание резервной копии базы данных."""
        try:
            backup_dir = "backups"
            os.makedirs(backup_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = os.path.join(backup_dir, f"users_backup_{timestamp}.db")
            with sqlite3.connect(self.db_path) as src, sqlite3.connect(backup_path) as dest:
                src.backup(dest)
            logger.info(f"Резервная копия базы создана: {backup_path}")
        except Exception as e:
            logger.error(f"Ошибка создания резервной копии базы: {e}")

    def add_users(self, users: List[Dict]):
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

    def add_group(self, group_id: str, niche: str):
        """Добавление информации о спарсенной группе."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR IGNORE INTO groups (group_id, niche)
                    VALUES (?, ?)
                """, (group_id, niche))
                conn.commit()
                logger.info(f"Добавлена информация о группе {group_id} для ниши {niche}.")
        except Exception as e:
            logger.error(f"Ошибка добавления информации о группе: {e}")
            raise

    def get_parsed_groups(self, niche: str) -> List[str]:
        """Получение списка уже спарсенных групп из базы данных."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT group_id FROM groups WHERE niche = ?", (niche,))
                rows = cursor.fetchall()
                return [str(row[0]) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения списка спарсенных групп: {e}")
            return []

    def is_group_parsed(self, group_id: str, niche: str) -> bool:
        """Проверка, была ли группа уже спарсена."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM groups WHERE group_id = ? AND niche = ?", (group_id, niche))
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Ошибка проверки группы {group_id}: {e}")
            return False

    def get_unsent_users(self) -> List[Dict]:
        """Получение пользователей, которым ещё не отправлено сообщение."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id, first_name, last_name FROM users WHERE sent = 0")
                rows = cursor.fetchall()
                users = []
                for row in rows:
                    users.append({
                        "ID": row[0],
                        "first_name": row[1],
                        "last_name": row[2]
                    })
                return users
        except Exception as e:
            logger.error(f"Ошибка получения пользователей: {e}")
            raise

    def update_sent_status(self, user_id: int, sent: bool):
        """Обновление статуса отправки сообщения пользователю."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE users SET sent = ? WHERE id = ?", (sent, user_id))
                conn.commit()
                logger.info(f"Статус отправки для пользователя {user_id} обновлён.")
        except Exception as e:
            logger.error(f"Ошибка обновления статуса для пользователя {user_id}: {e}")
            raise
