import sqlite3
import logging
from typing import List, Dict, Optional
import os
from datetime import datetime
from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)

class DatabaseEncryption:
    def __init__(self, password: str):
        self.key = Fernet.generate_key_from_password(password.encode())

    def encrypt_file(self, file_path: str, encrypted_file_path: str):
        fernet = Fernet(self.key)
        with open(file_path, 'rb') as file:
            original = file.read()
        encrypted = fernet.encrypt(original)
        with open(encrypted_file_path, 'wb') as encrypted_file:
            encrypted_file.write(encrypted)

    def decrypt_file(self, encrypted_file_path: str, decrypted_file_path: str):
        fernet = Fernet(self.key)
        with open(encrypted_file_path, 'rb') as enc_file:
            encrypted = enc_file.read()
        decrypted = fernet.decrypt(encrypted)
        with open(decrypted_file_path, 'wb') as dec_file:
            dec_file.write(decrypted)

class VKUserDatabase:
    def __init__(self, db_path="../users.db", password=None):
        self.db_path = db_path
        self.encrypted_db_path = f"{db_path}.enc"
        self.password = password
        self.encryption = DatabaseEncryption(password) if password else None
        self._init_db()

    def _decrypt_db(self):
        if self.encryption and os.path.exists(self.encrypted_db_path):
            self.encryption.decrypt_file(self.encrypted_db_path, self.db_path)

    def _encrypt_db(self):
        if self.encryption:
            self.encryption.encrypt_file(self.db_path, self.encrypted_db_path)
            os.remove(self.db_path)

    def _init_db(self):
        """Инициализация базы данных и таблиц."""
        try:
            self._decrypt_db()
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
            self._encrypt_db()
        except Exception as e:
            logger.error(f"Ошибка инициализации базы: {e}")
            raise

    def _connect(self):
        """Создание подключения к базе данных."""
        self._decrypt_db()
        conn = sqlite3.connect(self.db_path)
        return conn

    def _close(self, conn):
        """Закрытие подключения к базе данных."""
        conn.close()
        self._encrypt_db()

    def backup_db(self):
        """Создание резервной копии базы данных."""
        try:
            self._decrypt_db()
            backup_dir = "backups"
            os.makedirs(backup_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = os.path.join(backup_dir, f"users_backup_{timestamp}.db")
            with sqlite3.connect(self.db_path) as src, sqlite3.connect(backup_path) as dest:
                src.backup(dest)
            logger.info(f"Резервная копия базы создана: {backup_path}")
            self._encrypt_db()
        except Exception as e:
            logger.error(f"Ошибка создания резервной копии базы: {e}")

    def add_users(self, users: List[Dict]):
        """Добавление пользователей в базу данных (игнорируются дубликаты)."""
        try:
            self._decrypt_db()
            with self._connect() as conn:
                cursor = conn.cursor()
                for user in users:
                    cursor.execute("""
                        INSERT OR IGNORE INTO users (id, first_name, last_name, url)
                        VALUES (?, ?, ?, ?)
                    """, (user["id"], user.get("first_name", ""), user.get("last_name", ""), f"https://vk.com/id{user['id']}"))
                conn.commit()
                logger.info(f"Добавлено {len(users)} пользователей в базу.")
            self._encrypt_db()
        except Exception as e:
            logger.error(f"Ошибка добавления пользователей: {e}")
            raise

    def add_group(self, group_id: str, niche: str):
        """Добавление информации о спарсенной группе."""
        try:
            self._decrypt_db()
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR IGNORE INTO groups (group_id, niche)
                    VALUES (?, ?)
                """, (group_id, niche))
                conn.commit()
                logger.info(f"Добавлена информация о группе {group_id} для ниши {niche}.")
            self._encrypt_db()
        except Exception as e:
            logger.error(f"Ошибка добавления информации о группе: {e}")
            raise

    def get_parsed_groups(self, niche: str) -> List[str]:
        """Получение списка уже спарсенных групп из базы данных."""
        try:
            self._decrypt_db()
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT group_id FROM groups WHERE niche = ?", (niche,))
                rows = cursor.fetchall()
                result = [str(row[0]) for row in rows]
            self._encrypt_db()
            return result
        except Exception as e:
            logger.error(f"Ошибка получения списка спарсенных групп: {e}")
            return []

    def is_group_parsed(self, group_id: str, niche: str) -> bool:
        """Проверка, была ли группа уже спарсена."""
        try:
            self._decrypt_db()
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM groups WHERE group_id = ? AND niche = ?", (group_id, niche))
                result = cursor.fetchone() is not None
            self._encrypt_db()
            return result
        except Exception as e:
            logger.error(f"Ошибка проверки группы {group_id}: {e}")
            return False

    def get_unsent_users(self, limit: int = 20) -> List[Dict]:
        """Получение пользователей, которым ещё не отправлено сообщение."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id, first_name, last_name FROM users WHERE sent = 0 LIMIT ?", (limit,))
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
        try:
            self._decrypt_db()
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE users SET sent = ?, sent_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (sent, user_id)
                )
                conn.commit()
                logger.info(f"Статус отправки для пользователя {user_id} обновлён.")
            self._encrypt_db()
        except Exception as e:
            logger.error(f"Ошибка обновления статуса для пользователя {user_id}: {e}")
            raise

    def get_users_for_reminder(self) -> List[Dict]:
        try:
            self._decrypt_db()
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                               SELECT id, first_name, last_name
                               FROM users
                               WHERE sent = 1
                                 AND sent_at <= DATETIME('now', '-3 days')
                                 AND reminder_sent = 0
                               """)
                rows = cursor.fetchall()
                users = []
                for row in rows:
                    users.append({
                        "ID": row[0],
                        "first_name": row[1],
                        "last_name": row[2]
                    })
            self._encrypt_db()
            return users
        except Exception as e:
            logger.error(f"Ошибка получения пользователей для напоминания: {e}")
            raise

    def update_reminder_status(self, user_id: int, reminder_sent: bool):
        try:
            self._decrypt_db()
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE users SET reminder_sent = ? WHERE id = ?",
                    (reminder_sent, user_id)
                )
                conn.commit()
                logger.info(f"Статус повторного уведомления для пользователя {user_id} обновлён.")
            self._encrypt_db()
        except Exception as e:
            logger.error(f"Ошибка обновления статуса повторного уведомления для пользователя {user_id}: {e}")
            raise

