import sqlite3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clear_groups_table(db_path="../users.db"):
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM groups")
            conn.commit()
            logger.info("Все записи из таблицы groups успешно удалены.")
    except sqlite3.OperationalError as e:
        logger.error(f"Ошибка при удалении записей из таблицы groups: {e}")
        logger.info("Попытка создать таблицу groups...")
        cursor = conn.cursor()
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
        logger.info("Таблица groups создана.")
    except Exception as e:
        logger.error(f"Ошибка при удалении записей из таблицы groups: {e}")

if __name__ == "__main__":
    clear_groups_table()
