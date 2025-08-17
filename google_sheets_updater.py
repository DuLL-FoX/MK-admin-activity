import os
import re
import time
from functools import wraps
from typing import List, Dict, Optional

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

try:
    from console_formatter import print_success, print_warning, print_error, print_info
except ImportError:
    def print_success(msg):
        print(f"✓ {msg}")


    def print_warning(msg):
        print(f"⚠ {msg}")


    def print_error(msg):
        print(f"✗ {msg}")


    def print_info(msg):
        print(f"ℹ {msg}")


def rate_limit(calls_per_minute=50):
    min_interval = 60.0 / calls_per_minute
    last_called = [0.0]

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            elapsed = time.time() - last_called[0]
            left_to_wait = min_interval - elapsed
            if left_to_wait > 0:
                time.sleep(left_to_wait)
            ret = func(*args, **kwargs)
            last_called[0] = time.time()
            return ret

        return wrapper

    return decorator


def retry_on_quota_error(max_retries=3, base_delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if "429" in str(e) or "Quota exceeded" in str(e):
                        if attempt < max_retries:
                            delay = base_delay * (2 ** attempt)
                            print_warning(f"Превышена квота API. Повторная попытка через {delay} секунд...")
                            time.sleep(delay)
                            continue
                    raise e
            return None

        return wrapper

    return decorator


class GoogleSheetUpdater:
    SERVER_COLUMN_MAPPING = {
        "титан": "E", "фобос": "F", "деймос": "G",
        "cоюз-1": "H", "фронтир": "I",
    }
    ADMIN_NAME_COLUMN = 'B'
    REACTIONS_COLUMN = 'O'

    def __init__(self, credentials_file: str, spreadsheet_id: str, worksheet_name: str):
        if not os.path.exists(credentials_file):
            raise FileNotFoundError(f"Файл учетных данных Google не найден по пути: {credentials_file}")

        self.spreadsheet_id = spreadsheet_id
        self.worksheet_name = worksheet_name
        self.gc = None
        self.worksheet = None

        self._admin_cache = None
        self._last_cache_update = 0
        self._cache_timeout = 300  

        self._setup_connection(credentials_file)

    def _setup_connection(self, credentials_file: str):
        try:
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = Credentials.from_service_account_file(credentials_file, scopes=scope)
            self.gc = gspread.authorize(creds)
            spreadsheet = self.gc.open_by_key(self.spreadsheet_id)
            self.worksheet = spreadsheet.worksheet(self.worksheet_name)
            print_success(f"Успешно подключено к Google-таблице: '{spreadsheet.title}', Лист: '{self.worksheet.title}'")
        except gspread.exceptions.WorksheetNotFound:
            print_error(f"Лист '{self.worksheet_name}' не найден в таблице.")
            raise
        except Exception as e:
            print_error(f"Ошибка при установке соединения с Google Sheets: {e}")
            raise

    def normalize_name_for_comparison(self, name: str) -> str:
        if not isinstance(name, str):
            return str(name).lower()

        normalized = name.strip()

        if '|' in normalized:
            normalized = normalized.split('|')[0].strip()

        normalized = re.sub(r'\([^)]*\)', '', normalized).strip()

        if '/' in normalized:
            normalized = normalized.split('/')[0].strip()

        normalized = normalized.replace('_', ' ')

        normalized = re.sub(r'\s+', ' ', normalized).strip().lower()

        return normalized

    def find_best_name_match(self, target_name: str, table_names: List[str]) -> Optional[str]:
        if not isinstance(target_name, str):
            target_name = str(target_name)

        target_normalized = self.normalize_name_for_comparison(target_name)

        for table_name in table_names:
            if self.normalize_name_for_comparison(table_name) == target_normalized:
                return table_name

        for table_name in table_names:
            table_normalized = self.normalize_name_for_comparison(table_name)
            if table_normalized.startswith(target_normalized) or target_normalized.startswith(table_normalized):
                return table_name

        for table_name in table_names:
            table_normalized = self.normalize_name_for_comparison(table_name)
            if target_normalized in table_normalized or table_normalized in target_normalized:
                return table_name

        target_words = set(target_normalized.split())
        for table_name in table_names:
            table_words = set(self.normalize_name_for_comparison(table_name).split())
            if target_words & table_words:
                return table_name

        return None

    @retry_on_quota_error(max_retries=3, base_delay=2)
    @rate_limit(calls_per_minute=40)
    def get_admin_row_map(self, force_refresh: bool = False) -> Dict[str, int]:
        current_time = time.time()

        if (not force_refresh and
                self._admin_cache is not None and
                (current_time - self._last_cache_update) < self._cache_timeout):
            print_info("Используются кэшированные данные администраторов")
            return self._admin_cache

        try:
            print_info("Загрузка данных администраторов из Google Sheets...")
            admin_col_index = ord(self.ADMIN_NAME_COLUMN.upper()) - ord('A')

            all_values = self.worksheet.get_all_values()

            admin_map = {}
            table_names = []

            for i, row in enumerate(all_values):
                if len(row) > admin_col_index and row[admin_col_index].strip():
                    name = row[admin_col_index].strip()
                    table_names.append(name)
                    admin_map[name.lower()] = i + 1

            self.table_names = table_names
            self._admin_cache = admin_map
            self._last_cache_update = current_time

            print_success(f"Найдено {len(admin_map)} администраторов в Google-таблице.")
            return admin_map

        except Exception as e:
            print_error(f"Ошибка при чтении данных администраторов из таблицы: {e}")
            raise

    def find_admin_row(self, admin_name: str) -> Optional[int]:
        if not hasattr(self, 'table_names') or self.table_names is None:
            self.get_admin_row_map(force_refresh=True)

        best_match = self.find_best_name_match(admin_name, self.table_names)
        if best_match:

            admin_col_index = ord(self.ADMIN_NAME_COLUMN.upper()) - ord('A')

            if hasattr(self, '_admin_cache') and self._admin_cache:
                for cached_name, row_num in self._admin_cache.items():
                    if best_match.lower() == cached_name:
                        return row_num

            try:
                all_values = self.worksheet.get_all_values()
                for i, row in enumerate(all_values):
                    if len(row) > admin_col_index and row[admin_col_index].strip() == best_match:
                        return i + 1
            except Exception as e:
                print_warning(f"Не удалось найти строку для {admin_name}: {e}")

        return None

    @retry_on_quota_error(max_retries=3, base_delay=2)
    def update_ahelp_stats(self, df_global: pd.DataFrame, dry_run: bool = True):
        print_info("Запуск процесса обновления статистики Ahelp в Google-таблице...")

        
        admin_row_map = self.get_admin_row_map()

        updates = []
        missing_key_personnel = []
        key_roles = ["модератор", "гейм-мастер", "судья"]

        df_cols_to_sheet_cols = {}
        for server_key, sheet_col in self.SERVER_COLUMN_MAPPING.items():
            ahelps_col = f"{server_key} Ahelps".lower()
            for df_col in df_global.columns:
                if df_col.lower() == ahelps_col:
                    df_cols_to_sheet_cols[df_col] = sheet_col
                    break

        print_info(f"Сопоставлены колонки DataFrame и таблицы: {df_cols_to_sheet_cols}")

        
        for _, row_data in df_global.iterrows():
            admin_name = str(row_data["Administrator"]).strip()
            admin_role = str(row_data["Role"])

            sheet_row_num = self.find_admin_row(admin_name)
            if sheet_row_num:
                for df_col, sheet_col in df_cols_to_sheet_cols.items():
                    if df_col in row_data:
                        cell_value = int(row_data[df_col])
                        if cell_value > 0:
                            updates.append({'range': f'{sheet_col}{sheet_row_num}', 'values': [[cell_value]]})
            else:
                if any(role in str(admin_role).lower() for role in key_roles):
                    missing_key_personnel.append(f"  - {admin_name} (Роль: {admin_role})")

        if missing_key_personnel:
            print_warning(
                "\n!!! ВНИМАНИЕ: Следующие ключевые сотрудники найдены в данных, но ОТСУТСТВУЮТ в Google-таблице:")
            for admin_info in missing_key_personnel:
                print_warning(admin_info)
            print_warning("!!! Пожалуйста, добавьте их в таблицу вручную.\n")
        else:
            print_info("Отсутствующие ключевые сотрудники не найдены.")

        if not updates:
            print_info("Нет статистики Ahelp для обновления и отсутствующих ключевых сотрудников.")
            return

        if dry_run:
            print("\n--- ТЕСТОВЫЙ РЕЖИМ AHELP (DRY RUN) ---")
            print_info(f"[DRY RUN] Будет применено {len(updates)} обновлений ячеек для существующих администраторов.")
            if updates:
                print_info(f"[DRY RUN] Пример обновления Ahelp: {updates[0]}")
            print("--- КОНЕЦ ТЕСТОВОГО РЕЖИМА AHELP ---\n")
            return

        if updates:
            try:

                batch_size = 100
                total_batches = (len(updates) + batch_size - 1) // batch_size

                for i in range(0, len(updates), batch_size):
                    batch = updates[i:i + batch_size]
                    batch_num = (i // batch_size) + 1

                    print_info(f"Применение батча {batch_num}/{total_batches} ({len(batch)} обновлений)...")

                    if i > 0:
                        time.sleep(1)

                    self.worksheet.batch_update(batch, value_input_option='USER_ENTERED')

                print_success(f"Успешно применено {len(updates)} обновлений ячеек Ahelp в Google-таблице.")

            except Exception as e:
                print_error(f"Не удалось применить обновления Ahelp к Google-таблице: {e}")
                raise
        else:
            print_info("Не было применено ни одного обновления Ahelp (все значения активности были нулевыми).")

        print_info("Процесс обновления статистики Ahelp в Google-таблице завершен.")

    @retry_on_quota_error(max_retries=3, base_delay=2)
    def update_reaction_stats(self, reaction_data: Dict[str, int], dry_run: bool = True):
        print_info("Запуск процесса обновления статистики реакций в Google-таблице...")

        if not reaction_data:
            print_info("Нет данных реакций для обновления.")
            return

        if not hasattr(self, 'table_names') or self.table_names is None:
            self.get_admin_row_map(force_refresh=True)

        updates = []
        missing_admins = []
        matched_admins = []

        for admin_name, reaction_count in reaction_data.items():

            admin_name_str = str(admin_name).strip()

            if not admin_name_str or reaction_count <= 0:
                continue

            sheet_row_num = self.find_admin_row(admin_name_str)

            if sheet_row_num:
                updates.append({
                    'range': f'{self.REACTIONS_COLUMN}{sheet_row_num}',
                    'values': [[reaction_count]]
                })
                matched_admins.append(f"  - {admin_name_str} → строка {sheet_row_num} ({reaction_count} реакций)")
            else:
                missing_admins.append(f"  - {admin_name_str} ({reaction_count} реакций)")

        if matched_admins:
            print_info("Успешно сопоставлены пользователи:")
            for match in matched_admins[:5]:
                print_info(match)
            if len(matched_admins) > 5:
                print_info(f"  ... и еще {len(matched_admins) - 5} пользователей")

        if missing_admins:
            print_warning("Следующие пользователи с реакциями не найдены в Google-таблице:")
            for admin_name in missing_admins:
                print_warning(admin_name)

        if not updates:
            print_info("Нет обновлений реакций для применения.")
            return

        if dry_run:
            print("\n--- ТЕСТОВЫЙ РЕЖИМ РЕАКЦИЙ (DRY RUN) ---")
            print_info(f"[DRY RUN] Будет применено {len(updates)} обновлений реакций.")
            for update in updates[:3]:
                print_info(f"[DRY RUN] Пример обновления реакций: {update}")
            if len(updates) > 3:
                print_info(f"[DRY RUN] ... и еще {len(updates) - 3} обновлений")
            print("--- КОНЕЦ ТЕСТОВОГО РЕЖИМА РЕАКЦИЙ ---\n")
            return

        try:

            batch_size = 50
            total_batches = (len(updates) + batch_size - 1) // batch_size

            for i in range(0, len(updates), batch_size):
                batch = updates[i:i + batch_size]
                batch_num = (i // batch_size) + 1

                if total_batches > 1:
                    print_info(f"Применение батча реакций {batch_num}/{total_batches} ({len(batch)} обновлений)...")

                if i > 0:
                    time.sleep(1)

                self.worksheet.batch_update(batch, value_input_option='USER_ENTERED')

            print_success(f"Успешно применено {len(updates)} обновлений реакций в Google-таблице.")

        except Exception as e:
            print_error(f"Не удалось применить обновления реакций к Google-таблице: {e}")
            raise

        print_info("Процесс обновления статистики реакций в Google-таблице завершен.")

    def update_all_stats(self, df_global: Optional[pd.DataFrame] = None,
                         reaction_data: Optional[Dict[str, int]] = None,
                         dry_run: bool = True):
        if df_global is not None:
            self.update_ahelp_stats(df_global, dry_run)

        if reaction_data is not None:
            self.update_reaction_stats(reaction_data, dry_run)

        if df_global is None and reaction_data is None:
            print_warning("Не предоставлены данные для обновления.")
