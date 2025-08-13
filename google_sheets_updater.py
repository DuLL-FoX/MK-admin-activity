import logging
import os
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from typing import List, Dict


class GoogleSheetUpdater:
    SERVER_COLUMN_MAPPING = {
        "титан": "E", "фобос": "F", "деймос": "G",
        "cоюз-1": "H", "фронтир": "I",
    }
    ADMIN_NAME_COLUMN = 'B'

    def __init__(self, credentials_file: str, spreadsheet_id: str, worksheet_name: str):
        if not os.path.exists(credentials_file):
            raise FileNotFoundError(f"Файл учетных данных Google не найден по пути: {credentials_file}")

        self.spreadsheet_id = spreadsheet_id
        self.worksheet_name = worksheet_name
        self.gc = None
        self.worksheet = None
        self._setup_connection(credentials_file)

    def _setup_connection(self, credentials_file: str):
        try:
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = Credentials.from_service_account_file(credentials_file, scopes=scope)
            self.gc = gspread.authorize(creds)
            spreadsheet = self.gc.open_by_key(self.spreadsheet_id)
            self.worksheet = spreadsheet.worksheet(self.worksheet_name)
            logging.info(
                f"Успешно подключено к Google-таблице: '{spreadsheet.title}', Лист: '{self.worksheet.title}'")
        except gspread.exceptions.WorksheetNotFound:
            logging.error(f"Лист '{self.worksheet_name}' не найден в таблице.")
            raise
        except Exception as e:
            logging.error(f"Ошибка при установке соединения с Google Sheets: {e}")
            raise

    def get_admin_row_map(self) -> Dict[str, int]:
        try:
            admin_col_index = ord(self.ADMIN_NAME_COLUMN.upper()) - ord('A')
            all_values = self.worksheet.get_all_values()
            admin_map = {
                row[admin_col_index].strip().lower(): i + 1
                for i, row in enumerate(all_values)
                if len(row) > admin_col_index and row[admin_col_index].strip()
            }
            logging.info(f"Найдено {len(admin_map)} администраторов в Google-таблице.")
            return admin_map
        except Exception as e:
            logging.error(f"Ошибка при чтении данных администраторов из таблицы: {e}")
            raise

    def update_ahelp_stats(self, df_global: pd.DataFrame, dry_run: bool = True):
        logging.info("Запуск процесса обновления Google-таблицы...")
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
        logging.info(f"Сопоставлены колонки DataFrame и таблицы: {df_cols_to_sheet_cols}")

        for _, row_data in df_global.iterrows():
            admin_name = row_data["Administrator"]
            admin_role = row_data["Role"]
            admin_name_lower = admin_name.strip().lower()

            sheet_row_num = admin_row_map.get(admin_name_lower)
            if sheet_row_num:
                for df_col, sheet_col in df_cols_to_sheet_cols.items():
                    if df_col in row_data:
                        cell_value = int(row_data[df_col])
                        if cell_value > 0:
                            updates.append({'range': f'{sheet_col}{sheet_row_num}', 'values': [[cell_value]]})
            else:
                if any(role in str(admin_role).lower() for role in key_roles):
                    missing_key_personnel.append(f"  - {admin_name} (Роль: {admin_role})")

        if not updates and not missing_key_personnel:
            logging.info("Нет статистики для обновления и отсутствующих ключевых сотрудников.")
            return

        if missing_key_personnel:
            logging.warning(
                "\n!!! ВНИМАНИЕ: Следующие ключевые сотрудники найдены в данных, но ОТСУТСТВУЮТ в Google-таблице:")
            for admin_info in missing_key_personnel:
                logging.warning(admin_info)
            logging.warning("!!! Пожалуйста, добавьте их в таблицу вручную.\n")
        else:
            logging.info("Отсутствующие ключевые сотрудники не найдены.")

        if dry_run:
            print("\n--- ТЕСТОВЫЙ РЕЖИМ (DRY RUN) ---")
            if updates:
                logging.info("[DRY RUN] Будет применено %d обновлений ячеек для существующих администраторов.",
                             len(updates))
                if updates:
                    logging.info(f"[DRY RUN] Пример обновления: {updates[0]}")
            else:
                logging.info("[DRY RUN] Нет обновлений для существующих администраторов.")
            print("--- КОНЕЦ ТЕСТОВОГО РЕЖИМА ---\n")
            return

        if updates:
            try:
                self.worksheet.batch_update(updates, value_input_option='USER_ENTERED')
                logging.info(f"Успешно применено {len(updates)} обновлений ячеек в Google-таблице.")
            except Exception as e:
                logging.error(f"Не удалось применить обновления к Google-таблице: {e}")
                raise
        else:
            logging.info("Не было применено ни одного обновления (все значения активности были нулевыми).")

        logging.info("Процесс обновления Google-таблицы завершен.")