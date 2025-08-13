import argparse
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Tuple, Dict, List, Optional

import dotenv
from tqdm import tqdm

from data_processing import load_json_file, AdminStats, ServerStats, analyze_ahelp_data, merge_duplicate_admins, \
    fill_missing_roles
from download import main as download_main
from excel_exporter import save_all_data_to_excel, create_global_admins_dataframe
from utils import extract_server_name, configure_logging

try:
    from google_sheets_updater import GoogleSheetUpdater

    GOOGLE_SHEETS_AVAILABLE = True
except ImportError:
    GOOGLE_SHEETS_AVAILABLE = False


def get_user_date(prompt: str) -> datetime:
    """Запрашивает у пользователя дату до тех пор, пока не будет введен корректный формат."""
    while True:
        date_str = input(prompt)
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            print("Неверный формат даты. Пожалуйста, используйте ГГГГ-ММ-ДД.")


def aggregate_global_stats(
        files: List[str],
        progress_bar: Optional[tqdm] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
) -> Tuple[Dict[str, AdminStats], int, Dict[str, ServerStats]]:
    global_admin_stats = defaultdict(lambda: {
        "ahelps": 0, "mentions": 0, "role": "Unknown", "sessions": 0,
        "admin_only_ahelps": 0, "admin_only_mentions": 0, "admin_only_sessions": 0
    })
    global_chat_count = 0
    servers_stats = {}

    for i, file_path in enumerate(files):
        if progress_bar:
            progress_bar.set_description(f"Обработка {os.path.basename(file_path)}")

        data = load_json_file(file_path)
        if not data:
            continue

        if start_date or end_date:
            filtered_data = []
            for message in data:
                created_at = message.get('created_at', '')
                if not created_at: continue
                try:
                    msg_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    if start_date and msg_dt < start_date: continue
                    if end_date and msg_dt > end_date: continue
                    filtered_data.append(message)
                except:
                    filtered_data.append(message)
            data = filtered_data

        server_name = extract_server_name(file_path)
        server_stats = analyze_ahelp_data(data, server_name)
        servers_stats[server_name] = server_stats

        for admin, stats in server_stats["admin_stats"].items():
            global_admin_stats[admin]["ahelps"] += stats["ahelps"]
            global_admin_stats[admin]["mentions"] += stats["mentions"]
            global_admin_stats[admin]["admin_only_ahelps"] += stats["admin_only_ahelps"]
            global_admin_stats[admin]["admin_only_mentions"] += stats["admin_only_mentions"]
            global_admin_stats[admin]["admin_only_sessions"] += stats["admin_only_sessions"]
            if global_admin_stats[admin]["role"] == "Unknown" and stats["role"] != "Unknown":
                global_admin_stats[admin]["role"] = stats["role"]
            global_admin_stats[admin]["sessions"] += stats["sessions"]

        global_chat_count += server_stats["chat_count"]

        if progress_bar:
            progress_bar.update(1)

    return dict(global_admin_stats), global_chat_count, servers_stats


def get_downloaded_files(data_folder: str) -> List[str]:
    if not os.path.exists(data_folder):
        return []
    return [
        os.path.join(data_folder, f) for f in os.listdir(data_folder)
        if os.path.isfile(os.path.join(data_folder, f)) and f.endswith('.json')
    ]


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Анализатор статистики Ahelp в Discord")
    parser.add_argument("--download", action="store_true", help="Загрузить сообщения перед анализом.")
    parser.add_argument("--data-folder", type=str, help="Папка с JSON файлами (по умолчанию из .env).")
    parser.add_argument("--output", type=str, help="Имя выходного Excel файла (по умолчанию из .env).")
    parser.add_argument("--verbose", action="store_true", help="Включить подробное логирование.")
    parser.add_argument("--start-date", type=str, help="Дата начала отчета (формат: ГГГГ-ММ-ДД).")
    parser.add_argument("--end-date", type=str, help="Дата окончания отчета (формат: ГГГГ-ММ-ДД).")
    parser.add_argument("--days", type=int, help="Количество дней для включения в отчет (отсчет от сегодня).")
    return parser.parse_args()


def main() -> int:
    args = parse_arguments()

    dotenv_path = ".env"
    if not os.path.exists(dotenv_path):
        print("Ошибка: .env файл не найден. Пожалуйста, создайте его на основе .env.example")
        return 1
    dotenv.load_dotenv(dotenv_path)

    log_level = logging.INFO if not args.verbose else logging.DEBUG
    configure_logging(level=log_level)

    start_date, end_date = None, None
    should_download = args.download

    if len(sys.argv) == 1:
        print("\n--- Интерактивный режим анализа Ahelps ---")
        should_download = True
        start_date = get_user_date("Введите дату начала (ГГГГ-ММ-ДД): ")
        end_date = get_user_date("Введите дату окончания (ГГГГ-ММ-ДД): ") + timedelta(days=1)
        print(
            f"\nОтчет будет сгенерирован за период с {start_date.strftime('%Y-%m-%d')} по {end_date.strftime('%Y-%m-%d')}.")
        print("-------------------------------------------\n")

    else:
        if args.days:
            start_date = datetime.now() - timedelta(days=args.days)
            logging.info(f"Установлен период отчета: последние {args.days} дней.")
        if args.start_date:
            try:
                start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
            except ValueError:
                logging.error("Неверный формат даты начала. Используйте ГГГГ-ММ-ДД.")
                return 1
        if args.end_date:
            try:
                end_date = datetime.strptime(args.end_date, "%Y-%m-%d") + timedelta(days=1)
            except ValueError:
                logging.error("Неверный формат даты окончания. Используйте ГГГГ-ММ-ДД.")
                return 1

    data_folder = args.data_folder or os.getenv("DATA_FOLDER", "data")
    excel_filename = args.output or os.getenv("EXCEL_FILENAME", "ahelp_stats.xlsx")

    if should_download:
        logging.info(">>> Этап 1: Загрузка сообщений...")
        try:
            download_main(start_date=start_date, end_date=end_date)
        except Exception as e:
            logging.error(f"Ошибка при загрузке сообщений: {e}")
            return 1

    files = get_downloaded_files(data_folder)
    if not files:
        logging.error(f"В папке {data_folder} не найдены JSON файлы. Попробуйте запустить с флагом --download.")
        return 1

    logging.info(f">>> Этап 2: Обработка и анализ {len(files)} JSON файлов...")
    with tqdm(total=len(files), desc="Анализ файлов") as pbar:
        global_admin_stats, global_chat_count, servers_stats = aggregate_global_stats(
            files, pbar, start_date, end_date
        )

    total_ahelps = sum(stats["ahelps"] for stats in global_admin_stats.values())
    logging.info(f"Анализ завершен: обработано {total_ahelps} ахелпов от {len(global_admin_stats)} администраторов.")

    merged_global = merge_duplicate_admins(global_admin_stats)
    fill_missing_roles(merged_global, servers_stats)
    df_global = create_global_admins_dataframe(merged_global, servers_stats)

    logging.info(f">>> Этап 3: Сохранение данных в {excel_filename}...")
    try:
        save_all_data_to_excel(global_admin_stats, global_chat_count, servers_stats, df_global)
        logging.info(f"Данные успешно сохранены в {excel_filename}")
    except Exception as e:
        logging.error(f"Ошибка при сохранении данных в Excel: {e}")
        return 1

    if GOOGLE_SHEETS_AVAILABLE:
        print("\n" + "="*50)
        print(">>> Этап 4: Интеграция с Google Sheets")
        print("="*50)
        update_gsheet = input("\nХотите обновить Google-таблицу с этой статистикой? (y/N): ").lower().strip()
        if update_gsheet in ['y', 'yes', 'д', 'да']:
            try:
                creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE")
                sheet_id = os.getenv("GOOGLE_SHEET_ID")
                worksheet_name = os.getenv("GOOGLE_SHEET_WORKSHEET_NAME")

                if not all([creds_file, sheet_id, worksheet_name]):
                    logging.error("Конфигурация для Google Sheets отсутствует в .env файле.")
                    return 1

                updater = GoogleSheetUpdater(creds_file, sheet_id, worksheet_name)

                dry_run_response = input("Сначала выполнить тестовый запуск (dry run), чтобы увидеть изменения? (Y/n): ").lower().strip()
                if dry_run_response not in ['n', 'no', 'н', 'нет']:
                    updater.update_ahelp_stats(df_global, dry_run=True)

                apply_response = input("Применить эти изменения к Google-таблице? (y/N): ").lower().strip()
                if apply_response in ['y', 'yes', 'д', 'да']:
                    updater.update_ahelp_stats(df_global, dry_run=False)
                else:
                    logging.info("Обновление Google-таблицы отменено.")

            except FileNotFoundError as e:
                logging.error(f"Не удалось инициализировать обновление Google Sheets: {e}")
            except Exception as e:
                logging.error(f"Произошла ошибка во время обновления Google Sheets: {e}")
    else:
        logging.warning("\nБиблиотека для Google Sheets не найдена. Пропуск этапа обновления Google-таблицы.")

    print("\n" + "="*50)
    print("      Работа успешно завершена!      ")
    print("="*50 + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())