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
    from console_formatter import (
        console, print_header, print_section, print_success, print_warning,
        print_error, print_info, print_menu, print_stats_box, ask_user_choice,
        ask_user_input
    )

    CONSOLE_FORMATTER_AVAILABLE = True
except ImportError:
    CONSOLE_FORMATTER_AVAILABLE = False


    def print_header(title, subtitle=""):
        print(f"\n{title}\n" + "=" * 50)


    def print_section(title):
        print(f"\n{title}\n" + "-" * 30)


    def print_success(msg):
        print(f"✓ {msg}")


    def print_warning(msg):
        print(f"⚠ {msg}")


    def print_error(msg):
        print(f"✗ {msg}")


    def print_info(msg):
        print(f"ℹ {msg}")


    def print_menu(title, options, descriptions=None):
        print(f"\n{title}")
        for i, opt in enumerate(options, 1):
            print(f"{i}. {opt}")


    def print_stats_box(stats, title="Статистика"):
        print(f"\n{title}:")
        for k, v in stats.items():
            print(f"  {k}: {v}")


    def ask_user_choice(prompt, choices, default=""):
        return input(f"{prompt} [{'/'.join(choices)}]: ").strip()


    def ask_user_input(prompt, default="", validation_func=None):
        return input(f"{prompt}: ").strip()

try:
    from google_sheets_updater import GoogleSheetUpdater

    GOOGLE_SHEETS_AVAILABLE = True
except ImportError:
    GOOGLE_SHEETS_AVAILABLE = False

try:
    from reaction_analyzer import analyze_reactions_sync

    REACTION_ANALYZER_AVAILABLE = True
except ImportError:
    REACTION_ANALYZER_AVAILABLE = False


def validate_date_format(date_str: str) -> bool:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def get_user_date(prompt: str) -> datetime:
    def date_validator(date_str):
        return validate_date_format(date_str)

    while True:
        date_str = ask_user_input(
            prompt,
            validation_func=date_validator
        )

        if date_validator(date_str):
            return datetime.strptime(date_str, "%Y-%m-%d")
        else:
            print_error("Неверный формат даты. Используйте ГГГГ-ММ-ДД (например: 2025-08-01)")


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
        file_name = os.path.basename(file_path)

        if progress_bar:
            progress_bar.set_description(f"Обработка {file_name}")

        if CONSOLE_FORMATTER_AVAILABLE:
            console.print_progress_update(i, len(files), "Анализ файлов", file_name)

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

    if CONSOLE_FORMATTER_AVAILABLE:
        console.print_progress_update(len(files), len(files), "Анализ файлов", "Завершено")

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
    parser.add_argument("--analyze-reactions", action="store_true", help="Анализировать реакции пользователей.")
    parser.add_argument("--data-folder", type=str, help="Папка с JSON файлами (по умолчанию из .env).")
    parser.add_argument("--output", type=str, help="Имя выходного Excel файла (по умолчанию из .env).")
    parser.add_argument("--verbose", action="store_true", help="Включить подробное логирование.")
    parser.add_argument("--start-date", type=str, help="Дата начала отчета (формат: ГГГГ-ММ-ДД).")
    parser.add_argument("--end-date", type=str, help="Дата окончания отчета (формат: ГГГГ-ММ-ДД).")
    parser.add_argument("--days", type=int, help="Количество дней для включения в отчет (отсчет от сегодня).")
    return parser.parse_args()


def run_reaction_analysis(start_date: datetime, end_date: datetime) -> Dict[str, int]:
    if not REACTION_ANALYZER_AVAILABLE:
        print_error("Модуль анализа реакций недоступен.")
        return {}

    token = os.getenv("DISCORD_USER_TOKEN")
    if not token:
        print_error("DISCORD_USER_TOKEN не найден в .env файле для анализа реакций")
        return {}

    guild_id = int(os.getenv("REACTION_GUILD_ID", os.getenv("DISCORD_GUILD_ID", "1030160796401016883")))

    channel_ids_str = os.getenv("REACTION_CHANNEL_IDS", "")
    if not channel_ids_str:
        print_error("REACTION_CHANNEL_IDS не настроены в .env файле")
        return {}

    try:
        channel_ids = [int(cid.strip()) for cid in channel_ids_str.split(",") if cid.strip()]
    except ValueError:
        print_error("Неверный формат REACTION_CHANNEL_IDS в .env файле")
        return {}

    if not channel_ids:
        print_error("Не найдены валидные ID каналов для анализа реакций")
        return {}

    print_info(
        f"Запуск анализа реакций для {len(channel_ids)} каналов с {start_date.strftime('%Y-%m-%d')} по {end_date.strftime('%Y-%m-%d')}")

    try:
        import asyncio
        import threading

        result = {}
        exception_holder = [None]

        def run_analysis_in_thread():
            try:
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)

                try:
                    result.update(analyze_reactions_sync(token, guild_id, channel_ids, start_date, end_date))
                finally:
                    new_loop.close()

            except Exception as e:
                exception_holder[0] = e

        thread = threading.Thread(target=run_analysis_in_thread)
        thread.start()
        thread.join()

        if exception_holder[0]:
            raise exception_holder[0]

        validated_result = {}
        for name, count in result.items():
            if isinstance(name, str) and isinstance(count, int) and count > 0:
                validated_result[name.strip()] = count
            else:
                print_warning(f"Пропущена некорректная запись реакций: {name} -> {count}")

        print_success(f"Анализ реакций завершен: найдено {len(validated_result)} пользователей с реакциями")
        return validated_result

    except Exception as e:
        print_error(f"Ошибка при анализе реакций: {e}")
        return {}


def interactive_mode() -> Tuple[bool, bool, datetime, datetime]:
    print_header("АНАЛИЗАТОР АКТИВНОСТИ АДМИНИСТРАТОРОВ", "Интерактивный режим настройки")

    module_status = []
    if REACTION_ANALYZER_AVAILABLE:
        reaction_channels = os.getenv("REACTION_CHANNEL_IDS", "")
        if reaction_channels:
            module_status.append("✓ Анализ реакций: Доступен")
        else:
            module_status.append("⚠ Анализ реакций: Доступен, но не настроен REACTION_CHANNEL_IDS")
    else:
        module_status.append("✗ Анализ реакций: Недоступен (отсутствует модуль reaction_analyzer)")

    if GOOGLE_SHEETS_AVAILABLE:
        module_status.append("✓ Интеграция Google Sheets: Доступна")
    else:
        module_status.append("✗ Интеграция Google Sheets: Недоступна")

    print_section("Статус модулей")
    for status in module_status:
        if "✓" in status:
            print_success(status)
        elif "⚠" in status:
            print_warning(status)
        else:
            print_error(status)

    analysis_options = [
        "Полный анализ (Ahelp + реакции)",
        "Только Ahelp",
        "Только реакции"
    ]

    analysis_descriptions = [
        "Загрузка данных, анализ ahelp и реакций, создание отчетов",
        "Загрузка данных, анализ только ahelp, создание отчетов",
        "Анализ только реакций без ahelp данных"
    ]

    print_menu("Выберите тип анализа", analysis_options, analysis_descriptions)

    available_choices = ["1", "2"]
    if REACTION_ANALYZER_AVAILABLE:
        available_choices.append("3")

    choice = ask_user_choice("Введите номер", available_choices)

    should_download = False
    should_analyze_reactions = False

    if choice == "1":
        should_download = True
        should_analyze_reactions = REACTION_ANALYZER_AVAILABLE
        if not REACTION_ANALYZER_AVAILABLE:
            print_warning("Анализ реакций недоступен. Будет выполнен только анализ Ahelp.")
    elif choice == "2":
        should_download = True
        should_analyze_reactions = False
    elif choice == "3":
        should_download = False
        should_analyze_reactions = True

    print_section("Настройка периода анализа")
    start_date = get_user_date("Введите дату начала (ГГГГ-ММ-ДД)")
    end_date = get_user_date("Введите дату окончания (ГГГГ-ММ-ДД)") + timedelta(days=1)

    print_section("Подтверждение настроек")

    settings_summary = {
        "Загрузка данных": "Да" if should_download else "Нет",
        "Анализ реакций": "Да" if should_analyze_reactions else "Нет",
        "Период анализа": f"{start_date.strftime('%Y-%m-%d')} - {(end_date - timedelta(days=1)).strftime('%Y-%m-%d')}",
        "Дней в периоде": (end_date - start_date).days
    }

    print_stats_box(settings_summary, "Выбранные настройки")

    confirm = ask_user_choice("Продолжить с этими настройками?", ["y", "n"], "y")
    if confirm != "y":
        print_warning("Операция отменена пользователем")
        sys.exit(0)

    return should_download, should_analyze_reactions, start_date, end_date


def main() -> int:
    args = parse_arguments()

    dotenv_path = ".env"
    if not os.path.exists(dotenv_path):
        print_error(".env файл не найден. Пожалуйста, создайте его на основе .env.example")
        return 1
    dotenv.load_dotenv(dotenv_path)

    log_level = logging.INFO if not args.verbose else logging.DEBUG
    configure_logging(level=log_level)

    start_date, end_date = None, None
    should_download = args.download
    should_analyze_reactions = args.analyze_reactions

    
    if len(sys.argv) == 1:
        should_download, should_analyze_reactions, start_date, end_date = interactive_mode()
    else:

        if args.days:
            start_date = datetime.now() - timedelta(days=args.days)
            print_info(f"Установлен период отчета: последние {args.days} дней.")
        if args.start_date:
            try:
                start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
            except ValueError:
                print_error("Неверный формат даты начала. Используйте ГГГГ-ММ-ДД.")
                return 1
        if args.end_date:
            try:
                end_date = datetime.strptime(args.end_date, "%Y-%m-%d") + timedelta(days=1)
            except ValueError:
                print_error("Неверный формат даты окончания. Используйте ГГГГ-ММ-ДД.")
                return 1

    data_folder = args.data_folder or os.getenv("DATA_FOLDER", "data")
    excel_filename = args.output or os.getenv("EXCEL_FILENAME", "ahelp_stats.xlsx")

    completed_operations = []
    output_files = []

    
    if should_download:
        print_header("ЭТАП 1: ЗАГРУЗКА СООБЩЕНИЙ",
                     f"Период: {start_date.strftime('%Y-%m-%d')} - {(end_date - timedelta(days=1)).strftime('%Y-%m-%d')}")

        try:
            download_main(start_date=start_date, end_date=end_date)
            print_success("Загрузка сообщений завершена успешно")
            completed_operations.append("Загрузка сообщений Discord")
        except Exception as e:
            print_error(f"Ошибка при загрузке сообщений: {e}")
            return 1

    df_global = None

    if should_download:
        files = get_downloaded_files(data_folder)

        if not files:
            print_error(f"В папке {data_folder} не найдены JSON файлы после загрузки.")
            return 1

        print_header("ЭТАП 2: АНАЛИЗ ДАННЫХ", f"Обработка {len(files)} файлов")

        with tqdm(total=len(files), desc="Анализ файлов", ncols=100) as pbar:
            global_admin_stats, global_chat_count, servers_stats = aggregate_global_stats(
                files, pbar, start_date, end_date
            )

        total_ahelps = sum(stats["ahelps"] for stats in global_admin_stats.values())
        total_admins = len(global_admin_stats)

        analysis_stats = {
            "Обработано файлов": len(files),
            "Найдено администраторов": total_admins,
            "Всего ahelp-ов": total_ahelps,
            "Общее количество чатов": global_chat_count
        }

        print_stats_box(analysis_stats, "Результаты анализа")

        merged_global = merge_duplicate_admins(global_admin_stats)
        fill_missing_roles(merged_global, servers_stats)
        df_global = create_global_admins_dataframe(merged_global, servers_stats)

        print_header("ЭТАП 3: СОЗДАНИЕ ОТЧЕТОВ", f"Сохранение в {excel_filename}")
        try:
            save_all_data_to_excel(global_admin_stats, global_chat_count, servers_stats, df_global)
            print_success(f"Данные успешно сохранены в {excel_filename}")
            completed_operations.append("Анализ Ahelp данных")
            completed_operations.append("Создание Excel отчета")
            output_files.append(excel_filename)
        except Exception as e:
            print_error(f"Ошибка при сохранении данных в Excel: {e}")
            return 1

    user_reactions = {}
    if should_analyze_reactions:
        if not start_date or not end_date:
            print_error("Для анализа реакций необходимо указать даты начала и окончания.")
            return 1

        print_header("ЭТАП: АНАЛИЗ РЕАКЦИЙ",
                     f"Период: {start_date.strftime('%Y-%m-%d')} - {(end_date - timedelta(days=1)).strftime('%Y-%m-%d')}")
        user_reactions = run_reaction_analysis(start_date, end_date)

        if user_reactions:
            completed_operations.append("Анализ реакций пользователей")
        else:
            print_warning("Данные реакций не найдены.")

    
    if GOOGLE_SHEETS_AVAILABLE:
        print_header("ЭТАП 4: ИНТЕГРАЦИЯ С GOOGLE SHEETS")

        has_ahelp_data = df_global is not None and not df_global.empty
        has_reaction_data = bool(user_reactions)

        if not has_ahelp_data and not has_reaction_data:
            print_warning("Нет данных для обновления Google таблицы.")
        else:

            update_types = []
            if has_ahelp_data:
                update_types.append("Ahelp")
            if has_reaction_data:
                update_types.append("реакции")

            update_prompt = f"Хотите обновить Google-таблицу с данными ({', '.join(update_types)})?"
            update_gsheet = ask_user_choice(update_prompt, ["y", "n"], "n")

            if update_gsheet == "y":
                try:
                    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE")
                    sheet_id = os.getenv("GOOGLE_SHEET_ID")
                    worksheet_name = os.getenv("GOOGLE_SHEET_WORKSHEET_NAME")

                    if not all([creds_file, sheet_id, worksheet_name]):
                        print_error("Конфигурация для Google Sheets отсутствует в .env файле.")
                        return 1

                    updater = GoogleSheetUpdater(creds_file, sheet_id, worksheet_name)

                    if has_ahelp_data:
                        print_section("Обновление статистики Ahelp")

                        dry_run = ask_user_choice("Сначала выполнить тестовый запуск для ahelp (dry run)?", ["y", "n"],
                                                  "y")
                        if dry_run == "y":
                            updater.update_ahelp_stats(df_global, dry_run=True)

                        apply_response = ask_user_choice("Применить изменения ahelp к Google-таблице?", ["y", "n"], "n")
                        if apply_response == "y":
                            updater.update_ahelp_stats(df_global, dry_run=False)
                            print_success("Статистика Ahelp успешно обновлена в Google Sheets")
                            completed_operations.append("Обновление Google Sheets (Ahelp)")
                        else:
                            print_info("Обновление ahelp статистики отменено.")

                    if has_reaction_data:
                        print_section("Обновление статистики реакций")

                        dry_run = ask_user_choice("Сначала выполнить тестовый запуск для реакций (dry run)?",
                                                  ["y", "n"], "y")
                        if dry_run == "y":
                            updater.update_reaction_stats(user_reactions, dry_run=True)

                        apply_response = ask_user_choice("Применить изменения реакций к Google-таблице?", ["y", "n"],
                                                         "n")
                        if apply_response == "y":
                            updater.update_reaction_stats(user_reactions, dry_run=False)
                            print_success("Статистика реакций успешно обновлена в Google Sheets")
                            completed_operations.append("Обновление Google Sheets (реакции)")
                        else:
                            print_info("Обновление статистики реакций отменено.")

                except FileNotFoundError as e:
                    print_error(f"Не удалось инициализировать обновление Google Sheets: {e}")
                except Exception as e:
                    print_error(f"Произошла ошибка во время обновления Google Sheets: {e}")
            else:
                print_info("Обновление Google Sheets пропущено.")
    else:
        print_warning("Библиотека для Google Sheets не найдена. Пропуск этапа обновления Google-таблицы.")

    if CONSOLE_FORMATTER_AVAILABLE:
        console.print_final_summary(completed_operations, output_files)
    else:
        print_header("РАБОТА УСПЕШНО ЗАВЕРШЕНА!")
        for operation in completed_operations:
            print_success(operation)

    return 0


if __name__ == "__main__":
    sys.exit(main())
