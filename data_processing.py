import json
import logging
import os
from collections import defaultdict
from datetime import datetime, date
from typing import Tuple, Optional, Dict, Any, TypedDict, List

from utils import extract_admin_info, parse_message_time, normalize_admin_string

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AdminStats(TypedDict):
    ahelps: int
    mentions: int
    role: str
    sessions: int

class ServerStats(TypedDict):
    admin_stats: Dict[str, AdminStats]
    chat_count: int
    daily_ahelps: Dict[date, Dict[str, int]]
    hourly_ahelps: Dict[date, Dict[int, Dict[str, int]]]

DEFAULT_ADMIN_STATS: AdminStats = {
    "ahelps": 0,
    "mentions": 0,
    "role": "Не указано",
    "sessions": 0
}

def process_embed_data(
        embeds: List[Dict[str, Any]],
        message_datetime: Optional[datetime]
) -> Tuple[Dict[str, AdminStats], int, Dict[str, int], int, int]:
    admin_stats: Dict[str, AdminStats] = defaultdict(lambda: dict(DEFAULT_ADMIN_STATS))
    daily_ahelps: Dict[str, int] = defaultdict(int)
    chat_count = 0
    total_ahelps_count = 0
    processed_ahelps_count = 0

    for embed in embeds:
        description = embed.get('description', '')
        if not description:
            continue

        lines = description.split('\n')
        is_chat = any(':inbox_tray:' in line or ':outbox_tray:' in line for line in lines)
        if not is_chat:
            continue

        admins_in_session = set()
        has_ahelp = False
        admin_responded = False

        for line in lines:
            if ":outbox_tray:" in line:
                admin_info = extract_admin_info(line)
                if admin_info:
                    admin_name, admin_role = admin_info
                    if admin_name:
                        normalized_admin = normalize_admin_string(admin_name)
                        admins_in_session.add(normalized_admin)
                        if admin_role and admin_role != "Не указано":
                            admin_stats[normalized_admin]["role"] = admin_role
                admin_responded = True

            elif ":inbox_tray:" in line:
                has_ahelp = True
                for admin in admins_in_session:
                    admin_stats[admin]["mentions"] += 1

        if is_chat:
            chat_count += 1
            for admin in admins_in_session:
                admin_stats[admin]["sessions"] += 1
                if has_ahelp:
                    admin_stats[admin]["ahelps"] += 1
                    if message_datetime:
                        daily_ahelps[admin] += 1

        if has_ahelp:
            total_ahelps_count += 1
            if admin_responded:
                processed_ahelps_count += 1

    return admin_stats, chat_count, daily_ahelps, total_ahelps_count, processed_ahelps_count

def load_json_file(file_path: str) -> Optional[Any]:
    if not os.path.isfile(file_path):
        logging.error(f"Файл не найден: {file_path}")
        return None

    if not file_path.lower().endswith('.json'):
        logging.error(f"Файл не является JSON: {file_path}")
        return None

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            logging.info(f"JSON файл успешно загружен: {file_path}")
            return data
    except json.JSONDecodeError as e:
        logging.error(f"Ошибка декодирования JSON в файле {file_path}: {e}")
    except Exception as e:
        logging.error(f"Ошибка при обработке файла {file_path}: {e}")

    return None

def analyze_ahelp_data(data: Any, server_name: str) -> ServerStats:
    admin_stats: Dict[str, AdminStats] = defaultdict(lambda: dict(DEFAULT_ADMIN_STATS))
    total_chat_count = 0
    daily_ahelps: Dict[date, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    hourly_ahelps: Dict[date, Dict[int, Dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: {"total": 0, "processed": 0})
    )

    if not isinstance(data, list):
        logging.error("Данные не являются списком сообщений.")
        return {
            "admin_stats": {},
            "chat_count": 0,
            "daily_ahelps": {},
            "hourly_ahelps": {}
        }

    for message in data:
        embeds = message.get('embeds', [])
        message_timestamp = message.get('created_at', '')
        msg_dt = parse_message_time(message_timestamp)

        if not msg_dt:
            logging.warning(f"Неверное или отсутствующее поле 'created_at' в сообщении ID {message.get('id')}; пропуск.")
            continue

        local_stats, local_chats, local_daily_ahelps, total_ahelps_count, processed_ahelps_count = process_embed_data(
            embeds, msg_dt
        )

        for admin, stats in local_stats.items():
            admin_stats[admin]["ahelps"] += stats["ahelps"]
            admin_stats[admin]["mentions"] += stats["mentions"]
            if stats["role"] != "Не указано":
                admin_stats[admin]["role"] = stats["role"]
            admin_stats[admin]["sessions"] += stats["sessions"]

        current_date = msg_dt.date()
        for admin, count in local_daily_ahelps.items():
            daily_ahelps[current_date][admin] += count

        if total_ahelps_count > 0:
            current_hour = msg_dt.hour
            hourly_ahelps[current_date][current_hour]["total"] += total_ahelps_count
            hourly_ahelps[current_date][current_hour]["processed"] += processed_ahelps_count

        total_chat_count += local_chats

    return {
        "admin_stats": dict(admin_stats),
        "chat_count": total_chat_count,
        "daily_ahelps": {d: dict(admins) for d, admins in daily_ahelps.items()},
        "hourly_ahelps": {
            d: {h: vals.copy() for h, vals in hours.items()}
            for d, hours in hourly_ahelps.items()
        }
    }

def merge_duplicate_admins(admin_stats: Dict[str, AdminStats]) -> Dict[str, AdminStats]:
    merged_stats: Dict[str, AdminStats] = defaultdict(lambda: dict(DEFAULT_ADMIN_STATS))

    for admin, stats in admin_stats.items():
        normalized_admin = normalize_admin_string(admin)
        merged_stats[normalized_admin]["ahelps"] += stats["ahelps"]
        merged_stats[normalized_admin]["mentions"] += stats["mentions"]
        if stats["role"] != "Не указано":
            merged_stats[normalized_admin]["role"] = stats["role"]
        merged_stats[normalized_admin]["sessions"] += stats["sessions"]

    return dict(merged_stats)

def fill_missing_roles(
        merged_admin_stats: Dict[str, AdminStats],
        servers_stats: Dict[str, ServerStats]
) -> None:
    for admin, stats in merged_admin_stats.items():
        if stats["role"] == "Не указано":
            for server_stats in servers_stats.values():
                server_admin_stats = server_stats["admin_stats"]
                if admin in server_admin_stats and server_admin_stats[admin]["role"] != "Не указано":
                    merged_admin_stats[admin]["role"] = server_admin_stats[admin]["role"]
                    logging.info(f"Заполнена отсутствующая роль для администратора '{admin}' из статистики сервера.")
                    break