import json
import logging
import os
from collections import defaultdict
from datetime import datetime, date
from typing import Tuple, Optional, Dict, Any, TypedDict

from utils import extract_admin_info, parse_message_time, normalize_admin_string


class AdminStats(TypedDict):
    ahelps: int
    mentions: int
    role: str
    sessions: int


class ServerStats(TypedDict):
    admin_stats: Dict[str, AdminStats]
    chat_count: int
    daily_ahelps: Dict[date, Dict[str, int]]


DEFAULT_ADMIN_STATS: AdminStats = {"ahelps": 0, "mentions": 0, "role": "Не указано", "sessions": 0}


def process_embed_data(
        embeds: list,
        message_datetime: Optional[datetime]
) -> Tuple[Dict[str, AdminStats], int, Dict[str, int]]:
    admin_stats = defaultdict(lambda: dict(DEFAULT_ADMIN_STATS))
    daily_ahelps = defaultdict(int)
    chat_count = 0

    for embed in embeds:
        description = embed.get('description', '')
        if not description:
            continue

        lines = description.split('\n')
        is_chat = any(':inbox_tray:' in line or ':outbox_tray:' in line for line in lines)
        admins_in_session = set()
        has_ahelp = False

        for line in lines:
            if ":outbox_tray:" in line:
                admin_name, admin_role = extract_admin_info(line)
                if admin_name is not None:
                    admins_in_session.add(admin_name)
                    if admin_role != "Не указано":
                        admin_stats[admin_name]["role"] = admin_role

            elif ":inbox_tray:" in line:
                for admin_name in admins_in_session:
                    admin_stats[admin_name]["mentions"] += 1
                has_ahelp = True

        if is_chat:
            chat_count += 1
            for admin_name in admins_in_session:
                admin_stats[admin_name]["sessions"] += 1
                if has_ahelp:
                    admin_stats[admin_name]["ahelps"] += 1
                    if message_datetime:
                        daily_ahelps[admin_name] += 1

    return admin_stats, chat_count, daily_ahelps


def load_json_file(file_path: str) -> Optional[Any]:
    if not os.path.isfile(file_path):
        logging.error(f"File not found: {file_path}")
        return None
    if not file_path.lower().endswith('.json'):
        logging.error(f"Not a JSON file: {file_path}")
        return None

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error in {file_path}: {e}")
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")
    return None


def analyze_ahelp_data(data: Any, server_name: str) -> ServerStats:
    admin_stats = defaultdict(lambda: dict(DEFAULT_ADMIN_STATS))
    total_chat_count = 0
    daily_ahelps = defaultdict(lambda: defaultdict(int))

    if isinstance(data, list):
        messages = data
    else:
        messages = []

    for message in messages:
        embeds = message.get('embeds', [])
        message_timestamp = message.get('created_at', '')
        msg_dt = parse_message_time(message_timestamp)

        local_stats, local_chats, local_daily_ahelps = process_embed_data(embeds, msg_dt)

        for admin, stats in local_stats.items():
            admin_stats[admin]["ahelps"] += stats["ahelps"]
            admin_stats[admin]["mentions"] += stats["mentions"]
            if stats["role"] != "Не указано":
                admin_stats[admin]["role"] = stats["role"]
            admin_stats[admin]["sessions"] += stats["sessions"]

        if msg_dt:
            for admin, ahelps_count in local_daily_ahelps.items():
                daily_ahelps[msg_dt.date()][admin] += ahelps_count

        total_chat_count += local_chats

    return {
        "admin_stats": dict(admin_stats),
        "chat_count": total_chat_count,
        "daily_ahelps": dict(daily_ahelps),
    }


def merge_duplicate_admins(admin_stats: Dict[str, AdminStats]) -> Dict[str, AdminStats]:
    merged_stats = defaultdict(lambda: DEFAULT_ADMIN_STATS.copy())
    for admin, stats in admin_stats.items():
        normalized_admin = normalize_admin_string(admin)
        merged_stats[normalized_admin]["ahelps"] += stats["ahelps"]
        merged_stats[normalized_admin]["mentions"] += stats["mentions"]
        if stats["role"] != "Не указано":
            merged_stats[normalized_admin]["role"] = stats["role"]
        merged_stats[normalized_admin]["sessions"] += stats["sessions"]
    return merged_stats


def fill_missing_roles(merged_admin_stats: Dict[str, AdminStats], servers_stats: Dict[str, ServerStats]) -> None:
    for admin, stats in merged_admin_stats.items():
        if stats["role"] == "Не указано":
            for server_name, server_stats in servers_stats.items():
                if admin in server_stats["admin_stats"] and server_stats["admin_stats"][admin]["role"] != "Не указано":
                    merged_admin_stats[admin]["role"] = server_stats["admin_stats"][admin]["role"]
                    break
