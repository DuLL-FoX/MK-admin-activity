import json
import logging
import os
from collections import defaultdict
from datetime import datetime, date
from typing import (
    Tuple, Optional, Dict, Any, TypedDict, List, DefaultDict, Set,
)

from utils import (
    extract_admin_info,
    extract_player_name,
    parse_message_time,
    normalize_admin_string,
)


class AdminStats(TypedDict):
    ahelps: int
    mentions: int
    role: str
    sessions: int
    admin_only_ahelps: int
    admin_only_mentions: int
    admin_only_sessions: int


class ServerStats(TypedDict):
    admin_stats: Dict[str, AdminStats]
    chat_count: int
    daily_ahelps: Dict[date, Dict[str, int]]
    hourly_ahelps: Dict[date, Dict[int, Dict[str, int]]]
    daily_admin_only_ahelps: Dict[date, Dict[str, int]]


DEFAULT_ADMIN_STATS: AdminStats = {
    "ahelps": 0,
    "mentions": 0,
    "role": "Unknown",
    "sessions": 0,
    "admin_only_ahelps": 0,
    "admin_only_mentions": 0,
    "admin_only_sessions": 0,
}


def process_embed_data(
        embeds: List[Dict[str, Any]],
        message_datetime: Optional[datetime],
) -> Tuple[Dict[str, AdminStats], int, Dict[str, int], Dict[str, int], int, int]:
    admin_stats: DefaultDict[str, AdminStats] = defaultdict(
        lambda: dict(DEFAULT_ADMIN_STATS.copy())
    )
    daily_ahelps: DefaultDict[str, int] = defaultdict(int)
    daily_admin_only_ahelps: DefaultDict[str, int] = defaultdict(int)
    chat_count = 0
    total_ahelps_count = 0
    processed_ahelps_count = 0

    for embed in embeds:
        description = embed.get("description", "")
        if not description:
            continue

        lines = description.split("\n")
        is_chat = any(
            ":inbox_tray:" in l or ":outbox_tray:" in l
            for l in lines
        )
        if not is_chat:
            continue

        admins_in_session: Set[str] = set()
        admin_only_admins_in_session: Set[str] = set()
        players_in_session: Set[str] = set()
        has_ahelp = False
        has_admin_only_activity = False
        admin_responded = False

        for line in lines:
            if ":outbox_tray:" in line:
                name, role, is_admin_only = extract_admin_info(line)
                if name:
                    norm = normalize_admin_string(name)

                    if is_admin_only:
                        admin_only_admins_in_session.add(norm)
                        has_admin_only_activity = True
                    else:
                        admins_in_session.add(norm)
                        admin_responded = True

                    if role and role != "Unknown":
                        admin_stats[norm]["role"] = normalize_admin_string(role)

            elif ":inbox_tray:" in line:
                has_ahelp = True
                player = extract_player_name(line)
                if player:
                    players_in_session.add(player)

                for admin in admins_in_session:
                    if admin != player:
                        admin_stats[admin]["mentions"] += 1

                for admin in admin_only_admins_in_session:
                    if admin != player:
                        admin_stats[admin]["admin_only_mentions"] += 1

        if is_chat:
            chat_count += 1

            for admin in admins_in_session:
                admin_stats[admin]["sessions"] += 1

                if has_ahelp and admin not in players_in_session:
                    admin_stats[admin]["ahelps"] += 1
                    if message_datetime:
                        daily_ahelps[admin] += 1

            for admin in admin_only_admins_in_session:
                admin_stats[admin]["admin_only_sessions"] += 1

                if has_ahelp and admin not in players_in_session:
                    admin_stats[admin]["admin_only_ahelps"] += 1
                    if message_datetime:
                        daily_admin_only_ahelps[admin] += 1

        if has_ahelp:
            total_ahelps_count += 1
            if admin_responded:
                processed_ahelps_count += 1

    return (
        dict(admin_stats),
        chat_count,
        dict(daily_ahelps),
        dict(daily_admin_only_ahelps),
        total_ahelps_count,
        processed_ahelps_count,
    )


def load_json_file(file_path: str) -> Optional[Any]:
    if not os.path.isfile(file_path):
        logging.error(f"File not found: {file_path}")
        return None

    if not file_path.lower().endswith('.json'):
        logging.error(f"File is not JSON: {file_path}")
        return None

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            logging.info(f"Successfully loaded JSON file: {file_path}")
            return data
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error in file {file_path}: {e}")
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")

    return None


def analyze_ahelp_data(data: Any, server_name: str, start_date: Optional[datetime] = None,
                       end_date: Optional[datetime] = None) -> ServerStats:
    admin_stats: DefaultDict[str, AdminStats] = defaultdict(lambda: DEFAULT_ADMIN_STATS.copy())
    total_chat_count = 0
    daily_ahelps: DefaultDict[date, DefaultDict[str, int]] = defaultdict(lambda: defaultdict(int))
    daily_admin_only_ahelps: DefaultDict[date, DefaultDict[str, int]] = defaultdict(lambda: defaultdict(int))
    hourly_ahelps: DefaultDict[date, DefaultDict[int, Dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: {"total": 0, "processed": 0})
    )

    if not isinstance(data, list):
        logging.error("Data is not a list of messages.")
        return {
            "admin_stats": {},
            "chat_count": 0,
            "daily_ahelps": {},
            "hourly_ahelps": {},
            "daily_admin_only_ahelps": {}
        }

    logging.info(f"Analyzing {len(data)} messages from server {server_name}")

    for message in data:
        if start_date or end_date:
            message_timestamp = message.get('created_at', '')
            msg_dt = parse_message_time(message_timestamp)

            if not msg_dt:
                continue

            if start_date and msg_dt < start_date:
                continue

            if end_date and msg_dt > end_date:
                continue

        embeds = message.get('embeds', [])
        if not embeds:
            continue

        message_timestamp = message.get('created_at', '')
        msg_dt = parse_message_time(message_timestamp)

        if not msg_dt:
            logging.warning(f"Invalid or missing 'created_at' field in message ID {message.get('id')}; skipping.")
            continue

        local_stats, local_chats, local_daily_ahelps, local_daily_admin_only_ahelps, total_ahelps_count, processed_ahelps_count = process_embed_data(
            embeds, msg_dt
        )

        for admin, stats in local_stats.items():
            admin_stats[admin]["ahelps"] += stats["ahelps"]
            admin_stats[admin]["mentions"] += stats["mentions"]
            admin_stats[admin]["admin_only_ahelps"] += stats["admin_only_ahelps"]
            admin_stats[admin]["admin_only_mentions"] += stats["admin_only_mentions"]
            admin_stats[admin]["admin_only_sessions"] += stats["admin_only_sessions"]
            if stats["role"] != "Unknown":
                admin_stats[admin]["role"] = normalize_admin_string(stats["role"])
            admin_stats[admin]["sessions"] += stats["sessions"]

        current_date = msg_dt.date()
        for admin, count in local_daily_ahelps.items():
            daily_ahelps[current_date][admin] += count

        for admin, count in local_daily_admin_only_ahelps.items():
            daily_admin_only_ahelps[current_date][admin] += count

        if total_ahelps_count > 0:
            current_hour = msg_dt.hour
            hourly_ahelps[current_date][current_hour]["total"] += total_ahelps_count
            hourly_ahelps[current_date][current_hour]["processed"] += processed_ahelps_count

        total_chat_count += local_chats

    logging.info(
        f"Found {total_chat_count} chats, {sum(stats['ahelps'] for stats in admin_stats.values())} regular ahelps, and {sum(stats['admin_only_ahelps'] for stats in admin_stats.values())} admin-only ahelps in server {server_name}")

    return {
        "admin_stats": dict(admin_stats),
        "chat_count": total_chat_count,
        "daily_ahelps": {d: dict(admins) for d, admins in daily_ahelps.items()},
        "daily_admin_only_ahelps": {d: dict(admins) for d, admins in daily_admin_only_ahelps.items()},
        "hourly_ahelps": {
            d: {h: vals.copy() for h, vals in hours.items()}
            for d, hours in hourly_ahelps.items()
        }
    }


def merge_duplicate_admins(admin_stats: Dict[str, AdminStats]) -> Dict[str, AdminStats]:
    merged_stats: DefaultDict[str, AdminStats] = defaultdict(lambda: DEFAULT_ADMIN_STATS.copy())

    for admin, stats in admin_stats.items():
        normalized_admin = normalize_admin_string(admin)
        merged_stats[normalized_admin]["ahelps"] += stats["ahelps"]
        merged_stats[normalized_admin]["mentions"] += stats["mentions"]
        merged_stats[normalized_admin]["admin_only_ahelps"] += stats["admin_only_ahelps"]
        merged_stats[normalized_admin]["admin_only_mentions"] += stats["admin_only_mentions"]
        merged_stats[normalized_admin]["admin_only_sessions"] += stats["admin_only_sessions"]
        if stats["role"] != "Unknown":
            normalized_role = normalize_admin_string(stats["role"])
            merged_stats[normalized_admin]["role"] = normalized_role
        merged_stats[normalized_admin]["sessions"] += stats["sessions"]

    return dict(merged_stats)


def fill_missing_roles(
        merged_admin_stats: Dict[str, AdminStats],
        servers_stats: Dict[str, ServerStats]
) -> None:
    for admin, stats in merged_admin_stats.items():
        if stats["role"] == "Unknown":
            for server_stats in servers_stats.values():
                server_admin_stats = server_stats["admin_stats"]
                if admin in server_admin_stats and server_admin_stats[admin]["role"] != "Unknown":
                    normalized_role = normalize_admin_string(server_admin_stats[admin]["role"])
                    merged_admin_stats[admin]["role"] = normalized_role
                    logging.info(f"Filled missing role for admin '{admin}' from server statistics.")
                    break