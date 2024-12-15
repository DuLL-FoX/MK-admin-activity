import logging
import os
import re
from datetime import datetime, timezone
from typing import Tuple, Optional


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger('matplotlib.font_manager').setLevel(logging.ERROR)


def clean_sheet_name(sheet_name: str) -> str:
    sheet_name = re.sub(r'[\\/*?:\[\]]', '_', sheet_name)
    sheet_name = re.sub(r'\W+', '_', sheet_name)
    return sheet_name[:31]


def normalize_admin_string(s: str) -> str:
    s = re.sub(r'^\(S\)\s*', '', s)
    s = s.replace("**", "").strip()
    return s


def extract_admin_info(line: str) -> Tuple[Optional[str], Optional[str]]:
    pattern = re.compile(
        r":outbox_tray:\s*(?:\*\*)?(?:[\d:]{5,8}|[\d:]{2,5})?\s*(?:\*\*)?\s*(.+?):\s*(.+)", re.UNICODE
    )
    match = pattern.search(line)
    if not match:
        return None, None

    admin_info_part = match.group(1)
    if '|' in admin_info_part:
        parts = [part.strip() for part in admin_info_part.split('|')]
        admin_name = parts[-1]
        admin_role = " | ".join(parts[:-1]) if parts[:-1] else "Не указано"
    else:
        admin_name = admin_info_part
        admin_role = "Не указано"

    admin_name = normalize_admin_string(admin_name)
    admin_role = normalize_admin_string(admin_role)

    return admin_name, admin_role


def parse_message_time(message_timestamp: str) -> Optional[datetime]:
    if not message_timestamp:
        return None
    try:
        msg_time = datetime.fromisoformat(message_timestamp)
        if msg_time.tzinfo is None:
            msg_time = msg_time.replace(tzinfo=timezone.utc)
        else:
            msg_time = msg_time.astimezone(timezone.utc)
        return msg_time
    except Exception as e:
        logging.error(f"Ошибка при парсинге времени сообщения '{message_timestamp}': {e}")
        return None


def extract_server_name(file_path: str) -> str:
    base_name = os.path.basename(file_path)
    match = re.search(r"ahelp-(.+?)\s*\[", base_name)
    if match:
        return match.group(1)
    return os.path.splitext(base_name)[0]
