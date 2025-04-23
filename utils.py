import logging
import os
import re
from datetime import datetime, timezone
from typing import Tuple, Optional, Pattern

ADMIN_INFO_PATTERN: Pattern = re.compile(
    r":outbox_tray:\s*(?:\*\*)?(?:[\d:]{5,8}|[\d:]{2,5})?\s*(?:\*\*)?\s*(.+?):\s*(.+)", re.UNICODE
)
SERVER_NAME_PATTERN: Pattern = re.compile(r"ahelp-(.+?)\s*\[")


def configure_logging(level: int = logging.INFO, log_file: Optional[str] = "ahelp_analyzer.log") -> None:
    import sys
    import io

    # if hasattr(sys.stdout, 'buffer'):
    #     sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    #     sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

    handlers = [logging.StreamHandler()]

    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding='utf-8'))

    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

    logging.getLogger('matplotlib.font_manager').setLevel(logging.ERROR)
    logging.getLogger('PIL').setLevel(logging.WARNING)
    logging.getLogger('discord').setLevel(logging.WARNING)


def clean_sheet_name(sheet_name: str) -> str:
    sheet_name = re.sub(r'[\\/*?:\[\]]', '_', sheet_name)
    sheet_name = re.sub(r'\W+', '_', sheet_name)

    return sheet_name[:31]


def normalize_admin_string(s: str) -> str:
    s = re.sub(r'\(S\)\s*', '', s)

    s = s.replace("**", "").strip()

    return s


def extract_admin_info(line: str) -> Tuple[Optional[str], Optional[str]]:
    match = ADMIN_INFO_PATTERN.search(line)
    if not match:
        return None, None

    admin_info_part = match.group(1)
    if '|' in admin_info_part:
        parts = [part.strip() for part in admin_info_part.split('|')]
        admin_name = parts[-1]
        admin_role = " | ".join(parts[:-1]) if parts[:-1] else "Unknown"
    else:
        admin_name = admin_info_part
        admin_role = "Unknown"

    admin_name = normalize_admin_string(admin_name)
    admin_role = normalize_admin_string(admin_role)

    return admin_name, admin_role


def parse_message_time(message_timestamp: str) -> Optional[datetime]:
    if not message_timestamp:
        return None
    try:
        msg_time = datetime.fromisoformat(message_timestamp.replace('Z', '+00:00'))
        if msg_time.tzinfo is None:
            msg_time = msg_time.replace(tzinfo=timezone.utc)
        else:
            msg_time = msg_time.astimezone(timezone.utc)
        return msg_time
    except Exception as e:
        logging.error(f"Error parsing message timestamp '{message_timestamp}': {e}")
        return None


def extract_server_name(file_path: str) -> str:
    base_name = os.path.basename(file_path)
    match = SERVER_NAME_PATTERN.search(base_name)
    if match:
        return match.group(1)
    return os.path.splitext(base_name)[0]


def format_date_range(start_date: Optional[str], end_date: Optional[str]) -> str:
    if start_date and end_date:
        return f"{start_date} to {end_date}"
    elif start_date:
        return f"From {start_date}"
    elif end_date:
        return f"Until {end_date}"
    else:
        return "All dates"