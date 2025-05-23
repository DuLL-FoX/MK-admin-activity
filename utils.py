import logging
import os
import re
from datetime import datetime, timezone
from typing import Tuple, Optional, Pattern

ADMIN_INFO_PATTERN: Pattern = re.compile(
    r":outbox_tray:\s*(?:\*\*)?(?:[\d:]{5,8}|[\d:]{2,5})?\s*(?:\*\*)?\s*(.+?):\s*(.+)",
    re.UNICODE,
)

PLAYER_INFO_PATTERN: Pattern = re.compile(
    r":inbox_tray:\s*(?:\*\*)?(?:[\d:]{2,8})?\s*(?:\*\*)?\s*(.+?):",
    re.UNICODE,
)

SERVER_NAME_PATTERN: Pattern = re.compile(r"ahelp-(.+?)\s*\[")

def configure_logging(level: int = logging.INFO,
                      log_file: Optional[str] = "ahelp_analyzer.log") -> None:
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )

    logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)
    logging.getLogger("PIL").setLevel(logging.WARNING)
    logging.getLogger("discord").setLevel(logging.WARNING)

def clean_sheet_name(sheet_name: str) -> str:
    sheet_name = re.sub(r"[\\/*?:\[\]]", "_", sheet_name)
    sheet_name = re.sub(r"\W+", "_", sheet_name)
    return sheet_name[:31]


def normalize_admin_string(s: str) -> str:
    """Strip markdown, discriminators, legacy tags, etc. and squash spaces."""
    s = re.sub(r"\(Admin Only\)", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\(S\)\s*", "", s)
    s = s.replace("**", "")
    s = re.sub(r"#\d{2,}$", "", s)
    return s.strip()


def extract_admin_info(line: str) -> Tuple[Optional[str], Optional[str]]:
    """
    From an :outbox_tray: line return (admin_name, admin_role).
    Role may be 'Unknown' if it cannot be determined.
    """
    match = ADMIN_INFO_PATTERN.search(line)
    if not match:
        return None, None

    admin_info_part = match.group(1)
    if "|" in admin_info_part:
        parts = [p.strip() for p in admin_info_part.split("|")]
        admin_name = parts[-1]
        admin_role = " | ".join(parts[:-1]) if parts[:-1] else "Unknown"
    else:
        admin_name = admin_info_part
        admin_role = "Unknown"

    return normalize_admin_string(admin_name), normalize_admin_string(admin_role)


def extract_player_name(line: str) -> Optional[str]:
    """Return the nickname that follows :inbox_tray:, or None if not found."""
    match = PLAYER_INFO_PATTERN.search(line)
    if not match:
        return None
    return normalize_admin_string(match.group(1))


def parse_message_time(message_timestamp: str) -> Optional[datetime]:
    if not message_timestamp:
        return None
    try:
        dt = datetime.fromisoformat(message_timestamp.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception as e:
        logging.error(f"Error parsing message timestamp '{message_timestamp}': {e}")
        return None


def extract_server_name(file_path: str) -> str:
    base = os.path.basename(file_path)
    m = SERVER_NAME_PATTERN.search(base)
    return m.group(1) if m else os.path.splitext(base)[0]


def format_date_range(start_date: Optional[str], end_date: Optional[str]) -> str:
    if start_date and end_date:
        return f"{start_date} to {end_date}"
    if start_date:
        return f"From {start_date}"
    if end_date:
        return f"Until {end_date}"
    return "All dates"
