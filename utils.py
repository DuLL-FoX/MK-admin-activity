import logging
import os
import re
from datetime import datetime, timezone
from typing import Tuple, Optional, Pattern, Dict, Any, List

# Compiled regex patterns for better performance
ADMIN_INFO_PATTERN: Pattern = re.compile(
    r":outbox_tray:\s*(?:\*\*)?(?:[\d:]{5,8}|[\d:]{2,5})?\s*(?:\*\*)?\s*(.+?):\s*(.+)",
    re.UNICODE | re.IGNORECASE,
)

PLAYER_INFO_PATTERN: Pattern = re.compile(
    r":inbox_tray:\s*(?:\*\*)?(?:[\d:]{2,8})?\s*(?:\*\*)?\s*(.+?):",
    re.UNICODE | re.IGNORECASE,
)

SERVER_NAME_PATTERN: Pattern = re.compile(r"ahelp-(.+?)\s*\[")

# Additional patterns for better parsing
TIMESTAMP_PATTERN: Pattern = re.compile(r"[\d:]{2,8}")
MARKDOWN_PATTERN: Pattern = re.compile(r"\*\*(.+?)\*\*")
DISCRIMINATOR_PATTERN: Pattern = re.compile(r"#\d{2,}$")
ADMIN_TAG_PATTERN: Pattern = re.compile(r"\(Admin Only\)", re.IGNORECASE)
LEGACY_TAG_PATTERN: Pattern = re.compile(r"\(S\)\s*")

# Character limits for Excel compatibility
MAX_SHEET_NAME_LENGTH = 31
MAX_CELL_LENGTH = 32767


def configure_logging(
        level: int = logging.INFO,
        log_file: Optional[str] = "ahelp_analyzer.log",
        max_bytes: int = 10 * 1024 * 1024,  # 10MB
        backup_count: int = 5
) -> None:
    """
    Configure enhanced logging with rotation and better formatting.

    Args:
        level: Logging level
        log_file: Log file path (None to disable file logging)
        max_bytes: Maximum log file size before rotation
        backup_count: Number of backup files to keep
    """

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Setup handlers
    handlers = []

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)

    # File handler with rotation
    if log_file:
        try:
            from logging.handlers import RotatingFileHandler

            # Ensure log directory exists
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)

            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            handlers.append(file_handler)

        except Exception as e:
            print(f"Warning: Could not setup file logging: {e}")

    # Configure root logger
    logging.basicConfig(
        level=level,
        handlers=handlers,
        force=True  # Override any existing configuration
    )

    # Suppress noisy loggers
    noisy_loggers = [
        "matplotlib.font_manager",
        "PIL",
        "discord",
        "urllib3",
        "requests"
    ]

    for logger_name in noisy_loggers:
        logging.getLogger(logger_name).setLevel(logging.WARNING)


def clean_sheet_name(sheet_name: str) -> str:
    """
    Clean and validate Excel sheet name.

    Args:
        sheet_name: Raw sheet name

    Returns:
        Clean sheet name compatible with Excel
    """
    if not sheet_name:
        return "Sheet1"

    # Remove or replace invalid characters
    invalid_chars = r'[\\/*?:\[\]]'
    sheet_name = re.sub(invalid_chars, "_", sheet_name)

    # Replace other problematic characters
    sheet_name = re.sub(r'\s+', '_', sheet_name)  # Multiple spaces to single underscore
    sheet_name = re.sub(r'[^\w\-_.\s]', '', sheet_name)  # Remove non-word chars except dash, underscore, dot

    # Trim and limit length
    sheet_name = sheet_name.strip('_. ')
    if len(sheet_name) > MAX_SHEET_NAME_LENGTH:
        sheet_name = sheet_name[:MAX_SHEET_NAME_LENGTH - 3] + "..."

    # Ensure it's not empty
    if not sheet_name:
        return "Sheet1"

    return sheet_name


def normalize_admin_string(s: str) -> str:
    """
    Normalize admin name/role string for consistent comparison.

    Args:
        s: Raw admin string

    Returns:
        Normalized string
    """
    if not s or not isinstance(s, str):
        return "Unknown"

    original = s

    try:
        # Remove admin tags
        s = ADMIN_TAG_PATTERN.sub("", s)
        s = LEGACY_TAG_PATTERN.sub("", s)

        # Remove markdown formatting
        s = s.replace("**", "")
        s = s.replace("*", "")
        s = s.replace("__", "")
        s = s.replace("~~", "")

        # Remove Discord discriminators
        s = DISCRIMINATOR_PATTERN.sub("", s)

        # Clean up whitespace
        s = re.sub(r'\s+', ' ', s)
        s = s.strip()

        # Handle empty result
        if not s:
            logging.warning(f"Admin string normalized to empty: '{original}'")
            return "Unknown"

        return s

    except Exception as e:
        logging.warning(f"Error normalizing admin string '{original}': {e}")
        return original.strip() if original else "Unknown"


def extract_admin_info(line: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract admin name and role from outbox line with enhanced error handling.

    Args:
        line: Discord message line containing :outbox_tray:

    Returns:
        Tuple of (admin_name, admin_role) or (None, None) if extraction fails
    """
    if not line or not isinstance(line, str):
        return None, None

    try:
        match = ADMIN_INFO_PATTERN.search(line)
        if not match:
            return None, None

        admin_info_part = match.group(1).strip()
        if not admin_info_part:
            return None, None

        # Split by pipe character for role separation
        if "|" in admin_info_part:
            parts = [p.strip() for p in admin_info_part.split("|")]
            if len(parts) >= 2:
                admin_name = parts[-1]  # Last part is usually the name
                admin_role = " | ".join(parts[:-1])  # Everything else is role
            else:
                admin_name = admin_info_part
                admin_role = "Unknown"
        else:
            admin_name = admin_info_part
            admin_role = "Unknown"

        # Normalize both name and role
        normalized_name = normalize_admin_string(admin_name)
        normalized_role = normalize_admin_string(admin_role)

        return normalized_name, normalized_role

    except Exception as e:
        logging.debug(f"Error extracting admin info from line '{line[:100]}...': {e}")
        return None, None


def extract_player_name(line: str) -> Optional[str]:
    """
    Extract player name from inbox line with enhanced error handling.

    Args:
        line: Discord message line containing :inbox_tray:

    Returns:
        Player name or None if extraction fails
    """
    if not line or not isinstance(line, str):
        return None

    try:
        match = PLAYER_INFO_PATTERN.search(line)
        if not match:
            return None

        player_name = match.group(1).strip()
        if not player_name:
            return None

        return normalize_admin_string(player_name)

    except Exception as e:
        logging.debug(f"Error extracting player name from line '{line[:100]}...': {e}")
        return None


def parse_message_time(message_timestamp: str) -> Optional[datetime]:
    """
    Parse Discord message timestamp with enhanced error handling.

    Args:
        message_timestamp: ISO timestamp string from Discord

    Returns:
        UTC datetime object or None if parsing fails
    """
    if not message_timestamp or not isinstance(message_timestamp, str):
        return None

    try:
        # Handle different timestamp formats
        timestamp = message_timestamp.strip()

        # Remove 'Z' suffix and replace with UTC offset
        if timestamp.endswith('Z'):
            timestamp = timestamp[:-1] + '+00:00'

        # Parse ISO format
        dt = datetime.fromisoformat(timestamp)

        # Ensure UTC timezone
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        return dt

    except (ValueError, AttributeError) as e:
        logging.debug(f"Error parsing timestamp '{message_timestamp}': {e}")
        return None
    except Exception as e:
        logging.warning(f"Unexpected error parsing timestamp '{message_timestamp}': {e}")
        return None


def extract_server_name(file_path: str) -> str:
    """
    Extract server name from file path with fallback handling.

    Args:
        file_path: Path to the JSON file

    Returns:
        Server name extracted from filename
    """
    if not file_path:
        return "Unknown"

    try:
        base = os.path.basename(file_path)

        # Try to extract from ahelp pattern
        match = SERVER_NAME_PATTERN.search(base)
        if match:
            return match.group(1)

        # Fallback to filename without extension
        name = os.path.splitext(base)[0]

        # Clean up common prefixes/suffixes
        prefixes_to_remove = ["ahelp-", "ahelp_", "🤔┇ahelp-"]
        for prefix in prefixes_to_remove:
            if name.startswith(prefix):
                name = name[len(prefix):]
                break

        return name if name else "Unknown"

    except Exception as e:
        logging.warning(f"Error extracting server name from '{file_path}': {e}")
        return "Unknown"


def format_date_range(start_date: Optional[str], end_date: Optional[str]) -> str:
    """
    Format date range for display with better handling.

    Args:
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)

    Returns:
        Formatted date range string
    """
    if start_date and end_date:
        if start_date == end_date:
            return f"Date: {start_date}"
        return f"Period: {start_date} to {end_date}"
    elif start_date:
        return f"From: {start_date}"
    elif end_date:
        return f"Until: {end_date}"
    else:
        return "All available data"


def truncate_cell_content(content: str, max_length: int = MAX_CELL_LENGTH) -> str:
    """
    Truncate content to fit Excel cell limits.

    Args:
        content: String content
        max_length: Maximum allowed length

    Returns:
        Truncated string if necessary
    """
    if not content or len(content) <= max_length:
        return content

    return content[:max_length - 3] + "..."


def validate_discord_message(message: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate Discord message structure.

    Args:
        message: Discord message dictionary

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    if not isinstance(message, dict):
        errors.append("Message is not a dictionary")
        return False, errors

    # Check required fields
    required_fields = {
        'id': (str, int),
        'created_at': str,
        'embeds': list
    }

    for field, expected_types in required_fields.items():
        if field not in message:
            errors.append(f"Missing required field: {field}")
        elif not isinstance(message[field], expected_types):
            errors.append(f"Field '{field}' has wrong type: expected {expected_types}, got {type(message[field])}")

    # Validate embeds structure
    if 'embeds' in message:
        for i, embed in enumerate(message['embeds']):
            if not isinstance(embed, dict):
                errors.append(f"Embed {i} is not a dictionary")
            elif 'description' not in embed:
                errors.append(f"Embed {i} missing description field")

    return len(errors) == 0, errors


def safe_filename(filename: str, max_length: int = 255) -> str:
    """
    Create a safe filename by removing/replacing problematic characters.

    Args:
        filename: Original filename
        max_length: Maximum filename length

    Returns:
        Safe filename
    """
    if not filename:
        return "file"

    # Remove/replace problematic characters
    safe_chars = re.sub(r'[<>:"/\\|?*]', '_', filename)
    safe_chars = re.sub(r'\s+', '_', safe_chars)
    safe_chars = safe_chars.strip('._')

    # Limit length
    if len(safe_chars) > max_length:
        name, ext = os.path.splitext(safe_chars)
        available_length = max_length - len(ext) - 3  # Reserve space for "..." and extension
        safe_chars = name[:available_length] + "..." + ext

    return safe_chars if safe_chars else "file"


def get_file_info(file_path: str) -> Dict[str, Any]:
    """
    Get comprehensive file information.

    Args:
        file_path: Path to file

    Returns:
        Dictionary with file information
    """
    info = {
        "path": file_path,
        "exists": False,
        "size": 0,
        "size_mb": 0.0,
        "modified": None,
        "readable": False,
        "error": None
    }

    try:
        if os.path.exists(file_path):
            info["exists"] = True
            info["size"] = os.path.getsize(file_path)
            info["size_mb"] = round(info["size"] / (1024 * 1024), 2)
            info["modified"] = datetime.fromtimestamp(os.path.getmtime(file_path))
            info["readable"] = os.access(file_path, os.R_OK)

    except Exception as e:
        info["error"] = str(e)

    return info


def estimate_processing_time(file_size_mb: float, files_count: int = 1) -> str:
    """
    Estimate processing time based on file size.

    Args:
        file_size_mb: Total file size in MB
        files_count: Number of files

    Returns:
        Estimated time string
    """
    # Rough estimates based on typical performance
    mb_per_second = 2.0  # Adjust based on your system
    base_time_per_file = 1.0  # Base overhead per file

    estimated_seconds = (file_size_mb / mb_per_second) + (files_count * base_time_per_file)

    if estimated_seconds < 60:
        return f"~{estimated_seconds:.0f} seconds"
    elif estimated_seconds < 3600:
        return f"~{estimated_seconds / 60:.1f} minutes"
    else:
        return f"~{estimated_seconds / 3600:.1f} hours"


# Logging helper functions
def log_performance(func_name: str, duration: float, items_processed: int = 0):
    """Log performance metrics for a function."""
    rate = f" ({items_processed / duration:.1f} items/sec)" if items_processed and duration > 0 else ""
    logging.debug(f"Performance: {func_name} took {duration:.3f}s{rate}")


def log_data_summary(data_type: str, count: int, details: Optional[Dict] = None):
    """Log a summary of processed data."""
    detail_str = ""
    if details:
        detail_parts = [f"{k}={v}" for k, v in details.items()]
        detail_str = f" ({', '.join(detail_parts)})"

    logging.info(f"Processed {count:,} {data_type}{detail_str}")


# Compatibility functions (maintain backward compatibility)
def format_duration(seconds: float) -> str:
    """Format duration in human-readable format (moved from main.py)."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds // 60:.0f}m {seconds % 60:.0f}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours:.0f}h {minutes:.0f}m {seconds % 60:.0f}s"