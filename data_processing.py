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
    """Type definition for admin statistics."""
    ahelps: int
    mentions: int
    role: str
    sessions: int


class ServerStats(TypedDict):
    """Type definition for server statistics."""
    admin_stats: Dict[str, AdminStats]
    chat_count: int
    daily_ahelps: Dict[date, Dict[str, int]]
    hourly_ahelps: Dict[date, Dict[int, Dict[str, int]]]


class DataValidationError(Exception):
    """Custom exception for data validation errors."""
    pass


class ProcessingStats:
    """Class to track processing statistics and provide insights."""

    def __init__(self):
        self.total_messages = 0
        self.processed_messages = 0
        self.invalid_messages = 0
        self.empty_embeds = 0
        self.chat_sessions = 0
        self.ahelp_sessions = 0
        self.processing_errors = []

    def add_error(self, error: str, context: str = ""):
        """Add a processing error with context."""
        self.processing_errors.append(f"{error} - {context}" if context else error)

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of processing statistics."""
        return {
            "total_messages": self.total_messages,
            "processed_messages": self.processed_messages,
            "invalid_messages": self.invalid_messages,
            "empty_embeds": self.empty_embeds,
            "chat_sessions": self.chat_sessions,
            "ahelp_sessions": self.ahelp_sessions,
            "processing_errors": len(self.processing_errors),
            "success_rate": round((self.processed_messages / max(self.total_messages, 1)) * 100, 2)
        }


DEFAULT_ADMIN_STATS: AdminStats = {
    "ahelps": 0,
    "mentions": 0,
    "role": "Unknown",
    "sessions": 0,
}


def validate_message_data(message: Dict[str, Any]) -> bool:
    """Validate message data structure."""
    required_fields = ['id', 'created_at', 'embeds']

    for field in required_fields:
        if field not in message:
            return False

    # Check if embeds is a list
    if not isinstance(message['embeds'], list):
        return False

    return True


def validate_embed_data(embed: Dict[str, Any]) -> bool:
    """Validate embed data structure."""
    return isinstance(embed, dict) and 'description' in embed


def process_embed_data_enhanced(
        embeds: List[Dict[str, Any]],
        message_datetime: Optional[datetime],
        processing_stats: Optional[ProcessingStats] = None
) -> Tuple[Dict[str, AdminStats], int, Dict[str, int], int, int]:
    """
    Enhanced version of embed processing with better error handling and validation.

    Returns:
        admin_stats, chat_count, daily_ahelps, total_ahelps, processed_ahelps
    """
    admin_stats: DefaultDict[str, AdminStats] = defaultdict(
        lambda: dict(DEFAULT_ADMIN_STATS.copy())
    )
    daily_ahelps: DefaultDict[str, int] = defaultdict(int)
    chat_count = 0
    total_ahelps_count = 0
    processed_ahelps_count = 0

    if not embeds:
        if processing_stats:
            processing_stats.empty_embeds += 1
        return dict(admin_stats), 0, dict(daily_ahelps), 0, 0

    try:
        for embed_idx, embed in enumerate(embeds):
            if not validate_embed_data(embed):
                if processing_stats:
                    processing_stats.add_error(f"Invalid embed structure at index {embed_idx}")
                continue

            description = embed.get("description", "")
            if not description:
                continue

            lines = description.split("\n")

            # Check if this is a chat session
            is_chat = any(
                ":inbox_tray:" in line or ":outbox_tray:" in line
                for line in lines
            )

            if not is_chat:
                continue

            # Process chat session
            admins_in_session: Set[str] = set()
            players_in_session: Set[str] = set()
            has_ahelp = False
            admin_responded = False

            for line_idx, line in enumerate(lines):
                try:
                    if ":outbox_tray:" in line:
                        name, role = extract_admin_info(line)
                        if name:
                            norm_name = normalize_admin_string(name)
                            admins_in_session.add(norm_name)

                            # Update role if we have better information
                            if role and role != "Unknown":
                                normalized_role = normalize_admin_string(role)
                                admin_stats[norm_name]["role"] = normalized_role

                        admin_responded = True

                    elif ":inbox_tray:" in line:
                        has_ahelp = True
                        player = extract_player_name(line)
                        if player:
                            players_in_session.add(player)

                        # Credit mentions to all admins in session (except the player if they're also an admin)
                        for admin in admins_in_session:
                            if admin != player:
                                admin_stats[admin]["mentions"] += 1

                except Exception as e:
                    if processing_stats:
                        processing_stats.add_error(
                            f"Error processing line {line_idx} in embed {embed_idx}: {str(e)}"
                        )
                    continue

            # Update session statistics
            if is_chat:
                chat_count += 1
                if processing_stats:
                    processing_stats.chat_sessions += 1

                for admin in admins_in_session:
                    admin_stats[admin]["sessions"] += 1

                    # Count ahelps handled (admin participated in ahelp session and wasn't the requester)
                    if has_ahelp and admin not in players_in_session:
                        admin_stats[admin]["ahelps"] += 1
                        if message_datetime:
                            daily_ahelps[admin] += 1

            # Track ahelp statistics
            if has_ahelp:
                total_ahelps_count += 1
                if processing_stats:
                    processing_stats.ahelp_sessions += 1

                if admin_responded:
                    processed_ahelps_count += 1

    except Exception as e:
        if processing_stats:
            processing_stats.add_error(f"Critical error in embed processing: {str(e)}")
        logging.error(f"Critical error in embed processing: {e}")

    return (
        dict(admin_stats),
        chat_count,
        dict(daily_ahelps),
        total_ahelps_count,
        processed_ahelps_count,
    )


def load_json_file_enhanced(file_path: str) -> Tuple[Optional[Any], Optional[str]]:
    """
    Enhanced JSON file loading with better error reporting.

    Returns:
        (data, error_message)
    """
    if not os.path.isfile(file_path):
        error_msg = f"File not found: {file_path}"
        logging.error(error_msg)
        return None, error_msg

    if not file_path.lower().endswith('.json'):
        error_msg = f"File is not JSON: {file_path}"
        logging.error(error_msg)
        return None, error_msg

    try:
        file_size = os.path.getsize(file_path)
        if file_size == 0:
            error_msg = f"File is empty: {file_path}"
            logging.warning(error_msg)
            return None, error_msg

        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        logging.info(f"Successfully loaded JSON file: {file_path} ({file_size:,} bytes)")
        return data, None

    except json.JSONDecodeError as e:
        error_msg = f"JSON decode error in file {file_path}: {e}"
        logging.error(error_msg)
        return None, error_msg

    except UnicodeDecodeError as e:
        error_msg = f"Unicode decode error in file {file_path}: {e}"
        logging.error(error_msg)
        return None, error_msg

    except Exception as e:
        error_msg = f"Unexpected error processing file {file_path}: {e}"
        logging.error(error_msg)
        return None, error_msg


def analyze_ahelp_data_enhanced(
        data: Any,
        server_name: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
) -> Tuple[ServerStats, ProcessingStats]:
    """
    Enhanced analysis with comprehensive error handling and statistics tracking.

    Returns:
        (server_stats, processing_stats)
    """
    admin_stats: DefaultDict[str, AdminStats] = defaultdict(lambda: DEFAULT_ADMIN_STATS.copy())
    total_chat_count = 0
    daily_ahelps: DefaultDict[date, DefaultDict[str, int]] = defaultdict(lambda: defaultdict(int))
    hourly_ahelps: DefaultDict[date, DefaultDict[int, Dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: {"total": 0, "processed": 0})
    )

    processing_stats = ProcessingStats()

    # Validate input data
    if not isinstance(data, list):
        error_msg = f"Data is not a list of messages for server {server_name}"
        logging.error(error_msg)
        processing_stats.add_error(error_msg)
        return {
            "admin_stats": {},
            "chat_count": 0,
            "daily_ahelps": {},
            "hourly_ahelps": {}
        }, processing_stats

    processing_stats.total_messages = len(data)
    logging.info(f"Analyzing {len(data)} messages from server {server_name}")

    # Filter messages by date range if specified
    filtered_data = []
    if start_date or end_date:
        for message in data:
            if not validate_message_data(message):
                processing_stats.invalid_messages += 1
                continue

            message_timestamp = message.get('created_at', '')
            msg_dt = parse_message_time(message_timestamp)

            if not msg_dt:
                processing_stats.invalid_messages += 1
                continue

            if start_date and msg_dt < start_date:
                continue
            if end_date and msg_dt > end_date:
                continue

            filtered_data.append(message)
    else:
        filtered_data = [msg for msg in data if validate_message_data(msg)]
        processing_stats.invalid_messages = len(data) - len(filtered_data)

    logging.info(f"Processing {len(filtered_data)} valid messages after filtering")

    # Process each message
    for message_idx, message in enumerate(filtered_data):
        try:
            embeds = message.get('embeds', [])
            if not embeds:
                processing_stats.empty_embeds += 1
                continue

            message_timestamp = message.get('created_at', '')
            msg_dt = parse_message_time(message_timestamp)

            if not msg_dt:
                processing_stats.add_error(
                    f"Invalid timestamp in message {message.get('id', 'unknown')}"
                )
                continue

            # Process embeds
            local_stats, local_chats, local_daily_ahelps, total_ahelps_count, processed_ahelps_count = \
                process_embed_data_enhanced(embeds, msg_dt, processing_stats)

            # Aggregate statistics
            for admin, stats in local_stats.items():
                admin_stats[admin]["ahelps"] += stats["ahelps"]
                admin_stats[admin]["mentions"] += stats["mentions"]
                admin_stats[admin]["sessions"] += stats["sessions"]

                # Update role information
                if stats["role"] != "Unknown":
                    admin_stats[admin]["role"] = normalize_admin_string(stats["role"])

            # Update daily statistics
            current_date = msg_dt.date()
            for admin, count in local_daily_ahelps.items():
                daily_ahelps[current_date][admin] += count

            # Update hourly statistics
            if total_ahelps_count > 0:
                current_hour = msg_dt.hour
                hourly_ahelps[current_date][current_hour]["total"] += total_ahelps_count
                hourly_ahelps[current_date][current_hour]["processed"] += processed_ahelps_count

            total_chat_count += local_chats
            processing_stats.processed_messages += 1

        except Exception as e:
            processing_stats.add_error(
                f"Error processing message {message_idx}: {str(e)}"
            )
            continue

    # Calculate final statistics
    total_ahelps = sum(stats['ahelps'] for stats in admin_stats.values())

    logging.info(f"Server {server_name} analysis complete:")
    logging.info(f"  - {total_chat_count} chat sessions")
    logging.info(f"  - {total_ahelps} ahelps handled")
    logging.info(f"  - {len(admin_stats)} unique administrators")
    logging.info(f"  - Success rate: {processing_stats.get_summary()['success_rate']}%")

    if processing_stats.processing_errors:
        logging.warning(f"  - {len(processing_stats.processing_errors)} processing errors occurred")

    server_stats: ServerStats = {
        "admin_stats": dict(admin_stats),
        "chat_count": total_chat_count,
        "daily_ahelps": {d: dict(admins) for d, admins in daily_ahelps.items()},
        "hourly_ahelps": {
            d: {h: vals.copy() for h, vals in hours.items()}
            for d, hours in hourly_ahelps.items()
        }
    }

    return server_stats, processing_stats


def merge_duplicate_admins_enhanced(admin_stats: Dict[str, AdminStats]) -> Dict[str, AdminStats]:
    """
    Enhanced admin merging with better conflict resolution.
    """
    merged_stats: DefaultDict[str, AdminStats] = defaultdict(lambda: DEFAULT_ADMIN_STATS.copy())
    merge_log = []

    for admin, stats in admin_stats.items():
        normalized_admin = normalize_admin_string(admin)

        # Track if we're merging duplicate entries
        if normalized_admin in merged_stats and merged_stats[normalized_admin] != DEFAULT_ADMIN_STATS:
            merge_log.append(f"Merging duplicate admin: '{admin}' -> '{normalized_admin}'")

        # Aggregate numeric statistics
        merged_stats[normalized_admin]["ahelps"] += stats["ahelps"]
        merged_stats[normalized_admin]["mentions"] += stats["mentions"]
        merged_stats[normalized_admin]["sessions"] += stats["sessions"]

        # Handle role conflicts - prefer more specific roles
        current_role = merged_stats[normalized_admin]["role"]
        new_role = stats["role"]

        if current_role == "Unknown" and new_role != "Unknown":
            merged_stats[normalized_admin]["role"] = normalize_admin_string(new_role)
        elif current_role != "Unknown" and new_role != "Unknown" and current_role != new_role:
            # Log role conflicts
            logging.warning(f"Role conflict for admin '{normalized_admin}': '{current_role}' vs '{new_role}'")
            # Keep the more specific role (longer string usually means more specific)
            if len(new_role) > len(current_role):
                merged_stats[normalized_admin]["role"] = normalize_admin_string(new_role)

    if merge_log:
        logging.info(f"Merged {len(merge_log)} duplicate admin entries")
        for log_entry in merge_log[:5]:  # Show first 5 merges
            logging.debug(log_entry)
        if len(merge_log) > 5:
            logging.debug(f"... and {len(merge_log) - 5} more merges")

    return dict(merged_stats)


def fill_missing_roles_enhanced(
        merged_admin_stats: Dict[str, AdminStats],
        servers_stats: Dict[str, ServerStats]
) -> int:
    """
    Enhanced role filling with better tracking.

    Returns:
        Number of roles filled
    """
    roles_filled = 0

    for admin, stats in merged_admin_stats.items():
        if stats["role"] == "Unknown":
            # Search for role information in server statistics
            for server_name, server_stats in servers_stats.items():
                server_admin_stats = server_stats["admin_stats"]
                if admin in server_admin_stats:
                    server_role = server_admin_stats[admin]["role"]
                    if server_role != "Unknown":
                        normalized_role = normalize_admin_string(server_role)
                        merged_admin_stats[admin]["role"] = normalized_role
                        roles_filled += 1
                        logging.debug(f"Filled role for admin '{admin}': {normalized_role} (from {server_name})")
                        break

    if roles_filled > 0:
        logging.info(f"Filled {roles_filled} missing admin roles from server data")

    return roles_filled


# Maintain backward compatibility
def process_embed_data(embeds, message_datetime):
    """Backward compatibility wrapper."""
    return process_embed_data_enhanced(embeds, message_datetime)


def load_json_file(file_path):
    """Backward compatibility wrapper."""
    data, error = load_json_file_enhanced(file_path)
    return data


def analyze_ahelp_data(data, server_name, start_date=None, end_date=None):
    """Backward compatibility wrapper."""
    server_stats, _ = analyze_ahelp_data_enhanced(data, server_name, start_date, end_date)
    return server_stats


def merge_duplicate_admins(admin_stats):
    """Backward compatibility wrapper."""
    return merge_duplicate_admins_enhanced(admin_stats)


def fill_missing_roles(merged_admin_stats, servers_stats):
    """Backward compatibility wrapper."""
    fill_missing_roles_enhanced(merged_admin_stats, servers_stats)