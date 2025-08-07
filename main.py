import argparse
import os
import sys
from collections import defaultdict
from typing import Tuple, Dict, List, Optional
from datetime import datetime, timedelta

import dotenv
from tqdm import tqdm

from data_processing import load_json_file, AdminStats, ServerStats, analyze_ahelp_data
from download import main as download_main
from excel_exporter import save_all_data_to_excel
from utils import extract_server_name, configure_logging, format_date_range


def aggregate_global_stats(
        files: List[str],
        progress_bar: Optional[tqdm] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
) -> Tuple[Dict[str, AdminStats], int, Dict[str, ServerStats]]:
    global_admin_stats = defaultdict(lambda: {
        "ahelps": 0,
        "mentions": 0,
        "role": "Unknown",
        "sessions": 0,
        "admin_only_ahelps": 0,
        "admin_only_mentions": 0,
        "admin_only_sessions": 0
    })
    global_chat_count = 0
    servers_stats = {}

    for i, file_path in enumerate(files):
        if progress_bar:
            progress_bar.set_description(f"Processing {os.path.basename(file_path)}")

        data = load_json_file(file_path)
        if not data:
            continue

        if start_date or end_date:
            filtered_data = []
            for message in data:
                created_at = message.get('created_at', '')
                if not created_at:
                    continue

                try:
                    msg_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))

                    if start_date and msg_dt < start_date:
                        continue
                    if end_date and msg_dt > end_date:
                        continue

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
    parser = argparse.ArgumentParser(description="Discord Ahelp Statistics Analyzer")

    parser.add_argument(
        "--download",
        action="store_true",
        help="Download messages before analysis"
    )

    parser.add_argument(
        "--data-folder",
        type=str,
        help="Folder containing JSON files (default: from .env)"
    )

    parser.add_argument(
        "--output",
        type=str,
        help="Output Excel filename (default: from .env)"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for report (format: YYYY-MM-DD)"
    )

    parser.add_argument(
        "--end-date",
        type=str,
        help="Start date for report (format: YYYY-MM-DD)"
    )

    parser.add_argument(
        "--days",
        type=int,
        help="Number of days to include in report (from today backwards)"
    )

    return parser.parse_args()


def main() -> int:
    args = parse_arguments()

    dotenv_path = ".env"
    if not os.path.exists(dotenv_path):
        print(f"Error: .env file not found. Please create one based on .env.example")
        return 1

    dotenv.load_dotenv(dotenv_path)

    log_level = logging.INFO if not args.verbose else logging.DEBUG
    configure_logging(level=log_level)

    start_date = None
    end_date = None

    if args.days:
        start_date = datetime.now() - timedelta(days=args.days)
        logging.info(f"Setting report period to last {args.days} days (from {start_date.strftime('%Y-%m-%d')})")

    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
            logging.info(f"Setting report start date to {args.start_date}")
        except ValueError:
            logging.error(f"Invalid start date format: {args.start_date}. Using YYYY-MM-DD format.")
            return 1

    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d") + timedelta(days=1)
            logging.info(f"Setting report end date to {args.end_date}")
        except ValueError:
            logging.error(f"Invalid end date format: {args.end_date}. Using YYYY-MM-DD format.")
            return 1

    data_folder = args.data_folder or os.getenv("DATA_FOLDER", "data")
    excel_filename = args.output or os.getenv("EXCEL_FILENAME", "ahelp_stats.xlsx")

    if args.download:
        logging.info("Downloading messages...")
        try:
            download_main(start_date=start_date, end_date=end_date)
        except Exception as e:
            logging.error(f"Error downloading messages: {e}")
            return 1

    files = get_downloaded_files(data_folder)
    if not files:
        logging.error(f"No JSON files found in {data_folder}")
        return 1

    logging.info(f"Found {len(files)} JSON files to process")

    with tqdm(total=len(files), desc="Processing files") as pbar:
        global_admin_stats, global_chat_count, servers_stats = aggregate_global_stats(
            files, pbar, start_date, end_date
        )

    total_ahelps = sum(stats["ahelps"] for stats in global_admin_stats.values())
    total_admin_only_ahelps = sum(stats["admin_only_ahelps"] for stats in global_admin_stats.values())
    total_admins = len(global_admin_stats)

    logging.info(f"Analysis complete: {total_ahelps} regular ahelps, {total_admin_only_ahelps} admin-only ahelps, {global_chat_count} chats, {total_admins} admins")

    all_dates = []
    for server_stats in servers_stats.values():
        for day in server_stats["daily_ahelps"]:
            all_dates.append(day)

    date_range = ""
    if all_dates:
        min_date = min(all_dates).strftime("%Y-%m-%d")
        max_date = max(all_dates).strftime("%Y-%m-%d")
        date_range = format_date_range(min_date, max_date)
        logging.info(f"Data range: {date_range}")

    logging.info(f"Saving data to {excel_filename}...")
    try:
        save_all_data_to_excel(
            global_admin_stats,
            global_chat_count,
            servers_stats,
        )
        logging.info(f"Data successfully saved to {excel_filename}")
    except Exception as e:
        logging.error(f"Error saving data to Excel: {e}")
        return 1

    return 0


if __name__ == "__main__":
    import logging

    sys.exit(main())