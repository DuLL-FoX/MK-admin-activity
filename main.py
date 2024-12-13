import os
from collections import defaultdict
from typing import Tuple, Dict

from data_processing import load_json_file, AdminStats, ServerStats, analyze_ahelp_data
from download import client, USER_TOKEN, DATA_FOLDER
from excel_exporter import save_all_data_to_excel
from utils import extract_server_name, configure_logging


def aggregate_global_stats(
        files: list[str]
) -> Tuple[Dict[str, AdminStats], int, Dict[str, ServerStats]]:
    global_admin_stats = defaultdict(lambda: {"ahelps": 0, "mentions": 0, "role": "Не указано", "sessions": 0})
    global_chat_count = 0
    servers_stats = {}

    for file_path in files:
        data = load_json_file(file_path)
        if not data:
            continue

        server_name = extract_server_name(file_path)
        server_stats = analyze_ahelp_data(data, server_name)
        servers_stats[server_name] = server_stats

        for admin, stats in server_stats["admin_stats"].items():
            global_admin_stats[admin]["ahelps"] += stats["ahelps"]
            global_admin_stats[admin]["mentions"] += stats["mentions"]
            if global_admin_stats[admin]["role"] == "Не указано" and stats["role"] != "Не указано":
                global_admin_stats[admin]["role"] = stats["role"]
            global_admin_stats[admin]["sessions"] += stats["sessions"]

        global_chat_count += server_stats["chat_count"]

    return global_admin_stats, global_chat_count, servers_stats


def get_downloaded_files(data_folder: str) -> list[str]:
    return [os.path.join(data_folder, f) for f in os.listdir(data_folder) if
            os.path.isfile(os.path.join(data_folder, f)) and f.endswith('.json')]


def main() -> None:
    configure_logging()

    client.run(USER_TOKEN, bot=False)

    files = get_downloaded_files(DATA_FOLDER)

    (global_admin_stats,
     global_chat_count,
     servers_stats) = aggregate_global_stats(files)

    save_all_data_to_excel(
        global_admin_stats,
        global_chat_count,
        servers_stats,
    )


if __name__ == "__main__":
    main()
