import logging
import os
from collections import defaultdict
from datetime import date
from typing import Dict

import pandas as pd

from data_processing import AdminStats, ServerStats, merge_duplicate_admins, fill_missing_roles
from utils import clean_sheet_name


def write_df_to_excel(
        df: pd.DataFrame,
        excel_filename: str,
        sheet_name: str
) -> None:
    sheet_name = clean_sheet_name(sheet_name)
    if os.path.exists(excel_filename):
        with pd.ExcelWriter(excel_filename, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    else:
        with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)


def create_daily_ahelps_dataframe(daily_ahelps: defaultdict[date, Dict[str, int]]) -> pd.DataFrame:
    if not daily_ahelps:
        return pd.DataFrame()

    all_dates = sorted(daily_ahelps.keys())
    all_admins = set()
    for daily_data in daily_ahelps.values():
        all_admins.update(daily_data.keys())
    all_admins = sorted(list(all_admins))

    data = []
    for admin in all_admins:
        admin_row = [admin]
        for day in all_dates:
            admin_row.append(daily_ahelps[day].get(admin, 0))
        data.append(admin_row)

    df = pd.DataFrame(data, columns=["Администратор"] + [day.strftime("%Y-%m-%d") for day in all_dates])
    return df


def aggregate_daily_ahelps(servers_stats: Dict[str, ServerStats]) -> defaultdict[date, Dict[str, int]]:
    global_daily_ahelps: defaultdict[date, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for server_stats in servers_stats.values():
        for day, daily_data in server_stats["daily_ahelps"].items():
            for admin, count in daily_data.items():
                global_daily_ahelps[day][admin] += count

    return global_daily_ahelps


def save_all_data_to_excel(
        global_admin_stats: Dict[str, AdminStats],
        global_chat_count: int,
        servers_stats: Dict[str, ServerStats],
) -> None:
    excel_filename = "united_stats.xlsx"
    if os.path.exists(excel_filename):
        os.remove(excel_filename)

    merged_global = merge_duplicate_admins(global_admin_stats)
    fill_missing_roles(merged_global, servers_stats)

    admin_server_ahelps = defaultdict(dict)
    server_names = sorted(servers_stats.keys())

    for server_name, sstats in servers_stats.items():
        merged_server_stats = merge_duplicate_admins(sstats["admin_stats"])
        for admin, stats in merged_server_stats.items():
            admin_server_ahelps[admin][server_name] = stats['ahelps']

    global_data = []
    for admin, stats in merged_global.items():
        row = [admin, stats['role'], stats['ahelps']]
        for srv in server_names:
            row.append(admin_server_ahelps[admin].get(srv, 0))
        global_data.append(row)

    global_columns = ["Администратор", "Роль", "Ahelps"] + [f"Ahelps_{srv}" for srv in server_names]
    df_global = pd.DataFrame(global_data, columns=global_columns)
    df_global = df_global.sort_values(by=["Ahelps"], ascending=False)
    write_df_to_excel(df_global, excel_filename, "Global")

    moderator_data = []
    for admin, stats in merged_global.items():
        if stats['role'] != "Не указано" and ("Модератор" in stats["role"] or
                                              "Гейм-Мастер" in stats["role"] or
                                              "модератор" in stats["role"] or
                                              "гейм-мастер" in stats["role"]):
            row = [admin, stats['role'], stats['ahelps']]
            for srv in server_names:
                row.append(admin_server_ahelps[admin].get(srv, 0))
            moderator_data.append(row)

    df_moderators = pd.DataFrame(moderator_data, columns=global_columns)
    df_moderators = df_moderators.sort_values(by=["Ahelps"], ascending=False)
    write_df_to_excel(df_moderators, excel_filename, "Moderators")

    for server_name, stats in servers_stats.items():
        daily_ahelps_data = create_daily_ahelps_dataframe(stats["daily_ahelps"])
        write_df_to_excel(daily_ahelps_data, excel_filename, f"Daily_Ahelps_{server_name}")

    global_daily_ahelps = aggregate_daily_ahelps(servers_stats)
    global_daily_ahelps_df = create_daily_ahelps_dataframe(global_daily_ahelps)
    write_df_to_excel(global_daily_ahelps_df, excel_filename, "Daily_Ahelps_Global")

    logging.info(f"Сохранено в {excel_filename}")
