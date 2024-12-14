import logging
import os
from collections import defaultdict
from datetime import date
from typing import Dict, List

import pandas as pd
from openpyxl import Workbook
from openpyxl.reader.excel import load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils.dataframe import dataframe_to_rows

from data_processing import AdminStats, ServerStats, merge_duplicate_admins, fill_missing_roles
from utils import clean_sheet_name

def clean_server_name(srv_name: str) -> str:
    prefix = "🤔┇ahelp-"
    if srv_name.startswith(prefix):
        srv_name = srv_name[len(prefix):]
    srv_name = srv_name.strip("_")
    srv_name = srv_name.replace("_", "-")
    return srv_name

def write_df_to_excel_enhanced(
    df: pd.DataFrame,
    excel_filename: str,
    sheet_name: str,
    highlight_moderators: bool = False,
    global_sheet: bool = False,
    moderators_sheet: bool = False,
) -> None:
    sheet_name = clean_sheet_name(sheet_name)

    if not os.path.exists(excel_filename):
        wb = Workbook()
        if "Sheet" in wb.sheetnames:
            del wb["Sheet"]
    else:
        wb = load_workbook(excel_filename)

    if sheet_name in wb.sheetnames:
        del wb[sheet_name]

    ws = wb.create_sheet(title=sheet_name)

    for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
        for c_idx, value in enumerate(row, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=value)
            cell.alignment = Alignment(horizontal="center")

            if r_idx == 1:
                cell.font = Font(bold=True)
                cell.fill = PatternFill(start_color="D3D3D3", end_color="D3D3D3", fill_type="solid")

            if highlight_moderators and global_sheet and r_idx > 1:
                role_cell = ws.cell(row=r_idx, column=2)
                role_value = role_cell.value
                if role_value and ("Модератор" in role_value or "Гейм-Мастер" in role_value or
                                  "модератор" in role_value or "гейм-мастер" in role_value):
                    for c in range(1, len(row) + 1):
                        ws.cell(row=r_idx, column=c).fill = PatternFill(start_color="FFFFE0", end_color="FFFFE0", fill_type="solid")

    ws.auto_filter.ref = ws.dimensions
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = (max_length + 2)
        ws.column_dimensions[column].width = adjusted_width

    wb.save(excel_filename)

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

def create_hourly_ahelps_dataframe(hourly_ahelps: Dict[date, Dict[int, Dict[str, int]]]) -> pd.DataFrame:
    rows = []
    for d, hours_data in hourly_ahelps.items():
        for h, vals in hours_data.items():
            rows.append([d.strftime("%Y-%m-%d"), h, vals["total"], vals["processed"]])

    df = pd.DataFrame(rows, columns=["Дата", "Час", "Всего Ахелпов", "Обработано Админами"])
    df = df.sort_values(by=["Дата", "Час"])
    return df

def aggregate_hourly_ahelps(servers_stats: Dict[str, ServerStats]) -> Dict[date, Dict[int, Dict[str, int]]]:
    global_hourly_ahelps = defaultdict(lambda: defaultdict(lambda: {"total": 0, "processed": 0}))
    for server_stats in servers_stats.values():
        for d, hours_data in server_stats["hourly_ahelps"].items():
            for h, vals in hours_data.items():
                global_hourly_ahelps[d][h]["total"] += vals["total"]
                global_hourly_ahelps[d][h]["processed"] += vals["processed"]
    return {d: {h: vals for h, vals in hours.items()} for d, hours in global_hourly_ahelps.items()}

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

    cleaned_server_names = [clean_server_name(srv) for srv in server_names]

    for server_name, sstats in servers_stats.items():
        merged_server_stats = merge_duplicate_admins(sstats["admin_stats"])
        for admin, stats in merged_server_stats.items():
            admin_server_ahelps[admin][server_name] = stats['ahelps']

    global_data: List[List] = []
    for admin, stats in merged_global.items():
        row = [admin, stats['role'], stats['ahelps']]
        for srv in server_names:
            row.append(admin_server_ahelps[admin].get(srv, 0))
        global_data.append(row)

    global_columns = ["Администратор", "Роль", "Ahelps"] + cleaned_server_names
    df_global = pd.DataFrame(global_data, columns=global_columns)
    df_global = df_global.sort_values(by=["Ahelps"], ascending=False)
    write_df_to_excel_enhanced(df_global, excel_filename, "Global", highlight_moderators=True, global_sheet=True)

    moderator_data = []
    for admin, stats in merged_global.items():
        if stats['role'] != "Не указано" and any(
            keyword in stats["role"].lower() for keyword in ["модератор", "гейм-мастер"]
        ):
            row = [admin, stats['role'], stats['ahelps']]
            for srv in server_names:
                row.append(admin_server_ahelps[admin].get(srv, 0))
            moderator_data.append(row)

    df_moderators = pd.DataFrame(moderator_data, columns=global_columns)
    df_moderators = df_moderators.sort_values(by=["Ahelps"], ascending=False)
    write_df_to_excel_enhanced(df_moderators, excel_filename, "Moderators", moderators_sheet=True)

    for server_name, stats in servers_stats.items():
        daily_ahelps_data = create_daily_ahelps_dataframe(stats["daily_ahelps"])
        write_df_to_excel_enhanced(daily_ahelps_data, excel_filename, clean_server_name(server_name) + "_Daily_Ahelps")

        hourly_df = create_hourly_ahelps_dataframe(stats["hourly_ahelps"])
        write_df_to_excel_enhanced(hourly_df, excel_filename, clean_server_name(server_name) + "_Hourly_Ahelps")

    global_daily_ahelps = aggregate_daily_ahelps(servers_stats)
    global_daily_ahelps_df = create_daily_ahelps_dataframe(global_daily_ahelps)
    write_df_to_excel_enhanced(global_daily_ahelps_df, excel_filename, "Daily_Ahelps_Global")

    global_hourly_ahelps = aggregate_hourly_ahelps(servers_stats)
    global_hourly_ahelps_df = create_hourly_ahelps_dataframe(global_hourly_ahelps)
    write_df_to_excel_enhanced(global_hourly_ahelps_df, excel_filename, "Hourly_Ahelps_Global")

    logging.info(f"Сохранено в {excel_filename}")