import logging
import os
import traceback
from collections import defaultdict
from datetime import date, datetime
from typing import Dict, List, Optional, DefaultDict, Any, Tuple

import pandas as pd
from openpyxl import Workbook
from openpyxl.reader.excel import load_workbook
from openpyxl.styles import (
    Alignment, Font, PatternFill, Border, Side, NamedStyle
)
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.cell.cell import MergedCell

from data_processing import AdminStats, ServerStats, merge_duplicate_admins, fill_missing_roles
from utils import clean_sheet_name, normalize_admin_string

HEADER_FILL = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
HEADER_FONT = Font(bold=True, color="FFFFFF", size=12)
ALT_ROW_FILL = PatternFill(start_color="E6F0FF", end_color="E6F0FF", fill_type="solid")
MODERATOR_FILL = PatternFill(start_color="FFD966", end_color="FFD966", fill_type="solid")
HIGHLIGHT_FILL = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
BORDER = Border(
    left=Side(style='thin', color='000000'),
    right=Side(style='thin', color='000000'),
    top=Side(style='thin', color='000000'),
    bottom=Side(style='thin', color='000000')
)


def clean_server_name(srv_name: str) -> str:
    prefix = "🤔┇ahelp-"
    if srv_name.startswith(prefix):
        srv_name = srv_name[len(prefix):]
    srv_name = srv_name.strip("_")
    srv_name = srv_name.replace("_", "-")
    return srv_name


def setup_worksheet_styles(workbook: Workbook) -> None:
    header_style = NamedStyle(name="header_style")
    header_style.font = HEADER_FONT
    header_style.fill = HEADER_FILL
    header_style.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    header_style.border = BORDER

    cell_style = NamedStyle(name="cell_style")
    cell_style.alignment = Alignment(horizontal="center", vertical="center")
    cell_style.border = BORDER

    alt_row_style = NamedStyle(name="alt_row_style")
    alt_row_style.fill = ALT_ROW_FILL
    alt_row_style.alignment = Alignment(horizontal="center", vertical="center")
    alt_row_style.border = BORDER

    for style in [header_style, cell_style, alt_row_style]:
        if style.name not in workbook.named_styles:
            workbook.add_named_style(style)


def adjust_column_widths(worksheet: Worksheet) -> None:
    column_widths = {
        'A': 50,  # Administrator/Server name
        'B': 50,  # Role
        'C': 15,  # Ahelps/Total
        'D': 15,  # Mentions/Processed
        'E': 15  # Sessions/Percentage
    }

    for i in range(6, 27):
        col_letter = get_column_letter(i)
        column_widths[col_letter] = 15

    for col_letter, width in column_widths.items():
        try:
            worksheet.column_dimensions[col_letter].width = width
        except:
            pass


def apply_worksheet_formatting(
        worksheet: Worksheet,
        has_header: bool = True,
        highlight_moderators: bool = False,
        highlight_column: Optional[int] = None
) -> None:
    if has_header:
        for cell in worksheet[1]:
            if not isinstance(cell, MergedCell):
                cell.style = "header_style"

    start_row = 2 if has_header else 1
    for row_idx, row in enumerate(worksheet.iter_rows(min_row=start_row), start=start_row):
        style_name = "alt_row_style" if row_idx % 2 == 0 else "cell_style"

        for cell in row:
            if isinstance(cell, MergedCell):
                continue

            cell.style = style_name

            if highlight_column and cell.column == highlight_column:
                cell.fill = HIGHLIGHT_FILL

        if highlight_moderators and len(row) > 1 and not isinstance(row[1], MergedCell):
            role_cell = row[1]
            role_value = str(role_cell.value).lower() if role_cell.value else ""
            if role_value and ("модератор" in role_value or "гейм-мастер" in role_value):
                for cell in row:
                    if not isinstance(cell, MergedCell):
                        cell.fill = MODERATOR_FILL


def add_metadata_to_worksheet(
        worksheet: Worksheet,
        title: str,
        description: str = "",
        data_range: str = "",
        server_name: str = ""
) -> None:
    worksheet.insert_rows(1, 4)

    # Add title
    worksheet.cell(1, 1, title)
    worksheet.cell(1, 1).font = Font(bold=True, size=16)
    worksheet.merge_cells(start_row=1, start_column=1, end_row=1, end_column=5)

    if server_name:
        worksheet.cell(2, 1, f"Server: {server_name}")
        worksheet.cell(2, 1).font = Font(bold=True)

    if description:
        worksheet.cell(3, 1, description)

    if data_range:
        worksheet.cell(3, 3, f"Data range: {data_range}")

    worksheet.cell(4, 1, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    worksheet.row_dimensions[5].height = 10


def write_df_to_excel_enhanced(
        df: pd.DataFrame,
        excel_filename: str,
        sheet_name: str,
        highlight_moderators: bool = False,
        global_sheet: bool = False,
        moderators_sheet: bool = False,
        sort_column: Optional[str] = None,
        ascending: bool = False,
        metadata: Optional[Dict[str, str]] = None,
) -> None:
    try:
        sheet_name = clean_sheet_name(sheet_name)

        if sort_column and sort_column in df.columns:
            df = df.sort_values(by=sort_column, ascending=ascending)

        if not os.path.exists(excel_filename):
            wb = Workbook()
            if "Sheet" in wb.sheetnames:
                del wb["Sheet"]
            setup_worksheet_styles(wb)
        else:
            wb = load_workbook(excel_filename)

        if sheet_name in wb.sheetnames:
            del wb[sheet_name]

        ws = wb.create_sheet(title=sheet_name)

        if metadata:
            add_metadata_to_worksheet(
                ws,
                title=metadata.get('title', sheet_name),
                description=metadata.get('description', ''),
                data_range=metadata.get('data_range', ''),
                server_name=metadata.get('server_name', '')
            )
            start_row = 6
        else:
            start_row = 1

        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), start_row):
            for c_idx, value in enumerate(row, 1):
                cell = ws.cell(row=r_idx, column=c_idx, value=value)

        highlight_column = None
        if sort_column:
            try:
                highlight_column = df.columns.get_loc(sort_column) + 1
            except:
                pass

        apply_worksheet_formatting(
            ws,
            has_header=True,
            highlight_moderators=highlight_moderators,
            highlight_column=highlight_column
        )

        if len(df) > 0:
            header_row = start_row
            last_row = start_row + len(df)
            last_col = len(df.columns)
            filter_range = f"A{header_row}:{get_column_letter(last_col)}{last_row}"
            try:
                ws.auto_filter.ref = filter_range
            except:
                logging.warning(f"Could not set filter for range {filter_range}")

        adjust_column_widths(ws)

        try:
            ws.freeze_panes = ws.cell(start_row + 1, 1)
        except:
            logging.warning("Could not freeze panes")

        wb.save(excel_filename)
    except Exception as e:
        logging.error(f"Error in write_df_to_excel_enhanced: {e}")
        logging.error(traceback.format_exc())
        raise


def create_daily_ahelps_dataframe(daily_ahelps: DefaultDict[date, Dict[str, int]]) -> pd.DataFrame:
    if not daily_ahelps:
        return pd.DataFrame()

    all_dates = sorted(daily_ahelps.keys())

    normalized_daily_ahelps: DefaultDict[date, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for dt, admin_counts in daily_ahelps.items():
        for admin, count in admin_counts.items():
            normalized_admin = normalize_admin_string(admin)
            normalized_daily_ahelps[dt][normalized_admin] += count

    all_admins = set()
    for daily_data in normalized_daily_ahelps.values():
        all_admins.update(daily_data.keys())
    all_admins = sorted(list(all_admins))

    data = []
    for admin in all_admins:
        admin_row = [admin]
        total_ahelps = 0
        for day in all_dates:
            count = normalized_daily_ahelps[day].get(admin, 0)
            total_ahelps += count
            admin_row.append(count)
        admin_row.append(total_ahelps)
        data.append(admin_row)

    df = pd.DataFrame(
        data,
        columns=["Administrator"] + [day.strftime("%Y-%m-%d") for day in all_dates] + ["Total"]
    )
    return df.sort_values(by="Total", ascending=False)


def aggregate_daily_ahelps(servers_stats: Dict[str, ServerStats]) -> DefaultDict[date, Dict[str, int]]:
    global_daily_ahelps: DefaultDict[date, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for server_stats in servers_stats.values():
        for day, daily_data in server_stats["daily_ahelps"].items():
            for admin, count in daily_data.items():
                global_daily_ahelps[day][admin] += count

    return global_daily_ahelps


def create_hourly_ahelps_dataframe(hourly_ahelps: Dict[date, Dict[Any, Dict[str, int]]]) -> pd.DataFrame:
    rows = []
    for d, hours_data in hourly_ahelps.items():
        for h, vals in hours_data.items():
            try:
                hour = int(h)
            except (ValueError, TypeError):
                logging.warning(f"Converting hour {h} to integer")
                try:
                    hour = int(float(h))
                except:
                    logging.error(f"Cannot convert hour to integer: {h}, type: {type(h)}")
                    hour = 0

            total = vals["total"]
            processed = vals["processed"]
            percentage = round(processed / total * 100, 1) if total > 0 else 0

            rows.append([
                d.strftime("%Y-%m-%d"),
                hour,
                total,
                processed,
                percentage
            ])

    if not rows:
        return pd.DataFrame(columns=["Date", "Hour", "Total Ahelps", "Processed", "Processed %"])

    df = pd.DataFrame(
        rows,
        columns=["Date", "Hour", "Total Ahelps", "Processed", "Processed %"]
    )
    return df.sort_values(by=["Date", "Hour"])


def aggregate_hourly_ahelps(servers_stats: Dict[str, ServerStats]) -> Dict[date, Dict[int, Dict[str, int]]]:
    global_hourly_ahelps = defaultdict(lambda: defaultdict(lambda: {"total": 0, "processed": 0}))

    for server_stats in servers_stats.values():
        for d, hours_data in server_stats["hourly_ahelps"].items():
            for h, vals in hours_data.items():
                try:
                    hour = int(h)
                except (ValueError, TypeError):
                    logging.warning(f"Converting hour {h} to integer")
                    try:
                        hour = int(float(h))
                    except:
                        logging.error(f"Cannot convert hour to integer: {h}, type: {type(h)}")
                        hour = 0

                global_hourly_ahelps[d][hour]["total"] += vals["total"]
                global_hourly_ahelps[d][hour]["processed"] += vals["processed"]

    result = {}
    for d, hours in global_hourly_ahelps.items():
        result[d] = {}
        for h, vals in hours.items():
            result[d][h] = vals.copy()

    return result


def create_summary_dataframe(
        global_admin_stats: Dict[str, AdminStats],
        global_chat_count: int,
        servers_stats: Dict[str, ServerStats]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    server_data = []

    for server_name, stats in servers_stats.items():
        admin_count = len(stats["admin_stats"])
        ahelp_count = sum(a["ahelps"] for a in stats["admin_stats"].values())
        mention_count = sum(a["mentions"] for a in stats["admin_stats"].values())
        moderator_count = sum(1 for a in stats["admin_stats"].values() if
                              "модератор" in a["role"].lower() or "гейм-мастер" in a["role"].lower())

        server_data.append([
            clean_server_name(server_name),
            stats["chat_count"],
            ahelp_count,
            admin_count,
            moderator_count,
            mention_count
        ])

    server_df = pd.DataFrame(
        server_data,
        columns=["Server", "Chats", "Ahelps", "Admins", "Moderators", "Mentions"]
    )

    normalized_global_admin_stats = {}
    for admin, stats in global_admin_stats.items():
        normalized_role = normalize_admin_string(stats["role"])
        normalized_stats = stats.copy()
        normalized_stats["role"] = normalized_role
        normalized_global_admin_stats[admin] = normalized_stats

    role_counts = defaultdict(lambda: {"count": 0, "ahelps": 0, "mentions": 0, "sessions": 0})

    for admin, stats in normalized_global_admin_stats.items():
        role = stats["role"]
        role_counts[role]["count"] += 1
        role_counts[role]["ahelps"] += stats["ahelps"]
        role_counts[role]["mentions"] += stats["mentions"]
        role_counts[role]["sessions"] += stats["sessions"]

    role_data = []
    for role, counts in role_counts.items():
        role_data.append([
            role,
            counts["count"],
            counts["ahelps"],
            counts["mentions"],
            counts["sessions"]
        ])

    role_df = pd.DataFrame(
        role_data,
        columns=["Role", "Admin Count", "Total Ahelps", "Total Mentions", "Total Sessions"]
    )

    return server_df, role_df


def save_all_data_to_excel(
        global_admin_stats: Dict[str, AdminStats],
        global_chat_count: int,
        servers_stats: Dict[str, ServerStats],
) -> None:
    try:
        excel_filename = os.getenv("EXCEL_FILENAME", "ahelp_stats.xlsx")

        if os.path.exists(excel_filename):
            os.remove(excel_filename)

        all_dates = []
        for server_stats in servers_stats.values():
            for day in server_stats["daily_ahelps"]:
                all_dates.append(day)

        date_range = ""
        if all_dates:
            min_date = min(all_dates).strftime("%Y-%m-%d")
            max_date = max(all_dates).strftime("%Y-%m-%d")
            date_range = f"{min_date} to {max_date}"

        server_summary_df, role_summary_df = create_summary_dataframe(
            global_admin_stats, global_chat_count, servers_stats
        )

        write_df_to_excel_enhanced(
            server_summary_df,
            excel_filename,
            "Summary",
            metadata={
                'title': 'Server Activity Summary',
                'description': 'Overview of all servers and their activity',
                'data_range': date_range
            },
            sort_column="Ahelps",
            ascending=False
        )

        write_df_to_excel_enhanced(
            role_summary_df,
            excel_filename,
            "Roles_Summary",
            metadata={
                'title': 'Admin Roles Summary',
                'description': 'Summary of admin activity by role',
                'data_range': date_range
            },
            sort_column="Total Ahelps",
            ascending=False
        )

        merged_global = merge_duplicate_admins(global_admin_stats)
        fill_missing_roles(merged_global, servers_stats)

        admin_server_ahelps = defaultdict(dict)
        server_names = sorted(servers_stats.keys())
        cleaned_server_names = [clean_server_name(srv) for srv in server_names]

        for server_name, sstats in servers_stats.items():
            merged_server_stats = merge_duplicate_admins(sstats["admin_stats"])
            for admin, stats in merged_server_stats.items():
                admin_server_ahelps[admin][clean_server_name(server_name)] = stats['ahelps']

        global_data = []
        for admin, stats in merged_global.items():
            row = [admin, stats['role'], stats['ahelps'], stats['mentions'], stats['sessions']]
            for srv in cleaned_server_names:
                row.append(admin_server_ahelps[admin].get(srv, 0))
            global_data.append(row)

        global_columns = [
                             "Administrator", "Role", "Total Ahelps", "Mentions", "Sessions"
                         ] + cleaned_server_names

        df_global = pd.DataFrame(global_data, columns=global_columns)

        write_df_to_excel_enhanced(
            df_global,
            excel_filename,
            "Admins_Global",
            highlight_moderators=True,
            global_sheet=True,
            metadata={
                'title': 'Global Admin Statistics',
                'description': 'Overview of all administrators across all servers',
                'data_range': date_range
            },
            sort_column="Total Ahelps",
            ascending=False
        )

        moderator_data = []
        for admin, stats in merged_global.items():
            if stats['role'] != "Unknown" and any(
                    keyword in stats["role"].lower() for keyword in ["модератор", "гейм-мастер"]
            ):
                row = [admin, stats['role'], stats['ahelps'], stats['mentions'], stats['sessions']]
                for srv in cleaned_server_names:
                    row.append(admin_server_ahelps[admin].get(srv, 0))
                moderator_data.append(row)

        df_moderators = pd.DataFrame(moderator_data, columns=global_columns)

        write_df_to_excel_enhanced(
            df_moderators,
            excel_filename,
            "Moderators",
            moderators_sheet=True,
            metadata={
                'title': 'Moderator Statistics',
                'description': 'Activity statistics for moderators only',
                'data_range': date_range
            },
            sort_column="Total Ahelps",
            ascending=False
        )

        for server_name, stats in servers_stats.items():
            clean_name = clean_server_name(server_name)

            daily_ahelps_data = create_daily_ahelps_dataframe(stats["daily_ahelps"])
            if not daily_ahelps_data.empty:
                write_df_to_excel_enhanced(
                    daily_ahelps_data,
                    excel_filename,
                    f"{clean_name}_Daily",
                    metadata={
                        'title': f'Daily Ahelps - {clean_name}',
                        'description': 'Daily ahelp counts per administrator',
                        'data_range': date_range,
                        'server_name': clean_name
                    },
                    sort_column="Total",
                    ascending=False
                )

            hourly_df = create_hourly_ahelps_dataframe(stats["hourly_ahelps"])
            if not hourly_df.empty:
                write_df_to_excel_enhanced(
                    hourly_df,
                    excel_filename,
                    f"{clean_name}_Hourly",
                    metadata={
                        'title': f'Hourly Ahelps - {clean_name}',
                        'description': 'Hourly ahelp statistics',
                        'data_range': date_range,
                        'server_name': clean_name
                    }
                )

        global_daily_ahelps = aggregate_daily_ahelps(servers_stats)
        global_daily_ahelps_df = create_daily_ahelps_dataframe(global_daily_ahelps)
        if not global_daily_ahelps_df.empty:
            write_df_to_excel_enhanced(
                global_daily_ahelps_df,
                excel_filename,
                "Daily_Global",
                metadata={
                    'title': 'Global Daily Ahelps',
                    'description': 'Daily ahelp counts across all servers',
                    'data_range': date_range
                },
                sort_column="Total",
                ascending=False
            )

        global_hourly_ahelps = aggregate_hourly_ahelps(servers_stats)
        global_hourly_ahelps_df = create_hourly_ahelps_dataframe(global_hourly_ahelps)
        if not global_hourly_ahelps_df.empty:
            write_df_to_excel_enhanced(
                global_hourly_ahelps_df,
                excel_filename,
                "Hourly_Global",
                metadata={
                    'title': 'Global Hourly Ahelps',
                    'description': 'Hourly ahelp statistics across all servers',
                    'data_range': date_range
                }
            )

        logging.info(f"All data saved to {excel_filename}")
    except Exception as e:
        logging.error(f"Error saving data to Excel: {e}")
        logging.error(traceback.format_exc())
        raise