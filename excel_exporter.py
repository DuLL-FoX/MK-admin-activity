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

COLORS = {
    'primary_blue': "2E75B6",
    'secondary_blue': "5B9BD5",
    'light_blue': "BDD7EE",
    'accent_green': "70AD47",
    'light_green': "C6EFCE",
    'accent_orange': "FF8C00",
    'light_orange': "FFE4B5",
    'accent_red': "E74C3C",
    'light_red': "FFE6E6",
    'dark_gray': "404040",
    'medium_gray': "808080",
    'light_gray': "F2F2F2",
    'white': "FFFFFF"
}

HEADER_FILL = PatternFill(start_color=COLORS['primary_blue'], end_color=COLORS['primary_blue'], fill_type="solid")
HEADER_FONT = Font(bold=True, color=COLORS['white'], size=12)

SUBHEADER_FILL = PatternFill(start_color=COLORS['secondary_blue'], end_color=COLORS['secondary_blue'],
                             fill_type="solid")
SUBHEADER_FONT = Font(bold=True, color=COLORS['white'], size=10)

ALT_ROW_FILL = PatternFill(start_color=COLORS['light_blue'], end_color=COLORS['light_blue'], fill_type="solid")
HIGH_PERFORMER_FILL = PatternFill(start_color=COLORS['light_green'], end_color=COLORS['light_green'], fill_type="solid")
MODERATOR_FILL = PatternFill(start_color=COLORS['light_orange'], end_color=COLORS['light_orange'], fill_type="solid")
LOW_ACTIVITY_FILL = PatternFill(start_color=COLORS['light_red'], end_color=COLORS['light_red'], fill_type="solid")

BORDER_THIN = Border(
    left=Side(style='thin', color=COLORS['medium_gray']),
    right=Side(style='thin', color=COLORS['medium_gray']),
    top=Side(style='thin', color=COLORS['medium_gray']),
    bottom=Side(style='thin', color=COLORS['medium_gray'])
)

BORDER_THICK_HEADER = Border(
    left=Side(style='medium', color=COLORS['primary_blue']),
    right=Side(style='medium', color=COLORS['primary_blue']),
    top=Side(style='medium', color=COLORS['primary_blue']),
    bottom=Side(style='medium', color=COLORS['primary_blue'])
)


def clean_server_name(srv_name: str) -> str:
    """Clean and standardize server names for display."""
    prefix = "🤔┇ahelp-"
    if srv_name.startswith(prefix):
        srv_name = srv_name[len(prefix):]
    srv_name = srv_name.strip("_ ")
    srv_name = srv_name.replace("_", "-")
    return srv_name.title()


def setup_enhanced_styles(workbook: Workbook) -> None:
    """Setup enhanced named styles in the workbook."""
    styles_to_register = {
        "header_style": {
            "font": HEADER_FONT,
            "fill": HEADER_FILL,
            "alignment": Alignment(horizontal="center", vertical="center", wrap_text=True),
            "border": BORDER_THICK_HEADER
        },
        "subheader_style": {
            "font": SUBHEADER_FONT,
            "fill": SUBHEADER_FILL,
            "alignment": Alignment(horizontal="left", vertical="center", wrap_text=True),
            "border": BORDER_THIN
        },
        "cell_style": {
            "alignment": Alignment(horizontal="left", vertical="center"),
            "border": BORDER_THIN,
            "font": Font(size=10, color=COLORS['dark_gray'])
        },
        "alt_row_style": {
            "fill": ALT_ROW_FILL,
            "alignment": Alignment(horizontal="left", vertical="center"),
            "border": BORDER_THIN,
            "font": Font(size=10, color=COLORS['dark_gray'])
        },
        "number_style": {
            "alignment": Alignment(horizontal="right", vertical="center"),
            "border": BORDER_THIN,
            "font": Font(size=10, color=COLORS['dark_gray']),
            "number_format": "#,##0"
        },
        "percent_style": {
            "alignment": Alignment(horizontal="right", vertical="center"),
            "border": BORDER_THIN,
            "font": Font(size=10, color=COLORS['dark_gray']),
            "number_format": "0.0%"
        },
        "high_performer_style": {
            "fill": HIGH_PERFORMER_FILL,
            "alignment": Alignment(horizontal="left", vertical="center"),
            "border": BORDER_THIN,
            "font": Font(size=10, bold=True, color=COLORS['dark_gray'])
        },
        "moderator_style": {
            "fill": MODERATOR_FILL,
            "alignment": Alignment(horizontal="left", vertical="center"),
            "border": BORDER_THIN,
            "font": Font(size=10, bold=True, color=COLORS['dark_gray'])
        },
        "low_activity_style": {
            "fill": LOW_ACTIVITY_FILL,
            "alignment": Alignment(horizontal="left", vertical="center"),
            "border": BORDER_THIN,
            "font": Font(size=10, color=COLORS['dark_gray'])
        }
    }

    for style_name, style_props in styles_to_register.items():
        if style_name not in workbook.named_styles:
            named_style = NamedStyle(name=style_name)
            for prop, value in style_props.items():
                setattr(named_style, prop, value)
            workbook.add_named_style(named_style)
        else:
            logging.debug(f"Style '{style_name}' already exists in workbook.")


def create_dashboard_summary(
        global_admin_stats: Dict[str, AdminStats],
        global_chat_count: int,
        servers_stats: Dict[str, ServerStats]
) -> pd.DataFrame:
    """Create a comprehensive dashboard summary DataFrame."""

    total_admins = len(global_admin_stats)
    total_ahelps = sum(stats["ahelps"] for stats in global_admin_stats.values())
    total_mentions = sum(stats["mentions"] for stats in global_admin_stats.values())
    total_sessions = sum(
        stats["sessions"] for stats in global_admin_stats.values())

    moderators = [admin for admin, stats in global_admin_stats.items()
                  if any(
            keyword in stats["role"].lower() for keyword in ["модератор", "гейм-мастер", "moderator", "game master"])]

    top_admin_by_ahelps = max(global_admin_stats.items(), key=lambda x: x[1]["ahelps"], default=(None, {"ahelps": 0}))

    most_active_server_obj = None
    max_server_ahelps = -1
    if servers_stats:
        most_active_server_obj = max(servers_stats.items(),
                                     key=lambda x: sum(s["ahelps"] for s in x[1]["admin_stats"].values()),
                                     default=(None, {"admin_stats": {}}))
        if most_active_server_obj[0] is not None:
            max_server_ahelps = sum(s["ahelps"] for s in most_active_server_obj[1]["admin_stats"].values())

    avg_ahelps_per_admin = round(total_ahelps / total_admins, 1) if total_admins > 0 else 0
    avg_sessions_per_admin = round(total_sessions / total_admins,
                                   1) if total_admins > 0 else 0

    summary_data = [
        ["📊 OVERVIEW", None],
        ["Total Administrators", total_admins],
        ["Total Ahelps Handled", total_ahelps],
        ["Total Chat Logs Processed", global_chat_count],
        ["Total Admin Mentions", total_mentions],
        ["Total Admin Sessions Tracked", total_sessions],
        [None, None],
        ["👥 PERFORMANCE METRICS", None],
        ["Average Ahelps per Admin", avg_ahelps_per_admin],
        ["Average Sessions per Admin", avg_sessions_per_admin],
        ["Total Moderators/GMs", len(moderators)],
        [None, None],
        ["🏆 TOP PERFORMERS", None],
        ["Most Active Admin (by Ahelps)", top_admin_by_ahelps[0] if top_admin_by_ahelps[0] else "N/A"],
        ["└─ Ahelps Handled by Top Admin", top_admin_by_ahelps[1]["ahelps"]],
        ["Most Active Server (by Ahelps)",
         clean_server_name(most_active_server_obj[0]) if most_active_server_obj and most_active_server_obj[
             0] else "N/A"],
        ["└─ Ahelps on Top Server", max_server_ahelps if max_server_ahelps != -1 else 0],
    ]

    df = pd.DataFrame(summary_data, columns=["Metric", "Value"])
    return df


def apply_enhanced_formatting(
        worksheet: Worksheet,
        df: pd.DataFrame,
        header_row_on_sheet: int,
        sheet_type: str = "default",
        sort_column_name: Optional[str] = None
) -> None:
    """Apply enhanced formatting using NamedStyles and conditional logic."""

    for cell in worksheet[header_row_on_sheet]:
        if not isinstance(cell, MergedCell):
            cell.style = "header_style"

    first_data_row_on_sheet = header_row_on_sheet + 1
    df_column_names = list(df.columns)

    for excel_row_idx, ws_row_cells in enumerate(
            worksheet.iter_rows(min_row=first_data_row_on_sheet,
                                max_row=first_data_row_on_sheet + len(df) - 1),
            start=first_data_row_on_sheet):

        df_row_index = excel_row_idx - first_data_row_on_sheet
        is_alt_row = df_row_index % 2 == 1
        base_style_name = "alt_row_style" if is_alt_row else "cell_style"

        current_row_style_override = None

        if sheet_type == "admin_stats":
            try:
                role_col_idx = df_column_names.index("Role")
                role_value = str(ws_row_cells[role_col_idx].value).lower() if ws_row_cells[role_col_idx].value else ""
                if any(keyword in role_value for keyword in ["модератор", "гейм-мастер", "moderator", "game master"]):
                    current_row_style_override = "moderator_style"
            except ValueError:
                pass

            if current_row_style_override is None and sort_column_name == "Total Ahelps" and len(df) > 0:
                top_10_percent_count = max(1, len(df) // 10)
                if df_row_index < top_10_percent_count:
                    current_row_style_override = "high_performer_style"
                else:
                    try:
                        ahelps_col_idx = df_column_names.index("Total Ahelps")
                        ahelp_value = ws_row_cells[ahelps_col_idx].value
                        if isinstance(ahelp_value, (int, float)) and ahelp_value < 5:
                            current_row_style_override = "low_activity_style"
                    except (ValueError, IndexError):
                        pass

        for col_idx, cell in enumerate(ws_row_cells):
            if isinstance(cell, MergedCell):
                continue

            chosen_style_name = current_row_style_override or base_style_name

            if sheet_type == "dashboard" and col_idx == 0:
                cell_value_str = str(cell.value) if cell.value else ""
                if cell_value_str.startswith(("📊", "👥", "🏆")):
                    worksheet.merge_cells(start_row=excel_row_idx, start_column=1, end_row=excel_row_idx,
                                          end_column=len(df_column_names))
                    worksheet.cell(excel_row_idx, 1).style = "subheader_style"
                    break
                elif cell_value_str.startswith("└─"):
                    cell.font = Font(size=9, italic=True, color=COLORS['medium_gray'])
                    cell.alignment = Alignment(horizontal="left", vertical="center", indent=1)

            if current_row_style_override is None:
                if isinstance(cell.value, (int, float)):
                    is_percent_col = False
                    if col_idx < len(df_column_names):
                        if "%" in df_column_names[col_idx] or "Rate" in df_column_names[
                            col_idx]:
                            is_percent_col = True

                    if is_percent_col:
                        cell.style = "percent_style"
                        if isinstance(cell.value,
                                      float) and cell.value <= 1.0 and cell.value >= 0:
                            pass
                        else:
                            cell.value = cell.value / 100.0

                    else:
                        cell.style = "number_style"
                else:
                    cell.style = chosen_style_name
            else:
                cell.style = chosen_style_name
                if isinstance(cell.value, (int, float)):
                    cell.alignment = Alignment(horizontal="right", vertical="center")
                    is_percent_col = False
                    if col_idx < len(df_column_names):
                        if "%" in df_column_names[col_idx] or "Rate" in df_column_names[col_idx]:
                            is_percent_col = True
                    if is_percent_col:
                        cell.number_format = "0.0%"
                        if isinstance(cell.value, float) and not (0 <= cell.value <= 1):
                            cell.value = cell.value / 100.0
                    else:
                        cell.number_format = "#,##0"


def create_enhanced_metadata(
        worksheet: Worksheet,
        title: str,
        current_row_idx: int,
        description: str = "",
        data_range: str = "",
        server_name: str = "",
        additional_info: Optional[Dict[str, Any]] = None
) -> int:
    """Create enhanced metadata section at the top of the worksheet."""

    worksheet.cell(current_row_idx, 1, title)
    title_cell = worksheet.cell(current_row_idx, 1)
    title_cell.font = Font(bold=True, size=16, color=COLORS['primary_blue'])
    title_cell.alignment = Alignment(horizontal="left", vertical="center")
    worksheet.merge_cells(start_row=current_row_idx, start_column=1, end_row=current_row_idx,
                          end_column=6)
    current_row_idx += 1

    if description:
        worksheet.cell(current_row_idx, 1, f"📋 Description: {description}")
        worksheet.cell(current_row_idx, 1).font = Font(size=11, color=COLORS['dark_gray'])
        worksheet.merge_cells(start_row=current_row_idx, start_column=1, end_row=current_row_idx, end_column=6)
        current_row_idx += 1

    meta_items_row_start = current_row_idx
    col1_idx = 1
    col2_idx = 3

    if server_name:
        worksheet.cell(current_row_idx, col1_idx, "📍 Server:")
        worksheet.cell(current_row_idx, col1_idx).font = Font(bold=True, size=10, color=COLORS['accent_green'])
        worksheet.cell(current_row_idx, col1_idx + 1, server_name)
        worksheet.cell(current_row_idx, col1_idx + 1).font = Font(size=10, color=COLORS['dark_gray'])

    if data_range:
        target_col = col2_idx if server_name else col1_idx
        worksheet.cell(current_row_idx, target_col, "📅 Period:")
        worksheet.cell(current_row_idx, target_col).font = Font(bold=True, size=10, color=COLORS['dark_gray'])
        worksheet.cell(current_row_idx, target_col + 1, data_range)
        worksheet.cell(current_row_idx, target_col + 1).font = Font(size=10, color=COLORS['dark_gray'])

    if server_name or data_range:
        current_row_idx += 1

    if additional_info:
        for key, value in additional_info.items():
            worksheet.cell(current_row_idx, 1, f"• {key}:")
            worksheet.cell(current_row_idx, 1).font = Font(bold=True, size=10, color=COLORS['medium_gray'])
            worksheet.cell(current_row_idx, 2, str(value))
            worksheet.cell(current_row_idx, 2).font = Font(size=10, color=COLORS['dark_gray'])
            worksheet.merge_cells(start_row=current_row_idx, start_column=2, end_row=current_row_idx,
                                  end_column=6)
            current_row_idx += 1

    worksheet.cell(current_row_idx, 1, f"🕒 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    worksheet.cell(current_row_idx, 1).font = Font(size=9, color=COLORS['medium_gray'], italic=True)
    worksheet.merge_cells(start_row=current_row_idx, start_column=1, end_row=current_row_idx, end_column=6)
    current_row_idx += 1

    worksheet.row_dimensions[current_row_idx].height = 6
    sep_fill = PatternFill(start_color=COLORS['light_gray'], end_color=COLORS['light_gray'], fill_type="solid")
    for c in range(1, 7):
        worksheet.cell(current_row_idx, c).fill = sep_fill
    current_row_idx += 1

    return current_row_idx


def calculate_enhanced_column_widths(df: pd.DataFrame, worksheet: Worksheet, df_header_row: int) -> None:
    """Calculate and set optimal column widths based on content, including headers and metadata if relevant."""

    min_widths = {}
    for r_idx in range(1, df_header_row):
        for c_idx, cell in enumerate(worksheet[r_idx]):
            if cell.value:
                is_merged = False
                for merged_range in worksheet.merged_cells.ranges:
                    if cell.coordinate in merged_range:
                        num_cols_in_merge = merged_range.max_col - merged_range.min_col + 1
                        min_widths[c_idx + 1] = max(min_widths.get(c_idx + 1, 0),
                                                    len(str(cell.value)) // num_cols_in_merge + 2)
                        is_merged = True
                        break
                if not is_merged:
                    min_widths[c_idx + 1] = max(min_widths.get(c_idx + 1, 0), len(str(cell.value)) + 2)

    for col_idx, column_name in enumerate(df.columns, 1):
        col_letter = get_column_letter(col_idx)
        max_length = len(str(column_name))

        try:
            sample_size = min(100, len(df))
            column_data_lengths = df.iloc[:sample_size, col_idx - 1].astype(str).map(len)
            if not column_data_lengths.empty:
                max_length = max(max_length, column_data_lengths.max())
        except Exception:
            for row_tuple in df.itertuples(index=False):
                if col_idx <= len(row_tuple):
                    cell_value = str(row_tuple[col_idx - 1])
                    max_length = max(max_length, len(cell_value))

        width = max(max_length + 2, min_widths.get(col_idx, 0), 10)

        if 'Admin' in column_name or 'Name' in column_name or 'Metric' in column_name:
            width = max(width, 20)
        elif 'Role' in column_name:
            width = max(width, 18)
        elif any(keyword in column_name for keyword in ['Total', 'Count', 'Sessions', 'Ahelps', 'Avg', 'Max']):
            width = max(width, 12)
        elif '%' in column_name or 'Rate' in column_name:
            width = max(width, 14)

        worksheet.column_dimensions[col_letter].width = min(width, 50)


def write_enhanced_excel_sheet(
        df: pd.DataFrame,
        workbook: Workbook,
        sheet_name: str,
        sheet_type: str = "default",
        sort_column: Optional[str] = None,
        ascending: bool = False,
        metadata_dict: Optional[Dict[str, Any]] = None,
) -> None:
    """Write DataFrame to an Excel sheet with enhanced formatting and features."""
    try:
        sheet_name = clean_sheet_name(sheet_name)

        if sort_column and sort_column in df.columns:
            df = df.sort_values(by=sort_column, ascending=ascending).reset_index(drop=True)

        if sheet_name in workbook.sheetnames:
            del workbook[sheet_name]
        ws = workbook.create_sheet(title=sheet_name)

        df_header_start_row = 1
        if metadata_dict:
            df_header_start_row = create_enhanced_metadata(
                ws,
                title=metadata_dict.get('title', sheet_name),
                current_row_idx=1,
                description=metadata_dict.get('description', ''),
                data_range=metadata_dict.get('data_range', ''),
                server_name=metadata_dict.get('server_name', ''),
                additional_info=metadata_dict.get('additional_info')
            )

        for r_idx, row_values in enumerate(dataframe_to_rows(df, index=False, header=True), start=df_header_start_row):
            for c_idx, value in enumerate(row_values, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)

        if not df.empty:
            apply_enhanced_formatting(ws, df, df_header_start_row, sheet_type, sort_column)
            calculate_enhanced_column_widths(df, ws, df_header_start_row)

            try:
                filter_range_end_row = df_header_start_row + len(df)
                filter_range_end_col = get_column_letter(len(df.columns))
                ws.auto_filter.ref = f"A{df_header_start_row}:{filter_range_end_col}{filter_range_end_row}"
            except Exception as e:
                logging.warning(f"Could not set auto-filter on '{sheet_name}': {e}")

            try:
                ws.freeze_panes = ws.cell(row=df_header_start_row + 1, column=1).coordinate
            except Exception as e:
                logging.warning(f"Could not freeze panes on '{sheet_name}': {e}")

        logging.info(f"Successfully prepared sheet '{sheet_name}'.")

    except Exception as e:
        logging.error(f"Error creating Excel sheet '{sheet_name}': {e}")
        logging.error(traceback.format_exc())
        raise


def create_comprehensive_daily_stats(daily_ahelps_raw: DefaultDict[date, Dict[str, int]]) -> pd.DataFrame:
    """Create comprehensive daily statistics with additional metrics."""

    if not daily_ahelps_raw:
        return pd.DataFrame(columns=["Administrator", "Total", "Avg/Day", "Max/Day", "Active Days", "Consistency %"])

    normalized_daily_ahelps: DefaultDict[date, DefaultDict[str, int]] = defaultdict(lambda: defaultdict(int))
    for dt, admin_counts in daily_ahelps_raw.items():
        for admin, count in admin_counts.items():
            normalized_admin = normalize_admin_string(admin)
            normalized_daily_ahelps[dt][normalized_admin] += count

    all_dates = sorted(normalized_daily_ahelps.keys())
    if not all_dates:
        return pd.DataFrame(columns=["Administrator", "Total", "Avg/Day", "Max/Day", "Active Days", "Consistency %"])

    all_admins = set()
    for daily_data in normalized_daily_ahelps.values():
        all_admins.update(daily_data.keys())
    all_admins_sorted = sorted(list(all_admins))

    data_rows = []
    num_days_in_period = len(all_dates)

    for admin in all_admins_sorted:
        admin_daily_counts = [normalized_daily_ahelps[day].get(admin, 0) for day in all_dates]

        total_ahelps = sum(admin_daily_counts)
        avg_daily = round(total_ahelps / num_days_in_period, 1) if num_days_in_period > 0 else 0
        max_daily = max(admin_daily_counts) if admin_daily_counts else 0
        active_days = sum(1 for count in admin_daily_counts if count > 0)
        consistency = round((active_days / num_days_in_period) * 100, 1) if num_days_in_period > 0 else 0

        row_data = [admin] + admin_daily_counts + [total_ahelps, avg_daily, max_daily, active_days, consistency]
        data_rows.append(row_data)

    date_columns = [day.strftime("%Y-%m-%d") for day in all_dates]
    metric_columns = ["Total", "Avg/Day", "Max/Day", "Active Days", "Consistency %"]
    df_columns = ["Administrator"] + date_columns + metric_columns

    df = pd.DataFrame(data_rows, columns=df_columns)
    return df


def save_enhanced_excel_report(
        global_admin_stats: Dict[str, AdminStats],
        global_chat_count: int,
        servers_stats: Dict[str, ServerStats],
) -> None:
    """Save comprehensive Excel report with enhanced visual presentation."""
    excel_filename = os.getenv("EXCEL_FILENAME", "ahelp_stats_enhanced.xlsx")
    try:
        if os.path.exists(excel_filename):
            try:
                os.remove(excel_filename)
            except PermissionError:
                logging.error(
                    f"Permission denied: Could not remove existing file {excel_filename}. Saving with a timestamp.")
                base, ext = os.path.splitext(excel_filename)
                excel_filename = f"{base}_{datetime.now().strftime('%Y%m%d%H%M%S')}{ext}"

        wb = Workbook()
        if "Sheet" in wb.sheetnames:
            del wb["Sheet"]
        setup_enhanced_styles(wb)

        all_dates_in_report = set()
        for server_name in servers_stats:
            all_dates_in_report.update(servers_stats[server_name]["daily_ahelps"].keys())

        date_range_str = ""
        num_days_in_data = 0
        if all_dates_in_report:
            min_date = min(all_dates_in_report).strftime("%Y-%m-%d")
            max_date = max(all_dates_in_report).strftime("%Y-%m-%d")
            date_range_str = f"{min_date} to {max_date}"
            num_days_in_data = len(all_dates_in_report)

        common_metadata_info = {
            "Analysis Period Duration": f"{num_days_in_data} days" if num_days_in_data else "N/A",
            "Total Servers Analyzed": str(len(servers_stats)),
        }

        dashboard_df = create_dashboard_summary(global_admin_stats, global_chat_count, servers_stats)
        write_enhanced_excel_sheet(
            dashboard_df, wb, "📊 Dashboard",
            sheet_type="dashboard",
            metadata_dict={
                'title': '📊 Administration Activity Dashboard',
                'description': 'High-level overview of admin performance and server activity.',
                'data_range': date_range_str,
                'additional_info': common_metadata_info.copy()
            }
        )

        processed_global_admin_stats = {k: v.copy() for k, v in global_admin_stats.items()}
        processed_global_admin_stats = merge_duplicate_admins(processed_global_admin_stats)
        fill_missing_roles(processed_global_admin_stats, servers_stats)

        admin_server_ahelps_breakdown: DefaultDict[str, Dict[str, int]] = defaultdict(dict)
        sorted_server_names_cleaned = sorted([clean_server_name(s_name) for s_name in servers_stats.keys()])

        for original_s_name, s_stats_val in servers_stats.items():
            cleaned_s_name = clean_server_name(original_s_name)
            server_admin_stats_processed = merge_duplicate_admins(s_stats_val["admin_stats"].copy())
            for admin_key, admin_data in server_admin_stats_processed.items():
                normalized_admin_key = normalize_admin_string(admin_key)
                admin_server_ahelps_breakdown[normalized_admin_key][cleaned_s_name] = admin_data.get('ahelps', 0)

        global_admin_data_rows = []
        for admin_name, stats in processed_global_admin_stats.items():
            normalized_admin_name = normalize_admin_string(admin_name)
            row = [
                admin_name,
                stats.get('role', 'N/A'),
                stats.get('ahelps', 0),
                stats.get('mentions', 0),
                stats.get('sessions', 0)
            ]
            for s_name_cleaned in sorted_server_names_cleaned:
                row.append(admin_server_ahelps_breakdown[normalized_admin_name].get(s_name_cleaned, 0))
            global_admin_data_rows.append(row)

        global_admin_columns = ["Administrator", "Role", "Total Ahelps", "Mentions",
                                "Sessions"] + sorted_server_names_cleaned
        df_global_admins = pd.DataFrame(global_admin_data_rows, columns=global_admin_columns)

        global_admins_metadata_add_info = common_metadata_info.copy()
        global_admins_metadata_add_info.update({
            "Total Unique Admins": str(len(processed_global_admin_stats)),
            "Highlight Key": "Green: Top 10% Ahelps, Orange: Moderators/GMs, Red: <5 Ahelps (Low Activity)"
        })
        write_enhanced_excel_sheet(
            df_global_admins, wb, "👥 All Admins",
            sheet_type="admin_stats", sort_column="Total Ahelps", ascending=False,
            metadata_dict={
                'title': '👥 Global Administrator Statistics',
                'description': 'Detailed statistics for all administrators, including per-server ahelp counts.',
                'data_range': date_range_str,
                'additional_info': global_admins_metadata_add_info
            }
        )

        server_summary_data = []
        for s_name, s_stats in servers_stats.items():
            s_admin_stats = s_stats.get("admin_stats", {})
            num_admins_on_server = len(s_admin_stats)
            total_ahelps_on_server = sum(a.get("ahelps", 0) for a in s_admin_stats.values())
            total_mentions_on_server = sum(a.get("mentions", 0) for a in s_admin_stats.values())
            server_chat_sessions = s_stats.get("chat_count", 0)

            moderator_count_on_server = sum(1 for a_stat in s_admin_stats.values()
                                            if any(keyword in a_stat.get("role", "").lower()
                                                   for keyword in
                                                   ["модератор", "гейм-мастер", "moderator", "game master"]))

            avg_ahelps_per_admin_server = round(total_ahelps_on_server / num_admins_on_server,
                                                1) if num_admins_on_server > 0 else 0
            ahelps_per_chat_session = round(total_ahelps_on_server / server_chat_sessions,
                                            2) if server_chat_sessions > 0 else 0

            server_summary_data.append([
                clean_server_name(s_name),
                server_chat_sessions,
                total_ahelps_on_server,
                num_admins_on_server,
                moderator_count_on_server,
                total_mentions_on_server,
                avg_ahelps_per_admin_server,
                ahelps_per_chat_session
            ])

        server_summary_df = pd.DataFrame(
            server_summary_data,
            columns=["Server", "Chat Interactions", "Total Ahelps", "Active Admins", "Moderators/GMs",
                     "Total Mentions", "Avg Ahelps/Admin", "Ahelps/Interaction"]
        )
        write_enhanced_excel_sheet(
            server_summary_df, wb, "🖥️ Server Summary",
            sort_column="Total Ahelps", ascending=False,
            metadata_dict={
                'title': '🖥️ Server Performance Summary',
                'description': 'Aggregated performance metrics for each server.',
                'data_range': date_range_str,
                'additional_info': common_metadata_info.copy()
            }
        )

        daily_stats_metadata_add_info = {
            "Data Columns": "Counts of ahelps handled by each admin per day.",
            "Metric Columns": "Total, Avg/Day, Max/Day, Active Days, Consistency % (days with activity / total days in period)"
        }
        for s_name, s_stats in servers_stats.items():
            if s_stats.get("daily_ahelps"):
                daily_df = create_comprehensive_daily_stats(s_stats["daily_ahelps"])
                cleaned_s_name_for_sheet = clean_server_name(s_name)

                if not daily_df.empty:
                    write_enhanced_excel_sheet(
                        daily_df, wb, f"📅 {cleaned_s_name_for_sheet[:20]} Daily",
                        sort_column="Total", ascending=False,
                        metadata_dict={
                            'title': f'📅 Daily Admin Activity - {cleaned_s_name_for_sheet}',
                            'description': 'Day-by-day breakdown of ahelp handling for each administrator on this server.',
                            'data_range': date_range_str,
                            'server_name': cleaned_s_name_for_sheet,
                            'additional_info': {**common_metadata_info.copy(), **daily_stats_metadata_add_info}
                        }
                    )

        global_daily_ahelps_agg: DefaultDict[date, DefaultDict[str, int]] = defaultdict(lambda: defaultdict(int))
        for s_stats_val in servers_stats.values():
            for day, admin_day_data in s_stats_val.get("daily_ahelps", {}).items():
                for admin_raw_name, count in admin_day_data.items():
                    normalized_admin_name = normalize_admin_string(admin_raw_name)
                    global_daily_ahelps_agg[day][normalized_admin_name] += count

        if global_daily_ahelps_agg:
            global_daily_df = create_comprehensive_daily_stats(global_daily_ahelps_agg)
            if not global_daily_df.empty:
                write_enhanced_excel_sheet(
                    global_daily_df, wb, "📅 Global Daily",
                    sort_column="Total", ascending=False,
                    metadata_dict={
                        'title': '📅 Global Combined Daily Admin Activity',
                        'description': 'Aggregated daily ahelp activity across all servers.',
                        'data_range': date_range_str,
                        'additional_info': {
                            **common_metadata_info.copy(),
                            **daily_stats_metadata_add_info,
                            "Data Source": "Combined from all analyzed servers."
                        }
                    }
                )

        wb.save(excel_filename)
        logging.info(f"Enhanced Excel report saved to: {excel_filename}")

    except Exception as e:
        logging.error(f"Fatal error creating enhanced Excel report: {e}")
        logging.error(traceback.format_exc())


def generate_excel_report(global_admin_stats, global_chat_count, servers_stats):
    """Main function to generate the enhanced Excel report."""
    save_enhanced_excel_report(global_admin_stats, global_chat_count, servers_stats)


def save_all_data_to_excel(global_admin_stats, global_chat_count, servers_stats):
    """Backward compatibility wrapper for the old function name."""
    logging.warning(
        "`save_all_data_to_excel` is deprecated. Please use `generate_excel_report` or `save_enhanced_excel_report`.")
    generate_excel_report(global_admin_stats, global_chat_count, servers_stats)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    if not os.path.exists("data_processing.py"):
        with open("data_processing.py", "w") as f:
            f.write("""
from typing import TypedDict, Dict, DefaultDict, List
from collections import defaultdict
from datetime import date

class AdminStats(TypedDict, total=False):
    ahelps: int
    mentions: int
    sessions: int # Assuming this is playtime or admin duty sessions
    role: str

class ServerStats(TypedDict, total=False):
    admin_stats: Dict[str, AdminStats]
    chat_count: int # Total messages or player chat sessions
    daily_ahelps: DefaultDict[date, Dict[str, int]] # Date -> Admin -> Count

def merge_duplicate_admins(admin_stats: Dict[str, AdminStats]) -> Dict[str, AdminStats]:
    # Simplified merge: just pass through for dummy test
    # Real implementation would merge "Admin Name" and "admin name"
    print(f"Merging duplicates for: {list(admin_stats.keys())[:2]}")
    # In a real scenario, you'd normalize keys and aggregate values.
    # For this test, let's assume names are already somewhat unique or normalized by normalize_admin_string
    return admin_stats

def fill_missing_roles(admin_stats: Dict[str, AdminStats], servers_stats: Dict[str, ServerStats]):
    # Simplified fill: ensure 'role' key exists
    print(f"Filling missing roles for: {list(admin_stats.keys())[:2]}")
    for admin_name, stats_data in admin_stats.items():
        if 'role' not in stats_data or not stats_data['role']:
            # Try to find role from server_stats if needed by iterating all servers
            found_role = "Unknown"
            for s_name, s_data in servers_stats.items():
                s_admin_data = s_data.get('admin_stats', {}).get(admin_name)
                if s_admin_data and s_admin_data.get('role'):
                    found_role = s_admin_data['role']
                    break
            stats_data['role'] = found_role
    return admin_stats
            """)

    if not os.path.exists("utils.py"):
        with open("utils.py", "w") as f:
            f.write("""
import re

def clean_sheet_name(name: str) -> str:
    # Basic cleaning for sheet names
    name = re.sub(r'[\\/*?:\[\]]', '_', name)
    return name[:31] # Max length for sheet names

def normalize_admin_string(admin_name: str) -> str:
    if not admin_name: return "Unknown_Admin"
    # Simple normalization: lowercase and replace common separators
    return admin_name.lower().replace('_', ' ').replace('-', ' ').strip()

            """)

    import importlib
    import data_processing
    import utils

    importlib.reload(data_processing)
    importlib.reload(utils)
    from data_processing import AdminStats, ServerStats, merge_duplicate_admins, fill_missing_roles
    from utils import clean_sheet_name, normalize_admin_string

    dummy_global_admin_stats: Dict[str, AdminStats] = {
        "John Doe (jd123)": {"ahelps": 150, "mentions": 20, "sessions": 30, "role": "Administrator"},
        "Jane Smith (jsmith)": {"ahelps": 200, "mentions": 25, "sessions": 35, "role": "Moderator"},
        "Peter Jones": {"ahelps": 5, "mentions": 1, "sessions": 5, "role": "Helper"},
        "john doe (jd123)": {"ahelps": 10, "mentions": 2, "sessions": 3, "role": "Administrator"},
        "HighPerformer_Admin": {"ahelps": 500, "mentions": 50, "sessions": 60, "role": "Senior Admin"},
    }

    dummy_global_chat_count = 10000

    d1 = date(2023, 1, 1)
    d2 = date(2023, 1, 2)
    d3 = date(2023, 1, 3)

    dummy_servers_stats: Dict[str, ServerStats] = {
        "🤔┇ahelp-Server_Alpha": {
            "admin_stats": {
                "John Doe (jd123)": {"ahelps": 70, "mentions": 10, "sessions": 15, "role": "Administrator"},
                "Jane Smith (jsmith)": {"ahelps": 80, "mentions": 10, "sessions": 15, "role": "Moderator"},
                "HighPerformer_Admin": {"ahelps": 250, "mentions": 20, "sessions": 30, "role": "Senior Admin"},
            },
            "chat_count": 5000,
            "daily_ahelps": defaultdict(dict, {
                d1: {"John Doe (jd123)": 20, "Jane Smith (jsmith)": 25, "HighPerformer_Admin": 100},
                d2: {"John Doe (jd123)": 25, "Jane Smith (jsmith)": 30, "HighPerformer_Admin": 75},
                d3: {"John Doe (jd123)": 25, "Jane Smith (jsmith)": 25, "HighPerformer_Admin": 75},
            })
        },
        "Server-Beta_Main": {
            "admin_stats": {
                "John Doe (jd123)": {"ahelps": 80, "mentions": 10, "sessions": 15, "role": "Administrator"},
                "Jane Smith (jsmith)": {"ahelps": 120, "mentions": 15, "sessions": 20, "role": "Head Moderator"},
                "Peter Jones": {"ahelps": 5, "mentions": 1, "sessions": 5, "role": "Helper"},
                "HighPerformer_Admin": {"ahelps": 250, "mentions": 30, "sessions": 30, "role": "Senior Admin"},
            },
            "chat_count": 5000,
            "daily_ahelps": defaultdict(dict, {
                d1: {"John Doe (jd123)": 30, "Jane Smith (jsmith)": 40, "HighPerformer_Admin": 100},
                d2: {"John Doe (jd123)": 20, "Jane Smith (jsmith)": 50, "HighPerformer_Admin": 80},
                d3: {"John Doe (jd123)": 30, "Jane Smith (jsmith)": 30, "HighPerformer_Admin": 70},
            })
        }
    }

    logging.info("Starting dummy report generation...")
    generate_excel_report(dummy_global_admin_stats, dummy_global_chat_count, dummy_servers_stats)
    logging.info("Dummy report generation finished.")