#!/usr/bin/env python3
"""
Discord Admin Help Statistics Analyzer

Enhanced version with improved error handling, progress tracking,
and comprehensive reporting capabilities.
"""

import argparse
import logging
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Tuple, Dict, List, Optional
from datetime import datetime, timedelta

import dotenv
from tqdm import tqdm

from data_processing import (
    load_json_file_enhanced, AdminStats, ServerStats,
    analyze_ahelp_data_enhanced, ProcessingStats
)
from download import main as download_main
from excel_exporter import save_enhanced_excel_report
from utils import extract_server_name, configure_logging, format_date_range


class AnalysisSession:
    """Class to manage and track the entire analysis session."""

    def __init__(self):
        self.start_time = time.time()
        self.files_processed = 0
        self.files_failed = 0
        self.total_messages = 0
        self.total_errors = 0
        self.processing_stats = []

    def add_file_result(self, success: bool, processing_stats: Optional[ProcessingStats] = None):
        """Add result from processing a file."""
        if success:
            self.files_processed += 1
        else:
            self.files_failed += 1

        if processing_stats:
            self.processing_stats.append(processing_stats)
            self.total_messages += processing_stats.total_messages
            self.total_errors += len(processing_stats.processing_errors)

    def get_duration(self) -> float:
        """Get session duration in seconds."""
        return time.time() - self.start_time

    def get_summary(self) -> Dict:
        """Get comprehensive session summary."""
        duration = self.get_duration()
        total_files = self.files_processed + self.files_failed

        return {
            "duration_seconds": round(duration, 2),
            "duration_formatted": format_duration(duration),
            "total_files": total_files,
            "files_processed": self.files_processed,
            "files_failed": self.files_failed,
            "success_rate": round((self.files_processed / max(total_files, 1)) * 100, 2),
            "total_messages": self.total_messages,
            "total_errors": self.total_errors,
            "messages_per_second": round(self.total_messages / max(duration, 1), 2),
        }


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds // 60:.0f}m {seconds % 60:.0f}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours:.0f}h {minutes:.0f}m {seconds % 60:.0f}s"


def validate_environment() -> Tuple[bool, List[str]]:
    """Validate environment configuration."""
    errors = []

    if not os.path.exists(".env"):
        errors.append(".env file not found. Please create one based on .env.example")
        return False, errors

    # Load environment
    dotenv.load_dotenv(".env")

    # Check critical settings
    required_vars = []
    optional_vars = {
        "DATA_FOLDER": "data",
        "EXCEL_FILENAME": "ahelp_stats_enhanced.xlsx"
    }

    for var in required_vars:
        if not os.getenv(var):
            errors.append(f"Required environment variable {var} not set")

    # Check if we can create output directory
    data_folder = os.getenv("DATA_FOLDER", "data")
    try:
        Path(data_folder).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        errors.append(f"Cannot create data folder '{data_folder}': {e}")

    return len(errors) == 0, errors


def get_downloaded_files_enhanced(data_folder: str) -> Tuple[List[str], Dict]:
    """
    Get downloaded files with additional metadata.

    Returns:
        (file_list, metadata)
    """
    if not os.path.exists(data_folder):
        return [], {"error": f"Data folder '{data_folder}' does not exist"}

    all_files = []
    total_size = 0
    file_stats = {}

    try:
        for filename in os.listdir(data_folder):
            file_path = os.path.join(data_folder, filename)

            if not os.path.isfile(file_path) or not filename.endswith('.json'):
                continue

            file_size = os.path.getsize(file_path)
            file_mtime = os.path.getmtime(file_path)

            all_files.append(file_path)
            total_size += file_size
            file_stats[filename] = {
                "size": file_size,
                "modified": datetime.fromtimestamp(file_mtime),
                "size_mb": round(file_size / (1024 * 1024), 2)
            }

    except Exception as e:
        return [], {"error": f"Error scanning data folder: {e}"}

    metadata = {
        "total_files": len(all_files),
        "total_size_mb": round(total_size / (1024 * 1024), 2),
        "file_stats": file_stats,
        "data_folder": data_folder
    }

    return sorted(all_files), metadata


def aggregate_global_stats_enhanced(
        files: List[str],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        session: Optional[AnalysisSession] = None
) -> Tuple[Dict[str, AdminStats], int, Dict[str, ServerStats]]:
    """
    Enhanced aggregation with better progress tracking and error handling.
    """
    global_admin_stats = defaultdict(lambda: {"ahelps": 0, "mentions": 0, "role": "Unknown", "sessions": 0})
    global_chat_count = 0
    servers_stats = {}

    with tqdm(total=len(files), desc="Processing files", unit="file") as pbar:
        for i, file_path in enumerate(files):
            filename = os.path.basename(file_path)
            pbar.set_description(f"Processing {filename}")

            try:
                # Load and validate file
                data, error = load_json_file_enhanced(file_path)
                if error:
                    logging.error(f"Failed to load {filename}: {error}")
                    if session:
                        session.add_file_result(False)
                    pbar.update(1)
                    continue

                # Filter data by date range if needed
                if start_date or end_date:
                    original_count = len(data) if isinstance(data, list) else 0
                    filtered_data = []

                    if isinstance(data, list):
                        for message in data:
                            if not isinstance(message, dict) or 'created_at' not in message:
                                continue

                            try:
                                msg_dt = datetime.fromisoformat(
                                    message['created_at'].replace('Z', '+00:00')
                                )

                                if start_date and msg_dt < start_date:
                                    continue
                                if end_date and msg_dt > end_date:
                                    continue

                                filtered_data.append(message)

                            except (ValueError, AttributeError):
                                # Keep message if date parsing fails
                                filtered_data.append(message)

                    data = filtered_data
                    filtered_count = len(data)

                    if original_count > 0 and filtered_count != original_count:
                        logging.info(f"{filename}: Filtered {original_count} -> {filtered_count} messages")

                # Analyze server data
                server_name = extract_server_name(file_path)
                server_stats, processing_stats = analyze_ahelp_data_enhanced(
                    data, server_name, start_date, end_date
                )

                servers_stats[server_name] = server_stats

                # Aggregate global statistics
                for admin, stats in server_stats["admin_stats"].items():
                    global_admin_stats[admin]["ahelps"] += stats["ahelps"]
                    global_admin_stats[admin]["mentions"] += stats["mentions"]
                    global_admin_stats[admin]["sessions"] += stats["sessions"]

                    # Update role with better information
                    if global_admin_stats[admin]["role"] == "Unknown" and stats["role"] != "Unknown":
                        global_admin_stats[admin]["role"] = stats["role"]

                global_chat_count += server_stats["chat_count"]

                if session:
                    session.add_file_result(True, processing_stats)

                # Update progress bar with current stats
                total_ahelps = sum(stats["ahelps"] for stats in global_admin_stats.values())
                pbar.set_postfix({
                    "Admins": len(global_admin_stats),
                    "Ahelps": total_ahelps,
                    "Chats": global_chat_count
                })

            except Exception as e:
                logging.error(f"Unexpected error processing {filename}: {e}")
                if session:
                    session.add_file_result(False)

            pbar.update(1)

    return dict(global_admin_stats), global_chat_count, servers_stats


def print_analysis_summary(
        global_admin_stats: Dict[str, AdminStats],
        global_chat_count: int,
        servers_stats: Dict[str, ServerStats],
        session: AnalysisSession
):
    """Print comprehensive analysis summary."""

    session_summary = session.get_summary()
    total_ahelps = sum(stats["ahelps"] for stats in global_admin_stats.values())
    total_admins = len(global_admin_stats)

    print("\n" + "=" * 60)
    print("📊 ANALYSIS COMPLETE")
    print("=" * 60)

    print(f"⏱️  Processing Time: {session_summary['duration_formatted']}")
    print(f"📁 Files Processed: {session_summary['files_processed']}/{session_summary['total_files']} "
          f"({session_summary['success_rate']:.1f}% success)")

    if session_summary['files_failed'] > 0:
        print(f"❌ Failed Files: {session_summary['files_failed']}")

    print(f"💬 Total Messages: {session_summary['total_messages']:,}")
    print(f"⚡ Processing Speed: {session_summary['messages_per_second']:.1f} messages/sec")

    if session_summary['total_errors'] > 0:
        print(f"⚠️  Processing Errors: {session_summary['total_errors']:,}")

    print("\n📈 STATISTICS SUMMARY:")
    print(f"   🔧 Total Administrators: {total_admins:,}")
    print(f"   🎫 Total Ahelps Handled: {total_ahelps:,}")
    print(f"   💬 Total Chat Sessions: {global_chat_count:,}")
    print(f"   🖥️  Servers Analyzed: {len(servers_stats):,}")

    if total_admins > 0:
        avg_ahelps = total_ahelps / total_admins
        print(f"   📊 Average Ahelps per Admin: {avg_ahelps:.1f}")

    # Show top performers
    if global_admin_stats:
        top_admins = sorted(global_admin_stats.items(), key=lambda x: x[1]["ahelps"], reverse=True)[:3]
        print(f"\n🏆 TOP PERFORMERS:")
        for i, (admin, stats) in enumerate(top_admins, 1):
            print(f"   {i}. {admin}: {stats['ahelps']} ahelps")

    # Show server breakdown
    if len(servers_stats) > 1:
        print(f"\n🖥️  SERVER BREAKDOWN:")
        server_ahelps = {}
        for server_name, stats in servers_stats.items():
            server_total = sum(admin["ahelps"] for admin in stats["admin_stats"].values())
            server_ahelps[server_name] = server_total

        for server, ahelps in sorted(server_ahelps.items(), key=lambda x: x[1], reverse=True)[:5]:
            clean_name = server.replace("🤔┇ahelp-", "").replace("_", "-")
            print(f"   • {clean_name}: {ahelps} ahelps")

    print("=" * 60)


def parse_arguments() -> argparse.Namespace:
    """Parse and validate command line arguments."""

    parser = argparse.ArgumentParser(
        description="Discord Admin Help Statistics Analyzer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --download --days 30                    # Download and analyze last 30 days
  %(prog)s --start-date 2024-01-01 --end-date 2024-01-31  # Analyze January 2024
  %(prog)s --verbose --output monthly_report.xlsx  # Verbose mode with custom output
  %(prog)s --download --data-folder /path/to/data  # Custom data folder
        """)

    # Action arguments
    action_group = parser.add_argument_group('Actions')
    action_group.add_argument(
        "--download",
        action="store_true",
        help="Download messages before analysis"
    )

    action_group.add_argument(
        "--analyze-only",
        action="store_true",
        help="Only analyze existing data (skip download check)"
    )

    # Data source arguments
    data_group = parser.add_argument_group('Data Source')
    data_group.add_argument(
        "--data-folder",
        type=str,
        help="Folder containing JSON files (default: from .env)"
    )

    # Time range arguments
    time_group = parser.add_argument_group('Time Range')
    time_group.add_argument(
        "--start-date",
        type=str,
        help="Start date for analysis (format: YYYY-MM-DD)"
    )

    time_group.add_argument(
        "--end-date",
        type=str,
        help="End date for analysis (format: YYYY-MM-DD)"
    )

    time_group.add_argument(
        "--days",
        type=int,
        help="Number of days to analyze (from today backwards)"
    )

    # Output arguments
    output_group = parser.add_argument_group('Output')
    output_group.add_argument(
        "--output",
        type=str,
        help="Output Excel filename (default: from .env)"
    )

    output_group.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )

    output_group.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress non-error output"
    )

    args = parser.parse_args()

    # Validate arguments
    if args.verbose and args.quiet:
        parser.error("Cannot use both --verbose and --quiet")

    if args.start_date and args.days:
        parser.error("Cannot use both --start-date and --days")

    return args


def main() -> int:
    """Main entry point with enhanced error handling and user experience."""

    try:
        args = parse_arguments()

        # Validate environment
        env_valid, env_errors = validate_environment()
        if not env_valid:
            print("❌ Environment validation failed:")
            for error in env_errors:
                print(f"   • {error}")
            return 1

        # Configure logging
        log_level = logging.WARNING if args.quiet else (logging.DEBUG if args.verbose else logging.INFO)
        configure_logging(level=log_level)

        # Initialize session tracking
        session = AnalysisSession()

        if not args.quiet:
            print("🚀 Discord Admin Help Statistics Analyzer")
            print("=" * 50)

        # Parse date arguments
        start_date = None
        end_date = None

        if args.days:
            start_date = datetime.now() - timedelta(days=args.days)
            if not args.quiet:
                print(f"📅 Analyzing last {args.days} days (from {start_date.strftime('%Y-%m-%d')})")

        if args.start_date:
            try:
                start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
                if not args.quiet:
                    print(f"📅 Start date: {args.start_date}")
            except ValueError:
                print(f"❌ Invalid start date format: {args.start_date}")
                print("   Use YYYY-MM-DD format (e.g., 2024-01-15)")
                return 1

        if args.end_date:
            try:
                end_date = datetime.strptime(args.end_date, "%Y-%m-%d") + timedelta(days=1)
                if not args.quiet:
                    print(f"📅 End date: {args.end_date}")
            except ValueError:
                print(f"❌ Invalid end date format: {args.end_date}")
                print("   Use YYYY-MM-DD format (e.g., 2024-01-31)")
                return 1

        # Validate date range
        if start_date and end_date and start_date >= end_date:
            print("❌ Start date must be before end date")
            return 1

        # Get configuration
        data_folder = args.data_folder or os.getenv("DATA_FOLDER", "data")
        excel_filename = args.output or os.getenv("EXCEL_FILENAME", "ahelp_stats_enhanced.xlsx")

        # Download if requested
        if args.download:
            if not args.quiet:
                print("\n📥 Downloading messages...")
            try:
                download_main(start_date=start_date, end_date=end_date)
                if not args.quiet:
                    print("✅ Download completed successfully")
            except Exception as e:
                logging.error(f"Download failed: {e}")
                print(f"❌ Download failed: {e}")
                return 1

        # Get files to process
        files, file_metadata = get_downloaded_files_enhanced(data_folder)

        if "error" in file_metadata:
            print(f"❌ {file_metadata['error']}")
            return 1

        if not files:
            print(f"❌ No JSON files found in '{data_folder}'")
            if not args.download:
                print("   💡 Try using --download to fetch data first")
            return 1

        if not args.quiet:
            print(f"\n📁 Found {file_metadata['total_files']} files ({file_metadata['total_size_mb']:.1f} MB)")
            if args.verbose:
                print("   File details:")
                for filename, stats in file_metadata['file_stats'].items():
                    print(f"   • {filename}: {stats['size_mb']:.1f} MB "
                          f"(modified: {stats['modified'].strftime('%Y-%m-%d %H:%M')})")

        # Process files
        if not args.quiet:
            print(f"\n🔄 Processing {len(files)} files...")

        global_admin_stats, global_chat_count, servers_stats = aggregate_global_stats_enhanced(
            files, start_date, end_date, session
        )

        # Validate results
        if not global_admin_stats:
            print("⚠️  No admin statistics found in the data")
            print("   This might indicate:")
            print("   • No valid ahelp data in the files")
            print("   • Date range filters excluded all data")
            print("   • Data format issues")
            return 1

        # Print summary
        if not args.quiet:
            print_analysis_summary(global_admin_stats, global_chat_count, servers_stats, session)

        # Generate Excel report
        if not args.quiet:
            print(f"\n📊 Generating Excel report: {excel_filename}")

        try:
            save_enhanced_excel_report(
                global_admin_stats,
                global_chat_count,
                servers_stats,
            )

            if not args.quiet:
                file_size = os.path.getsize(excel_filename) / (1024 * 1024)  # MB
                print(f"✅ Report saved successfully ({file_size:.1f} MB)")
                print(f"   📄 File: {os.path.abspath(excel_filename)}")

        except Exception as e:
            logging.error(f"Failed to save Excel report: {e}")
            print(f"❌ Failed to save Excel report: {e}")
            return 1

        if not args.quiet:
            print(f"\n🎉 Analysis completed in {format_duration(session.get_duration())}")

        return 0

    except KeyboardInterrupt:
        print("\n⏹️  Analysis interrupted by user")
        return 130

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        print(f"❌ Unexpected error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())