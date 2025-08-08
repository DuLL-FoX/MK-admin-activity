# Discord Admin Activity Analyzer

This project is a comprehensive toolkit for downloading, analyzing, and reporting on administrator activity from "ahelp" (admin help) channels on Discord. It processes message logs to extract detailed statistics about admin responses, mentions, and sessions, then compiles this data into a multi-sheet, richly-formatted Excel report for easy review and analysis.

> **Warning:** This script is designed to use a Discord User Token for downloading message history. Please be aware that using a user token to automate user actions (often called "self-botting") is against Discord's Terms of Service and may put your account at risk of termination. Use this feature with extreme caution and at your own risk. It is recommended to run this script on an alternative account.

## Features

- **Message Downloading**: Fetches complete message history from multiple specified Discord channels within a given date range.
- **Detailed Activity Parsing**: Analyzes messages to identify admin responses and player help requests (`:outbox_tray:` and `:inbox_tray:`).
- **Comprehensive Admin Stats**: Calculates key performance metrics for each administrator, including:
    - Number of ahelps answered (`ahelps`)
    - Total mentions in help threads (`mentions`)
    - Number of help sessions participated in (`sessions`)
- **Admin-Only Tracking**: Separately tracks activity that occurs in private, admin-only contexts.
- **Role & Server Aggregation**: Groups statistics by admin role and across different servers.
- **Advanced Excel Reporting**: Generates a professional, multi-sheet Excel workbook with:
    - High-level summaries for servers and roles.
    - Global and per-server leaderboards for admins.
    - A dedicated sheet for Moderator/Game-Master activity.
    - Daily and hourly activity trend analysis.
    - Rich formatting, including colors, filters, and frozen panes for readability.
- **Flexible Configuration**: Easily configure target channels, date ranges, and output files using a `.env` file and command-line arguments.

## Project Structure

The project is organized into several modules, each with a specific responsibility:

| File                  | Description                                                                                                      |
| --------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `main.py`             | The main entry point for the application. Handles command-line arguments and orchestrates the download/analysis process. |
| `download.py`         | Contains all the logic for connecting to Discord and downloading messages from the specified channels.           |
| `data_processing.py`  | Responsible for parsing the downloaded JSON data, extracting statistics, and structuring the analysis results. |
| `excel_exporter.py`   | Takes the analyzed data and generates the final, formatted multi-sheet Excel report using Pandas and OpenPyXL.   |
| `utils.py`            | A collection of helper functions for tasks like string normalization, date parsing, and logging configuration.    |
| `.env.example`        | An example file showing the required environment variables for configuration.                                    |
| `requirements.txt`    | A list of Python libraries required to run the project.                                                          |

## Prerequisites

- Python 3.8 or newer
- Pip (Python package installer)

## Installation and Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/MK-admin-activity.git
    cd MK-admin-activity
    ```

2.  **Create a Python virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3.  **Install the required dependencies:**
    *(A `requirements.txt` would be generated from the project's imports)*
    ```bash
    pip install discord.py python-dotenv pandas openpyxl tqdm
    ```

4.  **Create your configuration file:**
    -   Rename the `.env.example` file to `.env`.
    -   Open the new `.env` file and fill in the required values as described in the **Configuration** section below.

## How to Use

The script is run from the command line and can either analyze existing data, download new data, or do both.

**Basic Usage**

-   **Download message logs and then generate the report:**
    ```bash
    python main.py --download
    ```

-   **Only analyze existing JSON files in the data folder:**
    ```bash
    python main.py
    ```

**Command-Line Arguments**

You can use command-line arguments to override the settings in your `.env` file for a single run.

-   `--download`: Download messages before running the analysis.
-   `--data-folder <folder_name>`: Specify the folder where JSON files are stored.
-   `--output <filename.xlsx>`: Specify the name of the output Excel file.
-   `--start-date <YYYY-MM-DD>`: Set a start date for the report, overrides `.env`.
-   `--end-date <YYYY-MM-DD>`: Set an end date for the report, overrides `.env`.
-   `--days <number>`: Set the report range to the last `N` days, overrides `.env`.
-   `--verbose`: Enable detailed debug logging for troubleshooting.

**Example:** Download logs for the last 7 days and save the report as `weekly_report.xlsx`.
```bash
python main.py --download --days 7 --output weekly_report.xlsx
```

## Configuration (`.env` file)

All configuration is handled through the `.env` file.

```dotenv
# --- Discord Credentials ---
# Your Discord user token. (REQUIRED - see warning above)
DISCORD_USER_TOKEN=your_discord_token_here

# --- Date Range Options ---
# Determines which date range method to use by default.
# 1 = Use FROM_DATE and TO_DATE.
# 2 = Use DAYS to get the last N days of logs.
DATE_OPTION=1

# Option 1: A specific date range.
FROM_DATE=2025-03-01
TO_DATE=2025-03-31

# Option 2: A relative number of days from today.
DAYS=14

# --- File Configuration ---
# The name of the generated Excel report.
EXCEL_FILENAME=ahelp_stats.xlsx

# The folder to store downloaded JSON message logs.
DATA_FOLDER=data

# --- Download Configuration ---
# A comma-separated list of URLs for the ahelp channels you want to analyze.
CHANNEL_URLS=https://discord.com/channels/GUILD_ID/CHANNEL_ID_1,https://discord.com/channels/GUILD_ID/CHANNEL_ID_2

# If set to "true", the script will re-download and overwrite existing JSON files.
# If "false", it will skip downloading for channels that already have a file.
FORCE_OVERWRITE=true
```

## Output Excel Report

The script generates a single `.xlsx` file containing multiple sheets for in-depth analysis:

-   **Summary**: A high-level overview of activity (chats, ahelps, admins) for each server.
-   **Roles_Summary**: Aggregated statistics for each unique admin role across all servers.
-   **Admins_Global**: A detailed leaderboard of all administrators, showing their total activity and a breakdown of their ahelps on each server.
-   **Moderators**: A filtered version of the `Admins_Global` sheet, showing statistics only for users with a "Moderator" or "Game-Master" role.
-   **Daily_Global**: A pivot table showing the total number of ahelps processed per admin for each day in the reporting period.
-   **Hourly_Global**: A breakdown of total and processed ahelps by hour of the day across all servers.
-   **[ServerName]_Daily**: A per-server version of the daily ahelp breakdown.
-   **[ServerName]_Hourly**: A per-server version of the hourly ahelp breakdown.