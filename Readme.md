# Discord Admin & User Activity Analyzer

This project is a comprehensive toolkit for downloading, analyzing, and reporting on administrator and user activity on Discord. It primarily processes message logs from "ahelp" (admin help) channels to extract detailed statistics about admin responses. Additionally, it can analyze user emoji reactions across specified channels. The compiled data is used to generate a multi-sheet, richly-formatted Excel report and can optionally update a pre-existing Google Sheet with the results.

> **Warning:** This script is designed to use a Discord User Token for downloading message history and analyzing reactions. Please be aware that using a user token to automate user actions (often called "self-botting") is against Discord's Terms of Service and may put your account at risk of termination. Use this feature with extreme caution and at your own risk. It is recommended to run this script on an alternative account.

## Features

-   **Message & Reaction Downloading**: Fetches complete message history from multiple ahelp channels and analyzes emoji reactions from specified channels within a given date range.
-   **Detailed Ahelp Parsing**: Analyzes messages to identify admin responses and player help requests (`:outbox_tray:` and `:inbox_tray:`).
-   **Comprehensive Admin Stats**: Calculates key performance metrics for each administrator from ahelp channels, including:
    -   Number of ahelps answered (`ahelps`)
    -   Total mentions in help threads (`mentions`)
    -   Number of help sessions participated in (`sessions`)
-   **User Reaction Analysis**: Counts the total number of emoji reactions left by each user in designated channels, providing insights into user engagement.
-   **Admin-Only Tracking**: Separately tracks admin activity that occurs in private, admin-only contexts within ahelp threads.
-   **Advanced Excel Reporting**: Generates a professional, multi-sheet Excel workbook with:
    -   High-level summaries for servers and roles.
    -   Global and per-server leaderboards for admins.
    -   A dedicated sheet for Moderator/Game-Master activity.
    -   Daily and hourly activity trend analysis for ahelps.
    -   Rich formatting, including colors, filters, and frozen panes for readability.
-   **Google Sheets Integration**: After generating reports, the script can connect to a Google Sheet, compare new stats against existing data, and update columns for both ahelp counts and reaction scores. Features advanced name-matching to reliably find users.
-   **Rich Console Interface**: An interactive, color-coded command-line interface guides the user through the process, providing clear status updates and well-formatted results.
-   **Flexible Configuration**: Easily configure target channels, date ranges, and output files using a `.env` file and command-line arguments.

## Project Structure

The project is organized into several modules, each with a specific responsibility:

| File | Description |
| :--- | :--- |
| `main.py` | The main entry point. Handles arguments, the interactive user interface, and orchestrates the entire process. |
| `download.py` | Contains the logic for connecting to Discord and downloading messages from ahelp channels. |
| `data_processing.py` | Responsible for parsing the downloaded JSON data, extracting ahelp statistics, and structuring the analysis results. |
| `excel_exporter.py` | Takes the analyzed ahelp data and generates the final, formatted multi-sheet Excel report. |
| `google_sheets_updater.py`| Handles all logic for connecting to and updating the specified Google Sheet with ahelp and reaction data. |
| `reaction_analyzer.py` | Connects to Discord to analyze emoji reactions from specified channels within a date range. |
| `console_formatter.py` | Provides a rich, color-coded user interface for the command line. |
| `utils.py` | A collection of helper functions for tasks like string normalization, date parsing, and logging configuration. |
| `.env.example` | An example file showing the required environment variables for configuration. |
| `requirements.txt` | A list of Python libraries required to run the project. |

## Prerequisites

-   Python 3.8 or newer
-   Pip (Python package installer)

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
    ```bash
    pip install -r requirements.txt
    ```
    *(If a `requirements.txt` is not available, you can install the packages manually)*
    ```bash
    # Core dependencies
    pip install discord.py python-dotenv pandas openpyxl tqdm colorama
    
    # Optional: For Google Sheets integration
    pip install gspread google-auth-oauthlib
    ```

4.  **Create your configuration file:**
    -   Rename the `.env.example` file to `.env`.
    -   Open the new `.env` file and fill in the required values as described in the **Configuration** section below.
    -   If using the Google Sheets feature, ensure you have a `credentials.json` file from Google Cloud and have enabled the Sheets and Drive APIs.

## How to Use

The script is best run in its interactive mode, which provides prompts and guidance for all operations.

### Interactive Mode

Simply run the script without any arguments. It will guide you through the process.

```bash
python main.py
```

You will be prompted to:
1.  Choose the type of analysis: Ahelp only, Reactions only, or a full analysis of both.
2.  Enter a start and end date for the analysis.
3.  The script will then download the necessary data.
4.  It will process the data and create an Excel report (for ahelp analysis).
5.  Finally, it will ask if you want to proceed with updating the Google Sheet.

### Command-Line Arguments

You can use command-line arguments to automate the script or override settings in your `.env` file for a single run.

-   `--download`: Download ahelp messages before running the analysis. If omitted, the script will only analyze existing local data.
-   `--analyze-reactions`: Run the user reaction analysis.
-   `--data-folder <folder_name>`: Specify the folder where JSON ahelp logs are stored.
-   `--output <filename.xlsx>`: Specify the name of the output Excel file.
-   `--start-date <YYYY-MM-DD>`: Set a start date for the report.
-   `--end-date <YYYY-MM-DD>`: Set an end date for the report.
-   `--days <number>`: Set the report range to the last `N` days.
-   `--verbose`: Enable detailed debug logging for troubleshooting.

**Example:** Download ahelp logs for the last 7 days, analyze them, and save the report as `weekly_report.xlsx`.
```bash
python main.py --download --days 7 --output weekly_report.xlsx
```

### Updating Google Sheets

If you have the necessary libraries installed and configured in `.env`, the script will offer to update a Google Sheet.

1.  **Confirmation:** It will first ask if you want to proceed with the update.
2.  **Dry Run:** It will then ask if you want to perform a "dry run." This is highly recommended, as it will print all the changes it *would* make to the sheet without actually making them.
3.  **Apply Changes:** Finally, it will ask for confirmation to apply the changes.

## Configuration (`.env` file)

All configuration is handled through the `.env` file. Copy `.env.example` to `.env` and fill it out.

```dotenv
# --- Discord Credentials ---
# Your Discord user token. (REQUIRED - see warning at the top)
DISCORD_USER_TOKEN=your_discord_token_here

# --- Date Range Options (for non-interactive mode) ---
# 1 = Use a specific date range (FROM_DATE, TO_DATE).
# 2 = Use a relative number of days from today (DAYS).
DATE_OPTION=1
FROM_DATE=2025-03-01
TO_DATE=2025-03-31
DAYS=14

# --- File Configuration ---
# The name of the generated Excel report.
EXCEL_FILENAME=ahelp_stats.xlsx
# The folder to store downloaded JSON message logs for ahelps.
DATA_FOLDER=data

# --- Ahelp Download Configuration ---
# A comma-separated list of URLs for the ahelp channels you want to analyze.
CHANNEL_URLS=https://discord.com/channels/GUILD_ID/CHANNEL_ID_1,https://discord.com/channels/GUILD_ID/CHANNEL_ID_2
# If "true", the script will re-download and overwrite existing JSON files.
FORCE_OVERWRITE=true

# --- Reaction Analysis Configuration ---
# The ID of the Discord server (guild) where reactions will be analyzed.
REACTION_GUILD_ID=1030160796401016883
# A comma-separated list of channel IDs to scan for reactions.
REACTION_CHANNEL_IDS=123456789012345678,987654321098765432

# --- Google Sheets Integration (Optional) ---
# The path to your Google service account credentials JSON file.
GOOGLE_CREDENTIALS_FILE=your_google_credentials.json
# The ID of the spreadsheet you want to update.
GOOGLE_SHEET_ID=your_spreadsheet_id_here
# The name of the specific worksheet (tab) to update within the spreadsheet.
GOOGLE_SHEET_WORKSHEET_NAME=Sheet1
```

## Output Excel Report

For ahelp analysis, the script generates a single `.xlsx` file containing multiple sheets:

-   **Summary**: A high-level overview of activity (chats, ahelps, admins) for each server.
-   **Roles_Summary**: Aggregated statistics for each unique admin role across all servers.
-   **Admins_Global**: A detailed leaderboard of all administrators, showing their total activity and a breakdown of their ahelps on each server.
-   **Moderators**: A filtered version of the `Admins_Global` sheet, showing statistics only for users with a "Moderator" or "Game-Master" role.
-   **Daily_Global**: A pivot table showing the total number of ahelps processed per admin for each day in the reporting period.
-   **Hourly_Global**: A breakdown of total and processed ahelps by hour of the day.
-   **[ServerName]_Daily** & **[ServerName]_Hourly**: Per-server versions of the daily and hourly breakdowns.