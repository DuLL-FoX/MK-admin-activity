import os
import sys
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any

try:
    from colorama import init, Fore, Back, Style

    init(autoreset=True)
    COLORAMA_AVAILABLE = True
except ImportError:
    COLORAMA_AVAILABLE = False


    class Fore:
        RED = GREEN = YELLOW = BLUE = MAGENTA = CYAN = WHITE = RESET = ""


    class Back:
        RED = GREEN = YELLOW = BLUE = MAGENTA = CYAN = WHITE = RESET = ""


    class Style:
        BRIGHT = DIM = NORMAL = RESET_ALL = ""


class LogLevel(Enum):
    INFO = "INFO"
    SUCCESS = "SUCCESS"
    WARNING = "WARNING"
    ERROR = "ERROR"
    DEBUG = "DEBUG"


class ConsoleFormatter:

    def __init__(self, use_colors: bool = True):
        self.use_colors = use_colors and COLORAMA_AVAILABLE
        self.terminal_width = self._get_terminal_width()

    def _get_terminal_width(self) -> int:
        try:
            return os.get_terminal_size().columns
        except OSError:
            return 80

    def _colorize(self, text: str, color: str = "", style: str = "") -> str:
        if not self.use_colors:
            return text
        return f"{style}{color}{text}{Style.RESET_ALL}"

    def print_header(self, title: str, subtitle: str = "", char: str = "=") -> None:
        width = min(self.terminal_width, 80)

        title_line = f" {title} "
        if len(title_line) < width - 4:
            padding = (width - len(title_line)) // 2
            title_line = f"{char * padding}{title_line}{char * (width - padding - len(title_line))}"
        else:
            title_line = char * width

        print()
        print(self._colorize(title_line, Fore.CYAN, Style.BRIGHT))

        if subtitle:
            subtitle_centered = subtitle.center(width)
            print(self._colorize(subtitle_centered, Fore.WHITE))
            print(self._colorize(char * width, Fore.CYAN))

        print()

    def print_section(self, title: str, char: str = "-") -> None:
        width = min(self.terminal_width, 60)
        section_line = f" {title} "

        if len(section_line) < width:
            padding = (width - len(section_line)) // 2
            section_line = f"{char * padding}{section_line}{char * (width - padding - len(section_line))}"

        print()
        print(self._colorize(section_line, Fore.YELLOW, Style.BRIGHT))

    def print_log(self, message: str, level: LogLevel = LogLevel.INFO,
                  prefix: bool = True) -> None:
        if prefix:
            timestamp = datetime.now().strftime("%H:%M:%S")

            if level == LogLevel.SUCCESS:
                color = Fore.GREEN
                symbol = "✓"
            elif level == LogLevel.WARNING:
                color = Fore.YELLOW
                symbol = "⚠"
            elif level == LogLevel.ERROR:
                color = Fore.RED
                symbol = "✗"
            elif level == LogLevel.DEBUG:
                color = Fore.MAGENTA
                symbol = "⚙"
            else:
                color = Fore.BLUE
                symbol = "ℹ"

            prefix_text = f"[{timestamp}] {symbol}"
            print(f"{self._colorize(prefix_text, color)} {message}")
        else:
            print(message)

    def print_success(self, message: str) -> None:
        self.print_log(message, LogLevel.SUCCESS)

    def print_warning(self, message: str) -> None:
        self.print_log(message, LogLevel.WARNING)

    def print_error(self, message: str) -> None:
        self.print_log(message, LogLevel.ERROR)

    def print_info(self, message: str) -> None:
        self.print_log(message, LogLevel.INFO)

    def print_menu(self, title: str, options: List[str],
                   descriptions: Optional[List[str]] = None) -> None:
        self.print_section(title)

        for i, option in enumerate(options, 1):
            option_text = f"{i}. {option}"
            if descriptions and i - 1 < len(descriptions):
                option_text += f" - {descriptions[i - 1]}"

            number_colored = self._colorize(f"{i}.", Fore.CYAN, Style.BRIGHT)
            rest_text = option_text[2:]

            print(f"  {number_colored} {rest_text}")
        print()

    def print_table(self, headers: List[str], rows: List[List[str]],
                    title: str = "") -> None:
        if title:
            self.print_section(title)

        if not rows:
            self.print_warning("Нет данных для отображения")
            return

        col_widths = []
        for i, header in enumerate(headers):
            max_width = len(header)
            for row in rows:
                if i < len(row):
                    max_width = max(max_width, len(str(row[i])))
            col_widths.append(min(max_width + 2, 30))

        header_line = "|"
        separator_line = "|"
        for i, header in enumerate(headers):
            cell = f" {header:<{col_widths[i] - 1}}"
            header_line += cell + "|"
            separator_line += "-" * col_widths[i] + "|"

        print(self._colorize(header_line, Fore.WHITE, Style.BRIGHT))
        print(self._colorize(separator_line, Fore.WHITE))

        for row in rows:
            row_line = "|"
            for i, cell in enumerate(row):
                if i < len(col_widths):
                    cell_text = str(cell)[:col_widths[i] - 2]
                    cell_formatted = f" {cell_text:<{col_widths[i] - 1}}"
                    row_line += cell_formatted + "|"
            print(row_line)
        print()

    def print_stats_box(self, stats: Dict[str, Any], title: str = "Статистика") -> None:
        self.print_section(title)

        max_key_length = max(len(str(key)) for key in stats.keys()) if stats else 0

        for key, value in stats.items():
            key_formatted = f"{key}:"
            key_colored = self._colorize(f"{key_formatted:<{max_key_length + 1}}",
                                         Fore.CYAN)

            if isinstance(value, int):
                value_colored = self._colorize(f"{value:,}", Fore.GREEN, Style.BRIGHT)
            elif isinstance(value, float):
                value_colored = self._colorize(f"{value:.2f}", Fore.GREEN, Style.BRIGHT)
            else:
                value_colored = self._colorize(str(value), Fore.WHITE)

            print(f"  {key_colored} {value_colored}")
        print()

    def print_progress_update(self, current: int, total: int,
                              description: str = "", details: str = "") -> None:
        if total > 0:
            percentage = (current / total) * 100
            progress_bar_length = 30
            filled_length = int(progress_bar_length * current / total)

            bar = "█" * filled_length + "░" * (progress_bar_length - filled_length)
            bar_colored = self._colorize(bar, Fore.GREEN)

            percentage_colored = self._colorize(f"{percentage:6.1f}%",
                                                Fore.CYAN, Style.BRIGHT)

            progress_text = f"\r  {description} [{bar_colored}] {percentage_colored} ({current}/{total})"
            if details:
                progress_text += f" - {details}"

            print(progress_text, end="", flush=True)

            if current >= total:
                print()

    def print_reaction_results(self, results: List[tuple],
                               total_processed: Dict[str, int]) -> None:
        self.print_header("РЕЗУЛЬТАТЫ АНАЛИЗА РЕАКЦИЙ")

        stats = {
            "Проанализировано каналов": total_processed.get('channels', 0),
            "Обработано сообщений": total_processed.get('messages', 0),
            "Найдены реакции от пользователей": len(results)
        }
        self.print_stats_box(stats)

        if not results:
            self.print_warning("Реакции не найдены в указанном периоде")
            return

        for rank, (user_id, display_name, normalized_name, total_count, reaction_dict) in enumerate(results, 1):
            rank_colored = self._colorize(f"#{rank}", Fore.YELLOW, Style.BRIGHT)
            name_colored = self._colorize(display_name, Fore.CYAN, Style.BRIGHT)
            id_colored = self._colorize(f"(ID: {user_id})", Fore.WHITE, Style.DIM)

            print(f"{rank_colored} - {name_colored} {id_colored}")
            print(f"Нормализованное имя для таблицы: {self._colorize(normalized_name, Fore.MAGENTA)}")
            print(f"Всего реакций: {self._colorize(str(total_count), Fore.GREEN, Style.BRIGHT)}")
            print("Детализация по эмодзи:")

            sorted_reactions = sorted(reaction_dict.items(), key=lambda x: x[1], reverse=True)
            for emoji_str, count in sorted_reactions:
                emoji_colored = self._colorize(emoji_str, Fore.YELLOW)
                count_colored = self._colorize(str(count), Fore.GREEN)
                print(f"  {emoji_colored}: {count_colored}")
            print()

        total_reactions = sum(result[3] for result in results)
        avg_reactions = total_reactions / len(results) if results else 0

        all_emojis = {}
        for _, _, _, _, reaction_dict in results:
            for emoji, count in reaction_dict.items():
                all_emojis[emoji] = all_emojis.get(emoji, 0) + count

        most_popular_emoji = max(all_emojis.items(), key=lambda x: x[1]) if all_emojis else ("", 0)

        summary_stats = {
            "Всего проанализировано реакций": total_reactions,
            "Среднее количество реакций на пользователя": f"{avg_reactions:.2f}",
            "Самый активный пользователь": f"{results[0][1]} ({results[0][3]} реакций)" if results else "Нет данных",
            "Самое популярное эмодзи": f"{most_popular_emoji[0]} ({most_popular_emoji[1]} использований)" if
            most_popular_emoji[0] else "Нет данных"
        }

        self.print_stats_box(summary_stats, "СВОДНАЯ СТАТИСТИКА")

    def ask_user_choice(self, prompt: str, valid_choices: List[str],
                        default: str = "") -> str:
        choices_colored = self._colorize(f"[{'/'.join(valid_choices)}]", Fore.CYAN)

        if default:
            prompt_text = f"{prompt} {choices_colored} (по умолчанию: {default}): "
        else:
            prompt_text = f"{prompt} {choices_colored}: "

        while True:
            try:
                choice = input(prompt_text).strip()
                if not choice and default:
                    return default
                if choice.lower() in [c.lower() for c in valid_choices]:
                    return choice.lower()

                self.print_error(f"Неверный выбор. Пожалуйста, выберите из: {', '.join(valid_choices)}")
            except KeyboardInterrupt:
                print("\n")
                self.print_warning("Операция отменена пользователем")
                sys.exit(0)

    def ask_user_input(self, prompt: str, default: str = "",
                       validation_func: Optional[callable] = None) -> str:
        if default:
            prompt_text = f"{prompt} (по умолчанию: {default}): "
        else:
            prompt_text = f"{prompt}: "

        while True:
            try:
                user_input = input(prompt_text).strip()
                if not user_input and default:
                    return default

                if validation_func:
                    if validation_func(user_input):
                        return user_input
                    else:
                        self.print_error("Неверный формат ввода. Попробуйте снова.")
                        continue

                return user_input
            except KeyboardInterrupt:
                print("\n")
                self.print_warning("Операция отменена пользователем")
                sys.exit(0)

    def print_final_summary(self, operations_completed: List[str],
                            output_files: List[str] = None) -> None:
        self.print_header("РАБОТА УСПЕШНО ЗАВЕРШЕНА!", "Все операции выполнены")

        if operations_completed:
            self.print_section("Выполненные операции")
            for i, operation in enumerate(operations_completed, 1):
                checkmark = self._colorize("✓", Fore.GREEN, Style.BRIGHT)
                print(f"  {checkmark} {operation}")
            print()

        if output_files:
            self.print_section("Созданные файлы")
            for file_path in output_files:
                if os.path.exists(file_path):
                    file_size = os.path.getsize(file_path)
                    file_size_str = self._format_file_size(file_size)
                    file_colored = self._colorize(file_path, Fore.CYAN)
                    size_colored = self._colorize(f"({file_size_str})", Fore.WHITE, Style.DIM)
                    checkmark = self._colorize("📄", Fore.BLUE)
                    print(f"  {checkmark} {file_colored} {size_colored}")
                else:
                    cross = self._colorize("✗", Fore.RED)
                    print(f"  {cross} {file_path} (файл не найден)")
            print()

        completion_time = datetime.now().strftime("%d.%m.%Y в %H:%M:%S")
        time_colored = self._colorize(completion_time, Fore.GREEN, Style.BRIGHT)
        print(f"Завершено: {time_colored}")
        print()

    def _format_file_size(self, size_bytes: int) -> str:
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 ** 2:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 ** 3:
            return f"{size_bytes / (1024 ** 2):.1f} MB"
        else:
            return f"{size_bytes / (1024 ** 3):.1f} GB"


console = ConsoleFormatter()


def print_header(title: str, subtitle: str = "") -> None:
    console.print_header(title, subtitle)


def print_section(title: str) -> None:
    console.print_section(title)


def print_success(message: str) -> None:
    console.print_success(message)


def print_warning(message: str) -> None:
    console.print_warning(message)


def print_error(message: str) -> None:
    console.print_error(message)


def print_info(message: str) -> None:
    console.print_info(message)


def print_menu(title: str, options: List[str], descriptions: List[str] = None) -> None:
    console.print_menu(title, options, descriptions)


def print_stats_box(stats: Dict[str, Any], title: str = "Статистика") -> None:
    console.print_stats_box(stats, title)


def ask_user_choice(prompt: str, valid_choices: List[str], default: str = "") -> str:
    return console.ask_user_choice(prompt, valid_choices, default)


def ask_user_input(prompt: str, default: str = "", validation_func: callable = None) -> str:
    return console.ask_user_input(prompt, default, validation_func)
