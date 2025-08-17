import logging
from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Tuple, Optional

import discord

try:
    from console_formatter import console, print_success, print_warning, print_error, print_info

    CONSOLE_FORMATTER_AVAILABLE = True
except ImportError:
    CONSOLE_FORMATTER_AVAILABLE = False


    def print_success(msg):
        print(f"✓ {msg}")


    def print_warning(msg):
        print(f"⚠ {msg}")


    def print_error(msg):
        print(f"✗ {msg}")


    def print_info(msg):
        print(f"ℹ {msg}")

logger = logging.getLogger(__name__)


class ReactionAnalyzer:
    def __init__(self, token: str, guild_id: int, channel_ids: List[int]):
        self.token = token
        self.guild_id = guild_id
        self.channel_ids = channel_ids

        intents = discord.Intents.default()
        intents.members = True

        self.client = discord.Client(intents=intents)
        self.setup_events()

        self.user_reactions = defaultdict(lambda: defaultdict(int))
        self.processed_messages = 0
        self.processed_channels = 0
        self.results = {}
        self.guild_cache = None

    def setup_events(self):
        @self.client.event
        async def on_ready():
            await self.analyze_reactions()

    async def fetch_guild_and_channels(self) -> Tuple[Optional[discord.Guild], List[discord.TextChannel]]:
        guild = self.client.get_guild(self.guild_id)
        if guild is None:
            print_error(f"Не удалось найти сервер с ID {self.guild_id}")
            return None, []

        self.guild_cache = guild

        channels = []
        for channel_id in self.channel_ids:
            channel = guild.get_channel(channel_id)
            if channel is None:
                print_warning(f"Не удалось найти канал с ID {channel_id} на сервере {guild.name}")
                continue
            if not isinstance(channel, discord.TextChannel):
                print_warning(f"Канал с ID {channel_id} не является текстовым каналом")
                continue
            channels.append(channel)

        return guild, channels

    async def process_message_reactions(self, message: discord.Message) -> None:
        if not message.reactions:
            return

        try:
            for reaction in message.reactions:
                try:
                    users_who_reacted = await reaction.users().flatten()
                    emoji_repr = self.get_emoji_representation(reaction.emoji)

                    for user in users_who_reacted:
                        if not user.bot:
                            self.user_reactions[user.id][emoji_repr] += 1

                except discord.errors.Forbidden:
                    logger.warning(f"Нет прав для получения пользователей реакции {reaction.emoji}")
                except Exception as e:
                    logger.warning(f"Ошибка при обработке реакции {reaction.emoji}: {e}")

        except Exception as e:
            logger.error(f"Ошибка при обработке сообщения {message.id}: {e}")

    def get_emoji_representation(self, emoji) -> str:
        if isinstance(emoji, discord.PartialEmoji) and not emoji.is_unicode_emoji():
            return f"<:{emoji.name}:{emoji.id}>"
        return str(emoji)

    def normalize_discord_name(self, member: discord.Member) -> str:
        if not member:
            return "Неизвестный пользователь"

        names_to_try = []

        clean_username = member.name
        names_to_try.append(clean_username)

        if member.display_name and member.display_name != member.name:
            names_to_try.append(member.display_name)

        if member.nick:

            nick_clean = member.nick

            if '|' in nick_clean:
                nick_clean = nick_clean.split('|')[0].strip()

            nick_clean = nick_clean.split('(')[0].strip()

            if '/' in nick_clean:
                nick_clean = nick_clean.split('/')[0].strip()
            names_to_try.append(nick_clean)

        for name in reversed(names_to_try):
            if name and len(name.strip()) > 0:
                return name.strip()

        return clean_username

    def get_display_name(self, member: Optional[discord.Member]) -> str:
        if member is None:
            return "Неизвестный пользователь"

        display_parts = []

        main_name = member.nick or member.display_name
        if main_name and main_name != member.name:
            display_parts.append(main_name)

        display_parts.append(f"({member.name})")

        return "".join(display_parts) if len(display_parts) > 1 else member.name

    async def analyze_channel(self, channel: discord.TextChannel, start_date: datetime, end_date: datetime) -> int:
        print_info(f"Анализ канала: {channel.name} (ID: {channel.id})")

        message_count = 0
        start_time = datetime.now()

        try:
            async for message in channel.history(
                    limit=None,
                    after=start_date,
                    before=end_date,
                    oldest_first=True
            ):
                message_count += 1
                if message_count % 500 == 0:
                    elapsed_time = (datetime.now() - start_time).total_seconds()

                    if CONSOLE_FORMATTER_AVAILABLE:
                        console.print_progress_update(
                            message_count,
                            message_count + 100,
                            f"Обработка {channel.name}",
                            f"{message_count} сообщений за {elapsed_time:.2f}с"
                        )
                    else:
                        print_info(f"  Обработано {message_count} сообщений в {channel.name}... "
                                   f"Время: {elapsed_time:.2f}с")

                await self.process_message_reactions(message)

        except discord.errors.Forbidden:
            print_error(f"Доступ запрещен к каналу {channel.name}. Проверьте права бота.")
            return 0
        except discord.errors.HTTPException as e:
            print_error(f"HTTP ошибка в канале {channel.name}: {e}")
            return 0
        except Exception as e:
            print_error(f"Неожиданная ошибка в канале {channel.name}: {e}")
            return 0

        elapsed_time = (datetime.now() - start_time).total_seconds()
        print_success(f"Завершен анализ {channel.name}: {message_count} сообщений за {elapsed_time:.2f}с")

        return message_count

    async def analyze_reactions(self) -> None:
        print_info(f"Вход выполнен как: {self.client.user} (ID: {self.client.user.id})")

        start_date = getattr(self.client, "start_date", None)
        end_date = getattr(self.client, "end_date", None)

        if not start_date or not end_date:
            print_error("Даты анализа не установлены")
            await self.client.close()
            return

        print_info(f"Анализ реакций с {start_date} по {end_date}")

        guild, channels = await self.fetch_guild_and_channels()
        if not channels:
            print_error("Не найдено валидных каналов для анализа")
            await self.client.close()
            return

        print_info(f"Найдено {len(channels)} каналов для анализа на сервере: {guild.name}")

        total_start_time = datetime.now()

        for channel in channels:
            channel_messages = await self.analyze_channel(channel, start_date, end_date)
            self.processed_messages += channel_messages
            self.processed_channels += 1

        self.results = self.generate_results(guild)

        total_elapsed = (datetime.now() - total_start_time).total_seconds()
        print_success(f"Общий анализ завершен: {self.processed_messages} сообщений "
                      f"из {self.processed_channels} каналов за {total_elapsed:.2f}с")

        await self.client.close()

    def generate_results(self, guild: discord.Guild) -> Dict[str, int]:
        if not self.user_reactions:
            print_warning("Реакции не найдены в указанном периоде")
            return {}

        final_results = []
        user_totals = {}

        for user_id, reaction_dict in self.user_reactions.items():
            member = guild.get_member(user_id)
            display_name = self.get_display_name(member)
            normalized_name = self.normalize_discord_name(member) if member else f"User_{user_id}"
            total_count = sum(reaction_dict.values())

            user_totals[normalized_name] = total_count
            final_results.append((user_id, display_name, normalized_name, total_count, dict(reaction_dict)))

        final_results.sort(key=lambda x: x[3], reverse=True)

        self.display_console_results(final_results)

        return user_totals

    def display_console_results(self, final_results: List[Tuple[int, str, str, int, Dict[str, int]]]) -> None:

        if CONSOLE_FORMATTER_AVAILABLE:

            total_processed = {
                'channels': self.processed_channels,
                'messages': self.processed_messages
            }
            console.print_reaction_results(final_results, total_processed)
        else:

            print("\n" + "=" * 60)
            print("РЕЗУЛЬТАТЫ АНАЛИЗА РЕАКЦИЙ")
            print("=" * 60)
            print(f"Проанализировано каналов: {self.processed_channels}")
            print(f"Обработано сообщений: {self.processed_messages}")
            print(f"Найдены реакции от пользователей: {len(final_results)}")
            print("=" * 60)

            for rank, (user_id, display_name, normalized_name, total_count, reaction_dict) in enumerate(final_results,
                                                                                                        1):
                print(f"\n#{rank} - {display_name} (ID: {user_id})")
                print(f"Нормализованное имя для таблицы: {normalized_name}")
                print(f"Всего реакций: {total_count}")
                print("Детализация по эмодзи:")

                sorted_reactions = sorted(reaction_dict.items(), key=lambda x: x[1], reverse=True)
                for emoji_str, count in sorted_reactions:
                    print(f"  {emoji_str}: {count}")

            print("\n" + "=" * 60)
            print("СВОДНАЯ СТАТИСТИКА")
            print("=" * 60)

            total_reactions = sum(result[3] for result in final_results)
            avg_reactions_per_user = total_reactions / len(final_results) if final_results else 0

            print(f"Всего проанализировано реакций: {total_reactions}")
            print(f"Среднее количество реакций на пользователя: {avg_reactions_per_user:.2f}")

            if final_results:
                print(f"Самый активный пользователь: {final_results[0][1]} ({final_results[0][3]} реакций)")

                all_emojis = defaultdict(int)
                for _, _, _, _, reaction_dict in final_results:
                    for emoji, count in reaction_dict.items():
                        all_emojis[emoji] += count

                if all_emojis:
                    most_popular_emoji = max(all_emojis.items(), key=lambda x: x[1])
                    print(f"Самое популярное эмодзи: {most_popular_emoji[0]} ({most_popular_emoji[1]} использований)")


def analyze_reactions_sync(
        token: str,
        guild_id: int,
        channel_ids: List[int],
        start_date: datetime,
        end_date: datetime
) -> Dict[str, int]:
    analyzer = ReactionAnalyzer(token, guild_id, channel_ids)

    analyzer.client.start_date = start_date
    analyzer.client.end_date = end_date

    try:
        analyzer.client.run(token, bot=False)
        return analyzer.results
    except Exception as e:
        print_error(f"Ошибка при запуске анализатора реакций: {e}")
        return {}
