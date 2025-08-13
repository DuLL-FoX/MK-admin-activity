import io
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import List, Optional

import discord
import dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

dotenv.load_dotenv()


def setup_discord_client() -> discord.Client:
    intents = discord.Intents.default()
    intents.members = False
    intents.messages = True
    intents.guilds = True
    return discord.Client(intents=intents)


client = setup_discord_client()


async def get_channel_name(channel_id: int) -> str:
    channel = client.get_channel(channel_id)
    if channel:
        return channel.name
    return str(channel_id)


async def fetch_messages(
        channel_id: int,
        after: Optional[datetime] = None,
        before: Optional[datetime] = None
) -> List[dict]:
    channel = client.get_channel(channel_id)
    if channel is None:
        logging.warning(f"Не удалось найти канал {channel_id}.")
        return []

    messages_list = []
    try:
        async for message in channel.history(limit=None, after=after, before=before):
            messages_list.append({
                "id": message.id,
                "author_id": message.author.id,
                "author_name": str(message.author),
                "content": message.content,
                "created_at": message.created_at.isoformat(),
                "attachments": [att.url for att in message.attachments],
                "embeds": [embed.to_dict() for embed in message.embeds],
            })
            if len(messages_list) % 1000 == 0:
                logging.info(f"Загружено {len(messages_list)} сообщений...")
    except discord.errors.Forbidden:
        logging.error(f"Нет прав для доступа к каналу {channel_id}")
    except discord.errors.HTTPException as e:
        logging.error(f"Ошибка API Discord: {e}")
    except Exception as e:
        logging.error(f"Ошибка при загрузке сообщений из канала {channel_id}: {e}")

    return messages_list


@client.event
async def on_ready():
    logging.info(f"Вход выполнен как {client.user}.")

    data_folder = os.getenv("DATA_FOLDER", "data")
    channel_urls = os.getenv("CHANNEL_URLS", "").split(",")

    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
        logging.info(f"Создана папка для данных: {data_folder}")

    after_time = getattr(client, "after_time", None)
    before_time = getattr(client, "before_time", None)
    date_info = getattr(client, "date_info", "указанный период")

    logging.info(f"Загрузка сообщений за {date_info}")
    logging.info(f"Каналов для загрузки: {len(channel_urls)}")

    for url in channel_urls:
        if not url: continue
        url = url.strip()
        parts = url.split('/')
        if len(parts) < 3:
            logging.warning(f"Неверный URL канала: {url}")
            continue

        try:
            guild_id = int(parts[-2])
            channel_id = int(parts[-1])
        except ValueError:
            logging.warning(f"Не удалось извлечь guild_id или channel_id из: {url}")
            continue

        channel_name = await get_channel_name(channel_id)
        logging.info(f"Загрузка сообщений из канала '{channel_name}' ({channel_id}) за {date_info}.")
        messages = await fetch_messages(channel_id, after_time, before_time)
        file_path = os.path.join(data_folder, f"{channel_name}.json")

        if os.path.exists(file_path) and not os.getenv("FORCE_OVERWRITE", "").lower() == "true":
            logging.warning(
                f"Файл {file_path} уже существует. Установите FORCE_OVERWRITE=true в .env для перезаписи.")
            continue

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(messages, f, ensure_ascii=False, indent=2)
        logging.info(f"Сохранено {len(messages)} сообщений в {file_path}")

    logging.info("Загрузка сообщений завершена.")
    await client.close()


def main(start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(), logging.FileHandler("discord_download.log", encoding='utf-8')]
    )

    if not os.path.exists(".env"):
        logging.error(".env файл не найден. Пожалуйста, создайте его на основе .env.example")
        sys.exit(1)

    token = os.getenv("DISCORD_USER_TOKEN")
    if not token:
        logging.error("DISCORD_USER_TOKEN не найден в .env файле")
        sys.exit(1)

    date_option = int(os.getenv("DATE_OPTION", "1"))
    after_time, before_time, date_info = None, None, ""

    if start_date is not None:
        after_time = start_date
        date_info = f"с {start_date.strftime('%Y-%m-%d')}"
        if end_date is not None:
            before_time = end_date
            date_info += f" по {end_date.strftime('%Y-%m-%d')}"
    elif date_option == 1:
        try:
            from_date = os.getenv("FROM_DATE")
            to_date = os.getenv("TO_DATE")
            if not from_date or not to_date:
                logging.error("FROM_DATE и TO_DATE должны быть установлены в .env при DATE_OPTION=1")
                sys.exit(1)
            after_time = datetime.strptime(from_date, "%Y-%m-%d")
            before_time = datetime.strptime(to_date, "%Y-%m-%d") + timedelta(days=1)
            date_info = f"с {from_date} по {to_date}"
        except ValueError:
            logging.error("Неверный формат даты. Используйте ГГГГ-ММ-ДД.")
            sys.exit(1)
    elif date_option == 2:
        days = int(os.getenv("DAYS", "14"))
        after_time = datetime.utcnow() - timedelta(days=days)
        before_time = None
        date_info = f"последние {days} дней"
    else:
        logging.error("Неверная DATE_OPTION. Должно быть 1 или 2.")
        sys.exit(1)

    setattr(client, "after_time", after_time)
    setattr(client, "before_time", before_time)
    setattr(client, "date_info", date_info)

    try:
        client.run(token, bot=False)
    except discord.errors.LoginFailure:
        logging.error("Неверный токен Discord. Проверьте DISCORD_USER_TOKEN в .env")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Ошибка при запуске клиента Discord: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()