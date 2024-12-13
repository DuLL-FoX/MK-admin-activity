import json
import logging
import os
from datetime import datetime, timedelta

import discord

USER_TOKEN = "USER-TOKEN"
CHANNEL_URLS = [
    ### Ссылки на каналы в дисе
]
DATA_FOLDER = "data"

# Вариант 1: Диапазон дат
FROM_DATE = "2024-12-01"
TO_DATE = "2024-12-20"

# Вариант 2: Количество дней
DAYS = 14

# Выбор варианта
DATE_OPTION = 1

intents = discord.Intents.default()
intents.members = False
intents.messages = True
intents.guilds = True

client = discord.Client(intents=intents)


async def get_channel_name(channel_id: int) -> str:
    channel = client.get_channel(channel_id)
    if channel:
        return channel.name
    return str(channel_id)


async def fetch_messages(channel_id: int, after: datetime = None, before: datetime = None):
    channel = client.get_channel(channel_id)
    if channel is None:
        logging.warning(f"Не удалось найти канал {channel_id}.")
        return []

    messages_list = []
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
    return messages_list


@client.event
async def on_ready():
    logging.info(f"Залогинились как {client.user}.")
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    if DATE_OPTION == 1:
        try:
            after_time = datetime.strptime(FROM_DATE, "%Y-%m-%d")
            before_time = datetime.strptime(TO_DATE, "%Y-%m-%d") + timedelta(days=1)
        except ValueError:
            logging.error("Неверный формат даты. Используйте ГГГГ-ММ-ДД.")
            await client.close()
            return
    elif DATE_OPTION == 2:
        after_time = datetime.utcnow() - timedelta(days=DAYS)
        before_time = None
    else:
        logging.error("Неверное значение DATE_OPTION. Допустимые значения: 1 или 2.")
        await client.close()
        return

    for url in CHANNEL_URLS:
        parts = url.strip().split('/')
        if len(parts) < 3:
            logging.warning(f"Некорректная ссылка на канал: {url}")
            continue
        try:
            guild_id = int(parts[-2])
            channel_id = int(parts[-1])
        except ValueError:
            logging.warning(f"Не удалось распарсить guild_id или channel_id: {url}")
            continue

        channel_name = await get_channel_name(channel_id)

        if DATE_OPTION == 1:
            logging.info(f"Скачиваем сообщения из канала {channel_name} ({channel_id}) с {FROM_DATE} по {TO_DATE}.")
        else:
            logging.info(f"Скачиваем сообщения из канала {channel_name} ({channel_id}) за последние {DAYS} дней.")

        messages = await fetch_messages(channel_id, after_time, before_time)

        file_path = os.path.join(DATA_FOLDER, f"{channel_name}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(messages, f, ensure_ascii=False, indent=4)
        logging.info(f"Сохранено {len(messages)} сообщений в {file_path}")

    logging.info("Загрузка сообщений завершена.")
    await client.close()
