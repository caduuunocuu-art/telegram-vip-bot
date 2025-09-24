# -*- coding: utf-8 -*-
import logging
import asyncio
import os
from datetime import datetime, timedelta
import pytz
import aiosqlite
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import ChatMemberUpdated
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from aiogram.utils.exceptions import (
    RetryAfter, BotBlocked, ChatNotFound, UserDeactivated, Unauthorized
)

API_TOKEN = os.getenv("TG_BOT_TOKEN")
PREVIEWS_GROUP_ID = int(os.getenv("PREVIEWS_GROUP_ID"))
PREVIEWS_GROUP_INVITE_LINK = os.getenv("PREVIEWS_GROUP_INVITE_LINK")
VIDEO_FILE_ID = os.getenv("VIDEO_FILE_ID")
PURCHASE_LINK = os.getenv("PURCHASE_LINK")
ADMINS = os.getenv("ADMINS", "").split(",")

TIMEZONE = pytz.timezone("America/Sao_Paulo")
MAX_MESSAGE_RETRIES = 3
SEND_IMMEDIATE_DELAY_SECONDS = 5
DAYS_OF_PREVIEW = 7

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)
scheduler = AsyncIOScheduler(timezone=TIMEZONE)

DB_FILE = "bot_database.db"

MESSAGES_SCHEDULE = {
    # Mensagens de prévia dia 1..7
    1: {"12:00": "👉 {name}, você caiu no lugar certo. A prévia é só o começo. {link}"},
    2: {"12:00": "👉 {name}, ontem foi só uma amostra. VIP é completo! {link}"},
    3: {"12:00": "👉 {name}, o jogo continua. Não perca. {link}"},
    4: {"12:00": "👉 {name}, prévia quase acabando. {link}"},
    5: {"12:00": "👉 {name}, últimos dias grátis. {link}"},
    6: {"12:00": "👉 {name}, amanhã acaba. Decida agora: {link}"},
    7: {"12:00": "👉 {name}, última chance. {link}"},
}

CTA_TEXT = "🔥 Seu acesso gratuito está terminando, {name}!\n👉 Entre agora: {link}"

async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                joined_group INTEGER DEFAULT 0,
                removed INTEGER DEFAULT 0,
                banned INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                timestamp TEXT,
                reason TEXT
            )
        """)
        await db.commit()

async def safe_send_message(user_id, text, name_for_cta):
    retries = 0
    while retries < MAX_MESSAGE_RETRIES:
        try:
            if text:
                await bot.send_message(user_id, text)
            await bot.send_video(user_id, VIDEO_FILE_ID, caption=CTA_TEXT.format(name=name_for_cta, link=PURCHASE_LINK))
            return True
        except RetryAfter as e:
            await asyncio.sleep(e.timeout)
            retries += 1
        except (BotBlocked, ChatNotFound, UserDeactivated, Unauthorized):
            logger.warning(f"Não foi possível enviar para {user_id}")
            return False
        except Exception as e:
            logger.error(f"Erro ao enviar para {user_id}: {e}")
            retries += 1
    return False

async def get_user_info(user_id):
    async with aiosqlite.connect(DB_FILE) as db:
        cursor = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        return row

async def update_user_joined(user_id, username, first_name, last_name):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            INSERT OR REPLACE INTO users (user_id, username, first_name, last_name, joined_group, removed, banned)
            VALUES (?, ?, ?, ?, 1, 0, 0)
        """, (user_id, username, first_name, last_name))
        await db.commit()

async def record_attempt(user_id, reason):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT INTO attempts (user_id, timestamp, reason) VALUES (?, ?, ?)",
                         (user_id, datetime.now().isoformat(), reason))
        await db.commit()

async def send_daily_message(user_id, day):
    user_info = await get_user_info(user_id)
    if not user_info:
        return
    _, username, first_name, last_name, joined_group, removed, banned = user_info[:7]
    if removed or banned:
        return
    name = first_name or "Usuário"
    message_text = MESSAGES_SCHEDULE.get(day, {}).get("12:00", "").format(name=name, link=PURCHASE_LINK)
    await safe_send_message(user_id, message_text, name_for_cta=name)

async def schedule_user_messages(user_id):
    for day in range(1, DAYS_OF_PREVIEW + 1):
        run_dt = datetime.now(TIMEZONE) + timedelta(days=day)
        scheduler.add_job(send_daily_message, 'date', run_date=run_dt, args=[user_id, day], id=f"user_{user_id}_day_{day}", replace_existing=True)

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id, username, first_name, last_name) VALUES (?, ?, ?, ?)",
                         (user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))
        await db.commit()
    await message.answer(f"🎉 Bem-vindo! Entre no grupo: {PREVIEWS_GROUP_INVITE_LINK}")

@dp.message_handler(commands=["reset_usuario"])
async def reset_usuario(message: types.Message):
    if str(message.from_user.id) not in ADMINS:
        await message.answer("❌ Sem permissão.")
        return
    args = message.get_args().strip()
    if not args.isdigit():
        await message.answer("⚠️ Use: /reset_usuario <user_id>")
        return
    user_id = int(args)
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE users SET removed=0, banned=0, joined_group=0 WHERE user_id=?", (user_id,))
        await db.commit()
    await schedule_user_messages(user_id)
    await send_daily_message(user_id, 1)
    await message.answer(f"✅ Usuário {user_id} resetado com {DAYS_OF_PREVIEW} dias de acesso.")

@dp.chat_member_handler(chat_id=PREVIEWS_GROUP_ID)
async def handle_chat_member_update(update: ChatMemberUpdated):
    if update.new_chat_member.status == 'member':
        user = update.new_chat_member.user
        user_id = user.id
        user_info = await get_user_info(user_id)

        if user_info and user_info[5]:  # removed
            await record_attempt(user_id, "Tentativa de retorno")
            await bot.ban_chat_member(PREVIEWS_GROUP_ID, user_id)
            await bot.unban_chat_member(PREVIEWS_GROUP_ID, user_id)
            await bot.send_message(user_id, f"Seu acesso expirou. VIP: {PURCHASE_LINK}")
            return

        if not user_info or not user_info[4]:  # joined_group
            await update_user_joined(user_id, user.username, user.first_name, user.last_name)
            await schedule_user_messages(user_id)
            await asyncio.sleep(SEND_IMMEDIATE_DELAY_SECONDS)
            await send_daily_message(user_id, 1)
            logger.info(f"Usuário {user_id} ({user.first_name}) entrou no grupo e recebeu vídeo + CTA")

async def on_startup(dp):
    await init_db()
    scheduler.start()
    logger.info("Bot iniciado")

if __name__ == "__main__":
    executor.start_polling(dp, on_startup=on_startup)
