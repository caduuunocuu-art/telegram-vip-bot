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
VIDEO_FILE_ID = os.getenv("VIDEO_FILE_ID")
PURCHASE_LINK = os.getenv("PURCHASE_LINK")
ADMINS = os.getenv("ADMINS", "").split(",")

# Configurações gerais
TIMEZONE = pytz.timezone("America/Sao_Paulo")
MAX_MESSAGE_RETRIES = 3
SEND_IMMEDIATE_DELAY_SECONDS = 5

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)
scheduler = AsyncIOScheduler(timezone=TIMEZONE)

DB_FILE = "bot_database.db"

# Mensagens estruturadas
MESSAGES_SCHEDULE = {
    1: {
        "12:00": "{name}, você caiu no lugar certo... {link}",
        "18:00": "Mano, já vi uns 10 prints... {link}",
        "22:00": "Antes de dormir, um aviso... {link}",
    },
    2: {
        "12:00": "{name}, acordou? O VIP não espera... {link}",
        "18:00": "Tem dois tipos de gente... {link}",
        "22:00": "Enquanto você enrola... {link}",
    },
    3: {
        "12:00": "{name}, a pergunta é simples... {link}",
        "18:00": "Hoje já entrou mais gente... {link}",
        "22:00": "Não confunda prévia... {link}",
    },
    4: {
        "12:00": "{name}, chega de enrolar... {link}",
        "18:00": "Sabe o que todo mundo diz?... {link}",
        "22:00": "Última chamada de hoje... {link}",
    },
    5: {
        "12:00": "{name}, mais um dia, mais uma leva... {link}",
        "18:00": "Tem gente que entrou ontem... {link}",
        "22:00": "O que você não pega hoje... {link}",
    },
    6: {
        "12:00": "{name}, se em 6 dias você ainda não entrou... {link}",
        "18:00": "Hoje já caiu mais material... {link}",
        "22:00": "Amanhã é o ultimato... {link}",
    },
    7: {
        "12:00": "⚠️ Último dia de acesso grátis... {link}",
        "18:00": "Seu tempo tá acabando... {link}",
        "22:00": "Game over: amanhã você roda... {link}",
    },
    "retarget": {
        1: {
            "12:00": "Você já tá fora do grupo... {link}",
            "18:00": "A sensação de ficar de fora... {link}",
            "22:00": "Enquanto você pensa... {link}",
        },
        2: {
            "12:00": "Mais um dia longe do conteúdo... {link}",
            "18:00": "Todo mundo evoluindo no VIP... {link}",
            "22:00": "Dormir com arrependimento... {link}",
        },
        3: {
            "12:00": "Último chamado: VIP ou nada... {link}",
            "18:00": "Sua última chance tá na sua frente... {link}",
            "22:00": "Game over: sem VIP não tem mais nada... {link}",
        },
    },
}

async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                joined_group INTEGER DEFAULT 0,
                removed INTEGER DEFAULT 0,
                banned INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                timestamp TEXT
            )
        """)
        await db.commit()

async def safe_send_message(user_id, text):
    retries = 0
    while retries < MAX_MESSAGE_RETRIES:
        try:
            await bot.send_message(user_id, text)
            await bot.send_video(user_id, VIDEO_FILE_ID, caption=f"🔥 Clique aqui e entre agora: {PURCHASE_LINK}")
            return
        except RetryAfter as e:
            await asyncio.sleep(e.timeout)
            retries += 1
        except (BotBlocked, ChatNotFound, UserDeactivated, Unauthorized):
            logger.warning(f"Não foi possível enviar para {user_id}")
            return
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem para {user_id}: {e}")
            retries += 1
    logger.error(f"Falha ao enviar mensagem para {user_id} após {MAX_MESSAGE_RETRIES} tentativas.")

async def send_scheduled_message(user_id, day, hour, is_retarget=False):
    try:
        if not is_retarget:
            message = MESSAGES_SCHEDULE.get(day, {}).get(hour)
        else:
            message = MESSAGES_SCHEDULE["retarget"].get(day, {}).get(hour)
        if not message:
            return
        formatted = message.format(
            name="mano",
            days=day,
            remaining=7-day if not is_retarget else 3-day,
            link=PURCHASE_LINK
        )
        await safe_send_message(user_id, formatted)
    except Exception as e:
        logger.error(f"Erro ao processar mensagem para {user_id}: {e}")

async def schedule_user_messages(user_id):
    now = datetime.now(TIMEZONE)
    for day in range(1, 8):
        for hour in ["12:00", "18:00", "22:00"]:
            send_time = TIMEZONE.localize(datetime.combine((now + timedelta(days=day-1)).date(),
                                                          datetime.strptime(hour, "%H:%M").time()))
            scheduler.add_job(
                send_scheduled_message,
                trigger=DateTrigger(run_date=send_time),
                args=[user_id, day, hour, False],
                replace_existing=True
            )
    # Agendar retarget
    for rday in range(1, 4):
        for hour in ["12:00", "18:00", "22:00"]:
            send_time = TIMEZONE.localize(datetime.combine((now + timedelta(days=7+rday-1)).date(),
                                                          datetime.strptime(hour, "%H:%M").time()))
            scheduler.add_job(
                send_scheduled_message,
                trigger=DateTrigger(run_date=send_time),
                args=[user_id, rday, hour, True],
                replace_existing=True
            )

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)", (user_id, message.from_user.username))
        await db.commit()
    await message.answer("Bem-vindo! Você foi adicionado ao funil.")
    await schedule_user_messages(user_id)

async def on_startup(dp):
    await init_db()
    scheduler.start()
    logger.info("Bot iniciado com sucesso.")

if __name__ == "__main__":
    from aiogram import executor
    executor.start_polling(dp, on_startup=on_startup)
