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

# Configurações gerais
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

# Mensagens estruturadas (funil completo)
MESSAGES_SCHEDULE = {
    1: {
        "12:00": "👉 {name}, você caiu no lugar certo. O que tá rolando aqui é só uma prévia do que a galera VIP já tá devorando. Quer dar o próximo passo? Clica e entra: {link}",
        "18:00": "👉 Mano, já vi uns 10 prints da galera VIP hoje rindo da prévia. Tá na cara: quem tá dentro tá no lucro. E você, vai continuar de fora? {link}",
        "22:00": "👉 Antes de dormir, um aviso: o que você viu hoje não é nem metade. No VIP é o jogo completo. Vai ficar só sonhando? {link}",
    },
    2: {
        "12:00": "👉 {name}, acordou? O VIP não espera. O que você viu ontem já tá velho, o que subiu hoje só tá lá dentro. Quer acesso real? {link}",
        "18:00": "👉 Tem dois tipos de gente: quem assiste a prévia e quem manda no VIP. Tá em qual lado? Decide agora: {link}",
        "22:00": "👉 Enquanto você enrola, o grupo VIP cresce. E cada minuto que passa, mais conteúdo escapa da sua mão. Só tem um jeito de parar essa perda: {link}",
    },
    3: {
        "12:00": "👉 {name}, a pergunta é simples: você tá satisfeito só com migalha ou vai atrás do banquete? O VIP é onde tá o verdadeiro jogo: {link}",
        "18:00": "👉 Hoje já entrou mais gente no VIP só pra não perder nada. Você ainda aí, só olhando a porta? Tá aberta agora: {link}",
        "22:00": "👉 Não confunda prévia com conteúdo de verdade. Aqui é só degustação. O prato principal tá te esperando no VIP: {link}",
    },
    4: {
        "12:00": "👉 {name}, chega de enrolar. O grupo VIP é onde o pau quebra. A prévia não vai te dar nada além de vontade. Clica e resolve: {link}",
        "18:00": "👉 Sabe o que todo mundo que já entrou no VIP diz? Que enrolou demais. Você vai ser o próximo arrependido ou vai resolver logo? {link}",
        "22:00": "👉 Última chamada de hoje: VIP é acesso total, sem censura, sem espera. Tá pronto ou vai dormir na vontade? {link}",
    },
    5: {
        "12:00": "👉 {name}, mais um dia, mais uma leva de conteúdo no VIP. Aqui fora você só assiste trailer. Vai continuar nesse ciclo? {link}",
        "18:00": "👉 Tem gente que entrou ontem e já tá dizendo que foi a melhor escolha do mês. E você, ainda pensando? {link}",
        "22:00": "👉 O que você não pega hoje, não volta amanhã. VIP é movimento, não é museu. Quer ver ou quer perder? {link}",
    },
    6: {
        "12:00": "👉 {name}, se em 6 dias você ainda não entrou no VIP, só tem dois motivos: ou tá enrolando ou tá com medo. Qual é o seu caso? {link}",
        "18:00": "👉 Hoje já caiu mais material no VIP do que você viu em todos esses dias de prévia. E adivinha? Você ficou de fora. Vai corrigir isso agora? {link}",
        "22:00": "👉 Amanhã é o ultimato. Seu tempo grátis acaba. Se ainda não decidiu, prepara: ou você vai pro VIP ou vai rodar. Antecipe: {link}",
    },
    7: {
        "12:00": "👉 {name} — é agora. Hoje é o último dia da sua prévia. Depois disso, adeus acesso gratuito. VIP é vida — entra agora e garante tudo antes que cortem seu acesso: {link}",
        "18:00": "👉 Cara, se você tá enrolando, olha a real: quem volta depois chora. O VIP tem tudo que você não vai ver mais aqui. Últimas horas — decide AGORA: {link}",
        "22:00": "👉 {name}, acabou. À meia-noite seu acesso some. Ou você entra no VIP e fica com tudo, ou fica olhando o resto só por fora. Escolha: {link} — é a última chamada.",
    },
    "retarget": {
        1: {
            "12:00": "👉 {name}, você perdeu o acesso à prévia. Quem tá lá dentro tá aproveitando full. Voltar só no VIP — entra agora: {link}",
            "18:00": "👉 {name}, alguém acabou de postar algo INSANO no VIP. Você ficou de fora. Quer voltar? Só no VIP: {link}",
            "22:00": "👉 Última chance do dia — se não você perde o que já rolou. Promo? Só por pouco tempo: {discount_link}",
        },
        2: {
            "12:00": "👉 A real é: quem não entrou já se arrependeu. Não seja mais um que ficou só na vontade. VIP agora: {link}",
            "18:00": "👉 {name}, você tá perdendo vantagem. Quem comprou já tá consumindo conteúdo exclusivo. Volta logo: {link}",
            "22:00": "👉 Oferta final do dia — desconto relâmpago pra quem agir agora: {discount_link}",
        },
        3: {
            "12:00": "👉 Último dia do resgate, {name}. Depois disso a oferta some. Quer entrar pro VIP ou vai ficar só lamentando? {link}",
            "18:00": "👉 Final real: é agora ou nunca. Decisão na sua mão — VIP e fim da história: {link}",
            "22:00": "👉 É a última mensagem que você vai receber. Após isso, nada. Não diga que não avisei. Última chance com desconto: {discount_link}",
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
            link=PURCHASE_LINK,
            discount_link=PURCHASE_LINK
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
    welcome_message = (
        f"🎉 Bem-vindo ao VIP Funnel Bot!\n\n"
        f"Clique no link abaixo para entrar no grupo de prévias:\n"
        f"{PREVIEWS_GROUP_INVITE_LINK}\n\n"
        f"• Você terá acesso por {DAYS_OF_PREVIEW} dias\n"
        f"• Depois disso, só no VIP\n"
        f"• Sistema anti-retorno ativo (não tente voltar sem pagar)"
    )
    await message.answer(welcome_message)
    await schedule_user_messages(user_id)

@dp.message_handler(commands=["reset_usuario"])
async def reset_usuario(message: types.Message):
    if str(message.from_user.id) not in ADMINS:
        await message.answer("❌ Você não tem permissão para usar este comando.")
        return

    args = message.get_args().strip()
    if not args.isdigit():
        await message.answer("⚠️ Use assim: /reset_usuario <user_id>")
        return

    user_id = int(args)
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            UPDATE users
            SET removed = 0, banned = 0
            WHERE user_id = ?
        """, (user_id,))
        await db.commit()

    await schedule_user_messages(user_id)

    await message.answer(f"✅ Usuário {user_id} foi resetado e terá acesso a mais {DAYS_OF_PREVIEW} dias.")

# -------------------------
# Enviar vídeo quando entra no grupo
# -------------------------
@dp.chat_member_handler(chat_id=PREVIEWS_GROUP_ID)
async def handle_chat_member_update(update: ChatMemberUpdated):
    if update.new_chat_member.status == 'member':
        user = update.new_chat_member.user
        user_id = user.id

        # Verifica se já entrou antes
        async with aiosqlite.connect(DB_FILE) as db:
            cursor = await db.execute("SELECT joined_group FROM users WHERE user_id = ?", (user_id,))
            row = await cursor.fetchone()

        if row is None:
            # Novo usuário
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute(
                    "INSERT INTO users (user_id, username, joined_group) VALUES (?, ?, ?)",
                    (user_id, user.username, 1)
                )
                await db.commit()
        elif row[0] == 0:
            # Marca como entrou
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute("UPDATE users SET joined_group = 1 WHERE user_id = ?", (user_id,))
                await db.commit()

        # Agenda mensagens do funil
        await schedule_user_messages(user_id)

        # Envia vídeo de vendas após delay
        await asyncio.sleep(SEND_IMMEDIATE_DELAY_SECONDS)
        await safe_send_message(user_id, f"🔥 Olá {user.first_name or 'mano'}! Confira seu vídeo de prévia e entre agora: {PURCHASE_LINK}")

async def on_startup(dp):
    await init_db()
    scheduler.start()
    logger.info("Bot iniciado com sucesso.")

if __name__ == "__main__":
    from aiogram import executor
    executor.start_polling(dp, on_startup=on_startup)
