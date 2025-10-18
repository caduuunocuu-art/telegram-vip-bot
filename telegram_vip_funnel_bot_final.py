# telegram_vip_funnel_bot_final.py
# ----------------------------------------------------------------------------
# Versão combinada: Código A com agendamento do Código B (3x/dia + retarget 3 dias)
# Dependências: aiogram, APScheduler, aiosqlite, pytz
# Instalação: pip install aiogram==2.* APScheduler aiosqlite pytz
# Execução: python telegram_vip_funnel_bot_final.py
# ----------------------------------------------------------------------------

import os
import asyncio
import logging
from datetime import datetime, timedelta
import pytz
import aiosqlite
from aiogram import Bot, Dispatcher, types
from aiogram.types import ChatType, ChatMemberUpdated
from aiogram.utils import executor
from aiogram.utils.exceptions import (
    RetryAfter, BotBlocked, ChatNotFound, UserDeactivated, Unauthorized,
    ChatAdminRequired, TelegramAPIError,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

# -------------------------
# CONFIG — EDITE/USE AMBIENTE
# -------------------------

API_TOKEN = os.getenv("TG_BOT_TOKEN", "PUT_YOUR_BOT_TOKEN_HERE")
PREVIEWS_GROUP_ID = int(os.getenv("PREVIEWS_GROUP_ID", "-1003053104506"))
PREVIEWS_GROUP_INVITE_LINK = os.getenv("PREVIEWS_GROUP_INVITE_LINK", "https://t.me/+wYpQExxUOzkyNDk5")
# Redirecionamento para o bot de vendas (clicável)
PURCHASE_LINK = os.getenv("PURCHASE_LINK", "https://t.me/Grupo_Vip_BR2bot")
DISCOUNT_LINK = os.getenv("DISCOUNT_LINK", PURCHASE_LINK)

# Janela de prévia (dias)
DAYS_OF_PREVIEW = int(os.getenv("DAYS_OF_PREVIEW", "2"))
# Quantidade de dias de retarget após o término da prévia
RETARGET_DAYS = int(os.getenv("RETARGET_DAYS", "5"))

DB_PATH = os.getenv("DB_PATH", "vip_funnel_async.db")
# Envio imediato da mensagem do Dia 1 (delay em segundos após entrar no grupo)
SEND_IMMEDIATE_DELAY_SECONDS = int(os.getenv("SEND_IMMEDIATE_DELAY_SECONDS", "10"))
MAX_MESSAGE_RETRIES = int(os.getenv("MAX_MESSAGE_RETRIES", "3"))

# Vídeo .mp4 direto (usado como CTA em todos os envios)
VIDEO_URL = os.getenv("VIDEO_URL", "https://botdiscarado.com.br/ingles.mp4/leve.mp4")

# Admins (IDs). Use vírgula para múltiplos IDs em ADMINS.
ADMINS = set(map(int, os.getenv("ADMINS", "7708241274").split(",")))

# CTA persuasivo (usa {name}) — será usado na legenda do vídeo
CTA_TEXT = """
⚡ ATTENTION, {name}! YOUR FREE ACCESS IS RUNNING OUT! ⏰

🎯 WHILE YOU READ THIS MESSAGE:
✅ VIP members are already accessing EXCLUSIVE CONTENT
✅ New content being added RIGHT NOW
✅ You're MISSING the BEST PARTS!

💎 WITH VIP, YOU'RE GUARANTEED:
🚀 FULL ACCESS 24/7
🔥 100% EXCLUSIVE CONTENT
🎯 UNCENSORED • UNLIMITED
⭐ DAILY UPDATES

🚨 DON'T BE LAST IN LINE! Those who wait ALWAYS get left behind...

👉 TALK TO THE BOT NOW: @Vips_Groupp_US_BOT
"""

# Horários configuráveis (formato "HH:MM")
MESSAGE_HOURS = os.getenv("MESSAGE_HOURS", "13:00,18:00,22:00").split(",")

# Timezone para agendamentos
TZ = pytz.timezone(os.getenv("TIMEZONE", "America/Sao_Paulo"))

# -------------------------
# Logging
# -------------------------

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -------------------------
# Bot & scheduler
# -------------------------

bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)
scheduler = AsyncIOScheduler(timezone=TZ)

# -------------------------
# DB schema (assíncrona)
# -------------------------

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    joined_group INTEGER DEFAULT 0,
    join_time INTEGER DEFAULT 0,
    removed INTEGER DEFAULT 0,
    banned INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    attempt_time INTEGER,
    reason TEXT
);
"""

_db_lock = asyncio.Lock()

# -------------------------
# Mensagens estruturadas (funil 2 dias x 5 envios/dia + retarget)
# (baseado no CODE B, adaptado)
# -------------------------

MESSAGES_SCHEDULE = {
  "1": {
    "12:00": "🔥 {name}, it's time to feel what few dare… 😏\n\nThis is just the preview, but the VIP is where everything happens — and you have no idea what you're missing. 💥\n\n🎯 Watch the video and taste the next level: {link}",
    "18:00": "👀 {name}, what's happening inside is leaving everyone breathless… and you're still just imagining it? 😬\n\nDon't just watch from the sidelines. Want to feel it? Hurry: {link}",
    "22:00": "🌙 {name}, before you sleep, think: while you're outside, the VIP is delivering experiences that would take your breath away… 💦\n\n💎 If you want in, now's the time: {link}"
  },
  "2": {
    "12:00": "⏰ {name}, day two of the preview… your time is running out. ⚡\n\nAfter today, only VIP members will keep experiencing what you've only dreamed of. 🔥\n\n🎯 Ready to enter the next level? {link}",
    "18:00": "🔥 {name}, every new scene inside hits like a punch of desire. And you're still outside, just imagining… 😏\n\n🚀 Watch the video, feel it, then join: {link}",
    "22:00": "⚠️ Last call before you lose access to the preview, {name}! ⏰\n\nWhat happens in VIP is on another level. Want a firsthand look? 💎 {link}"
  },
  "retarget": {
    "1": {
      "12:00": "💔 {name}, your preview has ended… but the VIP is exploding with everything you wanted. 🔥\n\n🎯 Those who joined yesterday are already immersed in experiences you've only dreamed of… {link}",
      "18:00": "👀 {name}, feel that? What's happening inside right now is for few — and you're still outside. 😏\n\n👉 Come back now and stop just imagining: {link}",
      "22:00": "🎁 Flash offer for former members! ⚡\n\nFull access + special condition available for a limited time! 💎 {discount_link}"
    },
    "2": {
      "12:00": "😏 {name}, every click you've missed is making your desire grow… 🔥\n\n💎 Come back while VIP is open: {link}",
      "18:00": "⚡ {name}, every minute outside is a sensation you're not feeling. Those inside are already experiencing it all. 👀\n\n🎯 Stop just wanting, come back: {link}",
      "22:00": "🚨 Special condition still available — but only for a very short time! ⏰\n\nLast hours to return with bonus and full access: {discount_link}"
    },
    "3": {
      "12:00": "📅 {name}, it's been 3 days outside… and every new VIP content is crazier than ever. 🔥\n\nWant to feel it for real? Come back now: {link}",
      "18:00": "💥 Every new scene is driving everyone insane… and you're still just imagining. 😏\n\n🎯 Stop wasting time, join now: {link}",
      "22:00": "⚡ Last chance with special condition! 💎\n\nAfter this, only normal access, no bonus. Last hours: {discount_link}"
    },
    "4": {
      "12:00": "⏰ {name}, 4 days outside… every minute is a pleasure you’re missing. 😬\n\n🎯 Come back now and experience everything happening inside: {link}",
      "18:00": "🔥 VIP is getting more intense by the minute. Those inside are at the peak… and you're still imagining? 😏\n\n💎 Secure your spot: {link}",
      "22:00": "⚡ Last hours of promotional access! ⏰\n\n💥 Come back now or lose it all: {discount_link}"
    },
    "5": {
      "12:00": "🚨 {name}, last day to recover access! 🕛\n\nAfter today, all special conditions end. 💔\n\n🎯 It's now or never: {link}",
      "18:00": "🔥 {name}, the door is almost closing! ⚡\n\nLast chance to join with active bonus and full access. 💎\n\nSecure it here: {discount_link}",
      "22:00": "💀 Last message, {name}! 🚨\n\nAfter this, your access is permanently closed. 🎯\n\nIf you want to experience it all firsthand, now's the moment: {discount_link}"
    }
  }
}


# -------------------------
# Inicialização do banco de dados
# -------------------------

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_SQL)
        await db.commit()
    logger.info("Banco de dados inicializado")

# -------------------------
# Handlers (mantidos do Código A)
# -------------------------

@dp.message_handler(commands=["start"], chat_type=ChatType.PRIVATE)
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    first_name = message.from_user.first_name or "Usuário"
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                INSERT OR IGNORE INTO users (user_id, username, first_name, last_name, joined_group, join_time)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    message.from_user.username,
                    first_name,
                    message.from_user.last_name,
                    0,
                    0,
                ),
            )
            await db.commit()

        start_text =start_text = """🎯 FREE ACCESS - PREVIEW GROUP 🎯

✅ Your temporary access has been successfully activated!

🔗 Join the group now: {invite_link}

🚨 Important information:
• Duration: {days} free days
• Anti-return system active (don't try to return without paying)
• VIP offers full benefits

👉 Tip: Join NOW and don't miss any content!""".format(
    invite_link=PREVIEWS_GROUP_INVITE_LINK,
    days=DAYS_OF_PREVIEW
)

        await message.answer(start_text)
        logger.info(f"Usuário {user_id} ({first_name}) recebeu link de convite via /start")
    except Exception as e:
        logger.exception(f"Erro no /start para {user_id}: {e}")
        await message.answer("❌ Ocorreu um erro. Tente novamente ou contate um administrador.")

# -------------------------
# Funções auxiliares (DB + envio)
# -------------------------

async def safe_send_message(chat_id: int, text: str, name_for_cta: str, max_retries: int = MAX_MESSAGE_RETRIES) -> bool:
    """Envia texto + vídeo com CTA, respeitando limites e re-tentativas."""
    attempt = 0
    while attempt < max_retries:
        try:
            if text:
                await bot.send_message(chat_id, text)
            # Envia o vídeo com legenda personalizada (CTA)
            caption = CTA_TEXT.format(name=name_for_cta)
            await bot.send_video(chat_id, VIDEO_URL, caption=caption)
            return True
        except RetryAfter as e:
            wait = getattr(e, 'timeout', getattr(e, 'retry_after', None)) or 5
            logger.info(f"RetryAfter: aguardando {wait}s antes de tentar novamente para {chat_id}")
            await asyncio.sleep(wait)
            attempt += 1
        except (BotBlocked, ChatNotFound, UserDeactivated, Unauthorized):
            logger.warning(f"Não foi possível enviar mensagem para {chat_id} (usuário bloqueou/desativado).")
            return False
        except Exception as e:
            logger.warning(f"Falha ao enviar mensagem para {chat_id} (tentativa {attempt+1}): {e}")
            attempt += 1
            await asyncio.sleep(2)

    logger.error(f"Falha permanente ao enviar mensagem para {chat_id} após {max_retries} tentativas.")
    return False

async def get_user_info(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT user_id, username, first_name, last_name, joined_group, join_time, removed, banned
            FROM users
            WHERE user_id = ?
            """,
            (user_id,),
        )
        row = await cursor.fetchone()
        return row if row else None

async def update_user_joined(user_id: int, username: str, first_name: str, last_name: str):
    join_time = int(datetime.now().timestamp())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT OR REPLACE INTO users (user_id, username, first_name, last_name, joined_group, join_time, removed, banned)
            VALUES (?, ?, ?, ?, 1, ?, 0, 0)
            """,
            (user_id, username, first_name, last_name, join_time),
        )
        await db.commit()

async def mark_user_removed(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET removed = 1 WHERE user_id = ?", (user_id,))
        await db.commit()

async def mark_user_banned(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET banned = 1 WHERE user_id = ?", (user_id,))
        await db.commit()

async def record_attempt(user_id: int, reason: str):
    attempt_time = int(datetime.now().timestamp())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO attempts (user_id, attempt_time, reason) VALUES (?, ?, ?)",
            (user_id, attempt_time, reason),
        )
        await db.commit()

# -------------------------
# Função que envia a mensagem agendada (texto formatado + vídeo CTA)
# -------------------------

async def send_scheduled_message(user_id: int, day: int, hour: str, is_retarget: bool = False):
    user_info = await get_user_info(user_id)
    if not user_info:
        logger.warning(f"Usuário {user_id} não encontrado no banco para envio agendado")
        return

    (
        _uid,
        username,
        first_name,
        last_name,
        joined_group,
        join_time,
        removed,
        banned,
    ) = user_info

    # Não enviar se removido/banido
    if removed or banned:
        logger.debug(f"Ignorado envio para {user_id}: removed={removed}, banned={banned}")
        return

    name = first_name or "Usuário"

    # Seleciona a mensagem conforme schedule ou retarget
    try:
        day_key = str(day)  # CORREÇÃO: converter int para str para lookup nas chaves do dict
        if not is_retarget:
            message_template = MESSAGES_SCHEDULE.get(day_key, {}).get(hour)
        else:
            message_template = MESSAGES_SCHEDULE.get("retarget", {}).get(day_key, {}).get(hour)

        if not message_template:
            logger.debug(f"Nenhuma mensagem configurada para day={day} hour={hour} retarget={is_retarget}")
            return

        remaining_days = max(DAYS_OF_PREVIEW - day, 0)
        formatted = message_template.format(
            name=name,
            days=DAYS_OF_PREVIEW,
            remaining=remaining_days,
            link=PURCHASE_LINK,
            discount_link=DISCOUNT_LINK
        )

        success = await safe_send_message(user_id, formatted, name_for_cta=name)
        if success:
            logger.info(f"Enviado (retarget={is_retarget}) dia {day} hora {hour} para {user_id} ({name})")
        else:
            logger.warning(f"Falha ao enviar (retarget={is_retarget}) dia {day} hora {hour} para {user_id}")
    except Exception as e:
        logger.exception(f"Erro em send_scheduled_message para {user_id}: {e}")

# -------------------------
# Agendamento das mensagens (7 dias x MESSAGE_HOURS) + retarget (RETARGET_DAYS)
# -------------------------

async def schedule_user_messages(user_id: int, username: str, first_name: str, last_name: str):
    now = datetime.now(TZ)

    # Agenda 7 dias (1..DAYS_OF_PREVIEW) com horas configuráveis
    for day in range(1, DAYS_OF_PREVIEW + 1):
        for hour in MESSAGE_HOURS:
            # calcula o run_dt: dia relativo + hora
            try:
                hour_dt = datetime.strptime(hour.strip(), "%H:%M").time()
            except Exception:
                logger.error(f"Formato de hora inválido em MESSAGE_HOURS: {hour}. Use HH:MM")
                continue

            target_date = (now + timedelta(days=day - 1)).date()
            run_dt = TZ.localize(datetime.combine(target_date, hour_dt))

            job_id = f"user_{user_id}_day_{day}_hour_{hour}"
            scheduler.add_job(
                send_scheduled_message,
                trigger=DateTrigger(run_date=run_dt),
                args=[user_id, day, hour.strip(), False],
                id=job_id,
                replace_existing=True,
            )

    # Agenda remoção no fim do período de prévia (após DAYS_OF_PREVIEW dias)
    removal_time = now + timedelta(days=DAYS_OF_PREVIEW)
    # marcar remoção no horário final do último dia (por segurança, adicionamos 1 minute)
    removal_dt = removal_time + timedelta(minutes=1)
    scheduler.add_job(
        remove_user_from_group,
        trigger=DateTrigger(run_date=removal_dt),
        args=[user_id],
        id=f"user_{user_id}_removal",
        replace_existing=True,
    )

    # Agenda retarget (dias seguintes ao fim da prévia): dia 1..RETARGET_DAYS
    # Esses retargets correspondem a day indexes 1..RETARGET_DAYS no bloco "retarget"
    for rday in range(1, RETARGET_DAYS + 1):
        for hour in MESSAGE_HOURS:
            try:
                hour_dt = datetime.strptime(hour.strip(), "%H:%M").time()
            except Exception:
                continue

            target_date = (now + timedelta(days=DAYS_OF_PREVIEW + rday - 1)).date()
            run_dt = TZ.localize(datetime.combine(target_date, hour_dt))

            job_id = f"user_{user_id}_retarget_day_{rday}_hour_{hour}"
            scheduler.add_job(
                send_scheduled_message,
                trigger=DateTrigger(run_date=run_dt),
                args=[user_id, rday, hour.strip(), True],
                id=job_id,
                replace_existing=True,
            )

    logger.info(f"Mensagens agendadas para o usuário {user_id} ({first_name}) — {DAYS_OF_PREVIEW} dias + {RETARGET_DAYS} retargets")

# -------------------------
# Remoção do usuário do grupo (kick) — mantém comportamento do Código A
# -------------------------

async def remove_user_from_group(user_id: int):
    try:
        # Tenta remover/banir e depois desbanir para "kick" efetivo
        await bot.ban_chat_member(PREVIEWS_GROUP_ID, user_id)
        # espera breve para garantir o kick (60s como antes)
        await asyncio.sleep(60)
        await bot.unban_chat_member(PREVIEWS_GROUP_ID, user_id)
        await mark_user_removed(user_id)

        user_info = await get_user_info(user_id)
        if user_info:
            name = user_info[2] or "Usuário"
            removal_text = "Your access to the preview group has ended, {name} ❌\n\nJoin VIP to continue: {link}".format(
                name=name,
                link=PURCHASE_LINK
            )
            await safe_send_message(user_id, removal_text, name_for_cta=name)

        logger.info(f"Usuário {user_id} removido do grupo de prévia")
    except (ChatAdminRequired, TelegramAPIError) as e:
        logger.error(f"Erro ao remover usuário {user_id} do grupo: {e}")
        # Notifica administradores
        for admin_id in ADMINS:
            try:
                await bot.send_message(admin_id, f"Erro ao remover usuário {user_id} do grupo: {e}")
            except Exception:
                pass

# -------------------------
# Handler para novos membros no grupo de prévia (anti-retorno)
# -------------------------

@dp.chat_member_handler(chat_id=PREVIEWS_GROUP_ID)
async def handle_chat_member_update(update: ChatMemberUpdated):
    # Quando alguém entra como 'member' no grupo de prévias
    try:
        if update.new_chat_member.status == 'member':
            user = update.new_chat_member.user
            user_id = user.id
            user_info = await get_user_info(user_id)

            # Se já foi removido: tentativa de retorno => ban + aviso
            if user_info and user_info[6]:  # removed flag
                await record_attempt(user_id, "Tentativa de retorno após remoção")
                try:
                    await bot.ban_chat_member(PREVIEWS_GROUP_ID, user_id)
                    await mark_user_banned(user_id)
                    name = user.first_name or "Usuário"
                    ban_text = "{name}, your free access has expired. To return, only in VIP: {link}".format(
                        name=name,
                        link=PURCHASE_LINK
                    )
                    await safe_send_message(user_id, ban_text, name_for_cta=name)
                    logger.info(f"Usuário {user_id} ({name}) banido por tentativa de retorno")
                except Exception as e:
                    logger.error(f"Erro ao banir usuário {user_id}: {e}")

            # Novo usuário (ou ainda não marcado como joined)
            elif not user_info or not user_info[4]:  # joined_group flag
                await update_user_joined(user_id, user.username, user.first_name, user.last_name)

                # --- AÇÃO ADICIONADA: envia vídeo CTA imediato na entrada do grupo ---
                try:
                    caption = CTA_TEXT.format(name=user.first_name or "Usuário")
                    await bot.send_video(user_id, VIDEO_URL, caption=caption)
                    logger.info(f"Vídeo CTA de boas-vindas enviado para {user_id}")
                except Exception as e:
                    logger.error(f"Erro ao enviar vídeo CTA de boas-vindas para {user_id}: {e}")
                # --- fim da ação adicionada ---

                await schedule_user_messages(user_id, user.username, user.first_name, user.last_name)

                # Mensagem imediata (dia 1) — enviamos a primeira mensagem após pequeno delay (configurável)
                await asyncio.sleep(SEND_IMMEDIATE_DELAY_SECONDS)
                # Primeiro envio: dia 1 - assumimos que uma das horas contém "12:00" etc., mas aqui chamamos diretamente:
                # Vamos enviar a mensagem do "day 1" para o horário corrente (com is_retarget=False)
                # Para consistência com o agendamento, enviamos a mensagem do dia 1 no horário do primeiro MESSAGE_HOURS[0]
                first_hour = MESSAGE_HOURS[0].strip() if MESSAGE_HOURS else "12:00"
                await send_scheduled_message(user_id, 1, first_hour, is_retarget=False)

                logger.info(f"Novo usuário {user_id} ({user.first_name}) adicionado ao grupo e agendado")
    except Exception as e:
        logger.exception(f"Erro ao processar chat_member_update: {e}")

# -------------------------
# Comandos administrativos (stats & broadcast) — adaptado do A
# -------------------------

@dp.message_handler(commands=['stats'], user_id=list(ADMINS), chat_type=ChatType.PRIVATE)
async def cmd_stats(message: types.Message):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            # Total de usuários
            cursor = await db.execute("SELECT COUNT(*) FROM users")
            total_users = (await cursor.fetchone())[0]

            # Usuários ativos
            cursor = await db.execute(
                "SELECT COUNT(*) FROM users WHERE joined_group = 1 AND removed = 0 AND banned = 0"
            )
            active_users = (await cursor.fetchone())[0]

            # Usuários removidos
            cursor = await db.execute("SELECT COUNT(*) FROM users WHERE removed = 1")
            removed_users = (await cursor.fetchone())[0]

            # Usuários banidos
            cursor = await db.execute("SELECT COUNT(*) FROM users WHERE banned = 1")
            banned_users = (await cursor.fetchone())[0]

            # Tentativas de retorno
            cursor = await db.execute("SELECT COUNT(*) FROM attempts")
            attempts = (await cursor.fetchone())[0]

        stats_text = (
            f"📊 Estatísticas do Bot VIP Funnel:\n\n"
            f"• Total de usuários: {total_users}\n"
            f"• Usuários ativos: {active_users}\n"
            f"• Usuários removidos: {removed_users}\n"
            f"• Usuários banidos: {banned_users}\n"
            f"• Tentativas de retorno: {attempts}"
        )
        await message.answer(stats_text)
    except Exception as e:
        logger.exception(f"Erro ao recuperar estatísticas: {e}")
        await message.answer("❌ Erro ao recuperar estatísticas.")

# Broadcast com confirmação simples em memória
_pending_broadcast = {}

@dp.message_handler(commands=['broadcast'], user_id=list(ADMINS), chat_type=ChatType.PRIVATE)
async def cmd_broadcast(message: types.Message):
    if not message.reply_to_message or not (message.reply_to_message.text or message.reply_to_message.caption):
        await message.answer("❌ Use este comando em *resposta* a uma mensagem de texto para fazer broadcast.", parse_mode="Markdown")
        return

    content = message.reply_to_message.text or message.reply_to_message.caption
    _pending_broadcast[message.from_user.id] = content

    preview = (content[:400] + '…') if len(content) > 400 else content
    await message.answer(
        "📢 Confirmar broadcast para *todos os usuários*?\n\n" +
        f"Prévia:\n\n{preview}\n\n" +
        "Digite /confirmar para prosseguir ou /cancelar para abortar.",
        parse_mode="Markdown",
    )

@dp.message_handler(commands=['confirmar'], user_id=list(ADMINS), chat_type=ChatType.PRIVATE)
async def cmd_confirm_broadcast(message: types.Message):
    admin_id = message.from_user.id
    content = _pending_broadcast.get(admin_id)
    if not content:
        await message.answer("Não há broadcast pendente. Use /broadcast respondendo a uma mensagem.")
        return

    sent = 0
    failed = 0
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT user_id, first_name FROM users") as cursor:
                async for row in cursor:
                    uid, fname = row
                    name = fname or "Usuário"
                    try:
                        await safe_send_message(uid, content, name_for_cta=name)
                        sent += 1
                    except Exception:
                        failed += 1
                    # Evita flood/hard rate-limit
                    await asyncio.sleep(0.05)
    finally:
        _pending_broadcast.pop(admin_id, None)

    await message.answer(f"✅ Broadcast finalizado. Enviados: {sent} | Falhas: {failed}")

@dp.message_handler(commands=['cancelar'], user_id=list(ADMINS), chat_type=ChatType.PRIVATE)
async def cmd_cancel_broadcast(message: types.Message):
    _pending_broadcast.pop(message.from_user.id, None)
    await message.answer("❌ Broadcast cancelado.")

# -------------------------
# Startup / Shutdown
# -------------------------

async def on_startup(_):
    await init_db()
    scheduler.start()
    logger.info("Bot iniciado e agendador ativado")

async def on_shutdown(_):
    scheduler.shutdown()
    logger.info("Bot desligado e agendador parado")

if __name__ == '__main__':
    executor.start_polling(
        dp,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        skip_updates=True,
        allowed_updates=["message", "chat_member"]  # <-- ADICIONADO
    )
