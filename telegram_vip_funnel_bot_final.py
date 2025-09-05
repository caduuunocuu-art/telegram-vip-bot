# Telegram VIP Funnel Bot — Versão Final (com vídeo + CTA + anti-retorno + estatísticas + broadcast)
# ----------------------------------------------------------------------------
# Dependências: aiogram, APScheduler, aiosqlite
# Instalação:   pip install aiogram==2.* APScheduler aiosqlite
# Execução:     python telegram_vip_funnel_bot_final.py
# ----------------------------------------------------------------------------
# AVISO IMPORTANT: NÃO COMMITAR TOKENS/SECRETS EM REPOS PÚBLICOS.
# Este arquivo utiliza a variável de ambiente TG_BOT_TOKEN. Configure no Railway
# ou no seu ambiente local antes de rodar. O valor abaixo é um placeholder.
# ----------------------------------------------------------------------------

import os
import asyncio
import logging
from datetime import datetime, timedelta

import aiosqlite
from aiogram import Bot, Dispatcher, types
from aiogram.types import ChatType, ChatMemberUpdated
from aiogram.utils import executor
from aiogram.utils.exceptions import (
    RetryAfter,
    BotBlocked,
    ChatNotFound,
    UserDeactivated,
    Unauthorized,
    ChatAdminRequired,
    TelegramAPIError,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# -------------------------
# CONFIG — EDITE/USE AMBIENTE
# -------------------------
API_TOKEN = os.getenv("TG_BOT_TOKEN", "PUT_YOUR_BOT_TOKEN_HERE")  # <-- substitua por variável de ambiente no Railway
PREVIEWS_GROUP_ID = int(os.getenv("PREVIEWS_GROUP_ID", "-1003053104506"))
PREVIEWS_GROUP_INVITE_LINK = os.getenv("PREVIEWS_GROUP_INVITE_LINK", "https://t.me/+wYpQExxUOzkyNDk5")

# Redirecionamento para o bot de vendas (clicável)
PURCHASE_LINK = os.getenv("PURCHASE_LINK", "https://t.me/Grupo_Vip_BR2bot")
# Link "de desconto" aponta para o mesmo destino (bot de vendas)
DISCOUNT_LINK = os.getenv("DISCOUNT_LINK", PURCHASE_LINK)

# Janela de prévia (dias)
DAYS_OF_PREVIEW = int(os.getenv("DAYS_OF_PREVIEW", "7"))
DB_PATH = os.getenv("DB_PATH", "vip_funnel_async.db")

# Envio imediato da mensagem do Dia 1 (delay em segundos após entrar no grupo)
SEND_IMMEDIATE_DELAY_SECONDS = int(os.getenv("SEND_IMMEDIATE_DELAY_SECONDS", "10"))
MAX_MESSAGE_RETRIES = int(os.getenv("MAX_MESSAGE_RETRIES", "3"))

# Vídeo .mp4 direto (testado e funcional)
VIDEO_URL = os.getenv("VIDEO_URL", "https://botdiscarado.com.br/video.mp4/leve.mp4")

# Admins (IDs). Use vírgula para múltiplos IDs em ADMINS.
ADMINS = set(map(int, os.getenv("ADMINS", "7708241274").split(",")))

# CTA persuasivo (usa {name}) — será usado na legenda do vídeo
CTA_TEXT = (
    "🔥 Seu acesso gratuito está terminando, {name}!\n\n"
    "No VIP você tem acesso completo e exclusivo, sem limitações.\n"
    "Não deixe para depois — quem entra agora garante todos os benefícios.\n\n"
    "👉 Fale agora com o bot: @Grupo_Vip_BR2bot"
)

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
# Timezone definido para evitar agendamentos em UTC sem querer
scheduler = AsyncIOScheduler(timezone="America/Sao_Paulo")

# -------------------------
# DB schema (assíncrona)
# -------------------------
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    joined_group INTEGER,
    join_time INTEGER,
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
# Mensagens (personalize aqui)
# -------------------------
MESSAGES = {
    'start': (
        "🎉 Bem-vindo ao VIP Funnel Bot!\n\n"
        "Clique no link abaixo para entrar no grupo de prévias:\n{invite_link}\n\n"
        "• Você terá acesso por {days} dias\n"
        "• Depois disso, só no VIP\n"
        "• Sistema anti-retorno ativo (não tente voltar sem pagar)"
    ),
    1: "Oi, {name}! 👋\n\nVocê entrou no Grupo de Prévia Exclusiva. Acompanhe por {days} dias!",
    2: "{name}, já viu a prévia que saiu hoje? 🔥\n\nRestam {remaining} dias de acesso.",
    3: "{name}, passaram 2 dias desde que você entrou.\n\nRestam {remaining} dias de acesso.",
    4: "{name}, várias pessoas já foram para o VIP…\n\nVale conferir!\nRestam {remaining} dias.",
    5: "Últimos {remaining} dias, {name}! ⏳\n\nDepois disso, só VIP.",
    6: "{name}, amanhã é seu último dia de prévia.\n\nOferta especial: fale com o bot @Grupo_Vip_BR2bot",
    7: "{name}, seu acesso termina hoje! ⏳\n\nÚltima chance para entrar no VIP: {link}",
    'removed': "Seu acesso ao grupo de prévia acabou, {name} ❌\n\nEntre no VIP para continuar: {link}",
    'banned_return': "{name}, seu acesso gratuito já expirou. Para voltar, só no VIP: {link}",
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
# Handlers
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

        start_text = MESSAGES['start'].format(
            invite_link=PREVIEWS_GROUP_INVITE_LINK,
            days=DAYS_OF_PREVIEW,
        )
        await message.answer(start_text)
        logger.info(f"Usuário {user_id} ({first_name}) recebeu link de convite via /start")
    except Exception as e:
        logger.exception(f"Erro no /start para {user_id}: {e}")
        await message.answer("❌ Ocorreu um erro. Tente novamente ou contate um administrador.")

# -------------------------
# Funções auxiliares
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
            await asyncio.sleep(wait)
            attempt += 1
        except (BotBlocked, ChatNotFound, UserDeactivated, Unauthorized):
            return False
        except Exception as e:
            logger.warning(f"Falha ao enviar mensagem para {chat_id} (tentativa {attempt+1}): {e}")
            attempt += 1
            await asyncio.sleep(2)
    return False

async def get_user_info(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT user_id, username, first_name, last_name, joined_group, join_time, removed, banned
            FROM users WHERE user_id = ?
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

async def send_daily_message(user_id: int, day: int):
    user_info = await get_user_info(user_id)
    if not user_info:
        logger.warning(f"Usuário {user_id} não encontrado no banco para envio de mensagem diária")
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

    if removed or banned:
        return

    name = first_name or "Usuário"
    remaining_days = max(DAYS_OF_PREVIEW - day, 0)

    message_text = None
    if day in MESSAGES:
        message_text = MESSAGES[day].format(
            name=name,
            days=DAYS_OF_PREVIEW,
            remaining=remaining_days,
            discount_link=DISCOUNT_LINK,
            link=PURCHASE_LINK,
        )

    success = await safe_send_message(user_id, message_text, name_for_cta=name)
    if success:
        logger.info(f"Mensagem do dia {day} enviada para {user_id} ({name})")
    else:
        logger.warning(f"Falha ao enviar mensagem do dia {day} para {user_id}")

async def schedule_user_messages(user_id: int, username: str, first_name: str, last_name: str):
    # Agendar mensagens para os próximos dias, 1..DAYS_OF_PREVIEW
    now = datetime.now()
    for day in range(1, DAYS_OF_PREVIEW + 1):
        run_dt = now + timedelta(days=day)
        scheduler.add_job(
            send_daily_message,
            'date',
            run_date=run_dt,
            args=[user_id, day],
            id=f"user_{user_id}_day_{day}",
            replace_existing=True,
        )

    # Agendar remoção do grupo após o período de prévia
    removal_time = now + timedelta(days=DAYS_OF_PREVIEW)
    scheduler.add_job(
        remove_user_from_group,
        'date',
        run_date=removal_time,
        args=[user_id],
        id=f"user_{user_id}_removal",
        replace_existing=True,
    )
    logger.info(f"Mensagens agendadas para o usuário {user_id} ({first_name})")

async def remove_user_from_group(user_id: int):
    try:
        # Tenta remover/banir e depois desbanir para "kick" efetivo
        await bot.ban_chat_member(PREVIEWS_GROUP_ID, user_id)
        await asyncio.sleep(60)
        await bot.unban_chat_member(PREVIEWS_GROUP_ID, user_id)

        await mark_user_removed(user_id)

        user_info = await get_user_info(user_id)
        if user_info:
            name = user_info[2] or "Usuário"
            removal_text = MESSAGES['removed'].format(name=name, link=PURCHASE_LINK)
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
# Handler para novos membros no grupo de prévia
# -------------------------
@dp.chat_member_handler(chat_id=PREVIEWS_GROUP_ID)
async def handle_chat_member_update(update: ChatMemberUpdated):
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
                ban_text = MESSAGES['banned_return'].format(name=name, link=PURCHASE_LINK)
                await safe_send_message(user_id, ban_text, name_for_cta=name)
                logger.info(f"Usuário {user_id} ({name}) banido por tentativa de retorno")
            except Exception as e:
                logger.error(f"Erro ao banir usuário {user_id}: {e}")

        # Novo usuário (ou ainda não marcado como joined)
        elif not user_info or not user_info[4]:  # joined_group flag
            await update_user_joined(user_id, user.username, user.first_name, user.last_name)
            await schedule_user_messages(user_id, user.username, user.first_name, user.last_name)

            # Mensagem imediata (dia 1)
            await asyncio.sleep(SEND_IMMEDIATE_DELAY_SECONDS)
            await send_daily_message(user_id, 1)
            logger.info(f"Novo usuário {user_id} ({user.first_name}) adicionado ao grupo e agendado")

# -------------------------
# Comandos administrativos
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
                        # Envia texto + vídeo CTA como nos envios diários
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
    )
