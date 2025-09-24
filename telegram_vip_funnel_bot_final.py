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
    RetryAfter,
    BotBlocked,
    ChatNotFound,
    UserDeactivated,
    Unauthorized,
    ChatAdminRequired,
    TelegramAPIError,
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
DAYS_OF_PREVIEW = int(os.getenv("DAYS_OF_PREVIEW", "7"))
# Quantidade de dias de retarget após o término da prévia
RETARGET_DAYS = int(os.getenv("RETARGET_DAYS", "3"))

DB_PATH = os.getenv("DB_PATH", "vip_funnel_async.db")

# Envio imediato da mensagem do Dia 1 (delay em segundos após entrar no grupo)
SEND_IMMEDIATE_DELAY_SECONDS = int(os.getenv("SEND_IMMEDIATE_DELAY_SECONDS", "10"))
MAX_MESSAGE_RETRIES = int(os.getenv("MAX_MESSAGE_RETRIES", "3"))

# Vídeo .mp4 direto (usado como CTA em todos os envios)
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

# Horários configuráveis (formato "HH:MM")
MESSAGE_HOURS = os.getenv("MESSAGE_HOURS", "12:00,18:00,22:00").split(",")

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
# Mensagens estruturadas (funil 7 dias x 3 envios/dia + retarget)
# (baseado no CODE B, adaptado)
# -------------------------
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

        start_text = (
            "🎉 Bem-vindo ao VIP Funnel Bot!\n\n"
            "Clique no link abaixo para entrar no grupo de prévias:\n{invite_link}\n\n"
            "• Você terá acesso por {days} dias\n"
            "• Depois disso, só no VIP\n"
            "• Sistema anti-retorno ativo (não tente voltar sem pagar)"
        ).format(invite_link=PREVIEWS_GROUP_INVITE_LINK, days=DAYS_OF_PREVIEW)

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
        if not is_retarget:
            message_template = MESSAGES_SCHEDULE.get(day, {}).get(hour)
        else:
            message_template = MESSAGES_SCHEDULE.get("retarget", {}).get(day, {}).get(hour)

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
            removal_text = "Seu acesso ao grupo de prévia acabou, {name} ❌\n\nEntre no VIP para continuar: {link}".format(
                name=name, link=PURCHASE_LINK
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
                    ban_text = "{name}, seu acesso gratuito já expirou. Para voltar, só no VIP: {link}".format(
                        name=name, link=PURCHASE_LINK
                    )
                    await safe_send_message(user_id, ban_text, name_for_cta=name)
                    logger.info(f"Usuário {user_id} ({name}) banido por tentativa de retorno")
                except Exception as e:
                    logger.error(f"Erro ao banir usuário {user_id}: {e}")

            # Novo usuário (ou ainda não marcado como joined)
            elif not user_info or not user_info[4]:  # joined_group flag
                await update_user_joined(user_id, user.username, user.first_name, user.last_name)
                await schedule_user_messages(user_id, user.username, user.first_name, user.last_name)

                # Mensagem imediata (dia 1) — enviamos a primeira mensagem após pequeno delay (configurável)
                await asyncio.sleep(SEND_IMMEDIATE_DELAY_SECONDS)
                # Primeiro envio: dia 1  - assumimos que uma das horas contém "12:00" etc., mas aqui chamamos diretamente:
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
    )
