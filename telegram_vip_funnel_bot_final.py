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
DAYS_OF_PREVIEW = int(os.getenv("DAYS_OF_PREVIEW", "2"))
# Quantidade de dias de retarget após o término da prévia
RETARGET_DAYS = int(os.getenv("RETARGET_DAYS", "5"))

DB_PATH = os.getenv("DB_PATH", "vip_funnel_async.db")

# Envio imediato da mensagem do Dia 1 (delay em segundos após entrar no grupo)
SEND_IMMEDIATE_DELAY_SECONDS = int(os.getenv("SEND_IMMEDIATE_DELAY_SECONDS", "10"))
MAX_MESSAGE_RETRIES = int(os.getenv("MAX_MESSAGE_RETRIES", "3"))

# Vídeo .mp4 direto (usado como CTA em todos os envios)
VIDEO_URL = os.getenv("VIDEO_URL", "https://botdiscarado.com.br/video.mp4/leve.mp4")

# Admins (IDs). Use vírgula para múltiplos IDs em ADMINS.
ADMINS = set(map(int, os.getenv("ADMINS", "7708241274").split(",")))

# CTA persuasivo (usa {name}) — será usado na legenda do vídeo
CTA_TEXT = """
⚡ ATENÇÃO, {name}! SEU ACESSO GRATUITO ESTÁ SE ESGOTANDO! ⏰

🎯 ENQUANTO VOCÊ LÊ ESTA MENSAGEM:
✅ Membros VIP já estão acessando CONTEÚDO EXCLUSIVO
✅ Novos materiais sendo adicionados AGORA MESMO
✅ Você está PERDENDO as MELHORES PARTES!

💎 NO VIP VOCÊ GARANTE:
🚀 ACESSO COMPLETO 24/7
🔥 CONTEÚDO 100% EXCLUSIVO
🎯 SEM CENSURA • SEM LIMITES
⭐ ATUALIZAÇÕES DIÁRIAS

🚨 NÃO SEJA O ÚLTIMO DA FILA!
Quem espera SEMPRE fica para trás...

👉 FALE AGORA COM O BOT: @Grupo_Vip_BR2bot
"""

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
# Mensagens estruturadas (funil 2 dias x 5 envios/dia + retarget)
# (baseado no CODE B, adaptado)
# -------------------------
MESSAGES_SCHEDULE = {
  "1": {
    "12:00": "🔥 {name}, chegou a hora de sentir o que poucos têm coragem… 😏\n\nAqui é só a prévia, mas o VIP é o que vai te deixar sem palavras. Cada clique é uma explosão de sensação que você ainda nem imagina. 💥\n\n🎯 Assiste ao vídeo e sente o próximo nível: {link}",
    "18:00": "👀 {name}, o que tá rolando lá dentro tá deixando todo mundo sem fôlego… e você ainda só imaginando? 😬\n\nNão fica só olhando. Quer sentir? Corre: {link}",
    "22:00": "🌙 {name}, antes de dormir, pensa: enquanto você tá de fora, o VIP tá entregando experiências que te fariam perder o ar… 💦\n\n💎 Se quer entrar nesse jogo, é agora: {link}"
  },
  "2": {
    "12:00": "⏰ {name}, segundo dia de prévia… teu tempo tá acabando. ⚡\n\nDepois de hoje, só quem é VIP vai continuar sentindo o que você ainda só sonhou. 🔥\n\n🎯 Quer se entregar ao próximo nível? {link}",
    "18:00": "🔥 {name}, cada cena que sai lá dentro é um soco de desejo. E você ainda de fora, só imaginando… 😏\n\n🚀 Assiste o vídeo e sente, depois entra: {link}",
    "22:00": "⚠️ Última chamada antes de perder o acesso à prévia, {name}! ⏰\n\nO que acontece no VIP não é pra qualquer um. Quer ver de perto? 💎 {link}"
  },
  "retarget": {
    "1": {
      "12:00": "💔 {name}, sua prévia acabou… mas o VIP tá explodindo com tudo que você desejava. 🔥\n\n🎯 Quem voltou ontem já tá vivendo sensações que você só imaginou… {link}",
      "18:00": "👀 {name}, sente isso? O que tá rolando lá dentro agora é pra poucos — e você ainda tá de fora. 😏\n\n👉 Volta agora e não fica só imaginando: {link}",
      "22:00": "🎁 Oferta relâmpago pra ex-membros! ⚡\n\nAcesso total + condição especial liberada por tempo limitado! 💎 {discount_link}"
    },
    "2": {
      "12:00": "😏 {name}, cada clique que você perdeu tá deixando teu desejo só crescendo… 🔥\n\n💎 Volta enquanto o VIP tá aberto: {link}",
      "18:00": "⚡ {name}, cada minuto fora é uma sensação que você não sente. Quem tá dentro já tá vivendo tudo. 👀\n\n🎯 Não fica só na vontade, volta: {link}",
      "22:00": "🚨 Condição especial ainda disponível — mas por pouquíssimo tempo! ⏰\n\nÚltimas horas pra voltar com bônus e acesso completo: {discount_link}"
    },
    "3": {
      "12:00": "📅 {name}, já são 3 dias fora… e cada conteúdo novo tá deixando o VIP ainda mais insano. 🔥\n\nQuer sentir de verdade? Volta agora: {link}",
      "18:00": "💥 Cada cena nova deixa todo mundo enlouquecido… e você ainda só imaginando. 😏\n\n🎯 Não perde mais tempo, entra: {link}",
      "22:00": "⚡ Última chance com condição especial! 💎\n\nDepois disso, só acesso normal, sem bônus. Últimas horas: {discount_link}"
    },
    "4": {
      "12:00": "⏰ {name}, 4 dias fora… cada minuto é um prazer que você não sente. 😬\n\n🎯 Volta agora e experimenta tudo que tá rolando: {link}",
      "18:00": "🔥 O VIP tá cada vez mais intenso. Quem tá dentro já tá no ápice… e você ainda imaginando? 😏\n\n💎 Garante teu lugar: {link}",
      "22:00": "⚡ Últimas horas do acesso promocional! ⏰\n\n💥 Volta agora ou perde tudo: {discount_link}"
    },
    "5": {
      "12:00": "🚨 {name}, último dia de recuperação! 🕛\n\nDepois de hoje, todas as condições especiais se encerram. 💔\n\n🎯 É agora ou nunca: {link}",
      "18:00": "🔥 {name}, a porta tá quase fechando! ⚡\n\nÚltima chance de entrar com bônus ativo e acesso completo. 💎\n\nGarante aqui: {discount_link}",
      "22:00": "💀 Última mensagem, {name}! 🚨\n\nDepois disso, teu acesso é encerrado de vez. 🎯\n\nSe quiser sentir tudo de perto, agora é o momento: {discount_link}"
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

        start_text = """🎯 ACESSO LIBERADO - GRUPO PRÉVIAS 🎯

✅ Seu acesso temporário foi ativado com sucesso!

🔗 Entre agora no grupo:
{invite_link}

🚨 Informações importantes:
• Duração: {days} dias gratuitos
• Sistema anti-retorno ativo (não tente voltar sem pagar)
• O VIP oferece benefícios completos

👉 Dica: Entre AGORA mesmo e não perca nenhum conteúdo!""".format(
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
            INSERT INTO users (user_id, username, first_name, last_name, joined_group, join_time)
            VALUES (?, ?, ?, ?, 1, ?)
            ON CONFLICT(user_id) DO UPDATE SET 
                username=excluded.username,
                first_name=excluded.first_name,
                last_name=excluded.last_name,
                joined_group=1
            """,
            (user_id, username, first_name, last_name, join_time)
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
        await bot.ban_chat_member(PREVIEWS_GROUP_ID, user_id)
        # espera breve, menor que 60s
        await asyncio.sleep(5)
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
