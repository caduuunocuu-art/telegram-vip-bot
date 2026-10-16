# telegram_vip_funnel_bot_final.py
# ----------------------------------------------------------------------------
# Versão 100% funcional - Mantém todas as funcionalidades originais
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
print("🔥 PREVIEWS_GROUP_ID EM USO:", PREVIEWS_GROUP_ID)
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
🚨 {name}, SEU TEMPO ESTÁ ACABANDO! ⏰

🚨 NO VIP VOCÊ VERIA AGORA:
✅ Cenas COMPLETAS sem cortes
✅ Ângulos EXCLUSIVOS
✅ Conteúdo 100% SEM CENSURA
✅ OnlyFans vazado HOJE

💎 NO VIP VOCÊ TEM ACESSO IMEDIATO A:
⭐ Conteúdo 100% ORIGINAL (nada repetido)
⭐ Atualizações DIÁRIAS garantidas
⭐ Suporte prioritário 24h
⭐ Grupo sigiloso e anônimo

📊 ENQUANTO VOCÊ VÊ PRÉVIA:
⭐ 47 pessoas entraram no VIP
⭐ 83 novos conteúdos postados

👉 GARANTA SUA VAGA: {link}
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
# -------------------------
MESSAGES_SCHEDULE = {
  "1": {
    "12:00": "🚨 {name}, SEU ACESSO À PRÉVIA COMEÇOU!\n\n⚠️ ATENÇÃO: Você tem 48h para aproveitar o conteúdo GRÁTIS antes do banimento automático!\n\n🔥 Enquanto isso, no VIP: +15 cenas EXCLUSIVAS por dia\n💎 Veja o que te espera: {link}",
        "18:00": "😈 {name}, AS CENAS MAIS PESADAS ESTÃO NO VIP!\n\nAqui você vê apenas amostras, lá tem:\n• Cenas COMPLETAS e sem censura\n• Conteúdo DIÁRIO nunca visto antes\n• OnlyFans vazado\n• Close Friends exclusivo\n\n⚡ Não se contente com migalhas: {link}",
        "22:00": "🌙 {name}, HOJE 23 PESSOAS MIGRARAM DA PRÉVIA PARA O VIP!\n\nCansaram de ver só o básico e foram para o conteúdo completo!\n\n🚀 Sua vez amanhã? {link}"
  },
  "2": {
     "12:00": "⏰ {name}, FALTAM 12H PARA O SEU BANIMENTO!\n\nSeu acesso gratuito termina HOJE às 22h!\n\n🔞 No VIP você teria acesso agora a:\n✅ +500 cenas COMPLETAS\n✅ +50 OnlyFans vazados\n✅ Conteúdo DIÁRIO\n\n💀 Vai perder essa chance? {link}",
        "18:00": "🚨 {name}, ALERTA VERMELHO: 6H RESTANTES!\n\nSeu banimento da prévia está chegando!\n\n🔥 Última chance para entrar no VIP com:\n• Acesso vitalício\n• Conteúdo sem censura\n• Atualizações diárias\n\n⚡ Corre antes que acabe: {link}",
        "22:00": "💀 {name}, ÚLTIMA HORA NA PRÉVIA!\n\nFALTAM APENAS 60 MINUTOS para o seu banimento!\n\n🎯 Dos 47 banidos hoje, 41 entraram no VIP!\n\n🚀 Última chamada: {link}"
  },
  "retarget": {
    "1": {
          "12:00": "💔 {name}, VOCÊ FOI REMOVIDO DA PRÉVIA…\n\nMas sua jornada adulta não precisa acabar aqui!\n\n🔞 No VIP você teria acesso AGORA a:\n• Cenas completas que você perdeu\n• OnlyFans exclusivos\n• Conteúdo SEM CENSURA\n\n⚡ Volte agora: {link}",
            "18:00": "😈 {name}, SENTIU FALTA DAS CENAS QUENTES?\n\nEnquanto você estava fora, soltaram +8 NOVAS cenas no VIP!\n\n🔥 Conteúdo que você NÃO encontra em nenhum outro lugar!\n💎 Acesso imediato: {link}",
            "22:00": "🌙 {name}, A NOITE ESTÁ PEGANDO FOGO NO VIP!\n\n23 ex-membros da prévia voltaram hoje e estão aproveitando o conteúdo premium!\n\n🚀 Sua vez? {link}"
    },
    "2": {
      "12:00": "🚨 {name}, ALERTA: TEM CONTEÚDO NOVO!\n\nEnquanto você está fora, o VIP está bombando com:\n• OnlyFans vazado HOJE\n• Close Friends exclusivo\n• Cenas completas sem cortes\n\n⚡ Não fique de fora: {link}",
            "18:00": "😈 {name}, HOJE O VIP ESTÁ EXPLODINDO!\n\nLançamos conteúdo EXPLÍCITO que vai te deixar maluco!\n\n🔥 Cenas inéditas!\n💎 Acesse agora: {link}",
            "22:00": "💀 {name}, ÚLTIMO CONVITE ESPECIAL DE HOJE!\n\nLiberamos vagas por TEMPO LIMITADO!\n\n🎯 Condições especiais para ex-membros da prévia!\n⚡ Entre agora: {link}"
    },
    "3": {
      "12:00": "⚡ {name}, PERIGO NA ÁREA!\n\nO conteúdo MAIS PESADO está no VIP agora!\n\n🔞 Cenas proibidas\n🔞 OnlyFans exclusivos\n🔞 Close Friends íntimo\n\n🚀 Tem coragem? {link}",
            "18:00": "😈 {name}, HOJE TEM CENA EXPLÍCITA NO VIP!\n\nConteúdo tão quente que quase derreteu o servidor!\n\n🔥 Só para os fortes!\n💎 Entra se tiver coragem: {link}",
            "22:00": "🌙 {name}, PRAZER A NOITE TODA NO VIP!\n\nEnquanto você dorme, o grupo está fervendo com conteúdo pesado!\n\n🚀 Última chance de hoje: {link}"
    },
    "4": {
      "12:00": "🎯 {name}, OFERTA RELÂMPAGO!\n\nSó HOJE: bônus exclusivo para quem voltar!\n\n🔞 Pacote de cenas inéditas\n🔞 OnlyFans nunca vazados antes\n🔞 Conteúdo extra quente\n\n⚡ Somente agora: {link}",
            "18:00": "🚨 {name}, AS VAGAS ESTÃO ACABANDO!\n\nRestam apenas 8 vagas com bônus especial!\n\n🔥 Conteúdo viciante!\n💎 Garanta já: {link}",
            "22:00": "💀 {name}, ÚLTIMA CHAMADA COM BÔNUS!\n\nFaltam só 2 horas para o bônus acabar!\n\n⚡ Não deixe para depois: {link}"
    },
    "5": {
      "12:00": "⌛ {name}, CONTAGEM REGRESSIVA FINAL!\n\nÚLTIMO DIA com condições especiais!\n\n🔞 O preço sobe 50% amanhã\n🔞 Os bônus expiram hoje\n\n🚀 Última chance real: {link}",
            "18:00": "⏳ {name}, FALTAM 6H PARA MUDAR TUDO!\n\nO VIP nunca mais estará tão barato quanto hoje!\n\n🔥 Última chance no preço atual\n💎 Amanhã será tarde: {link}",
            "22:00": "💀 {name}, ÚLTIMO ADEUS!\n\nEsta é sua ÚLTIMA mensagem do sistema!\n\n⚡ As oportunidades acabam em 2h\n🎯 O preço muda AMANHÃ\n\n🔞 Última chamada: {link}"
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

        start_text = """🎯 ACESSO GRATUITO - GRUPO DE PRÉVIA 🎯

✅ Seu acesso temporário foi ativado com sucesso!

🔗 Entre no grupo agora: {invite_link}

🚨 Informações importantes:
• Duração: {days} dias gratuitos
• Sistema anti-retorno ativo (não tente voltar sem pagar)
• O VIP desbloqueia todos os benefícios

👉 Dica: Entre AGORA e não perca nada!""".format(
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
    """Envia texto + vídeo com CTA - Garante que o vídeo seja enviado."""
    video_sent = False
    text_sent = False
    
    # PRIMEIRO: Tenta enviar o vídeo (mais importante)
    try:
        caption = CTA_TEXT.format(name=name_for_cta, link=PURCHASE_LINK)  # ← ADICIONE ISSO!
        await bot.send_video(chat_id, VIDEO_URL, caption=caption)
        video_sent = True
        logger.info(f"✅ Vídeo CTA enviado para {chat_id}")
    except Exception as e:
        logger.error(f"❌ Falha CRÍTICA: Não foi possível enviar vídeo para {chat_id}: {e}")
        return False  # Se o vídeo falha, retorna erro
    
    # SEGUNDO: Tenta enviar o texto (opcional)
    if text:
        attempt = 0
        while attempt < max_retries:
            try:
                await bot.send_message(chat_id, text)
                text_sent = True
                logger.info(f"✅ Mensagem de texto enviada para {chat_id}")
                break
            except RetryAfter as e:
                wait = getattr(e, 'timeout', getattr(e, 'retry_after', None)) or 5
                logger.info(f"RetryAfter: aguardando {wait}s para texto em {chat_id}")
                await asyncio.sleep(wait)
                attempt += 1
            except (BotBlocked, ChatNotFound, UserDeactivated, Unauthorized):
                logger.warning(f"Usuário {chat_id} bloqueou o bot - texto não enviado")
                break
            except Exception as e:
                logger.warning(f"Falha no texto para {chat_id} (tentativa {attempt+1}): {e}")
                attempt += 1
                await asyncio.sleep(2)
        
        if not text_sent:
            logger.warning(f"⚠️ Texto não enviado para {chat_id}, mas vídeo foi enviado")
    
    return video_sent  # Retorna True se pelo menos o vídeo foi enviado

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

async def unban_user(user_id: int):
    """Remove o banimento de um usuário (no banco de dados e no grupo)"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET banned = 0, removed = 0 WHERE user_id = ?", (user_id,))
        await db.commit()
    logger.info(f"Usuário {user_id} desbanido no banco de dados")

async def unban_user_in_group(user_id: int):
    """Remove o banimento do usuário no grupo do Telegram"""
    try:
        await bot.unban_chat_member(PREVIEWS_GROUP_ID, user_id)
        logger.info(f"Usuário {user_id} desbanido no grupo do Telegram")
        return True
    except Exception as e:
        logger.error(f"Erro ao desbanir usuário {user_id} no grupo: {e}")
        return False

async def record_attempt(user_id: int, reason: str):
    attempt_time = int(datetime.now().timestamp())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO attempts (user_id, attempt_time, reason) VALUES (?, ?, ?)",
            (user_id, attempt_time, reason),
        )
        await db.commit()

# -------------------------
# Função que envia a mensagem agendada
# -------------------------
async def send_scheduled_message(user_id: int, day: int, hour: str, is_retarget: bool = False):
    user_info = await get_user_info(user_id)
    if not user_info:
        logger.warning(f"Usuário {user_id} não encontrado no banco para envio agendado")
        return

    (_uid, username, first_name, last_name, joined_group, join_time, removed, banned) = user_info

    # CORREÇÃO: Lógica de verificação corrigida
    if banned:
        logger.debug(f"Ignorado envio para {user_id}: usuário banido")
        return
        
    if not is_retarget and removed:
        logger.debug(f"Ignorado envio de prévia para {user_id}: usuário removido")
        return

    name = first_name or "Usuário"

    try:
        day_key = str(day)

        if not is_retarget:
            message_template = MESSAGES_SCHEDULE.get(day_key, {}).get(hour)
            logger.info(f"Enviando PRÉVIA - Dia {day}, Hora {hour} para {user_id}")
        else:
            message_template = MESSAGES_SCHEDULE.get("retarget", {}).get(day_key, {}).get(hour)
            logger.info(f"Enviando RETARGET - Dia {day}, Hora {hour} para {user_id} (removed={removed})")

        if not message_template:
            logger.warning(f"Mensagem não configurada para day={day} hour={hour} retarget={is_retarget}")
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
            logger.info(f"✓ Enviado (retarget={is_retarget}) dia {day} hora {hour} para {user_id}")
        else:
            logger.warning(f"✗ Falha ao enviar (retarget={is_retarget}) dia {day} hora {hour} para {user_id}")
    except Exception as e:
        logger.exception(f"Erro em send_scheduled_message para {user_id}: {e}")

# -------------------------
# Agendamento das mensagens
# -------------------------
async def schedule_user_messages(user_id: int, username: str, first_name: str, last_name: str):
    now = datetime.now(TZ)
    
    # Agenda dias de prévia (1..DAYS_OF_PREVIEW) com horas configuráveis
    for day in range(1, DAYS_OF_PREVIEW + 1):
        for hour in MESSAGE_HOURS:
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
    removal_dt = removal_time + timedelta(minutes=1)
    scheduler.add_job(
        remove_user_from_group,
        trigger=DateTrigger(run_date=removal_dt),
        args=[user_id],
        id=f"user_{user_id}_removal",
        replace_existing=True,
    )

    # CORREÇÃO: Agenda retarget começando 1 DIA APÓS a remoção
    for rday in range(1, RETARGET_DAYS + 1):
        for hour in MESSAGE_HOURS:
            try:
                hour_dt = datetime.strptime(hour.strip(), "%H:%M").time()
            except Exception:
                continue
            # CORREÇÃO: Retarget começa 1 DIA APÓS o fim da prévia
            target_date = (now + timedelta(days=DAYS_OF_PREVIEW + 1 + (rday - 1))).date()
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
# Remoção do usuário do grupo
# -------------------------
async def remove_user_from_group(user_id: int):
    try:
        await bot.ban_chat_member(PREVIEWS_GROUP_ID, user_id)
        await asyncio.sleep(5)
        await bot.unban_chat_member(PREVIEWS_GROUP_ID, user_id)

        await mark_user_removed(user_id)

        user_info = await get_user_info(user_id)
        if user_info:
            name = user_info[2] or "Usuário"
            removal_text = "{name}, your free access has expired. To return, only VIP: {link}".format(
                name=name, link=PURCHASE_LINK
            )
            await safe_send_message(user_id, removal_text, name_for_cta=name)  # ← CORRIGIDO!

        logger.info(f"Usuário {user_id} removido do grupo de prévia")
    except (ChatAdminRequired, TelegramAPIError) as e:
        logger.error(f"Erro ao remover usuário {user_id} do grupo: {e}")
        for admin_id in ADMINS:
            try:
                await bot.send_message(admin_id, f"Erro ao remover usuário {user_id} do grupo: {e}")
            except Exception:
                pass
      
# -------------------------
# Handler para novos membros no grupo de prévia (ANTI-RETORNO CORRIGIDO)
# -------------------------
@dp.chat_member_handler(chat_id=PREVIEWS_GROUP_ID)
async def handle_chat_member_update(update: ChatMemberUpdated):
    try:
        if update.new_chat_member.status == 'member':
            user = update.new_chat_member.user
            user_id = user.id

            user_info = await get_user_info(user_id)

            # CORREÇÃO: Anti-retorno completo como no código original
            if user_info and user_info[6]:  # removed flag
                join_time = user_info[5]  # join_time
                current_time = int(datetime.now().timestamp())
                preview_end_time = join_time + (DAYS_OF_PREVIEW * 24 * 3600)
                
                # Só bane se realmente passou do tempo de prévia
                if current_time > preview_end_time:
                    await record_attempt(user_id, "Tentativa de retorno após período de prévia")
                    try:
                        await bot.ban_chat_member(PREVIEWS_GROUP_ID, user_id)
                        await mark_user_banned(user_id)
                        name = user.first_name or "Usuário"
                        ban_text = "{name}, your free access has expired. To return, only VIP: {link}".format(
    name=name, link=PURCHASE_LINK
)
                        await safe_send_message(user_id, ban_text, name_for_cta=name)
                        logger.info(f"Usuário {user_id} ({name}) banido por tentativa de retorno após período")
                    except Exception as e:
                        logger.error(f"Erro ao banir usuário {user_id}: {e}")
                else:
                    # Ainda está no período de prévia, permite voltar
                    logger.info(f"Usuário {user_id} voltou durante período de prévia - permitido")
                    await update_user_joined(user_id, user.username, user.first_name, user.last_name)

            # Novo usuário (ou ainda não marcado como joined)
            elif not user_info or not user_info[4]:  # joined_group flag
                await update_user_joined(user_id, user.username, user.first_name, user.last_name)

                               # Vídeo CTA de boas-vindas
                try:
                    caption = CTA_TEXT.format(name=user.first_name or "Usuário", link=PURCHASE_LINK)
                    await bot.send_video(user_id, VIDEO_URL, caption=caption)
                    logger.info(f"Vídeo CTA de boas-vindas enviado para {user_id}")
                except Exception as e:
                    logger.error(f"Erro ao enviar vídeo CTA de boas-vindas para {user_id}: {e}")
                    
                await schedule_user_messages(user_id, user.username, user.first_name, user.last_name)

                # Mensagem imediata (dia 1)
                await asyncio.sleep(SEND_IMMEDIATE_DELAY_SECONDS)
                first_hour = MESSAGE_HOURS[0].strip() if MESSAGE_HOURS else "12:00"
                await send_scheduled_message(user_id, 1, first_hour, is_retarget=False)
                logger.info(f"Novo usuário {user_id} ({user.first_name}) adicionado ao grupo e agendado")
    except Exception as e:
        logger.exception(f"Erro ao processar chat_member_update: {e}")

# -------------------------
# Comandos administrativos
# -------------------------
@dp.message_handler(commands=['stats'], user_id=list(ADMINS), chat_type=ChatType.PRIVATE)
async def cmd_stats(message: types.Message):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM users")
            total_users = (await cursor.fetchone())[0]

            cursor = await db.execute(
                "SELECT COUNT(*) FROM users WHERE joined_group = 1 AND removed = 0 AND banned = 0"
            )
            active_users = (await cursor.fetchone())[0]

            cursor = await db.execute("SELECT COUNT(*) FROM users WHERE removed = 1")
            removed_users = (await cursor.fetchone())[0]

            cursor = await db.execute("SELECT COUNT(*) FROM users WHERE banned = 1")
            banned_users = (await cursor.fetchone())[0]

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
                    await asyncio.sleep(0.05)
    finally:
        _pending_broadcast.pop(admin_id, None)

    await message.answer(f"✅ Broadcast finalizado. Enviados: {sent} | Falhas: {failed}")

@dp.message_handler(commands=['cancelar'], user_id=list(ADMINS), chat_type=ChatType.PRIVATE)
async def cmd_cancel_broadcast(message: types.Message):
    _pending_broadcast.pop(message.from_user.id, None)
    await message.answer("❌ Broadcast cancelado.")

# Comando para desbanir usuário
@dp.message_handler(commands=['desbanir'], user_id=list(ADMINS), chat_type=ChatType.PRIVATE)
async def cmd_unban(message: types.Message):
    """Desbanir um usuário (útil para testes)"""
    try:
        if message.reply_to_message:
            user_id = message.reply_to_message.from_user.id
            username = message.reply_to_message.from_user.username or "Sem username"
            first_name = message.reply_to_message.from_user.first_name or "Usuário"
        else:
            user_id = message.from_user.id
            username = message.from_user.username or "Sem username"
            first_name = message.from_user.first_name or "Usuário"

        await unban_user(user_id)
        group_unbanned = await unban_user_in_group(user_id)
        
        for job in scheduler.get_jobs():
            if f"user_{user_id}" in job.id:
                scheduler.remove_job(job.id)
                logger.info(f"Job removido: {job.id}")

        if group_unbanned:
            response = f"✅ Usuário @{username} ({first_name}) desbanido com sucesso!\n\n📊 Status:\n• Banimento removido do banco\n• Banimento removido do grupo\n• Jobs de remoção cancelados"
        else:
            response = f"⚠️ Usuário @{username} ({first_name}) desbanido parcialmente!\n\n📊 Status:\n• Banimento removido do banco ✅\n• Erro ao remover banimento do grupo ❌\n• Jobs de remoção cancelados ✅\n\n💡 O usuário pode não estar banido no grupo."

        await message.answer(response)
        logger.info(f"Admin {message.from_user.id} desbaniu o usuário {user_id}")

    except Exception as e:
        error_msg = f"❌ Erro ao desbanir usuário: {e}"
        await message.answer(error_msg)
        logger.error(f"Erro no comando desbanir: {e}")

   # -------------------------
   # Captura de file_id (admin envia vídeo e o bot responde com o ID)
   # -------------------------
@dp.message_handler(
       content_types=[types.ContentType.VIDEO, types.ContentType.DOCUMENT],
       user_id=list(ADMINS),
       chat_type=ChatType.PRIVATE,
   )
async def get_file_id(message: types.Message):
       media = message.video or message.document
       await message.answer(f"file_id:\n\n{media.file_id}")
       
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
        allowed_updates=["message", "chat_member", "my_chat_member"]
    )
