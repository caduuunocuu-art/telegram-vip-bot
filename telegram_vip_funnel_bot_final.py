# telegram_vip_funnel_bot_final.py
# ----------------------------------------------------------------------------
# Bot de PRÉVIAS do funil (versão 90 segundos, português)
#
# Fluxo:
#   1. Pessoa dá /start  -> recebe o aviso + link de convite de USO ÚNICO
#   2. Pessoa entra no grupo de prévias -> começa a contagem (PREVIEW_SECONDS)
#   3. Aos 90s o bot REMOVE a pessoa e manda NA HORA a 1ª mensagem de venda
#   4. Depois: +10min, +1h, +6h, dia 2 (manhã), dia 3 (noite), dia 5 (despedida)
#
# Toda mensagem de venda vai com o vídeo (VIDEO_URL) como legenda + botão
# "Liberar grupo VIP" que leva para o BOT DE VENDAS (PURCHASE_LINK).
#
# A fila de envios fica salva no banco (SQLite). Se o bot reiniciar ou fizer
# deploy, nada se perde: ao voltar ele continua de onde parou.
# ----------------------------------------------------------------------------

import os
import time
import hmac
import hashlib
import asyncio
import logging
from datetime import datetime, timedelta, time as dtime

import pytz
import aiosqlite
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.types import (
    ChatType,
    ChatMemberUpdated,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.utils import executor
from aiogram.utils.exceptions import (
    RetryAfter,
    BotBlocked,
    ChatNotFound,
    UserDeactivated,
    Unauthorized,
    TelegramAPIError,
    NetworkError,
)

# -------------------------
# CONFIG (tudo pelo ambiente / variáveis do Railway)
# -------------------------
API_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
PREVIEWS_GROUP_ID = int(os.getenv("PREVIEWS_GROUP_ID", "0"))

# Só é usado se o bot não conseguir criar o link de uso único
PREVIEWS_GROUP_INVITE_LINK = os.getenv("PREVIEWS_GROUP_INVITE_LINK", "").strip()


def normalize_link(value: str) -> str:
    value = (value or "").strip()
    if value.startswith("@"):
        return "https://t.me/" + value[1:]
    if value.startswith("t.me/"):
        return "https://" + value
    return value


# Bot de vendas (para onde vai o botão "Liberar grupo VIP")
PURCHASE_LINK = normalize_link(os.getenv("PURCHASE_LINK", ""))

# Tempo de prévia no grupo, em segundos (90 = 1 minuto e meio)
PREVIEW_SECONDS = int(os.getenv("PREVIEW_SECONDS", "90"))

# Validade do link de convite individual (horas)
INVITE_TTL_HOURS = int(os.getenv("INVITE_TTL_HOURS", "24"))

# Vídeo de vendas: file_id do Telegram (ou URL .mp4 direta)
VIDEO_URL = os.getenv("VIDEO_URL", "").strip()

DB_PATH = os.getenv("DB_PATH", "vip_funnel.db")
TZ = pytz.timezone(os.getenv("TIMEZONE", "America/Sao_Paulo"))

# Horários das mensagens de dias seguintes (hora cheia, fuso TIMEZONE)
D2_HOUR = int(os.getenv("D2_HOUR", "10"))   # dia 2, manhã
D3_HOUR = int(os.getenv("D3_HOUR", "21"))   # dia 3, noite
D5_HOUR = int(os.getenv("D5_HOUR", "12"))   # dia 5, despedida

# Não manda mensagem de venda de madrugada: antes dessa hora, empurra para 09:00
QUIET_UNTIL_HOUR = int(os.getenv("QUIET_UNTIL_HOUR", "8"))

# TEST_MODE=1 -> comprime os tempos (cada mensagem a cada 20s depois da remoção)
TEST_MODE = os.getenv("TEST_MODE", "0").strip() == "1"

MAX_RETRIES = int(os.getenv("MAX_MESSAGE_RETRIES", "3"))
POLL_SECONDS = 2

ADMINS = {int(x) for x in os.getenv("ADMINS", "").replace(" ", "").split(",") if x}
ADMIN_FILTER = list(ADMINS) or [0]

LANG_CODE = os.getenv("LANG_CODE", "pt")

# Rastreio de cliques no botão (opcional).
# Se PUBLIC_URL estiver vazio, o botão vai direto para o bot de vendas (sem contar cliques).
# Se preencher (ex.: https://seu-bot.up.railway.app), o botão passa por um link do
# próprio bot, que conta o clique e redireciona para o bot de vendas.
PUBLIC_URL = os.getenv("PUBLIC_URL", "").strip().rstrip("/")
PORT = int(os.getenv("PORT", "8080"))

# -------------------------
# Textos (português). A versão em inglês entra depois como MESSAGES["en"].
# Cada texto de venda vai como legenda do vídeo (limite do Telegram: 1024).
# -------------------------
MESSAGES = {
    "pt": {
        "button": "🔓 Liberar grupo VIP🔥",
        "start": (
            "⚠️ Aviso! O link abaixo é do grupo PRÉVIAS do VIP. "
            "Você terá apenas 1 minuto e meio de acesso.\n\n"
            "Aproveite para ver o que tem lá dentro. Para ter o grupo VIP completo, "
            "sem nenhuma interrupção, é só liberar depois 👇\n\n"
            "{link}"
        ),
        "used": (
            "👀 {name}, você já usou a sua prévia.\n\n"
            "O grupo VIP completo continua te esperando 🔥\n\n"
            "👇 Toque no botão e libere agora"
        ),
        "rem": (
            "⏰ {name}, acabou o tempo. Você foi removido da prévia.\n\n"
            "E o que você viu foi só uma amostra 🔥\n\n"
            "No VIP você tem:\n"
            "🎬 Vários vídeos em diferentes tópicos\n"
            "🕵️ 100% anônimo\n"
            "🔒 Seguro: tudo dentro do próprio Telegram\n"
            "⚡ O acesso é liberado automático\n\n"
            "👇 Toque no botão e libere agora"
        ),
        "m10": (
            "😏 {name}, deu pra sentir o gostinho, né?\n\n"
            "1 minuto e meio é pouco perto do que tem lá dentro. "
            "O resto está esperando por você no VIP 🔥\n\n"
            "⚡ Acesso automático depois do pagamento.\n\n"
            "👇 Toque e libere agora"
        ),
        "h1": (
            "👀 {name}, o grupo VIP continua aberto.\n\n"
            "Você viu uma amostra. Lá dentro tem os tópicos completos, "
            "com vários vídeos em cada um 🎬\n\n"
            "💰 A partir de R$ 24,90 · Pix ou cartão\n\n"
            "👇 Libere o seu acesso"
        ),
        "h6": (
            "🤔 {name}, ficou com alguma dúvida? Eu respondo:\n\n"
            "🕵️ É anônimo? Sim.\n"
            "🔒 É seguro? Sim. Você recebe tudo pelo próprio Telegram.\n"
            "⚡ Como recebo? Pagou, o bot libera sozinho. Em instantes você já está dentro.\n"
            "💳 Pagamento: Pix ou cartão.\n\n"
            "Sem enrolação. É só tocar no botão 👇"
        ),
        "d2": (
            "💡 {name}, uma conta rápida:\n\n"
            "• 30 dias: R$ 24,90 (o mais escolhido)\n"
            "• 90 dias: R$ 37,90\n"
            "• Vitalício: R$ 57,90\n\n"
            "Se você renovasse o de 30 dias duas vezes, já gastaria R$ 49,80. "
            "Por mais R$ 8,10 você leva o vitalício e nunca mais paga 😏\n\n"
            "👇 Escolha o seu plano"
        ),
        "d3": (
            "🔥 {name}, vou ser direto:\n\n"
            "Você chegou até aqui porque ficou curioso. "
            "A curiosidade só passa quando você vê o resto.\n\n"
            "🎬 Tópicos completos\n"
            "🕵️ Anônimo\n"
            "⚡ Acesso na hora\n\n"
            "💰 A partir de R$ 24,90\n\n"
            "👇 Libere agora"
        ),
        "d5": (
            "👋 {name}, essa é a minha última mensagem. Não vou mais te incomodar.\n\n"
            "Se você mudar de ideia, o botão abaixo continua funcionando.\n\n"
            "R$ 24,90 · R$ 37,90 · R$ 57,90 🔓"
        ),
    }
}
MSG = MESSAGES[LANG_CODE]

# Ordem das etapas de venda (usada nas estatísticas)
SALES_KINDS = ["rem", "m10", "h1", "h6", "d2", "d3", "d5"]

# -------------------------
# Logging / bot
# -------------------------
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

if not API_TOKEN:
    raise SystemExit("Defina a variável TG_BOT_TOKEN (token do bot).")
if not PREVIEWS_GROUP_ID:
    raise SystemExit("Defina a variável PREVIEWS_GROUP_ID (ID do grupo de prévias).")
if not PURCHASE_LINK:
    raise SystemExit("Defina a variável PURCHASE_LINK (link do bot de vendas).")

bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)
_worker_task = None

# -------------------------
# Banco de dados
# -------------------------
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    started_at INTEGER,
    invite_link TEXT,
    invite_created_at INTEGER,
    joined_at INTEGER,
    removed_at INTEGER,
    blocked INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    kind TEXT,
    run_at INTEGER,
    status TEXT DEFAULT 'pending',
    processed_at INTEGER
);

CREATE TABLE IF NOT EXISTS clicks (
    user_id INTEGER,
    kind TEXT,
    clicked_at INTEGER,
    PRIMARY KEY (user_id, kind)
);

CREATE INDEX IF NOT EXISTS idx_queue_due ON queue(status, run_at);
CREATE INDEX IF NOT EXISTS idx_queue_user ON queue(user_id);
"""


def _connect():
    return aiosqlite.connect(DB_PATH, timeout=30)


async def init_db():
    folder = os.path.dirname(os.path.abspath(DB_PATH))
    try:
        os.makedirs(folder, exist_ok=True)
    except Exception as e:
        logger.warning(f"Não consegui criar a pasta do banco ({folder}): {e}")
    async with _connect() as db:
        await db.executescript(CREATE_SQL)
        # Se o bot caiu no meio de um envio, devolve para a fila
        await db.execute("UPDATE queue SET status='pending' WHERE status='sending'")
        await db.commit()
    logger.info(f"Banco pronto em {DB_PATH}")


async def db_exec(query, params=()):
    async with _connect() as db:
        await db.execute(query, params)
        await db.commit()


async def db_one(query, params=()):
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, params)
        return await cur.fetchone()


async def db_all(query, params=()):
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, params)
        return await cur.fetchall()


async def get_user(user_id: int):
    return await db_one("SELECT * FROM users WHERE user_id = ?", (user_id,))


async def cancel_pending(user_id: int):
    await db_exec(
        "UPDATE queue SET status='cancelled' WHERE user_id=? AND status='pending'",
        (user_id,),
    )


# -------------------------
# Links e botão
# -------------------------
def purchase_url(kind: str) -> str:
    """Link do bot de vendas com parâmetro de rastreio (?start=prev_rem, prev_m10...)."""
    sep = "&" if "?" in PURCHASE_LINK else "?"
    return f"{PURCHASE_LINK}{sep}start=prev_{kind}"


def click_sig(user_id: int, kind: str) -> str:
    """Assinatura curta no link de rastreio, para ninguém inventar cliques."""
    raw = hmac.new(API_TOKEN.encode(), f"{user_id}:{kind}".encode(), hashlib.sha256).hexdigest()
    return raw[:12]


def button_url(kind: str, user_id: int) -> str:
    if PUBLIC_URL:
        return f"{PUBLIC_URL}/go/{kind}/{user_id}/{click_sig(user_id, kind)}"
    return purchase_url(kind)


def sales_keyboard(kind: str, user_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(MSG["button"], url=button_url(kind, user_id)))
    return kb


async def notify_admins(text: str):
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, text)
        except Exception:
            pass


# -------------------------
# Agendamento (calcula quando cada mensagem sai)
# -------------------------
def adjust_quiet(ts: int) -> int:
    """Se cair de madrugada (antes de QUIET_UNTIL_HOUR), empurra para 09:00."""
    if TEST_MODE:
        return ts
    dt = datetime.fromtimestamp(ts, TZ)
    if dt.hour < QUIET_UNTIL_HOUR:
        return int(TZ.localize(datetime(dt.year, dt.month, dt.day, 9, 0)).timestamp())
    return ts


def build_schedule(joined_ts: int):
    """Retorna [(kind, run_at_timestamp)] a partir do momento da entrada no grupo."""
    removal = joined_ts + PREVIEW_SECONDS

    if TEST_MODE:
        offsets = {"m10": 20, "h1": 40, "h6": 60, "d2": 80, "d3": 100, "d5": 120}
        return [("rem", removal)] + [(k, removal + s) for k, s in offsets.items()]

    m10 = adjust_quiet(removal + 600)
    # Mantém o espaçamento original entre as mensagens mesmo quando empurradas
    h1 = max(adjust_quiet(removal + 3600), m10 + 3000)
    h6 = max(adjust_quiet(removal + 21600), h1 + 18000)

    base_date = datetime.fromtimestamp(h6, TZ).date()

    def at(days: int, hour: int) -> int:
        d = base_date + timedelta(days=days)
        return int(TZ.localize(datetime.combine(d, dtime(hour, 0))).timestamp())

    return [
        ("rem", removal),
        ("m10", m10),
        ("h1", h1),
        ("h6", h6),
        ("d2", at(1, D2_HOUR)),
        ("d3", at(2, D3_HOUR)),
        ("d5", at(4, D5_HOUR)),
    ]


async def enqueue_schedule(user_id: int, schedule):
    async with _connect() as db:
        await db.executemany(
            "INSERT INTO queue (user_id, kind, run_at) VALUES (?, ?, ?)",
            [(user_id, kind, run_at) for kind, run_at in schedule],
        )
        await db.commit()


# -------------------------
# Envio das mensagens de venda (vídeo + texto + botão)
# -------------------------
_BLOCKED_ERRORS = (BotBlocked, ChatNotFound, UserDeactivated, Unauthorized)


async def send_with_button(chat_id: int, text: str, kind: str, with_video: bool = True) -> str:
    """Retorna 'ok', 'blocked' ou 'failed'."""
    kb = sales_keyboard(kind, chat_id)
    for attempt in range(MAX_RETRIES):
        try:
            if with_video and VIDEO_URL and len(text) <= 1024:
                try:
                    await bot.send_video(chat_id, VIDEO_URL, caption=text, reply_markup=kb)
                    return "ok"
                except (RetryAfter,) + _BLOCKED_ERRORS:
                    raise
                except TelegramAPIError as e:
                    # Se o vídeo falhar, manda só o texto com o botão: melhor que perder a venda
                    logger.error(f"Vídeo falhou para {chat_id} ({e}); enviando só o texto")
            await bot.send_message(chat_id, text, reply_markup=kb)
            return "ok"
        except RetryAfter as e:
            wait = getattr(e, "timeout", None) or 5
            logger.info(f"RetryAfter: aguardando {wait}s")
            await asyncio.sleep(wait)
        except _BLOCKED_ERRORS:
            return "blocked"
        except Exception as e:
            logger.warning(f"Falha ao enviar para {chat_id} (tentativa {attempt + 1}): {e}")
            await asyncio.sleep(2)
    return "failed"


async def tg_retry(func, *args, tries: int = 3, **kwargs):
    """Repete a chamada se a rede falhar (erro passageiro entre o servidor e o Telegram)."""
    for attempt in range(1, tries + 1):
        try:
            return await func(*args, **kwargs)
        except RetryAfter as e:
            await asyncio.sleep(getattr(e, "timeout", None) or 3)
        except (NetworkError, asyncio.TimeoutError, OSError) as e:
            if attempt == tries:
                raise
            logger.warning(f"Falha de rede ({e}); tentativa {attempt}/{tries}, tentando de novo")
            await asyncio.sleep(1.5 * attempt)
    return await func(*args, **kwargs)


async def kick_from_group(user_id: int, permanent: bool = False):
    """Remove do grupo. Se não for permanente, desbane na sequência (pode voltar, mas só com link novo)."""
    await tg_retry(bot.ban_chat_member, PREVIEWS_GROUP_ID, user_id)
    if not permanent:
        await asyncio.sleep(1)
        await tg_retry(bot.unban_chat_member, PREVIEWS_GROUP_ID, user_id, only_if_banned=True)


# -------------------------
# Worker: executa a fila
# -------------------------
async def handle_job(user_id: int, kind: str) -> str:
    user = await get_user(user_id)
    if not user:
        return "cancelled"
    if kind != "rem" and user["blocked"]:
        return "cancelled"

    name = user["first_name"] or "amigo"

    if kind == "rem":
        try:
            await kick_from_group(user_id)
            logger.info(f"Usuário {user_id} removido do grupo de prévia")
        except Exception as e:
            logger.error(f"Erro ao remover {user_id} do grupo: {e}")
            await notify_admins(
                f"⚠️ Não consegui remover o usuário {user_id} do grupo: {e}\n"
                "Confira se o bot é administrador com permissão de banir usuários."
            )
        await db_exec("UPDATE users SET removed_at=? WHERE user_id=?", (int(time.time()), user_id))

    result = await send_with_button(user_id, MSG[kind].format(name=name), kind)

    if result == "blocked":
        await db_exec("UPDATE users SET blocked=1 WHERE user_id=?", (user_id,))
        await cancel_pending(user_id)
        logger.info(f"Usuário {user_id} bloqueou o bot; fila cancelada")
        return "blocked"
    if result == "ok":
        logger.info(f"✓ '{kind}' enviado para {user_id}")
        return "done"
    logger.warning(f"✗ Falha ao enviar '{kind}' para {user_id}")
    return "failed"


async def process_due():
    now = int(time.time())
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT id, user_id, kind FROM queue "
            "WHERE status='pending' AND run_at<=? ORDER BY run_at, id LIMIT 25",
            (now,),
        )
        rows = await cur.fetchall()
        for r in rows:
            await db.execute(
                "UPDATE queue SET status='sending' WHERE id=? AND status='pending'", (r["id"],)
            )
        await db.commit()

    for r in rows:
        try:
            status = await handle_job(r["user_id"], r["kind"])
        except Exception as e:
            logger.exception(f"Erro no job {r['id']}: {e}")
            status = "failed"
        await db_exec(
            "UPDATE queue SET status=?, processed_at=? WHERE id=?",
            (status, int(time.time()), r["id"]),
        )
        await asyncio.sleep(0.05)


async def worker_loop():
    logger.info("Worker da fila iniciado")
    while True:
        try:
            await process_due()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(f"Erro no worker: {e}")
        await asyncio.sleep(POLL_SECONDS)


# -------------------------
# /start: aviso + link de convite de uso único
# -------------------------
async def get_or_create_invite(user_id: int, row) -> str:
    now = int(time.time())
    ttl = INVITE_TTL_HOURS * 3600
    if (
        row
        and row["invite_link"]
        and row["invite_created_at"]
        and now - row["invite_created_at"] < ttl - 600
    ):
        return row["invite_link"]

    try:
        inv = await tg_retry(
            bot.create_chat_invite_link,
            PREVIEWS_GROUP_ID,
            name=f"u{user_id}",
            expire_date=now + ttl,
            member_limit=1,
            tries=4,
        )
        link = inv.invite_link
    except Exception as e:
        logger.error(f"Não consegui criar o link individual: {e}")
        if isinstance(e, NetworkError):
            hint = "Foi falha de rede com o Telegram, mesmo depois de 4 tentativas."
        else:
            hint = "Confira se o bot é admin do grupo com permissão de convidar usuários."
        await notify_admins(
            f"⚠️ Não consegui criar o link de convite individual: {e}\n"
            f"Usei o link fixo (PREVIEWS_GROUP_INVITE_LINK). {hint}"
        )
        link = PREVIEWS_GROUP_INVITE_LINK
        if not link:
            raise

    await db_exec(
        "UPDATE users SET invite_link=?, invite_created_at=? WHERE user_id=?",
        (link, now, user_id),
    )
    return link


@dp.message_handler(commands=["start"], chat_type=ChatType.PRIVATE)
async def cmd_start(message: types.Message):
    u = message.from_user
    name = u.first_name or "amigo"
    try:
        await db_exec(
            """
            INSERT INTO users (user_id, username, first_name, started_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                blocked=0
            """,
            (u.id, u.username, u.first_name, int(time.time())),
        )
        row = await get_user(u.id)

        # Já usou a prévia: não ganha outra, vai direto para o bot de vendas
        if row["joined_at"]:
            await send_with_button(u.id, MSG["used"].format(name=name), "used", with_video=False)
            return

        link = await get_or_create_invite(u.id, row)
        await message.answer(MSG["start"].format(link=link))
        logger.info(f"Usuário {u.id} recebeu o link da prévia")
    except Exception as e:
        logger.exception(f"Erro no /start para {u.id}: {e}")
        await message.answer("❌ Ocorreu um erro. Tente novamente em instantes.")


# -------------------------
# Entrada no grupo: começa a contagem de 90s
# -------------------------
@dp.chat_member_handler(chat_id=PREVIEWS_GROUP_ID)
async def on_member_update(update: ChatMemberUpdated):
    try:
        new = update.new_chat_member
        old = update.old_chat_member
        if new.status != "member" or old.status not in ("left", "kicked"):
            return

        user = new.user
        if user.is_bot:
            return

        row = await get_user(user.id)

        # Entrou sem passar pelo bot (link vazado, adicionado por alguém): remove
        if not row:
            logger.info(f"Usuário {user.id} entrou sem /start; removendo")
            await kick_from_group(user.id)
            return

        # Já tinha usado a prévia: remove de novo e bloqueia a volta
        if row["joined_at"]:
            logger.info(f"Usuário {user.id} tentou voltar à prévia; banido")
            await kick_from_group(user.id, permanent=True)
            await send_with_button(
                user.id,
                MSG["used"].format(name=row["first_name"] or "amigo"),
                "used",
                with_video=False,
            )
            return

        now = int(time.time())
        await db_exec("UPDATE users SET joined_at=? WHERE user_id=?", (now, user.id))
        await enqueue_schedule(user.id, build_schedule(now))
        logger.info(f"Usuário {user.id} entrou na prévia; remoção em {PREVIEW_SECONDS}s")
    except Exception as e:
        logger.exception(f"Erro em on_member_update: {e}")


# -------------------------
# Rastreio de cliques (link do botão -> conta -> redireciona para o bot de vendas)
# -------------------------
_web_runner = None
CLICK_KINDS = SALES_KINDS + ["used"]


async def handle_click(request):
    """GET /go/<etapa>/<user_id>/<assinatura>: registra 1 clique por pessoa/etapa e redireciona."""
    kind = request.match_info.get("kind", "")
    target = purchase_url(kind) if kind in CLICK_KINDS else PURCHASE_LINK
    try:
        uid = int(request.match_info.get("uid", "0"))
        sig = request.match_info.get("sig", "")
        valid = kind in CLICK_KINDS and hmac.compare_digest(sig, click_sig(uid, kind))
        if valid and request.method == "GET":
            await db_exec(
                "INSERT OR IGNORE INTO clicks (user_id, kind, clicked_at) VALUES (?, ?, ?)",
                (uid, kind, int(time.time())),
            )
    except Exception as e:
        logger.warning(f"Erro ao registrar clique: {e}")
    raise web.HTTPFound(target)


async def handle_health(request):
    return web.Response(text="ok")


async def start_web():
    """Sobe o servidor de rastreio, só se PUBLIC_URL estiver configurada."""
    global _web_runner
    if not PUBLIC_URL:
        logger.info("Rastreio de cliques DESLIGADO (PUBLIC_URL vazia): botão vai direto ao bot de vendas")
        return
    app = web.Application()
    app.router.add_get("/", handle_health)
    app.router.add_get("/go/{kind}/{uid}/{sig}", handle_click)
    _web_runner = web.AppRunner(app)
    await _web_runner.setup()
    await web.TCPSite(_web_runner, "0.0.0.0", PORT).start()
    logger.info(f"Rastreio de cliques LIGADO na porta {PORT} ({PUBLIC_URL})")


# -------------------------
# Comandos de administrador
# -------------------------
@dp.message_handler(commands=["stats"], user_id=ADMIN_FILTER, chat_type=ChatType.PRIVATE)
async def cmd_stats(message: types.Message):
    try:
        total = (await db_one("SELECT COUNT(*) AS n FROM users"))["n"]
        joined = (await db_one("SELECT COUNT(*) AS n FROM users WHERE joined_at IS NOT NULL"))["n"]
        removed = (await db_one("SELECT COUNT(*) AS n FROM users WHERE removed_at IS NOT NULL"))["n"]
        blocked = (await db_one("SELECT COUNT(*) AS n FROM users WHERE blocked=1"))["n"]
        pending = (await db_one("SELECT COUNT(*) AS n FROM queue WHERE status='pending'"))["n"]
        clicked_users = (await db_one("SELECT COUNT(DISTINCT user_id) AS n FROM clicks"))["n"]

        sent = {r["kind"]: r["n"] for r in await db_all(
            "SELECT kind, COUNT(*) AS n FROM queue WHERE status='done' GROUP BY kind")}
        blocked_by = {r["kind"]: r["n"] for r in await db_all(
            "SELECT kind, COUNT(*) AS n FROM queue WHERE status='blocked' GROUP BY kind")}
        clicks = {r["kind"]: r["n"] for r in await db_all(
            "SELECT kind, COUNT(*) AS n FROM clicks GROUP BY kind")}

        lines = [
            "📊 Estatísticas do funil de prévias\n",
            f"• Deram /start: {total}",
            f"• Entraram na prévia: {joined}",
            f"• Já removidos: {removed}",
            f"• Bloquearam o bot: {blocked}",
            f"• Mensagens na fila: {pending}",
        ]
        if PUBLIC_URL:
            lines.append(f"• Pessoas que clicaram no botão: {clicked_users}")
        lines += ["", "Mensagens por etapa (enviadas | cliques no botão | bloquearam o bot):"]

        for k in SALES_KINDS:
            s_n = sent.get(k, 0)
            if PUBLIC_URL:
                c_n = clicks.get(k, 0)
                rate = f"{c_n * 100 / s_n:.0f}%" if s_n else "0%"
                click_txt = f"{c_n} cliques ({rate})"
            else:
                click_txt = "cliques: rastreio desligado"
            lines.append(f"  {k}: {s_n} enviadas | {click_txt} | {blocked_by.get(k, 0)} bloquearam")

        await message.answer("\n".join(lines))
    except Exception as e:
        logger.exception(f"Erro no /stats: {e}")
        await message.answer("❌ Erro ao recuperar estatísticas.")


def day_bounds(d):
    """Início (00:00) e fim (00:00 do dia seguinte) de um dia, no fuso TIMEZONE, em timestamp."""
    start = int(TZ.localize(datetime.combine(d, dtime(0, 0))).timestamp())
    end = int(TZ.localize(datetime.combine(d + timedelta(days=1), dtime(0, 0))).timestamp())
    return start, end


def parse_day_arg(arg: str):
    """'' ou 'hoje' -> hoje | 'ontem' | '05/11' | '05/11/2026'. Retorna None se não entender."""
    arg = (arg or "").strip().lower()
    today = datetime.now(TZ).date()
    if arg in ("", "hoje"):
        return today
    if arg == "ontem":
        return today - timedelta(days=1)
    parts = arg.replace("-", "/").split("/")
    try:
        day, month = int(parts[0]), int(parts[1])
        year = int(parts[2]) if len(parts) > 2 else today.year
        if year < 100:
            year += 2000
        d = datetime(year, month, day).date()
        if len(parts) <= 2 and d > today:
            d = datetime(year - 1, month, day).date()
        return d
    except Exception:
        return None


@dp.message_handler(commands=["statsdia"], user_id=ADMIN_FILTER, chat_type=ChatType.PRIVATE)
async def cmd_statsdia(message: types.Message):
    """Estatísticas só do dia (meia-noite a meia-noite). /statsdia, /statsdia ontem, /statsdia 05/11"""
    try:
        day = parse_day_arg(message.get_args())
        if day is None:
            await message.answer("Use: /statsdia (hoje), /statsdia ontem ou /statsdia 05/11")
            return
        s, e = day_bounds(day)
        rng = (s, e)
        today = datetime.now(TZ).date()
        tag = " (hoje)" if day == today else " (ontem)" if day == today - timedelta(days=1) else ""

        async def n(query):
            return (await db_one(query, rng))["n"]

        new_starts = await n("SELECT COUNT(*) AS n FROM users WHERE started_at>=? AND started_at<?")
        joined = await n("SELECT COUNT(*) AS n FROM users WHERE joined_at>=? AND joined_at<?")
        removed = await n("SELECT COUNT(*) AS n FROM users WHERE removed_at>=? AND removed_at<?")
        blocked_people = await n(
            "SELECT COUNT(DISTINCT user_id) AS n FROM queue WHERE status='blocked' "
            "AND processed_at>=? AND processed_at<?")
        clicked_people = await n(
            "SELECT COUNT(DISTINCT user_id) AS n FROM clicks WHERE clicked_at>=? AND clicked_at<?")

        async def by_kind(query):
            return {r["kind"]: r["n"] for r in await db_all(query, rng)}

        sent = await by_kind(
            "SELECT kind, COUNT(*) AS n FROM queue WHERE status='done' "
            "AND processed_at>=? AND processed_at<? GROUP BY kind")
        blocked_by = await by_kind(
            "SELECT kind, COUNT(*) AS n FROM queue WHERE status='blocked' "
            "AND processed_at>=? AND processed_at<? GROUP BY kind")
        clicks = await by_kind(
            "SELECT kind, COUNT(*) AS n FROM clicks WHERE clicked_at>=? AND clicked_at<? GROUP BY kind")

        lines = [
            f"📅 Estatísticas do dia {day.strftime('%d/%m/%Y')}{tag}\n",
            f"• Novos /start: {new_starts}",
            f"• Entraram na prévia: {joined}",
            f"• Removidos: {removed}",
            f"• Bloquearam o bot: {blocked_people}",
            f"• Mensagens enviadas no dia: {sum(sent.values())}",
        ]
        if PUBLIC_URL:
            lines.append(f"• Pessoas que clicaram no botão: {clicked_people}")
        if day == today:
            pending = (await db_one("SELECT COUNT(*) AS n FROM queue WHERE status='pending'"))["n"]
            lines.append(f"• Mensagens na fila agora: {pending}")
        lines += ["", "Mensagens por etapa (enviadas | cliques no botão | bloquearam o bot):"]

        for k in SALES_KINDS:
            s_n = sent.get(k, 0)
            if PUBLIC_URL:
                c_n = clicks.get(k, 0)
                rate = f"{c_n * 100 / s_n:.0f}%" if s_n else "0%"
                click_txt = f"{c_n} cliques ({rate})"
            else:
                click_txt = "cliques: rastreio desligado"
            lines.append(f"  {k}: {s_n} enviadas | {click_txt} | {blocked_by.get(k, 0)} bloquearam")

        await message.answer("\n".join(lines))
    except Exception as ex:
        logger.exception(f"Erro no /statsdia: {ex}")
        await message.answer("❌ Erro ao recuperar as estatísticas do dia.")


@dp.message_handler(commands=["reset"], user_id=ADMIN_FILTER, chat_type=ChatType.PRIVATE)
async def cmd_reset(message: types.Message):
    """Zera um usuário para testar de novo: /reset (você), /reset 123456 ou respondendo a uma mensagem."""
    args = (message.get_args() or "").strip()
    if args.isdigit():
        uid = int(args)
    elif message.reply_to_message:
        uid = message.reply_to_message.from_user.id
    else:
        uid = message.from_user.id
    await db_exec("DELETE FROM queue WHERE user_id=?", (uid,))
    await db_exec("DELETE FROM users WHERE user_id=?", (uid,))
    try:
        await bot.unban_chat_member(PREVIEWS_GROUP_ID, uid, only_if_banned=True)
    except Exception:
        pass
    await message.answer(f"✅ Usuário {uid} zerado. Pode dar /start de novo para testar.")


@dp.message_handler(
    content_types=[types.ContentType.VIDEO, types.ContentType.DOCUMENT, types.ContentType.ANIMATION],
    user_id=ADMIN_FILTER,
    chat_type=ChatType.PRIVATE,
)
async def get_file_id(message: types.Message):
    """Admin manda o vídeo e o bot responde com o file_id para colocar em VIDEO_URL."""
    media = message.video or message.document or message.animation
    await message.answer(f"file_id:\n\n{media.file_id}")


# -------------------------
# Startup / Shutdown
# -------------------------
async def on_startup(_):
    global _worker_task
    await init_db()
    _worker_task = asyncio.create_task(worker_loop())
    await start_web()
    logger.info(
        f"Bot iniciado | grupo={PREVIEWS_GROUP_ID} | prévia={PREVIEW_SECONDS}s | "
        f"TEST_MODE={'on' if TEST_MODE else 'off'} | vídeo={'ok' if VIDEO_URL else 'NÃO CONFIGURADO'}"
    )


async def on_shutdown(_):
    if _worker_task:
        _worker_task.cancel()
    if _web_runner:
        await _web_runner.cleanup()
    logger.info("Bot desligado")


if __name__ == "__main__":
    executor.start_polling(
        dp,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        skip_updates=False,  # processa entradas que aconteceram durante um deploy
        allowed_updates=["message", "chat_member", "my_chat_member"],
    )
